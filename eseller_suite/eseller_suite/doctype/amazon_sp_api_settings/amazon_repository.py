# Copyright (c) 2024, efeone and contributors
# For license information, please see license.txt
#/apps/eseller_suite/eseller_suite/eseller_suite/doctype/amazon_sp_api_settings/

import json
import time, random
import urllib.parse

import dateutil
import frappe
from frappe import _
from datetime import datetime
from eseller_suite.eseller_suite.utils import format_date_time_to_ist

from eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_sp_api import (
    CatalogItems,
    Finances,
    SPAPIError,
)
from eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_sp_api_settings import (
    AmazonSPAPISettings,
)
from frappe import scrub
from frappe.utils import getdate, add_days, get_datetime, nowdate, today
from requests.exceptions import HTTPError

try:
    # v14 / v15 (current)
    from erpnext.setup.doctype.currency_exchange.currency_exchange import get_exchange_rate
except ImportError:
    try:
        # v13
        from erpnext.accounts.utils import get_exchange_rate
    except ImportError:
        # v12 and earlier
        from erpnext.setup.utils import get_exchange_rate


# ---- raw SP-API helpers -----------------------------------------------
import datetime, requests, urllib.parse
AWS_REGION = "us-east-1"           # ← NA region; change if you sell elsewhere
AWS_SERVICE = "execute-api"
SP_DOMAIN  = "sellingpartnerapi-na.amazon.com"

# ======================================================================
# Basic LWA + helpers (self-contained; no external SDK required)
# ======================================================================
def _get_lwa_token(settings):
    if AmazonRepository._token and time.time() < AmazonRepository._token_expires:
        return AmazonRepository._token
    
    max_retry = 5  # Adjustable; matches _sp_get's default
    for attempt in range(max_retry):       
        try:
            resp = requests.post("https://api.amazon.com/auth/o2/token",
                data={"grant_type": "refresh_token", "refresh_token": settings.refresh_token, "client_id": settings.client_id, "client_secret": settings.get_password("client_secret")},
                timeout=30,
            )
            resp.raise_for_status()  # Raise on 4xx/5xx
            tok  = resp.json()
            AmazonRepository._token         = tok["access_token"]
            AmazonRepository._token_expires = time.time() + tok["expires_in"] - 30
            return AmazonRepository._token
        except requests.exceptions.RequestException as e:
            frappe.logger().warning(f"LWA token fetch failed (attempt {attempt+1}/{max_retry}): {str(e)}")
            if attempt == max_retry - 1:
                raise  # Re-raise after retries exhausted
            time.sleep((2 ** attempt) + random.random())  # Exponential backoff + jitter

def _sp_get(path, query, settings, rdt=None, max_retry: int = 10, return_full: bool = False):
    """
    Low-level GET helper (no SDK, no SigV4 – good enough for
    non-restricted GET endpoints such as /reports/…).
    - `query` can now be **dict OR str**.
    - 403 message is generic (reports:* OR finances:*).
    - Added `return_full` param: If True, returns full response JSON (e.g., for endpoints with top-level 'pagination').
    """
    # ――― 1.  build URL ------------------------------------------------
    if isinstance(query, dict):
        # safe="," keeps comma-separated lists intact
        query = urllib.parse.urlencode(query, safe=",")
    url = f"https://{SP_DOMAIN}{path}"
    if query:
        url = f"{url}?{query}"

    # ――― 2.  common headers ------------------------------------------
    token = _get_lwa_token(settings)
    access_token = rdt if rdt else token  # Use RDT if provided, else LWA
    headers = {
        "host": SP_DOMAIN,
        "user-agent": "ERPNext-eSellerSuite/1.0",
        "x-amz-access-token": access_token,
        "accept": "application/json",
    }

    # ――― 3.  retry / throttle loop -----------------------------------
    for attempt in range(max_retry):
        try:
             resp = requests.get(url, headers=headers, timeout=45)
        except requests.exceptions.RequestException as e:
            frappe.logger().warning(f"Amazon's SP-API endpoint connection error for {path}, attempt {attempt+1}/{max_retry}: {str(e)}")
            time.sleep((2 ** attempt) + random.random())  # Exponential backoff (2, 4, 8... sec) + jitter
            continue  # Retry next attempt

        if resp.status_code == 200:                     # ✓ success
            data = resp.json()
            data["__headers__"] = resp.headers
            if return_full:
                return data  # Return full JSON (includes top-level keys like "pagination")
            return data.get("payload", data)  # Existing behavior: return payload or full if no payload

        if resp.status_code == 403:                     # ↯ scope / perms
            scope_hint = "reports:read" if path.startswith("/reports") else "finances:read"
            print(f"SP-API 403 for {path} – check “{scope_hint}” scope")
            frappe.logger().warning(
                f"SP-API 403 for {path} – check “{scope_hint}” scope. "
                f"Response: {resp.text[:300]}"
            )
            return {}                                   # treat as “nothing yet”

        if resp.status_code not in [429, 500, 502, 503, 504]:                     # ↯ hard error
            frappe.logger().error(f"SP-API {resp.status_code} for {url}\n{resp.text[:500]}")
            resp.raise_for_status()

        retry_after = int(resp.headers.get("Retry-After", 0)) or (2 + attempt)
        frappe.logger().info(f"SP-API {resp.status_code}, sleeping {retry_after}s for {path}")
        time.sleep(retry_after + random.random())

    # If we get here, we never obtained a valid response (network errors every time).
    frappe.logger().error(f"Amazon SP-API endpoint request failed after {max_retry} attempts for {path} (network errors)")
    return {}  # degrade gracefully instead of raising

# ------------------------------------------------------------------
# Helper: turn “ATVPDKIKX0DER, A2EUQ1WTGCTBG2 ” → "ATVPDKIKX0DER,A2EUQ1WTGCTBG2"
# (strips blanks & consecutive commas)
# ------------------------------------------------------------------
def _clean_marketplace_ids(raw: str) -> str:
    return ",".join(i.strip() for i in raw.split(",") if i.strip())

def _to_float(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default

# ------------------------------------------------------------------
# Convert CAD↔USD, MXN↔USD with ERPNext’s built-in exchanger
# ------------------------------------------------------------------
def _fx_rate(from_ccy: str, to_ccy: str = "USD", posting_date: str | None = None) -> float:
    """
    Get FX rate for posting_date (defaults to today).
    Falls back to 1.0 if from_ccy == to_ccy.
    """
    if from_ccy == to_ccy:
        return 1
    posting_date = posting_date or today()
    try:
        return get_exchange_rate(from_ccy, to_ccy, posting_date)
    except Exception:
        # if ECB feed unavailable, you can choose a default or raise
        frappe.throw(f"Exchange rate {from_ccy}→{to_ccy} missing; add in Currency Exchange.")



# ---------- thin wrappers that mimic the old SDK -----------------------
def _list_orders(
    settings,
    updated_after=None,
    updated_before=None,
    next_token=None,
    order_statuses=None,           # e.g. "Shipped,InvoiceUnconfirmed"
    fulfillment_channels=None,     # e.g. "FBA,SellerFulfilled"
    max_results=25,
    amazon_order_ids=None,
):
    """
    Thin wrapper around GET /orders/v0/orders.
    Mirrors the kwargs you used with the SDK.
    """

    if next_token:
        #query = f"NextToken={next_token}"
        query = f"NextToken={urllib.parse.quote(next_token, safe='')}"
    else:
        qs = {
            "MarketplaceIds": _clean_marketplace_ids(settings.custom_marketplace or ""),
            "MaxResultsPerPage": str(max_results),
        }
        if updated_after:
            qs["CreatedAfter"]  = updated_after
        if updated_before:
            qs["CreatedBefore"] = updated_before
        if order_statuses:
            qs["OrderStatuses"] = order_statuses
        if fulfillment_channels:
            qs["FulfillmentChannels"] = fulfillment_channels
        # urllib handles the comma separators fine
        if amazon_order_ids:
            qs["AmazonOrderIds"] = amazon_order_ids        
        query = urllib.parse.urlencode(qs, safe=",")
    return _sp_get("/orders/v0/orders", query, settings)

def _list_order_items(settings, amazon_order_id, next_token=None):
    path  = f"/orders/v0/orders/{amazon_order_id}/orderItems"
    query = f"NextToken={urllib.parse.quote(next_token, safe='')}" if next_token else ""
    return _sp_get(path, query, settings)

def _list_financial_events(settings, amazon_order_id, next_token=None):
    """
    Thin wrapper around GET /finances/v0/orders/{orderId}/financialEvents.
    Uses the same _sp_get() auth / retry logic as orders + items.
    """
    path  = f"/finances/v0/orders/{amazon_order_id}/financialEvents"
    query = f"NextToken={urllib.parse.quote(next_token, safe='')}" if next_token else ""
    return _sp_get(path, query, settings)

def _page_pause(resp_headers, floor=2.0):
    retry = float(resp_headers.get("Retry-After", 0) or 0)
    time.sleep(max(retry, floor) + random.random())
    
def _create_restricted_data_token(settings, order_id, max_retry: int = 10):
    """
    POST /tokens/2021-03-01/restrictedDataToken to get RDT for PII access.
    Specifies resources for getOrderAddress and getOrderBuyerInfo.
    """
    url = f"https://{SP_DOMAIN}/tokens/2021-03-01/restrictedDataToken"
    token = _get_lwa_token(settings)
    headers = {
        "host": SP_DOMAIN,
        "user-agent": "ERPNext-eSellerSuite/1.0",
        "x-amz-access-token": token,
        "accept": "application/json",
        "content-type": "application/json",
    }
    body = {
        "restrictedResources": [
            {
                "method": "GET",
                "path": f"/orders/v0/orders/{order_id}/address"
            },
            {
                "method": "GET",
                "path": f"/orders/v0/orders/{order_id}/buyerInfo"
            }
        ]
    }

    for attempt in range(max_retry):
        try:
            resp = requests.post(url, headers=headers, json=body, timeout=45)
        except RequestException as e:
            frappe.logger().warning(f"Amazon SP-API endpoint network error (Restricted Data Token) on attempt {attempt+1}/{max_retry} for order {order_id}: {e}")
            time.sleep(2 + attempt)
            continue
        
        if resp.status_code == 200:
            data = resp.json()
            return data.get("restrictedDataToken")

        # Log the full response for debugging
        frappe.logger().error(f"SP-API RDT attempt {attempt+1} failed: {resp.status_code} - {resp.text[:500]}")

        if resp.status_code == 403:
            frappe.logger().warning(f"SP-API 403 for RDT on order {order_id} – check PII role approval.")
            return None

        if resp.status_code != 429:
            # Raise on non-throttle errors, but now with resp.text for better debugging
            raise HTTPError(f"SP-API {resp.status_code} for RDT on {order_id}\n{resp.text[:500]}")

        retry_after = int(resp.headers.get("Retry-After", 0)) or (2 + attempt)
        time.sleep(retry_after + random.random())

    frappe.logger().error(f"SP-API RDT request failed after {max_retry} attempts for {order_id}")
    return None


def _get_order_buyer_info(settings, amazon_order_id, rdt=None, max_retry: int = 10):
    """
    Thin wrapper around GET /orders/v0/orders/{orderId}/buyerInfo.
    Includes RDT if provided for PII access.
    """
    path = f"/orders/v0/orders/{amazon_order_id}/buyerInfo"
    return _sp_get(path, "", settings, rdt=rdt, max_retry=max_retry)  # Pass rdt to _sp_get

# Also update _get_order_address to accept rdt (similar to above)
def _get_order_address(settings, amazon_order_id, rdt=None, max_retry: int = 10):
    path = f"/orders/v0/orders/{amazon_order_id}/address"
    return _sp_get(path, "", settings, rdt=rdt, max_retry=max_retry)

# Helper function to convert string to proper upper/lower case
def to_proper_case(text: str) -> str:
    if not text:
        return text
    # Split into words, capitalize each, join back
    words = text.split()
    proper_words = []
    for word in words:
        # Handle special cases like 'PO' for PO Box, or state codes
        if len(word) == 2 and word.isupper():  # Likely state code like 'NV'
            proper_words.append(word)  # Keep uppercase
        else:
            proper_words.append(word.capitalize())
    return ' '.join(proper_words)

class AmazonRepository:
    _token         = None
    _token_expires = 0
    
    def __init__(self, amz_setting: str | AmazonSPAPISettings) -> None:
        if isinstance(amz_setting, str):
            amz_setting = frappe.get_doc("Amazon SP API Settings", amz_setting)

        self.amz_setting = amz_setting
        self.instance_params = dict(
            client_id=self.amz_setting.client_id,
            client_secret=self.amz_setting.get_password("client_secret"),
            refresh_token=self.amz_setting.refresh_token,
        )

    def return_as_list(self, input) -> list:
        if isinstance(input, list):
            return input
        else:
            return [input]

    def call_sp_api_method(self, sp_api_method, **kwargs) -> dict:
        errors = {}
        max_retries = self.amz_setting.max_retry_limit

        for x in range(max_retries):
            try:
                result = sp_api_method(**kwargs)
                return result.get("payload")
            except (SPAPIError, requests.exceptions.RequestException) as e:  # Add network catch
                if isinstance(e, SPAPIError):
                    if e.error not in errors:
                        errors[e.error] = e.error_description
                else:
                    frappe.logger().warning(f"Network error in {sp_api_method.__name__} (attempt {x+1}): {str(e)}")
                if x == max_retries - 1:
                    raise  # Re-raise after retries
                time.sleep((2 ** x) + random.random())  # Expo backoff + jitter (upgrade from fixed 1s)
            except SPAPIError as e:
                if e.error not in errors:
                    errors[e.error] = e.error_description

                time.sleep(1)
                continue

        for error in errors:
            msg = f"<b>Error:</b> {error}<br/><b>Error Description:</b> {errors.get(error)}"
            frappe.msgprint(msg, alert=True, indicator="red")
            frappe.log_error(
                message=f"{error}: {errors.get(error)}", title=f'Method "{sp_api_method.__name__}" failed',
            )

        self.amz_setting.enable_sync = 0
        self.amz_setting.save()

        frappe.throw(
            _("Scheduled sync has been temporarily disabled because maximum retries have been exceeded!")
        )

    def get_finances_instance(self) -> Finances:
        return Finances(**self.instance_params)

    def get_account(self, name) -> str:
        account_name = frappe.db.get_value("Account", {"account_name": "Amazon {0}".format(name)})

        if not account_name:
            new_account = frappe.new_doc("Account")
            new_account.account_name = "Amazon {0}".format(name)
            new_account.company = self.amz_setting.company
            new_account.parent_account = self.amz_setting.market_place_account_group
            new_account.insert(ignore_permissions=True)
            account_name = new_account.name

        return account_name

    def get_charges_and_fees(self, order_id) -> dict:
        try:
            financial_events_payload = _list_financial_events(self.amz_setting, order_id)
            # Print the Financial Events payload
            #print(f"Financial events for {order_id}: {json.dumps(financial_events_payload, indent=2)}", flush=True)
        except RequestException as e:
            frappe.log_error(message=f"SP-API finances fetch failed for {order_id}: {e}", title="Amazon Finances Fetch")
            return {"charges": [], "fees": [], "tds": [], "service_fees": [], "principal_amounts": {}, "additional_discount": 0}
        
        charges_and_fees = {"charges": [], "fees": [], "tds": [], "service_fees":[], "principal_amounts":{}, "additional_discount": 0}
  
        if not (
                financial_events_payload
                and len(financial_events_payload.get("FinancialEvents", {}))
            ):
                return charges_and_fees

        while True:
            shipment_event_list = financial_events_payload.get("FinancialEvents", {}).get(
                "ShipmentEventList", []
            )
            service_fee_event_list = financial_events_payload.get("FinancialEvents", {}).get(
                "ServiceFeeEventList", []
            )
            next_token = financial_events_payload.get("NextToken")
            principal_amounts = {}
            promotion_discount = 0
            seller_sku = ''
            for shipment_event in shipment_event_list:
                if shipment_event:
                    for shipment_item in shipment_event.get("ShipmentItemList", []):
                        promotion_list = shipment_item.get("PromotionList", [])
                        seller_sku = shipment_item.get("SellerSKU")
                        qty = shipment_item.get("QuantityShipped")
                        charges = shipment_item.get("ItemChargeList", [])
                        fees = shipment_item.get("ItemFeeList", [])
                        tds_list = shipment_item.get("ItemTaxWithheldList", [])
                        tdss = []
                        if tds_list:
                            tdss = tds_list[0].get("TaxesWithheld", [])

                        for charge in charges:
                            charge_type = charge.get("ChargeType")
                            amount = charge.get("ChargeAmount", {}).get("CurrencyAmount", 0)

                            if charge_type != "Principal" and float(amount) != 0:
                                charge_account = self.get_account(charge_type)
                                charges_and_fees.get("charges").append(
                                    {
                                        "charge_type": "Actual",
                                        "account_head": charge_account,
                                        "tax_amount": amount,
                                        "description": f"{charge_type} for {seller_sku if seller_sku else order_id}",
                                    }
                                )
                            if charge_type == 'Principal':
                                principal_amounts[seller_sku] = round((float(amount)/qty), 2)

                        for fee in fees:
                            fee_type = fee.get("FeeType")
                            amount = fee.get("FeeAmount", {}).get("CurrencyAmount", 0)

                            if float(amount) != 0:
                                fee_account = self.get_account(fee_type)
                                charges_and_fees.get("fees").append(
                                    {
                                        "charge_type": "Actual",
                                        "account_head": fee_account,
                                        "tax_amount": amount,
                                        "description": f"{fee_type} for {seller_sku if seller_sku else order_id}",
                                    }
                                )

                        for tds in tdss:
                            tds_type = tds.get("ChargeType")
                            amount = tds.get("ChargeAmount", {}).get("CurrencyAmount", 0)
                            if float(amount) != 0:
                                tds_account = self.get_account(tds_type)
                                charges_and_fees.get("tds").append(
                                    {
                                        "charge_type": "Actual",
                                        "account_head": tds_account,
                                        "tax_amount": amount,
                                        "description": f"{tds_type} for {seller_sku if seller_sku else order_id}",
                                    }
                                )

                        for promotion in promotion_list:
                            amount = promotion.get("PromotionAmount", {}).get("CurrencyAmount", 0)
                            promotion_discount += float(amount)

            charges_and_fees["principal_amounts"] = principal_amounts
            charges_and_fees["additional_discount"] = promotion_discount

            for service_fee in service_fee_event_list:
                if service_fee:
                    for service_fee_item in service_fee.get("FeeList", []):
                        fee_type = service_fee_item.get("FeeType")
                        amount = service_fee_item.get("FeeAmount", {}).get("CurrencyAmount", 0)
                        if float(amount) != 0:
                            fee_account = self.get_account(fee_type)
                            charges_and_fees.get("service_fees").append(
                                {
                                    "charge_type": "Actual",
                                    "account_head": fee_account,
                                    "tax_amount": amount,
                                    "description": f"{fee_type} for {seller_sku if seller_sku else order_id}",
                                }
                            )

            if not next_token:
                break

            financial_events_payload = _list_financial_events(
                self.amz_setting, order_id, next_token=next_token
            )

        return charges_and_fees

    def create_item(self, order_item, order_id) -> str:
        def create_item_group(amazon_item) -> str:
            if not amazon_item:
                return self.amz_setting.parent_item_group
            if not amazon_item.get("AttributeSets"):
                return self.amz_setting.parent_item_group
            item_group_name = amazon_item.get("AttributeSets")[0].get("ProductGroup")

            if item_group_name:
                item_group = frappe.db.get_value("Item Group", filters={"item_group_name": item_group_name})

                if not item_group:
                    new_item_group = frappe.new_doc("Item Group")
                    new_item_group.item_group_name = item_group_name
                    new_item_group.parent_item_group = self.amz_setting.parent_item_group
                    new_item_group.insert()
                    return new_item_group.item_group_name
                return item_group

            raise (KeyError("ProductGroup"))

        def create_brand(amazon_item) -> str:
            if not amazon_item:
                return
            if not amazon_item.get("AttributeSets"):
                return

            brand_name = amazon_item.get("AttributeSets")[0].get("Brand")

            if not brand_name:
                return

            existing_brand = frappe.db.get_value("Brand", filters={"brand": brand_name})

            if not existing_brand:
                brand = frappe.new_doc("Brand")
                brand.brand = brand_name
                brand.insert()
                return brand.brand
            return existing_brand

        def create_manufacturer(amazon_item) -> str:
            if not amazon_item:
                return
            if not amazon_item.get("AttributeSets"):
                return
      
            manufacturer_name = amazon_item.get("AttributeSets")[0].get("Manufacturer")

            if not manufacturer_name:
                return

            existing_manufacturer = frappe.db.get_value(
                "Manufacturer", filters={"short_name": manufacturer_name}
            )

            if not existing_manufacturer:
                manufacturer = frappe.new_doc("Manufacturer")
                manufacturer.short_name = manufacturer_name
                manufacturer.insert()
                return manufacturer.short_name
            return existing_manufacturer

        def create_item_price(amazon_item, item_code) -> None:
            if not amazon_item:
                return
            if not amazon_item.get("AttributeSets"):
                return
      
            item_price = frappe.new_doc("Item Price")
            item_price.price_list = self.amz_setting.price_list
            item_price.price_list_rate = (
                amazon_item.get("AttributeSets")[0].get("ListPrice", {}).get("Amount") or 0
            )
            item_price.item_code = item_code
            item_price.insert()

        catalog_items = self.get_catalog_items_instance()
        amazon_item = catalog_items.get_catalog_item(order_item["ASIN"]).get("payload", None)
  
        if not amazon_item:
            frappe.log_error("No Amazon Item found for ASIN: {0}. For Order: {1}".format(order_item["ASIN"], order_id))
            return None

        item = frappe.new_doc("Item")
        item.item_group = create_item_group(amazon_item)
        item.brand = create_brand(amazon_item)
        item.manufacturer = create_manufacturer(amazon_item)
        item.amazon_item_code = order_item["SellerSKU"]
        item.item_code = order_item["SellerSKU"]
        item.item_name = order_item["SellerSKU"]
        item.description = order_item["Title"]
        item.insert(ignore_permissions=True)

        create_item_price(amazon_item, item.item_code)

        return item.name

    def get_item_code(self, order_item, order_id) -> str:
        # 1 try your custom ASIN field first (support multiple comma-separated ASINs)
        asin = order_item.get("ASIN")
        if asin:
            # Use FIND_IN_SET after removing spaces to handle "ASIN1,ASIN2" or "ASIN1, ASIN2"
            items = frappe.db.sql("""
                SELECT name 
                FROM `tabItem` 
                WHERE FIND_IN_SET(%s, REPLACE(custom_asin, ' ', '')) > 0 
                LIMIT 2
            """, (asin,), as_dict=True)
            
            if items:
                if len(items) > 1:
                    frappe.log_error(
                        f"Multiple items found for ASIN {asin} in order {order_id}. Using first: {items[0].name}",
                        "Amazon Item Mapping"
                    )
                return items[0].name

        # 2 fall back to legacy SellerSKU look-up
        if frappe.db.exists("Item", {"amazon_item_code": order_item["SellerSKU"]}):
            return frappe.db.get_value("Item", {"amazon_item_code": order_item["SellerSKU"]})

        item_code = self.create_item(order_item, order_id)
        return item_code

    def get_order_items(self, order_id) -> list:
        try:
            order_items_payload = _list_order_items(self.amz_setting, order_id)
        except requests.exceptions.RequestException as e:
            frappe.log_error(
                title="Amazon Order Import",
                message=f"SP-API orderItems timeout for {order_id}\n{str(e)}"
            )
            # Return an empty list so the SO still gets created;
            # you can backfill items later if you like.
            return []
  
        if not order_items_payload:
            return []

        final_order_items = []
        warehouse = self.amz_setting.warehouse

        while True:
            order_items_list = order_items_payload.get("OrderItems")
            next_token = order_items_payload.get("NextToken")
            if next_token:
                time.sleep(1.1) 

            for order_item in order_items_list:
                zero_qty_flag = False
                actual_qty = 0
                if order_item.get("QuantityOrdered") >= 0:
                    item_amount = float(order_item.get("ItemPrice", {}).get("Amount", 0))
                    item_tax = float(order_item.get("ItemTax", {}).get("Amount", 0))
                    # shipping_price = float(order_item.get("ShippingPrice", {}).get("Amount", 0))
                    # shipping_discount = float(order_item.get("ShippingDiscount", {}).get("Amount", 0))
                    total_order_value = item_amount+item_tax
                    item_qty = float(order_item.get("QuantityOrdered", 0))
                    # In case of Cancelled orders Qty will be 0, Invoice will not get created
                    if not item_qty:
                        item_qty = 1
                        zero_qty_flag = True
                        actual_qty = order_item.get("ProductInfo").get("NumberOfItems")
                    item_rate = item_amount/item_qty
                    item_code = self.get_item_code(order_item, order_id)
                    if not item_code:
                        return []
                    actual_item = frappe.db.get_value("Item", item_code, "actual_item")
                    if actual_item:
                        item_code = actual_item
                    final_order_items.append(
                        {
                            "item_code": item_code,
                            "item_name": order_item.get("SellerSKU"),
                            "description": order_item.get("Title"),
                            "rate": item_rate,
                            #"base_rate": item_rate,
                            "qty": item_qty,
                            "amount": item_rate*item_qty,
                            #"base_amount": item_rate*item_qty,
                            "uom": "Nos",
                            "stock_uom": "Nos",
                            "warehouse": warehouse,
                            "conversion_factor": 1.0,
                            "allow_zero_valuation_rate": 1,
                            "total_order_value": total_order_value,
                            "zero_qty_flag": zero_qty_flag,
                            "actual_qty": actual_qty
                        }
                    )

            if not next_token:
                break

            order_items_payload = _list_order_items(
                self.amz_setting, order_id, next_token=next_token
            )

        return final_order_items

    def _fetch_order_by_id(self, order_id: str):
        data = _sp_get(
            "/orders/v0/orders",
            f"AmazonOrderIds={urllib.parse.quote(order_id, safe='')}",
            self.amz_setting,
        )
        if not data:
            return None
        orders = data.get("Orders") or data.get("payload", {}).get("Orders") or []
        return orders[0] if orders else None

    def reprocess_draft_orders(self, age_days=7):
        print(f"Starting reprocess_draft_orders with age_days={age_days}", flush=True)
       
        # Fetch drafts older than age_days with amazon_order_id
        print("Fetching draft Sales Orders older than {} days...".format(age_days), flush=True)
        drafts = frappe.get_all("Sales Order", filters={
            "docstatus": 0, # Draft
            "amazon_order_id": ["is", "set"],
            "creation": ["<", add_days(nowdate(), -age_days)]
        }, fields=["name", "amazon_order_id"])
       
        print(f"Fetched {len(drafts)} draft Sales Orders to reprocess.", flush=True)
       
        for d in drafts:
            # Limit to a single order for debugging
            #if d.amazon_order_id != "112-8643975-4194655":
            #    continue
            print(f"Processing draft SO: name={d.name}, amazon_order_id={d.amazon_order_id}", flush=True)
           
            # Re-fetch full order from Amazon (use _list_orders with AmazonOrderIds)
            print(f"Re-fetching order from Amazon for ID: {d.amazon_order_id}", flush=True)
            order_payload = _list_orders(self.amz_setting, amazon_order_ids=d.amazon_order_id)
            #print(f"Retrieved order_payload: {json.dumps(order_payload, indent=2)}", flush=True)
           
            if not order_payload or not order_payload.get("Orders"):
                print(f"No order payload found for {d.amazon_order_id}. Deleting orphan SO {d.name}.", flush=True)
                frappe.delete_doc("Sales Order", d.name) # Orphan: delete if gone from Amazon
                frappe.db.commit()
                continue
           
            print(f"Fetching detailed order by ID: {d.amazon_order_id}", flush=True)
            order = self._fetch_order_by_id(d.amazon_order_id)
            #print(f"Retrieved detailed order: {json.dumps(order, indent=2)}", flush=True)
            if not order:
                # Don’t delete on a transient API miss; just skip & log
                print(f"SP-API returned no order for {d.amazon_order_id}; keeping draft {d.name} and skipping.", flush=True)
                frappe.logger().warning(f"SP-API returned no order for {d.amazon_order_id}; keeping draft {d.name}")
                continue
            # Guard: Only process AFN orders; skip MFN and others
            fulfillment_channel = order.get("FulfillmentChannel")
            print(f"Order fulfillment channel: {fulfillment_channel}", flush=True)
            if fulfillment_channel != "AFN":
                print(f"Skipping non-AFN order {d.amazon_order_id}.", flush=True)
                continue
            status = order.get("OrderStatus")
            print(f"Order status: {status}", flush=True)
           
            if status in ["Unfulfillable", "Canceled"]:
                print(f"Deleting SO {d.name} due to status {status}.", flush=True)
                frappe.delete_doc("Sales Order", d.name, ignore_permissions=True)
                frappe.db.commit()
                continue
           
            if status not in ["Shipped", "InvoiceUnconfirmed"]:
                print(f"Skipping non-shipped order {d.amazon_order_id} with status {status}.", flush=True)
                continue # Skip non-shipped
           
            # Re-create/update SO with fresh data (forces finance re-fetch)
            print(f"Re-creating/updating SO for order {d.amazon_order_id}.", flush=True)
            self.create_sales_order(order) # Will submit if ready
            print(f"Finished processing order {d.amazon_order_id}.", flush=True)
            print(f"-")
       
        print("Finished reprocess_draft_orders.", flush=True)

    def create_sales_order(self, order) -> str | None:
        def create_customer(order) -> str:
            #print(f"---->Create Customer {order}", flush=True)
            """
            For MFN (merchant‑fulfilled) orders, create/fetch a unique Customer **using the buyer’s real details**.
            For FBA (AFN) orders, use a single 'Amazon FBA Customer' master record.
            """
            # 1. Fulfilment channel
            channel = (order.get("FulfillmentChannel") or "").upper()
            
            # ------------------------------------------------------------------
            # 2. MERCHANT‑FULFILLED (MFN)  → one Customer per buyer / order
            # ------------------------------------------------------------------
            if channel == "MFN":
                buyer_info   = order.get("BuyerInfo", {})
                buyer_name   = buyer_info.get("BuyerName") or "Amazon Buyer"
                buyer_email  = buyer_info.get("BuyerEmail")
                
                # Fetch RDT for PII access
                rdt = _create_restricted_data_token(self.amz_setting, order.get("AmazonOrderId"))
                if not rdt:
                    frappe.log_error(f"Failed to get RDT for order {order.get('AmazonOrderId')} – PII may be restricted.")

                # Fetch full buyer info with RDT
                buyer_info_payload = _get_order_buyer_info(self.amz_setting, order.get("AmazonOrderId"), rdt=rdt)
                if buyer_info_payload:
                    buyer_name = buyer_info_payload.get("BuyerName") or buyer_name
                    buyer_email = buyer_info_payload.get("BuyerEmail") or buyer_email

                # Always prefer RDT address for MFN, fallback to shallow order payload
                ship_details = {}
                full_addr_payload = _get_order_address(self.amz_setting, order.get("AmazonOrderId"), rdt=rdt)
                if full_addr_payload and full_addr_payload.get("ShippingAddress"):
                    ship_details = full_addr_payload["ShippingAddress"]
                else:
                    ship_details = order.get("ShippingAddress", {}) or {}

                # Update buyer_name to prefer the full shipping name if available (fixes partial name issue)
                buyer_name = ship_details.get("Name") or buyer_name
                buyer_name = to_proper_case(buyer_name)
        
                # We use AmazonOrderId as an *internal* unique key so duplicates can’t collide
                cust_key = order.get("AmazonOrderId")

                existing = frappe.db.get_value("Customer", {"name": cust_key}, "name")
                if existing:
                    return existing

                # 2a. Create Customer (real buyer name shown; unique key still order‑id)
                cust = frappe.new_doc("Customer")
                cust.name            = cust_key              # internal primary key
                cust.customer_name   = buyer_name            # what users see in ERPNext
                cust.customer_group  = self.amz_setting.custom_mfn_customer_group
                #cust.territory       = self.amz_setting.territory # We are not using territory
                cust.customer_type   = self.amz_setting.customer_type
                cust.insert(ignore_permissions=True)

                # 2b. Contact
                contact = frappe.new_doc("Contact")
                name_parts = buyer_name.split(" ")
                contact.first_name = name_parts[0]
                if len(name_parts) > 1:
                    contact.last_name = " ".join(name_parts[1:])
                if buyer_email:
                    contact.append("email_ids", {
                        "email_id": buyer_email,
                        "is_primary": 1
                    })
                contact.append("links", {
                    "link_doctype": "Customer",
                    "link_name": cust.name
                })
                contact.insert(ignore_permissions=True)

                # 2c. Shipping Address (optional but handy)
                if ship_details:
                    address = frappe.new_doc("Address")
                    # Use Name from shipping address if available, else fallback
                    addr_title = ship_details.get("Name") or buyer_name
                    address.address_title = to_proper_case(addr_title)
                    address.address_type  = "Shipping"
                    # Set defaults for missing fields to avoid mandatory errors
                    address.address_line1 = to_proper_case(ship_details.get("AddressLine1") or "Not Provided (PII Restricted)")  # ← NEW
                    address.address_line2 = to_proper_case(ship_details.get("AddressLine2") or "")
                    address.address_line3 = to_proper_case(ship_details.get("AddressLine3") or "")
                    address.city          = to_proper_case(ship_details.get("City") or "Not Provided")
                    address.state         = to_proper_case(ship_details.get("StateOrRegion") or "")  # ← (preserves 'NV')
                    address.pincode       = ship_details.get("PostalCode") or ""
                    # Map country code to full name
                    country_code = ship_details.get("CountryCode")
                    country_name = frappe.db.get_value("Country", {"code": (country_code or "").lower()}, "name") if country_code else "United States"
                    address.country = country_name or "United States"  # Fallback
                    
                    raw_phone = ship_details.get("Phone") or ""
                    import re
                    # Remove extension if present (handles "ext." consistently)
                    if "ext." in raw_phone.lower():
                        raw_phone = raw_phone.split("ext.", 1)[0].strip()
                    # Strip all non-digits for safety
                    digits = re.sub(r'\D', '', raw_phone)
                    # Remove leading 1 if it's an 11-digit US number
                    if digits.startswith('1') and len(digits) == 11:
                        digits = digits[1:]
                    # Format as (XXX) XXX-XXXX if 10 digits
                    if len(digits) == 10:
                        formatted_phone = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                    else:
                        formatted_phone = ""  # Or fallback to cleaned digits without formatting
                    address.phone = formatted_phone
                    
                    address.append("links", {
                        "link_doctype": "Customer",
                        "link_name": cust.name
                    })
                    address.insert(ignore_permissions=True)
                return cust.name

            # ------------------------------------------------------------------
            # 3. FBA / AFN  → single consolidated customer
            # ------------------------------------------------------------------
            order_ccy = order.get("OrderTotal", {}).get("CurrencyCode") or "USD"
            if order_ccy == "CAD":
                MASTER = self.amz_setting.custom_amazon_cad_fba_default_customer or "Amazon FBA Customer - Canada"
            elif order_ccy == "MXN":
                MASTER = self.amz_setting.custom_amazon_mxn_fba_default_customer or "Amazon FBA Customer - Mexico"
            else:  # Default to USD or unknown currencies
                MASTER = self.amz_setting.custom_amazon_fba_default_customer or "Amazon FBA Customer"

            master_name = frappe.db.get_value("Customer", {"customer_name": MASTER}, "name")
            if master_name:
                return master_name

            # Create master Amazon Customer on first use
            master = frappe.new_doc("Customer")
            master.customer_name  = MASTER
            master.customer_group = self.amz_setting.customer_group
            master.territory      = self.amz_setting.territory
            master.customer_type  = self.amz_setting.customer_type
            master.insert(ignore_permissions=True)
            contact = frappe.new_doc("Contact")
            contact.first_name = MASTER
            contact.append("links", {
                "link_doctype": "Customer",
                "link_name": master.name
            })
            contact.insert(ignore_permissions=True)

            return master.name

        def create_address(order, customer_name) -> str | None:
            """
            For FBA (AFN) orders re-use a single address named
            'Amazon FBA Customer-Shipping'.  MFN logic is unchanged.
            """
            if (order.get("FulfillmentChannel") or "").upper() == "AFN":
                fixed_name = "Amazon FBA Customer-Shipping"

                # If we've already created / renamed it once, just return it
                if frappe.db.exists("Address", fixed_name):
                    return fixed_name

                # Otherwise create it a single time
                addr = frappe.new_doc("Address")
                addr.name          = fixed_name            # prevents “-1,-2,-3 …”
                addr.address_title = "Amazon FBA Customer"
                addr.address_type  = "Shipping"
                addr.country = frappe.db.get_value("Country", {"code": "us"}, "name") or "United States"
                addr.append("links", {
                    "link_doctype": "Customer",
                    "link_name": customer_name,
                })
                addr.insert(ignore_permissions=True)
                return addr.name
         
            shipping_address = order.get("ShippingAddress")

            if not shipping_address:
                return
            else:
                make_address = frappe.new_doc("Address")
                make_address.address_line1 = shipping_address.get("AddressLine1", "Not Provided")
                make_address.city = shipping_address.get("City", "Not Provided")
                amazon_state = shipping_address.get("StateOrRegion")
                if frappe.db.get_single_value("Amazon SP API Settings", "map_state_data"):
                    if frappe.db.exists("Amazon State Mapping", {"amazon_state": amazon_state}):
                        make_address.state = frappe.db.get_value("Amazon State Mapping", {"amazon_state": amazon_state}, "state")
                    else:
                        failed_sync_record = frappe.new_doc('Amazon Failed Sync Record')
                        failed_sync_record.amazon_order_id = order_id
                        failed_sync_record.remarks = 'No State Mapping found for {0}'.format(amazon_state)
                        failed_sync_record.save(ignore_permissions=True)
                        return
                else:
                    make_address.state = amazon_state
                make_address.pincode = shipping_address.get("PostalCode")

                filters = [
                    ["Dynamic Link", "link_doctype", "=", "Customer"],
                    ["Dynamic Link", "link_name", "=", customer_name],
                    ["Dynamic Link", "parenttype", "=", "Address"],
                ]
                existing_address = frappe.get_list("Address", filters)

                for address in existing_address:
                    address_doc = frappe.get_doc("Address", address["name"])
                    if (
                        address_doc.address_line1 == make_address.address_line1
                        and address_doc.pincode == make_address.pincode
                    ):
                        return address

                make_address.append("links", {"link_doctype": "Customer", "link_name": customer_name})
                make_address.address_type = "Shipping"
                make_address.insert()

        order_id = order.get("AmazonOrderId")
        order_date = format_date_time_to_ist(order.get("PurchaseDate"))
        amazon_order_amount = order.get("OrderTotal", {}).get("Amount", 0)
        so_id = None
        so_docstatus = 0
        
        if frappe.db.exists("Sales Order", {"amazon_order_id": order_id}):
            so_id, so_docstatus = frappe.db.get_value("Sales Order", filters={"amazon_order_id": order_id}, fieldname=["name", "docstatus"])

        if so_docstatus and so_id:
            return so_id
        if not so_id:
            so = frappe.new_doc("Sales Order")
        else:
            so = frappe.get_doc('Sales Order', so_id)
        
        new_items = self.get_order_items(order_id)

        customer_name = create_customer(order)
        # Only AFN should go through create_address(); MFN is handled in create_customer()
        channel = (order.get("FulfillmentChannel") or "").upper()
        if channel == "AFN":
            create_address(order, customer_name)

        delivery_date = format_date_time_to_ist(order.get("LatestShipDate"))
        transaction_date = format_date_time_to_ist(order.get("PurchaseDate"))

        order_ccy = order.get("OrderTotal", {}).get("CurrencyCode") or "USD"
        so.currency = order_ccy


        so.amazon_order_id = order_id
        so.marketplace_id = order.get("MarketplaceId")
        so.amazon_order_status = order.get("OrderStatus")
        so.fulfillment_channel = order.get("FulfillmentChannel")
        so.replaced_order_id = order.get("ReplacedOrderId") or ''
        if amazon_order_amount:
            so.amazon_order_amount = amazon_order_amount
        so.amazon_order_status = order.get("OrderStatus")
        so.customer = customer_name
        so.delivery_date = delivery_date if getdate(delivery_date) > getdate(transaction_date) else transaction_date
        so.transaction_date = get_datetime(transaction_date).strftime('%Y-%m-%d')
        so.transaction_time = get_datetime(transaction_date).strftime('%H:%M:%S')
        
        so.conversion_rate = _fx_rate(order_ccy, "USD", so.transaction_date)
        
        so.company = self.amz_setting.company
        warehouse = self.amz_setting.warehouse
        if so.fulfillment_channel:
            if so.fulfillment_channel=='AFN':
                warehouse = self.amz_setting.afn_warehouse
        if self.amz_setting.temporary_stock_transfer_required:
            warehouse = self.amz_setting.temporary_order_warehouse
        if order.get("IsBusinessOrder"):
            so.amazon_customer_type = 'B2B'
        else:
            so.amazon_customer_type = 'B2C'
        so.set_warehouse = warehouse

        # Set payment terms template for MFN orders
        if channel == "MFN" and self.amz_setting.custom_mfn_payment_terms_template:
            so.payment_terms_template = self.amz_setting.custom_mfn_payment_terms_template

        # Guard: Before updating the SO compare the Amazon payload with the existing sales order to determine if a SO rebuild is required
        if so_id and not so_docstatus:  # Only for existing draft SOs
            # Early fetch of items and finances (as before)
            new_items = self.get_order_items(order_id)
            taxes_and_charges = self.amz_setting.taxes_charges
            new_charges_and_fees = self.get_charges_and_fees(order_id) if taxes_and_charges else {}

            if not new_items:
                # Handle no-items case (as before)
                pass  # Or return early if appropriate

            # Simplified guard: Check if new finances would add any taxes
            potential_taxes = (
                new_charges_and_fees.get("charges", []) +
                new_charges_and_fees.get("fees", []) +
                new_charges_and_fees.get("tds", []) +
                new_charges_and_fees.get("service_fees", [])
            )
            has_new_taxes = len(potential_taxes) > 0
            has_additional_discount = new_charges_and_fees.get("additional_discount", 0) != 0

            # Optional: Only rebuild if current SO lacks taxes but new data provides them
            # (Reduces over-rebuilding; remove if you want to always rebuild on presence)
            if len(so.taxes) > 0 or (not has_new_taxes and not has_additional_discount):
                # Also check if status changed (minimal robustness for non-tax updates)
                is_status_same = so.amazon_order_status == order.get("OrderStatus")
                if is_status_same:
                    return so.name  # No changes; skip rebuild
                # If status changed, fall through to rebuild
        

        # If changes detected, proceed to clear and rebuild
        items = new_items #self.get_order_items(order_id)

        if not items:
            if not so_id:
                return
            else:
                so.flags.ignore_mandatory = True
                so.disable_rounded_total = 1
                so.calculate_taxes_and_totals()
                if so.grand_total>=0:
                    so.save(ignore_permissions=True)
                elif not frappe.db.exists("Amazon Failed Sync Record", {"amazon_order_id":order_id}):
                    remarks = 'Failed to create Sales Order for {0}. Sales Order grand Total = {1}'.format(order_id, so.grand_total)
                    failed_sync_record = frappe.new_doc('Amazon Failed Sync Record')
                    failed_sync_record.amazon_order_id = order_id
                    failed_sync_record.remarks = remarks
                    failed_sync_record.payload = so.as_dict()
                    failed_sync_record.replaced_order_id = so.replaced_order_id
                    failed_sync_record.posting_date = so.transaction_date
                    failed_sync_record.amazon_order_date = so.transaction_date
                    failed_sync_record.grand_total = so.grand_total
                    failed_sync_record.amazon_order_amount = so.amazon_order_amount
                    if not frappe.db.exists('Amazon Failed Sync Record', { 'amazon_order_id':order_id, 'remarks':remarks, 'grand_total':so.grand_total }):
                        failed_sync_record.save(ignore_permissions=True)
                return

        so.items = []
        so.taxes = []
        so.taxes_and_charges = ''
        total_order_value = 0

        # Check if all items are zero-qty
        all_zero_qty = all(item.get("zero_qty_flag", False) for item in items)
        zero_qty_items = []

        for item in items:
            if not all_zero_qty and item.get("zero_qty_flag", False):
                zero_qty_items.append(item)
                continue

            total_order_value += item.get('total_order_value', 0)
            item["warehouse"] = warehouse
            so.append("items", item)

        if len(zero_qty_items) > 0:
            so.cancelled_items = []
            for zero_item in zero_qty_items:
                so.append("cancelled_items", {
                    "cancelled_item_code": zero_item.get("item_code"),
                    "cancelled_item_qty": zero_item.get("actual_qty")
                })

        if total_order_value:
            so.amazon_order_amount = total_order_value

        # Add replacement note if applicable
        if so.replaced_order_id:
            so.custom_additional_notes = f"Replacement for original Amazon Order ID: {so.replaced_order_id}"

        taxes_and_charges = self.amz_setting.taxes_charges

        item_lookup = {item["item_code"]: item.get('total_order_value', 0) for item in items}
        for row in so.items:
            total_value = item_lookup.get(row.item_code)
            if total_value:
                row.total_order_value = total_value

        if taxes_and_charges:
            charges_and_fees = self.get_charges_and_fees(order_id)
            
            if charges_and_fees.get("principal_amounts"):
                principal_amounts = charges_and_fees.get("principal_amounts")
                for item_row in so.items:
                    if item_row.item_name and principal_amounts.get(item_row.item_name):
                        pricipal_amount = float(principal_amounts.get(item_row.item_name)) or 0
                        qty = item_row.qty
                        if pricipal_amount:
                            item_row.rate = pricipal_amount
                            #item_row.base_rate = pricipal_amount
                            item_row.amount = pricipal_amount*qty
                            #item_row.base_amount = pricipal_amount*qty

            for charge in charges_and_fees.get("charges"):
                if charge:
                    so.append("taxes", charge)

            for fee in charges_and_fees.get("fees"):
                if fee:
                    so.append("taxes", fee)

            for tds in charges_and_fees.get("tds"):
                if tds:
                    so.append("taxes", tds)
            
            for service_fee in charges_and_fees.get("service_fees"):
                if service_fee:
                    mfn_postage_fee_account_head = frappe.db.get_value('Amazon SP API Settings', self.amz_setting.name, 'mfn_postage_fee_account_head')
                    if( not service_fee.get("account_head") == mfn_postage_fee_account_head) or so.replaced_order_id:
                        so.append("taxes", service_fee)
                    elif not frappe.db.exists("Journal Entry Account", {
                        "amazon_order_id": so.amazon_order_id,
                        "account": service_fee.get("account_head"),
                        "debit_in_account_currency": abs(_to_float(service_fee.get("tax_amount"), 0.0)),
                    }):
                        try:
                            jv_doc = frappe.new_doc('Journal Entry')
                            jv_doc.voucher_type = 'Journal Entry'
                            jv_doc.posting_date = so.transaction_date
                            jv_doc.user_remark = f'Amazon MFN Postage Fee for Order {so.amazon_order_id}'
                            jv_doc.amazon_order_id = so.amazon_order_id
                            tax_amount = abs(float(service_fee.get("tax_amount", 0)))
                            jv_row = jv_doc.append('accounts')
                            jv_row.account = service_fee.get("account_head")
                            jv_row.debit = tax_amount
                            jv_row.debit_in_account_currency = tax_amount
                            jv_row.user_remark = row.get('description')
                            jv_row.amazon_order_id = so.amazon_order_id
                            default_receivable_account = frappe.db.get_value('Company', self.amz_setting.company, 'default_receivable_account')
                            jv_row = jv_doc.append('accounts')
                            jv_row.credit = abs(float(service_fee.get("tax_amount", 0)))
                            jv_row.credit_in_account_currency = abs(float(service_fee.get("tax_amount", 0)))
                            jv_row.user_remark = f'Amazon MFN Postage Fee for Order {so.amazon_order_id}'
                            jv_row.amazon_order_id = so.amazon_order_id
                            jv_row.party_type = 'Customer'
                            jv_row.party = so.get('customer')
                            jv_row.account = default_receivable_account
                            jv_doc.flags.ignore_mandatory = True
                            jv_doc.save(ignore_permissions=True)
                            jv_doc.submit()
                        except Exception as e:
                            pass

            if charges_and_fees.get("additional_discount"):
                so.discount_amount = float(charges_and_fees.get("additional_discount")) * -1

        so.flags.ignore_mandatory = True
        so.disable_rounded_total = 1
        so.calculate_taxes_and_totals()
        if so.grand_total>=0:
            try:
                so.save(ignore_permissions=True)
            except Exception as e:
                frappe.log_error("Error saving Sales Order for Order {0}".format(so.amazon_order_id), e, "Sales Order")

            order_statuses = [
                "Shipped",
                "InvoiceUnconfirmed",
            ]
            if channel == "MFN":
                order_statuses += ["Unshipped", "PartiallyShipped"]

            order_status_valid = order.get("OrderStatus") in order_statuses
            has_taxes = len(so.taxes) > 0
            temp_transfer_required = self.amz_setting.temporary_stock_transfer_required

            transfer_exists = frappe.db.exists("Stock Entry", {
                "name": so.temporary_stock_tranfer_id,
                "docstatus": 1
            }) if temp_transfer_required else True

            is_replacement_zero = (so.grand_total == 0 and so.replaced_order_id)
            if order_status_valid and (channel == "MFN" or (has_taxes or is_replacement_zero)) and transfer_exists:
                try:
                    so.submit()
                    is_fulfilled = (channel == "AFN") or (channel == "MFN" and order.get("OrderStatus") in ["Shipped", "InvoiceUnconfirmed"])
                    if is_fulfilled:
                        for d in so.items:
                            d.delivered_qty = d.qty
                        so.per_delivered = 100
                        so.db_set("status", "Completed")
                        so.db_set("delivery_date", nowdate())
                        so.db_update()
                    frappe.db.commit()
                except Exception as e:
                    frappe.log_error("Error submitting Sales Order for Order {0}".format(so.amazon_order_id), e, "Sales Order")
            
        elif not frappe.db.exists("Amazon Failed Sync Record", {"amazon_order_id":order_id}):
            remarks = 'Failed to create Sales Order for {0}. Sales Order grand Total = {1}'.format(order_id, so.grand_total)
            failed_sync_record = frappe.new_doc('Amazon Failed Sync Record')
            failed_sync_record.amazon_order_id = order_id
            failed_sync_record.remarks = remarks
            failed_sync_record.replaced_order_id = so.replaced_order_id
            failed_sync_record.posting_date = so.transaction_date
            failed_sync_record.amazon_order_date = so.transaction_date
            failed_sync_record.grand_total = so.grand_total
            failed_sync_record.amazon_order_amount = so.amazon_order_amount
            if not so_id:
                failed_sync_record.payload = so.as_dict()
            if not frappe.db.exists('Amazon Failed Sync Record', { 'amazon_order_id':order_id, 'grand_total':so.grand_total, 'remarks':remarks }):
                failed_sync_record.save(ignore_permissions=True)

        return so.name

    def _fetch_and_process_orders(self, statuses, channel, last_updated_after, last_updated_before, sales_orders):
        # ── first fetch ──────────────────────────────────────────────────
        orders_payload = _list_orders(
            self.amz_setting,
            updated_after=last_updated_after,
            updated_before=last_updated_before,
            order_statuses=",".join(statuses),
            fulfillment_channels=channel,  # Note: Pass as str, not list (e.g., "AFN")
            max_results=50,
        )

        #print(f"Orders Payload: {orders_payload}", flush=True)

        # ── pagination loop ─────────────────────────────────────────────
        while True:
            if not orders_payload:
                break

            orders_list = orders_payload.get("Orders")
            next_token = orders_payload.get("NextToken")

            if not orders_list:
                break

            for order in orders_list:
                so = self.create_sales_order(order)
                time.sleep(1.1)
                if so:
                    sales_orders.append(so)

            if not next_token:
                break

            # ── throttle between pages ─────────────────────────────
            _page_pause(orders_payload.get("__headers__", {}))

            try:
                orders_payload = _list_orders(self.amz_setting, next_token=next_token)
            except HTTPError as e:
                frappe.logger().warning(f"Stopped pagination (throttle) for {channel}: {e}")
                break

    def get_orders(self, last_updated_after, sync_selected_date_only=0) -> list:
        afn_statuses = [
            "Shipped",              #All items fulfilled (MFN) or Amazon has handed the FBA parcel to the carrier.
            "InvoiceUnconfirmed",   #Order is shipped but Amazon has not yet generated the official invoice.
            #"PendingAvailability", #Pre-orders only – the item is listed but cannot yet charge the buyer.
            #"Pending",             #Buyer placed order, payment not authorised yet.
            #"Canceled",            #Seller or buyer (or Amazon) canceled the order before it was completely shipped.
            #"Unfulfillable",       #FBA stock-out, payment failure after shipping window, etc.
        ]
        mfn_statuses = [
            "Unshipped",            #Payment authorised; MFN orders are ready for you to fulfil.
            "PartiallyShipped",     #Multi-item order: at least one item shipped, others still pending.
            #"Shipped",             #All items fulfilled (MFN) or Amazon has handed the FBA parcel to the carrier.
            #"InvoiceUnconfirmed",  #Order is shipped but Amazon has not yet generated the official invoice.
        ]
        
        fulfillment_channels = ["AFN", "MFN"]
        #fulfillment_channels = ["MFN"]

        dt = getdate(last_updated_after)
        
        # NOTE: Now treating last_updated_after as the "created after" guard date for filtering.
        created_after = f"{dt.strftime('%Y-%m-%d')}T00:00:00Z"
        
        if sync_selected_date_only:
            last_updated_before = (add_days(getdate(created_after), 1).strftime("%Y-%m-%dT00:00:00Z"))
        else:
            last_updated_before = None
            
        sales_orders = []
        
        # Fetch AFN orders
        self._fetch_and_process_orders(afn_statuses, "AFN", created_after, last_updated_before, sales_orders)

        # Fetch MFN orders
        self._fetch_and_process_orders(mfn_statuses, "MFN", created_after, last_updated_before, sales_orders)

        frappe.enqueue("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_sp_api_settings.enq_si_submit", sales_orders=sales_orders)
        
        return sales_orders

    def get_order(self, amazon_order_ids) -> list:
        order_payload = _sp_get(
            "/orders/v0/orders",
            f"AmazonOrderIds={amazon_order_ids}",
            self.amz_setting,
        )
        sales_orders = []
        if order_payload:
            try:
                sales_order = self.create_sales_order(order_payload)
                if sales_order:
                    sales_orders.append(sales_order)
            except:
                pass
        # frappe.enqueue("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_sp_api_settings.enq_si_submit", sales_orders=sales_orders)
        return sales_orders

    def get_catalog_items_instance(self) -> CatalogItems:
        return CatalogItems(**self.instance_params)

def get_orders(amz_setting_name, last_updated_after, sync_selected_date_only=0) -> list:
    ar = AmazonRepository(amz_setting_name)
    return ar.get_orders(last_updated_after, sync_selected_date_only)
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_repository.reprocess_draft_orders_func", amz_setting_name="q3opu7c5ac")
"""
def reprocess_draft_orders_func(amz_setting_name, age_days=7):
    ar = AmazonRepository(amz_setting_name)
    ar.reprocess_draft_orders(age_days=age_days)

@frappe.whitelist()
def get_order(amz_setting_name, amazon_order_ids) -> list:
    ar = AmazonRepository(amz_setting_name)
    return ar.get_order(amazon_order_ids)




