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
from frappe.utils import getdate, add_days, add_to_date, get_datetime, now_datetime, nowdate, today
from requests.exceptions import HTTPError
from requests.exceptions import RequestException
from erpnext.accounts.party import get_party_account

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

# === ROBUSTNESS CONSTANTS (prevents indefinite socket/TCP hangs) ===
SPAPI_CONNECT_TIMEOUT = 12.0   # time to establish connection
SPAPI_READ_TIMEOUT    = 45.0   # time to read response body (orderItems/finances can be slow)
SPAPI_TIMEOUT         = (SPAPI_CONNECT_TIMEOUT, SPAPI_READ_TIMEOUT)
MFN_FINANCE_RETRY_INTERVAL_HOURS = 6
MFN_FINANCE_RETRY_HORIZON_DAYS = 7
MFN_FINANCE_RETRY_BATCH_SIZE = 25
MFN_ZERO_FEE_MATURITY_HOURS = 48
MFN_RECONCILIATION_MAX_TOLERANCE = 0.25

def _get_lwa_token(settings):
    if AmazonRepository._token and time.time() < AmazonRepository._token_expires:
        return AmazonRepository._token
    
    max_retry = 5  # Adjustable; matches _sp_get's default
    for attempt in range(max_retry):       
        try:
            resp = requests.post("https://api.amazon.com/auth/o2/token",
                data={"grant_type": "refresh_token", "refresh_token": settings.refresh_token, "client_id": settings.client_id, "client_secret": settings.get_password("client_secret")},
                timeout=SPAPI_TIMEOUT,
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

    timeout = SPAPI_TIMEOUT

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
            resp = requests.get(url, headers=headers, timeout=timeout)
        except requests.exceptions.RequestException as e:
            # More informative logging + smarter backoff for the exact failure you hit
            is_network_stall = isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError))
            log_level = "warning" if is_network_stall else "error"
            getattr(frappe.logger(), log_level)(
                f"SP-API {type(e).__name__} for {path} (attempt {attempt+1}/{max_retry}): {str(e)[:250]}"
            )
            sleep_time = min((2 ** attempt) + random.random() * 3, 20) if is_network_stall else (2 ** attempt) + random.random()
            time.sleep(sleep_time)
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
def _order_total(order) -> dict:
    """Orders API may return OrderTotal as JSON null; normalise to a dict."""
    return (order or {}).get("OrderTotal") or {}

def _order_total_amount(order) -> float:
    """Null-safe OrderTotal.Amount."""
    return _to_float(_order_total(order).get("Amount"), 0.0)

def _order_total_currency(order, default: str = "USD") -> str:
    """Null-safe OrderTotal.CurrencyCode."""
    return (_order_total(order).get("CurrencyCode") or default).upper()

def _amazon_last_update_age_hours(order) -> float | None:
    """Age of the current Orders API state, used only to bound zero-fee finalization."""
    raw = (order or {}).get("LastUpdateDate")
    if not raw:
        return None
    try:
        stamp = dateutil.parser.isoparse(str(raw))
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=datetime.timezone.utc)
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        return max((now_utc - stamp.astimezone(datetime.timezone.utc)).total_seconds() / 3600.0, 0.0)
    except Exception:
        return None

def _fx_rate(from_ccy: str, to_ccy: str = "USD", posting_date: str | None = None) -> float:
    """
    Enhanced FX rate handler for Amazon order import:
    1. Calls get_exchange_rate() (which pulls from https://api.frankfurter.app via ERPNext).
    2. On success: creates a Currency Exchange record for *today* if none exists yet (exactly one record per day per currency pair).
    3. On failure (e.g. 522, network timeout, etc.): falls back to the most recent record in the Currency Exchange doctype for the same from_currency → to_currency.
    """
    if from_ccy == to_ccy:
        return 1.0
    posting_date = posting_date or today()
    date_obj = getdate(posting_date)

    try:
        # 1. Fetch fresh rate (this is the "exchange rate is fetched" step)
        rate = get_exchange_rate(from_ccy, to_ccy, posting_date)

        # 2. After successful fetch: create Currency Exchange record for today if missing
        if not frappe.db.exists("Currency Exchange", {
            "date": date_obj,
            "from_currency": from_ccy,
            "to_currency": to_ccy
        }):
            ce = frappe.new_doc("Currency Exchange")
            ce.date = date_obj
            ce.from_currency = from_ccy
            ce.to_currency = to_ccy
            ce.exchange_rate = rate
            ce.insert(ignore_permissions=True)
            frappe.db.commit()  # make the record immediately available
            frappe.logger().info(f"Created daily Currency Exchange record: {from_ccy}→{to_ccy} = {rate} on {date_obj}")

        return rate

    except Exception as e:
        # 3. API fetch failed (e.g. frankfurter.app 522) → fallback to most recent record
        frappe.logger().warning(
            f"Exchange rate fetch failed for {from_ccy}→{to_ccy} on {date_obj}: {str(e)}. "
            f"Using most recent record from Currency Exchange doctype."
        )

        latest_rate = frappe.db.get_value(
            "Currency Exchange",
            filters={
                "from_currency": from_ccy,
                "to_currency": to_ccy
            },
            fieldname="exchange_rate",
            order_by="date DESC",
            limit=1
        )

        if latest_rate is not None:
            return float(latest_rate)

        # No historical data at all
        frappe.throw(
            f"Exchange rate {from_ccy}→{to_ccy} could not be fetched "
            f"and no previous record exists in Currency Exchange."
        )

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
    use_last_updated=False,
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
            qs["LastUpdatedAfter" if use_last_updated else "CreatedAfter"] = updated_after
        if updated_before:
            qs["LastUpdatedBefore" if use_last_updated else "CreatedBefore"] = updated_before
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
            resp = requests.post(url, headers=headers, json=body, timeout=SPAPI_TIMEOUT)
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
        # Track only Sales Orders submitted by this repository instance. Existing submitted
        # MFN orders may contain stale economics from the pre-fix lifecycle and must not be
        # handed back to the invoice submitter as though they were freshly finalized.
        self._submitted_this_run = set()

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
        """
        Return posted Amazon Financial Events for one order.

        Financial Events has no authoritative per-order "complete" flag, so this method does
        not claim completeness. It does, however, aggregate every returned page deterministically
        and publishes a compact non-PII summary that the MFN lifecycle can use as a conservative
        finalization signal.
        """
        empty = {
            "charges": [],
            "fees": [],
            "tds": [],
            "service_fees": [],
            "principal_amounts": {},
            "additional_discount": 0,
            "financial_event_summary": {
                "shipment_event_count": 0,
                "shipment_item_count": 0,
                "principal_count": 0,
                "principal_total": 0.0,
                "charge_count": 0,
                "charge_total": 0.0,
                "fee_count": 0,
                "fee_total": 0.0,
                "withheld_tax_count": 0,
                "withheld_tax_total": 0.0,
                "service_fee_count": 0,
                "service_fee_total": 0.0,
                "promotion_count": 0,
                "promotion_total": 0.0,
                "buyer_charge_total": 0.0,
            },
        }
        try:
            financial_events_payload = _list_financial_events(self.amz_setting, order_id)
        except RequestException as e:
            frappe.log_error(
                message=f"SP-API finances fetch failed for {order_id}: {e}",
                title="Amazon Finances Fetch",
            )
            return empty

        if not (
            financial_events_payload
            and len(financial_events_payload.get("FinancialEvents", {}))
        ):
            return empty

        charges_and_fees = {
            "charges": [],
            "fees": [],
            "tds": [],
            "service_fees": [],
            "principal_amounts": {},
            "additional_discount": 0,
            "financial_event_summary": dict(empty["financial_event_summary"]),
        }
        summary = charges_and_fees["financial_event_summary"]
        principal_totals = {}
        principal_qty = {}
        promotion_discount = 0.0

        while True:
            financial_events = financial_events_payload.get("FinancialEvents", {}) or {}
            shipment_event_list = financial_events.get("ShipmentEventList", []) or []
            service_fee_event_list = financial_events.get("ServiceFeeEventList", []) or []
            next_token = financial_events_payload.get("NextToken")
            summary["shipment_event_count"] += len(shipment_event_list)

            for shipment_event in shipment_event_list:
                if not shipment_event:
                    continue
                for shipment_item in shipment_event.get("ShipmentItemList", []) or []:
                    summary["shipment_item_count"] += 1
                    seller_sku = shipment_item.get("SellerSKU") or ""
                    qty = _to_float(shipment_item.get("QuantityShipped"), 0.0)
                    promotion_list = shipment_item.get("PromotionList", []) or []
                    charges = shipment_item.get("ItemChargeList", []) or []
                    fees = shipment_item.get("ItemFeeList", []) or []
                    tds_list = shipment_item.get("ItemTaxWithheldList", []) or []

                    for charge in charges:
                        charge_type = charge.get("ChargeType")
                        amount = _to_float(charge.get("ChargeAmount", {}).get("CurrencyAmount"), 0.0)
                        if charge_type == "Principal":
                            summary["principal_count"] += 1
                            summary["principal_total"] += amount
                            principal_totals[seller_sku] = principal_totals.get(seller_sku, 0.0) + amount
                            principal_qty[seller_sku] = principal_qty.get(seller_sku, 0.0) + qty
                            continue
                        if abs(amount) < 0.000001:
                            continue
                        summary["charge_count"] += 1
                        summary["charge_total"] += amount
                        charges_and_fees["charges"].append({
                            "charge_type": "Actual",
                            "account_head": self.get_account(charge_type),
                            "tax_amount": amount,
                            "description": f"{charge_type} for {seller_sku if seller_sku else order_id}",
                        })

                    for fee in fees:
                        fee_type = fee.get("FeeType")
                        amount = _to_float(fee.get("FeeAmount", {}).get("CurrencyAmount"), 0.0)
                        if abs(amount) < 0.000001:
                            continue
                        summary["fee_count"] += 1
                        summary["fee_total"] += amount
                        charges_and_fees["fees"].append({
                            "charge_type": "Actual",
                            "account_head": self.get_account(fee_type),
                            "tax_amount": amount,
                            "description": f"{fee_type} for {seller_sku if seller_sku else order_id}",
                        })

                    for tds_group in tds_list:
                        for tds in tds_group.get("TaxesWithheld", []) or []:
                            tds_type = tds.get("ChargeType")
                            amount = _to_float(tds.get("ChargeAmount", {}).get("CurrencyAmount"), 0.0)
                            if abs(amount) < 0.000001:
                                continue
                            summary["withheld_tax_count"] += 1
                            summary["withheld_tax_total"] += amount
                            charges_and_fees["tds"].append({
                                "charge_type": "Actual",
                                "account_head": self.get_account(tds_type),
                                "tax_amount": amount,
                                "description": f"{tds_type} for {seller_sku if seller_sku else order_id}",
                            })

                    for promotion in promotion_list:
                        amount = _to_float(promotion.get("PromotionAmount", {}).get("CurrencyAmount"), 0.0)
                        if abs(amount) < 0.000001:
                            continue
                        summary["promotion_count"] += 1
                        summary["promotion_total"] += amount
                        promotion_discount += amount

            for service_fee in service_fee_event_list:
                if not service_fee:
                    continue
                for service_fee_item in service_fee.get("FeeList", []) or []:
                    fee_type = service_fee_item.get("FeeType")
                    amount = _to_float(service_fee_item.get("FeeAmount", {}).get("CurrencyAmount"), 0.0)
                    if abs(amount) < 0.000001:
                        continue
                    summary["service_fee_count"] += 1
                    summary["service_fee_total"] += amount
                    charges_and_fees["service_fees"].append({
                        "charge_type": "Actual",
                        "account_head": self.get_account(fee_type),
                        "tax_amount": amount,
                        "description": f"{fee_type} for {order_id}",
                    })

            if not next_token:
                break
            financial_events_payload = _list_financial_events(
                self.amz_setting, order_id, next_token=next_token
            )
            if not financial_events_payload:
                frappe.logger().warning(
                    f"Amazon Financial Events pagination ended early for {order_id}; "
                    "keeping MFN accounting unfinalized until a later refresh"
                )
                break

        for seller_sku, total in principal_totals.items():
            qty = principal_qty.get(seller_sku, 0.0)
            if qty:
                charges_and_fees["principal_amounts"][seller_sku] = round(total / qty, 2)

        charges_and_fees["additional_discount"] = promotion_discount
        for key in (
            "principal_total", "charge_total", "fee_total", "withheld_tax_total",
            "service_fee_total", "promotion_total",
        ):
            summary[key] = round(summary[key], 2)
        summary["buyer_charge_total"] = round(
            summary["principal_total"] + summary["charge_total"] + summary["promotion_total"],
            2,
        )
        return charges_and_fees

    def _mfn_financial_events_ready(self, order, items, charges_and_fees) -> tuple[bool, str]:
        """
        Conservative MFN accounting-finalization gate.

        Amazon exposes no per-order completeness flag. The gate therefore proves the buyer side
        of the order from posted shipment Financial Events and uses seller-fee presence only as
        an early maturity signal. A genuinely zero-fee order is allowed after the current Amazon
        order state has been stable for the documented 48-hour Financial Events lag window.
        """
        if (order.get("FulfillmentChannel") or "").upper() != "MFN":
            return True, "not_mfn"

        summary = charges_and_fees.get("financial_event_summary") or {}
        shipment_items = int(summary.get("shipment_item_count") or 0)
        if shipment_items <= 0:
            return False, "no_shipment_financial_event"

        order_total_obj = _order_total(order)
        has_order_total = bool(order_total_obj) and order_total_obj.get("Amount") is not None
        order_total = _order_total_amount(order)
        merchandise_total = round(sum(_to_float(item.get("amount"), 0.0) for item in items), 2)
        principal_total = round(_to_float(summary.get("principal_total"), 0.0), 2)
        principal_count = int(summary.get("principal_count") or 0)

        if abs(order_total) < 0.01 and order.get("ReplacedOrderId"):
            return True, "zero_value_replacement"

        zero_consideration = abs(order_total) < 0.01 and abs(merchandise_total) < 0.01
        if not zero_consideration and principal_count <= 0:
            return False, "no_principal_financial_event"

        # Use a small, bounded line-count allowance rather than a percentage of order value.
        # This tolerates independent cent-rounding on multi-line orders without allowing a large
        # dollar discrepancy simply because the order itself is large.
        tolerance = min(
            MFN_RECONCILIATION_MAX_TOLERANCE,
            max(0.02, round(0.01 * shipment_items, 2)),
        )

        buyer_charge_total = round(_to_float(summary.get("buyer_charge_total"), 0.0), 2)
        if has_order_total:
            if abs(buyer_charge_total - order_total) > tolerance:
                return False, (
                    f"buyer_charge_mismatch financial={buyer_charge_total:.2f} "
                    f"orders_api={order_total:.2f} tolerance={tolerance:.2f}"
                )
        elif not zero_consideration:
            if abs(principal_total - merchandise_total) > tolerance:
                return False, (
                    f"principal_mismatch financial={principal_total:.2f} "
                    f"items={merchandise_total:.2f} tolerance={tolerance:.2f}"
                )

        # Seller fees can lag the shipment event. Do not make fee presence an accounting truth:
        # if a fee exists, the order can mature immediately; if no fee exists, wait through the
        # documented recent-event lag window and then permit a genuinely fee-free MFN order.
        if principal_count > 0 and int(summary.get("fee_count") or 0) <= 0:
            age_hours = _amazon_last_update_age_hours(order)
            if age_hours is None or age_hours < MFN_ZERO_FEE_MATURITY_HOURS:
                age_text = "unknown" if age_hours is None else f"{age_hours:.1f}h"
                return False, (
                    "no_posted_seller_fee_yet "
                    f"last_update_age={age_text}; require {MFN_ZERO_FEE_MATURITY_HOURS}h "
                    "before accepting a zero-fee order"
                )

        return True, "posted_financial_events_match_order"

    def _mfn_postage_je_exists(self, order_id: str, fee_account: str, remark: str, cheque_no: str) -> bool:
        """Deterministic-key first, then legacy remark/account shape for pre-fix vouchers."""
        if frappe.db.exists("Journal Entry", {"cheque_no": cheque_no, "docstatus": ["!=", 2]}):
            return True
        return bool(frappe.db.sql(
            """
            SELECT je.name
            FROM `tabJournal Entry` je
            JOIN `tabJournal Entry Account` jea ON jea.parent = je.name
            WHERE je.docstatus = 1
              AND je.user_remark = %s
              AND jea.amazon_order_id = %s
              AND jea.account = %s
              AND jea.debit_in_account_currency > 0
            LIMIT 1
            """,
            (remark, order_id, fee_account),
        ))

    def _record_sync_review(self, order_id: str, remarks: str) -> None:
        """One durable, non-duplicating operator-visible marker."""
        remarks = remarks[:1000]
        if frappe.db.exists(
            "Amazon Failed Sync Record", {"amazon_order_id": order_id, "remarks": remarks}
        ):
            return
        row = frappe.new_doc("Amazon Failed Sync Record")
        row.amazon_order_id = order_id
        row.remarks = remarks
        row.save(ignore_permissions=True)

    def _post_mfn_postage_service_fee(self, so, service_fee) -> None:
        """
        Post the configured MFN postage fee exactly once, outside SO/SI economics.

        Must only be called AFTER the Sales Order has been successfully submitted, so a failed
        Sales Order can never leave an orphan customer AR credit behind.
        """
        fee_account = service_fee.get("account_head")
        native_amount = abs(_to_float(service_fee.get("tax_amount"), 0.0))
        order_id = so.amazon_order_id
        if not fee_account or native_amount < 0.01:
            return

        company = self.amz_setting.company
        remark = f"Amazon MFN Postage Fee for Order {order_id}"
        cheque_no = f"MFN-POST-{order_id}"

        # Serialize all postage creation for this order on the submitted Sales Order row.
        # That makes the existence check + insert idempotent without requiring a schema change.
        if so.name:
            frappe.db.sql("SELECT name FROM `tabSales Order` WHERE name=%s FOR UPDATE", (so.name,))
        if self._mfn_postage_je_exists(order_id, fee_account, remark, cheque_no):
            return

        try:
            receivable_account = get_party_account("Customer", so.customer, company)
        except Exception:
            receivable_account = None
        if not receivable_account:
            receivable_account = frappe.db.get_value(
                "Company", company, "default_receivable_account"
            )
        if not receivable_account:
            self._record_sync_review(
                order_id, f"MFN postage JE skipped: no receivable account for {so.customer}"
            )
            return

        # Financial Events amounts are in the marketplace/order currency. Convert per account so
        # a CAD/MXN order never books its native amount straight into a USD ledger.
        company_ccy = (frappe.get_cached_value("Company", company, "default_currency") or "").upper()
        order_ccy = (so.currency or company_ccy).upper()
        rate_to_company = (
            1.0 if order_ccy == company_ccy
            else _fx_rate(order_ccy, company_ccy, so.transaction_date)
        )

        def _row_values(account):
            acct_ccy = (
                frappe.get_cached_value("Account", account, "account_currency") or company_ccy
            ).upper()
            if acct_ccy == order_ccy:
                return round(native_amount, 2), rate_to_company
            if acct_ccy == company_ccy:
                return round(native_amount * rate_to_company, 2), 1.0
            return None, None

        debit_amount, debit_rate = _row_values(fee_account)
        credit_amount, credit_rate = _row_values(receivable_account)
        if debit_amount is None or credit_amount is None:
            self._record_sync_review(
                order_id,
                f"MFN postage JE skipped: account currency for {fee_account}/{receivable_account} "
                f"is neither order currency {order_ccy} nor company currency {company_ccy}; "
                "manual accounting review required",
            )
            return

        frappe.db.savepoint("before_mfn_postage_je")
        try:
            if self._mfn_postage_je_exists(order_id, fee_account, remark, cheque_no):
                return
            jv_doc = frappe.new_doc("Journal Entry")
            jv_doc.voucher_type = "Journal Entry"
            jv_doc.company = company                      # was unset: JE would post to the wrong company
            jv_doc.multi_currency = 1 if (order_ccy != company_ccy or debit_rate != 1 or credit_rate != 1) else 0
            jv_doc.posting_date = so.transaction_date
            jv_doc.cheque_no = cheque_no                  # deterministic idempotency key
            jv_doc.cheque_date = so.transaction_date
            jv_doc.user_remark = remark
            jv_doc.amazon_order_id = order_id

            debit = jv_doc.append("accounts")
            debit.account = fee_account
            debit.debit_in_account_currency = debit_amount
            debit.exchange_rate = debit_rate
            debit.user_remark = service_fee.get("description") or remark
            debit.amazon_order_id = order_id

            credit = jv_doc.append("accounts")
            credit.account = receivable_account
            credit.party_type = "Customer"
            credit.party = so.customer
            credit.credit_in_account_currency = credit_amount
            credit.exchange_rate = credit_rate
            credit.user_remark = remark
            credit.amazon_order_id = order_id

            jv_doc.flags.ignore_mandatory = True
            jv_doc.save(ignore_permissions=True)
            jv_doc.submit()
        except Exception:
            frappe.db.rollback(save_point="before_mfn_postage_je")
            frappe.log_error(
                title=f"Amazon MFN Postage Posting {order_id}"[:140],
                message=frappe.get_traceback(),
            )
            self._record_sync_review(
                order_id, "MFN postage JE failed to post; see Error Log. Sales Order economics unaffected."
            )

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
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.RequestException) as e:
            frappe.log_error(
                title="Amazon Order Import",
                message=f"SP-API orderItems request failed for {order_id}\n{str(e)}"
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
       
        # ── Lightweight dispatcher: enqueue one isolated job per draft ─────
        # This replaces the old long-running batch loop that was hitting the 10800-second RQ worker timeout.
        drafts = frappe.get_all("Sales Order", filters={
            "docstatus": 0, # Draft
            "amazon_order_id": ["is", "set"],
            "fulfillment_channel": ["!=", "MFN"],
            "creation": ["<", add_days(nowdate(), -age_days)]
        }, fields=["name", "amazon_order_id"])
       
        print(f"Fetched {len(drafts)} draft Sales Orders to reprocess.", flush=True)
        
        if not drafts:
            print("No draft orders found to reprocess.", flush=True)
            return

        enqueued = 0
        for i, d in enumerate(drafts):
            job_name = f"Reprocess Amazon Draft {d.amazon_order_id} ({d.name})"

            # Prevent duplicate jobs
            if frappe.db.exists("RQ Job", {
                "job_name": job_name,
                "status": ["in", ["queued", "started"]]
            }):
                print(f"Job already queued for {d.amazon_order_id}, skipping.", flush=True)
                continue

            try:
                frappe.enqueue(
                    method="eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_repository.reprocess_single_draft_order",
                    queue="long",
                    job_name=job_name,
                    amz_setting_name=self.amz_setting.name,
                    sales_order_name=d.name,
                    amazon_order_id=d.amazon_order_id,
                    timeout=1800,                                      # 30 min per order
                    now=getattr(frappe.flags, "in_test", False),
                )
                enqueued += 1
            except Exception as enqueue_err:
                print(f"Failed to enqueue job for {d.amazon_order_id}: {enqueue_err}", flush=True)

            # Gentle stagger (prevent Redis/Amazon thundering herd)
            if i % 10 == 9:
                time.sleep(1.0)

        print(f"Successfully enqueued {enqueued} independent reprocess jobs.", flush=True)
        print("reprocess_draft_orders dispatcher finished (this job is now fast).", flush=True)
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
            order_ccy = _order_total_currency(order)
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
        order_total_obj = _order_total(order)
        has_authoritative_order_total = (
            bool(order_total_obj) and order_total_obj.get("Amount") is not None
        )
        amazon_order_amount = _order_total_amount(order)
        so_id = None
        so_docstatus = 0
        existing_order_status = None
        existing_modified = None
        existing_creation = None
        
        if frappe.db.exists("Sales Order", {"amazon_order_id": order_id}):
            so_id, so_docstatus, existing_order_status, existing_modified, existing_creation = frappe.db.get_value(
                "Sales Order",
                filters={"amazon_order_id": order_id},
                fieldname=["name", "docstatus", "amazon_order_status", "modified", "creation"],
            )

        channel_hint = (order.get("FulfillmentChannel") or "").upper()
        incoming_status = order.get("OrderStatus")
        if so_id and not so_docstatus and channel_hint == "MFN":
            # Normal Orders sync may rediscover the same MFN order every run because this
            # repository queries by CreatedAfter. Same-status drafts obey the retry cadence;
            # status transitions (especially -> Shipped/InvoiceUnconfirmed) bypass it once.
            same_status = existing_order_status == incoming_status
            horizon_cutoff = get_datetime(add_days(nowdate(), -MFN_FINANCE_RETRY_HORIZON_DAYS))
            if same_status and existing_creation and get_datetime(existing_creation) < horizon_cutoff:
                remarks = (
                    f"MFN financial accounting not finalized within "
                    f"{MFN_FINANCE_RETRY_HORIZON_DAYS}-day Financial Events retry horizon"
                )
                if not frappe.db.exists(
                    "Amazon Failed Sync Record",
                    {"amazon_order_id": order_id, "remarks": remarks},
                ):
                    review = frappe.new_doc("Amazon Failed Sync Record")
                    review.amazon_order_id = order_id
                    review.remarks = remarks
                    review.save(ignore_permissions=True)
                print(
                    f"[AMZ-MFN] Automatic finance refresh horizon expired for {order_id}; "
                    "manual accounting review required",
                    flush=True,
                )
                return so_id

            retry_before = add_to_date(
                now_datetime(), hours=-MFN_FINANCE_RETRY_INTERVAL_HOURS
            )
            if (
                same_status
                and existing_modified
                and get_datetime(existing_modified) > get_datetime(retry_before)
            ):
                print(
                    f"[AMZ-MFN] Deferring finance refresh for {order_id}; "
                    f"status={incoming_status} was checked recently",
                    flush=True,
                )
                return so_id

        if so_docstatus and so_id:
            if (order.get("FulfillmentChannel") or "").upper() == "MFN":
                print(
                    f"[AMZ-MFN] {order_id} already has submitted Sales Order {so_id}; "
                    "repository will not mutate submitted economics",
                    flush=True,
                )
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

        order_ccy = _order_total_currency(order)
        so.currency = order_ccy


        so.amazon_order_id = order_id
        so.marketplace_id = order.get("MarketplaceId")
        so.amazon_order_status = order.get("OrderStatus")
        so.fulfillment_channel = order.get("FulfillmentChannel")
        so.replaced_order_id = order.get("ReplacedOrderId") or ''
        # Preserve Amazon Orders API OrderTotal exactly, including legitimate zero-value
        # replacements. Do not later replace it with an item-only subtotal.
        if has_authoritative_order_total:
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
        #if channel == "MFN" and self.amz_setting.custom_mfn_payment_terms_template:
        #    so.payment_terms_template = self.amz_setting.custom_mfn_payment_terms_template

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
            if channel != "MFN" and (len(so.taxes) > 0 or (not has_new_taxes and not has_additional_discount)):
                # AFN keeps the legacy low-churn guard. MFN intentionally rebuilds every draft
                # refresh because an earlier Financial Events response may have been partial.
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

        # get_order_items().total_order_value is intentionally item-level (ItemPrice +
        # ItemTax) and excludes order-level components such as ShippingPrice. Use it only
        # as a fallback when the Orders API did not provide OrderTotal at all.
        if not has_authoritative_order_total and total_order_value:
            so.amazon_order_amount = total_order_value

        # Add replacement note if applicable
        if so.replaced_order_id:
            so.custom_additional_notes = f"Replacement for original Amazon Order ID: {so.replaced_order_id}"

        taxes_and_charges = self.amz_setting.taxes_charges
        charges_and_fees = {}
        mfn_finance_ready = channel != "MFN"
        mfn_finance_reason = "not_mfn"
        pending_postage_fees = []  # posted only after a successful so.submit()

        item_lookup = {item["item_code"]: item.get('total_order_value', 0) for item in items}
        for row in so.items:
            total_value = item_lookup.get(row.item_code)
            if total_value:
                row.total_order_value = total_value

        if taxes_and_charges:
            charges_and_fees = self.get_charges_and_fees(order_id)
            if channel == "MFN":
                mfn_finance_ready, mfn_finance_reason = self._mfn_financial_events_ready(
                    order, items, charges_and_fees
                )
                summary = charges_and_fees.get("financial_event_summary") or {}
                print(
                    f"[AMZ-MFN-FIN] order={order_id} status={order.get('OrderStatus')} "
                    f"ready={mfn_finance_ready} reason={mfn_finance_reason} "
                    f"ship_events={int(summary.get('shipment_event_count') or 0)} "
                    f"ship_items={int(summary.get('shipment_item_count') or 0)} "
                    f"principal={_to_float(summary.get('principal_total')):.2f} "
                    f"charges={int(summary.get('charge_count') or 0)}/"
                    f"{_to_float(summary.get('charge_total')):.2f} "
                    f"fees={int(summary.get('fee_count') or 0)}/"
                    f"{_to_float(summary.get('fee_total')):.2f} "
                    f"withheld={int(summary.get('withheld_tax_count') or 0)}/"
                    f"{_to_float(summary.get('withheld_tax_total')):.2f} "
                    f"service={int(summary.get('service_fee_count') or 0)}/"
                    f"{_to_float(summary.get('service_fee_total')):.2f} "
                    f"promotions={_to_float(summary.get('promotion_total')):.2f}",
                    flush=True,
                )
            
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
            
            # One lookup per order instead of one per service-fee row.
            mfn_postage_fee_account_head = frappe.db.get_value(
                "Amazon SP API Settings", self.amz_setting.name, "mfn_postage_fee_account_head"
            )
            for service_fee in charges_and_fees.get("service_fees"):
                if not service_fee:
                    continue
                is_separate_mfn_postage = (
                    channel == "MFN"
                    and not so.replaced_order_id
                    and mfn_postage_fee_account_head
                    and service_fee.get("account_head") == mfn_postage_fee_account_head
                )
                if is_separate_mfn_postage:
                    # Preserve the existing ownership boundary: configured MFN postage is not
                    # also placed on SO/SI. Queue it; it is only posted after the Sales Order
                    # actually submits, so a failed SO cannot leave an orphan AR credit.
                    if (
                        mfn_finance_ready
                        and order.get("OrderStatus") in ["Shipped", "InvoiceUnconfirmed"]
                    ):
                        pending_postage_fees.append(service_fee)
                    else:
                        print(
                            f"[AMZ-MFN] Deferring separate postage posting for {order_id}; "
                            f"finance reason={mfn_finance_reason}",
                            flush=True,
                        )
                else:
                    so.append("taxes", service_fee)

            if charges_and_fees.get("additional_discount"):
                so.discount_amount = float(charges_and_fees.get("additional_discount")) * -1
        elif channel == "MFN":
            mfn_finance_ready = False
            mfn_finance_reason = "taxes_and_charges_template_not_configured"

        so.flags.ignore_mandatory = True
        so.disable_rounded_total = 1
        so.calculate_taxes_and_totals()
        if so.grand_total>=0:
            try:
                if channel == "MFN":
                    so.payment_terms_template = ""
                    # If a schedule sneaks in via defaults, nuke it:
                    if getattr(so, "payment_schedule", None):
                        so.payment_schedule = []
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
            mfn_fulfilled = channel == "MFN" and order.get("OrderStatus") in ["Shipped", "InvoiceUnconfirmed"]
            accounting_ready = (
                (channel != "MFN" and (has_taxes or is_replacement_zero))
                or (channel == "MFN" and mfn_fulfilled and (mfn_finance_ready or is_replacement_zero))
            )
            if order_status_valid and accounting_ready and transfer_exists:
                try:
                    so.submit()
                    self._submitted_this_run.add(so.name)
                    is_fulfilled = (channel == "AFN") or mfn_fulfilled
                    if is_fulfilled:
                        for d in so.items:
                            d.delivered_qty = d.qty
                        so.per_delivered = 100
                        so.db_set("status", "Completed")
                        so.db_set("delivery_date", nowdate())
                        so.db_update()
                    # Separate postage ownership is only real once the SO economics are final.
                    for service_fee in pending_postage_fees:
                        self._post_mfn_postage_service_fee(so, service_fee)
                    frappe.db.commit()
                except Exception as e:
                    frappe.log_error("Error submitting Sales Order for Order {0}".format(so.amazon_order_id), e, "Sales Order")
            elif channel == "MFN" and order_status_valid and transfer_exists:
                print(
                    f"[AMZ-MFN] Keeping {so.name} Draft for {order_id}: "
                    f"status={order.get('OrderStatus')} fulfilled={mfn_fulfilled} "
                    f"finance_ready={mfn_finance_ready} reason={mfn_finance_reason}",
                    flush=True,
                )
            
        elif not frappe.db.exists("Amazon Failed Sync Record", {"amazon_order_id":order_id}):
            remarks = 'Failed to create Sales Order for {0}. Sales Order grand Total = {1}'.format(order_id, so.grand_total)
            if channel == "AFN":
                summary = charges_and_fees.get("financial_event_summary") or {}
                remarks += (
                    f"; AFN net-negative diagnostics: currency={order_ccy}, "
                    f"orders_api_total={amazon_order_amount:.2f}, "
                    f"principal={_to_float(summary.get('principal_total')):.2f}, "
                    f"buyer_charges={_to_float(summary.get('charge_total')):.2f}, "
                    f"seller_fees={_to_float(summary.get('fee_total')):.2f}, "
                    f"withheld_tax={_to_float(summary.get('withheld_tax_total')):.2f}, "
                    f"service_fees={_to_float(summary.get('service_fee_total')):.2f}, "
                    f"promotions={_to_float(summary.get('promotion_total')):.2f}"
                )
                print(f"[AMZ-AFN-NEGATIVE] {order_id}: {remarks}", flush=True)
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

    def _refresh_recent_mfn_drafts(
        self,
        *,
        horizon_days: int = MFN_FINANCE_RETRY_HORIZON_DAYS,
        retry_interval_hours: int = MFN_FINANCE_RETRY_INTERVAL_HOURS,
        limit: int = MFN_FINANCE_RETRY_BATCH_SIZE,
    ) -> list[str]:
        """
        Recheck only recent MFN drafts whose accounting has not finalized.

        This provides a bounded retry path for Financial Events lag without turning settlement
        processing into a permanent per-order Finances poller. Orders older than the horizon are
        left Draft for manual review and are not automatically queried again by this helper.
        """
        cutoff = add_days(nowdate(), -horizon_days)
        retry_before = add_to_date(now_datetime(), hours=-retry_interval_hours)
        drafts = frappe.get_all(
            "Sales Order",
            filters={
                "docstatus": 0,
                "fulfillment_channel": "MFN",
                "amazon_order_id": ["is", "set"],
                "creation": [">=", cutoff],
                "modified": ["<=", retry_before],
            },
            fields=["name", "amazon_order_id"],
            order_by="modified asc",
            limit_page_length=limit,
        )
        newly_submitted = []
        for row in drafts:
            try:
                order = self._fetch_order_by_id(row.amazon_order_id)
                if not order:
                    print(f"[AMZ-MFN] Retry found no Orders API payload for {row.amazon_order_id}", flush=True)
                    continue
                so_name = self.create_sales_order(order)
                if so_name in self._submitted_this_run:
                    newly_submitted.append(so_name)
            except (RequestException, HTTPError, SPAPIError) as exc:
                frappe.logger().warning(
                    f"MFN finance refresh deferred for {row.amazon_order_id}: {type(exc).__name__}: {exc}"
                )
            except Exception:
                frappe.log_error(
                    title=f"Amazon MFN Finance Refresh {row.amazon_order_id}"[:140],
                    message=frappe.get_traceback(),
                )

        expired_count = frappe.db.count(
            "Sales Order",
            filters={
                "docstatus": 0,
                "fulfillment_channel": "MFN",
                "amazon_order_id": ["is", "set"],
                "creation": ["<", cutoff],
            },
        )
        if expired_count:
            frappe.logger().warning(
                f"{expired_count} MFN Sales Order draft(s) are older than the {horizon_days}-day "
                "automatic Financial Events retry horizon and require manual accounting review"
            )
        return newly_submitted

    def _fetch_and_process_orders(self, statuses, channel, last_updated_after, last_updated_before, sales_orders):
        # ── first fetch ──────────────────────────────────────────────────
        orders_payload = _list_orders(
            self.amz_setting,
            updated_after=last_updated_after,
            updated_before=last_updated_before,
            order_statuses=",".join(statuses),
            fulfillment_channels=channel,  # Note: Pass as str, not list (e.g., "AFN")
            max_results=50,
            # MFN must be rediscovered when an older order changes status to Shipped.
            # Preserve the established AFN CreatedAfter behavior unchanged.
            use_last_updated=(channel == "MFN"),
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
                # One malformed order (or one failed accounting posting) must not abort the
                # remaining orders and pages of the whole sync run.
                try:
                    so = self.create_sales_order(order)
                except (RequestException, HTTPError, SPAPIError) as exc:
                    frappe.logger().warning(
                        f"Amazon order {order.get('AmazonOrderId')} deferred: "
                        f"{type(exc).__name__}: {exc}"
                    )
                    so = None
                except Exception:
                    frappe.log_error(
                        title=f"Amazon Order Import {order.get('AmazonOrderId')}"[:140],
                        message=frappe.get_traceback(),
                    )
                    so = None
                time.sleep(1.1)
                if so:
                    if channel != "MFN" or so in self._submitted_this_run:
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
            "Unshipped",            # Operational visibility before seller fulfillment.
            "PartiallyShipped",     # Keep refreshing while fulfillment is incomplete.
            "Shipped",              # Financial-finalization candidate; still requires posted-event readiness.
            "InvoiceUnconfirmed",   # Financial-finalization candidate; status alone is not sufficient.
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

        # Financial Events can lag. Recheck only recent MFN drafts on a bounded cadence, and
        # pass an MFN SO to the invoice submitter only when this repository just finalized it.
        for so_name in self._refresh_recent_mfn_drafts():
            if so_name not in sales_orders:
                sales_orders.append(so_name)

        if sales_orders:
            frappe.enqueue(
                "eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_sp_api_settings.enq_si_submit",
                sales_orders=sales_orders,
                amz_setting_name=self.amz_setting.name,
            )
        
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

#frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_repository.reprocess_single_draft_order", amz_setting_name="q3opu7c5ac", sales_order_name="SO26-15974", amazon_order_id="112-9037060-7281054")
@frappe.whitelist()
def reprocess_single_draft_order(amz_setting_name: str, sales_order_name: str, amazon_order_id: str):
    """
    Process EXACTLY ONE draft Sales Order in its own isolated RQ job.
    Failure/timeout here only affects this single order.
    """
    ar = AmazonRepository(amz_setting_name)
    start_time = time.time()

    try:
        print(f"[{amazon_order_id}] Processing single draft SO {sales_order_name}", flush=True)

        order_payload = _list_orders(ar.amz_setting, amazon_order_ids=amazon_order_id)
        if not order_payload or not order_payload.get("Orders"):
            print(f"[{amazon_order_id}] No order payload found. Deleting orphan SO {sales_order_name}.")
            frappe.delete_doc("Sales Order", sales_order_name, ignore_permissions=True)
            frappe.db.commit()
            return

        orders = order_payload.get("Orders") or []
        order = orders[0] if orders else None
        if not order:
            frappe.logger().warning(f"[{amazon_order_id}] SP-API returned no order data; keeping draft.")
            return

        so_name = ar.create_sales_order(order)
        if so_name in ar._submitted_this_run:
            frappe.enqueue(
                "eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_sp_api_settings.enq_si_submit",
                sales_orders=[so_name],
                amz_setting_name=ar.amz_setting.name,
            )
        duration = time.time() - start_time
        print(f"[{amazon_order_id}] Successfully reprocessed in {duration:.1f}s", flush=True)

    except (requests.exceptions.RequestException,
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            HTTPError,
            SPAPIError) as e:
        duration = time.time() - start_time
        msg = f"Transient Amazon API error for order {amazon_order_id} (SO {sales_order_name}) after {duration:.1f}s: {type(e).__name__} – {str(e)[:220]}"
        print(f"⚠️  {msg}", flush=True)
        frappe.logger().warning(msg)   # warning only — no error log spam

    except Exception as e:
        duration = time.time() - start_time
        error_msg = f"Failed to reprocess draft order {amazon_order_id} (SO {sales_order_name}) after {duration:.1f}s"
        full_trace = f"{error_msg}\n\n{frappe.get_traceback()}"
        frappe.log_error(
            title="Amazon Draft Reprocess - Single Order Failure",
            message=full_trace
        )
        print(f"❌ {error_msg}: {str(e)[:300]}", flush=True)

    finally:
        # Best-effort commit in case the job was partially successful
        try:
            frappe.db.commit()
        except Exception:
            pass

@frappe.whitelist()
def get_order(amz_setting_name, amazon_order_ids) -> list:
    ar = AmazonRepository(amz_setting_name)
    return ar.get_order(amazon_order_ids)




