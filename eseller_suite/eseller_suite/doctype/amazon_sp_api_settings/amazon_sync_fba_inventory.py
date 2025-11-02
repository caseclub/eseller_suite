# amazon_sync_fba_inventory.py
# =========================================
#  Syncs Amazon FBA inventory quantities to ERPNext once per day
#  using the FBA Inventory API (direct GET, no reports/POST needed).
# =========================================
from __future__ import annotations
import json, requests

from datetime import datetime, timedelta, timezone
import time
from zoneinfo import ZoneInfo
import frappe
from .amazon_repository import _sp_get, AmazonRepository

from urllib.parse import urlencode

from collections import defaultdict

from erpnext.stock.doctype.batch.batch import get_batch_qty
from erpnext.stock.stock_ledger import NegativeStockError

import pytz

# When True all docs are left as drafts and not submitted
DEBUG = False  # Toggle to False to disable all debug prints. Also set to True to run the progam on demand as opposed to during the set time

# ──────────────────────────────────────────
# Helper Functions
# ──────────────────────────────────────────
def parse_marketplaces(mkt_str: str) -> list[str]:
    if not mkt_str:
        return []
    return [m.strip() for m in mkt_str.split(',') if m.strip()]

# ──────────────────────────────────────────
# Inbound Processing
# ──────────────────────────────────────────
def process_inbound_inventory(asin_inbound, settings):
    prep_wh = settings.custom_amazon_fba_staging_area
    inbound_wh = settings.custom_amazon_inbound_warehouse
    company = settings.company
    adjustment_account = settings.custom_amazon_inventory_adjustment_account

    if DEBUG: print(f"[DEBUG] Starting inbound inventory processing for warehouse: {inbound_wh}")

    # First pass: collect transfers for increases
    transfer_items = []
    prep_reconcile_items = []
    transfer_pending = []
    for asin, target_qty in asin_inbound.items():
        if DEBUG: print(f"[DEBUG] Processing inbound ASIN: {asin} with target_qty: {target_qty}")
        item_code = frappe.db.get_value("Item", {"custom_asin": asin, "disabled": 0}, "name")
        if not item_code:
            if DEBUG: print(f"[DEBUG] No matching item_code found for ASIN: {asin}")
            continue

        # ADDED: Skip if not a stock item
        if not frappe.get_value("Item", item_code, "is_stock_item"):
            if DEBUG: print(f"[DEBUG] Skipping non-stock item: {item_code}")
            continue

        current_inbound = frappe.db.get_value("Bin", {"item_code": item_code, "warehouse": inbound_wh}, "actual_qty") or 0
        diff = target_qty - current_inbound
        if DEBUG: print(f"[DEBUG] Current inbound qty: {current_inbound}, diff: {diff}")
        if diff <= 0:
            continue

        current_prep = frappe.db.get_value("Bin", {"item_code": item_code, "warehouse": prep_wh}, "actual_qty") or 0
        transfer_qty = min(current_prep, diff)
        if DEBUG: print(f"[DEBUG] Current prep qty: {current_prep}, transfer_qty: {transfer_qty}")
        if transfer_qty <= 0:
            continue

        bin_data_prep = frappe.db.get_value(
            "Bin",
            {"item_code": item_code, "warehouse": prep_wh},
            ["valuation_rate"],
            as_dict=True
        ) or {}
        bin_rate = bin_data_prep.get("valuation_rate", 0)

        item_valuation_rate = frappe.get_value("Item", item_code, "valuation_rate") or 0
        val_rate = item_valuation_rate if item_valuation_rate > 0 else 0.01

        has_batch = frappe.get_value("Item", item_code, "has_batch_no")
        has_serial = frappe.get_value("Item", item_code, "has_serial_no")

        reconcile_needed = (bin_rate != val_rate)

        if reconcile_needed:
            item_reconcile_items = []
            if has_serial:
                serial_nos = frappe.db.sql_list("""SELECT name FROM `tabSerial No` WHERE item_code = %s AND warehouse = %s""", (item_code, prep_wh))
                if len(serial_nos) == current_prep:
                    item_reconcile_items.append({
                        "item_code": item_code,
                        "warehouse": prep_wh,
                        "qty": current_prep,
                        "valuation_rate": val_rate,
                        "serial_no": '\n'.join(serial_nos),
                    })
            elif has_batch:
                batches = frappe.get_all("Batch", filters={"item": item_code}, fields=["name"])
                for batch in batches:
                    batch_qty = get_batch_qty(batch.name, prep_wh, item_code) or 0
                    if batch_qty > 0:
                        item_reconcile_items.append({
                            "item_code": item_code,
                            "warehouse": prep_wh,
                            "qty": batch_qty,
                            "valuation_rate": val_rate,
                            "batch_no": batch.name,
                        })
            else:
                item_reconcile_items.append({
                    "item_code": item_code,
                    "warehouse": prep_wh,
                    "qty": current_prep,
                    "valuation_rate": val_rate,
                })
            if item_reconcile_items:
                prep_reconcile_items += item_reconcile_items
                transfer_pending.append((item_code, transfer_qty, has_batch, has_serial, val_rate))
            else:
                if DEBUG: print(f"[DEBUG] Could not create reconcile items for {item_code}, skipping transfer")
        else:
            transfer_pending.append((item_code, transfer_qty, has_batch, has_serial, val_rate))

    # Create and submit Prep Stock Reconciliation if needed
    if prep_reconcile_items:
        if DEBUG: print(f"[DEBUG] Creating Prep Stock Reconciliation with {len(prep_reconcile_items)} items...")
        try:  # ADDED: Wrap for error logging
            prep_sr = frappe.get_doc({
                "doctype": "Stock Reconciliation",
                "company": company,
                "posting_date": frappe.utils.today(),
                "purpose": "Stock Reconciliation",
                "expense_account": adjustment_account,
                "items": prep_reconcile_items,
            })
            prep_sr.insert(ignore_permissions=True)
            if DEBUG: print(f"[DEBUG] Inserted Prep SR: {prep_sr.name}")
            if DEBUG:
                if DEBUG: print(f"[DEBUG] DEBUG mode: leaving Prep SR {prep_sr.name} as DRAFT (not submitted)")
                frappe.db.commit()  # persist draft
            else:
                prep_sr.submit()
                frappe.db.commit()
                if DEBUG: print(f"[DEBUG] Submitted Prep SR: {prep_sr.name}")
        except Exception:
            frappe.log_error(frappe.get_traceback(), "Prep Stock Reconciliation Error")
            raise  # Re-raise to propagate if needed

    # Now process pending transfers
    for item_code, transfer_qty, has_batch, has_serial, val_rate in transfer_pending:
        current_prep = frappe.db.get_value("Bin", {"item_code": item_code, "warehouse": prep_wh}, "actual_qty") or 0
        transfer_qty = min(current_prep, transfer_qty)
        if transfer_qty <= 0:
            continue
        if has_serial:
            serial_nos = frappe.db.sql_list("""SELECT name FROM `tabSerial No` WHERE item_code = %s AND warehouse = %s LIMIT %s""", (item_code, prep_wh, transfer_qty))
            if len(serial_nos) == transfer_qty:
                transfer_items.append({
                    "item_code": item_code,
                    "s_warehouse": prep_wh,
                    "t_warehouse": inbound_wh,
                    "qty": transfer_qty,
                    "basic_rate": val_rate,
                    "serial_no": '\n'.join(serial_nos),
                })
            else:
                if DEBUG: print(f"[DEBUG] Insufficient serial nos for {item_code}, skipping transfer")
        elif has_batch:
            batches = frappe.get_all("Batch", filters={"item": item_code}, fields=["name"], order_by="creation asc")
            remaining = transfer_qty
            for batch in batches:
                if remaining <= 0:
                    break
                batch_qty = get_batch_qty(batch.name, prep_wh, item_code) or 0
                if batch_qty > 0:
                    move_qty = min(batch_qty, remaining)
                    transfer_items.append({
                        "item_code": item_code,
                        "s_warehouse": prep_wh,
                        "t_warehouse": inbound_wh,
                        "qty": move_qty,
                        "basic_rate": val_rate,
                        "batch_no": batch.name,
                    })
                    remaining -= move_qty
            if remaining > 0:
                if DEBUG: print(f"[DEBUG] Insufficient batch qty for {item_code}, transferred {transfer_qty - remaining}, remaining {remaining} will be handled by reconciliation")
        else:
            transfer_items.append({
                "item_code": item_code,
                "s_warehouse": prep_wh,
                "t_warehouse": inbound_wh,
                "qty": transfer_qty,
                "basic_rate": val_rate,
            })

    # Create and submit Stock Entry if needed
    if transfer_items:
        if DEBUG: print(f"[DEBUG] Creating Stock Entry with {len(transfer_items)} items...")
        se = frappe.get_doc({
            "doctype": "Stock Entry",
            "company": company,
            "stock_entry_type": "Material Transfer",
            "posting_date": frappe.utils.today(),
            "items": transfer_items,
        })
        se.insert(ignore_permissions=True)
        if DEBUG: print(f"[DEBUG] Inserted SE: {se.name}")
        try:
            if DEBUG:
                print(f"[DEBUG] DEBUG mode: leaving SE {se.name} as DRAFT (not submitted)")
                frappe.db.commit()
            else:
                se.submit()
                frappe.db.commit()
                if DEBUG: print(f"[DEBUG] Submitted SE: {se.name}")
        except NegativeStockError as e:
            if DEBUG: print(f"[DEBUG] NegativeStockError during submit: {str(e)}")
            # Safely delete draft
            try:
                se.reload()  # Reload to get current status
                if se.docstatus == 0:
                    se.delete()
                elif se.docstatus == 1:
                    se.cancel()
                    se.delete()
                frappe.db.commit()
            except Exception as del_e:
                if DEBUG: print(f"[DEBUG] Error during cleanup delete: {str(del_e)}")
                frappe.log_error(frappe.get_traceback(), "Stock Entry Cleanup Error")
            if DEBUG: print("[DEBUG] Deleted draft SE, falling back to reconciliation")
            frappe.log_error(frappe.get_traceback(), "Stock Entry NegativeStockError")  # ADDED: Log specific error
        except Exception as e:
            if DEBUG: print(f"[DEBUG] Unexpected error during SE submit: {str(e)}")
            # Safely delete
            try:
                se.reload()  # Reload to get current status
                if se.docstatus == 0:
                    se.delete()
                elif se.docstatus == 1:
                    se.cancel()
                    se.delete()
                frappe.db.commit()
            except Exception as del_e:
                if DEBUG: print(f"[DEBUG] Error during cleanup delete: {str(del_e)}")
                frappe.log_error(frappe.get_traceback(), "Stock Entry Cleanup Error")
            frappe.log_error(frappe.get_traceback(), "Stock Entry Submit Error")  # ADDED: Log with traceback
            raise

    # Second pass: collect reconciliations where qty doesn't match
    reconcile_items = []
    for asin, target_qty in asin_inbound.items():
        item_code = frappe.db.get_value("Item", {"custom_asin": asin, "disabled": 0}, "name")
        if not item_code:
            continue

        # ADDED: Skip if not a stock item
        if not frappe.get_value("Item", item_code, "is_stock_item"):
            if DEBUG: print(f"[DEBUG] Skipping non-stock item: {item_code}")
            continue

        current_inbound = frappe.db.get_value("Bin", {"item_code": item_code, "warehouse": inbound_wh}, "actual_qty") or 0
        if current_inbound == target_qty:
            if DEBUG: print(f"[DEBUG] Inbound qty matches for {item_code}: {current_inbound} == {target_qty}")
            continue

        if DEBUG: print(f"[DEBUG] Inbound qty mismatch for {item_code}: {current_inbound} != {target_qty}")
        item_valuation_rate = frappe.get_value("Item", item_code, "valuation_rate") or 0
        item_dict = {
            "item_code": item_code,
            "warehouse": inbound_wh,
            "qty": target_qty,
        }
        if item_valuation_rate > 0:
            item_dict["valuation_rate"] = item_valuation_rate
        else:
            item_dict["valuation_rate"] = 0.01
        reconcile_items.append(item_dict)

    # Fetch Amazon items in inbound warehouse with positive qty not reported by Amazon, assume 0
    inbound_amazon_items = frappe.db.sql("""
        SELECT i.name as item_code, i.custom_asin as asin, b.actual_qty, b.valuation_rate
        FROM `tabItem` i
        INNER JOIN `tabBin` b ON i.name = b.item_code
        WHERE b.warehouse = %s AND i.custom_asin IS NOT NULL AND b.actual_qty > 0 AND i.disabled = 0 AND i.is_stock_item = 1
    """, inbound_wh, as_dict=True)

    for row in inbound_amazon_items:
        if row.asin in asin_inbound:
            continue  # Already handled if needed
        item_valuation_rate = frappe.get_value("Item", row.item_code, "valuation_rate") or 0
        item_dict = {
            "item_code": row.item_code,
            "warehouse": inbound_wh,
            "qty": 0,
        }
        if item_valuation_rate > 0:
            item_dict["valuation_rate"] = item_valuation_rate
        else:
            item_dict["valuation_rate"] = 0.01
        reconcile_items.append(item_dict)

    # Create and submit Stock Reconciliation if needed
    if reconcile_items:
        if DEBUG: print(f"[DEBUG] Creating Stock Reconciliation with {len(reconcile_items)} items...")
        try:  # ADDED: Wrap for error logging
            sr = frappe.get_doc({
                "doctype": "Stock Reconciliation",
                "company": company,
                "posting_date": frappe.utils.today(),
                "purpose": "Stock Reconciliation",
                "expense_account": adjustment_account,
                "items": reconcile_items,
            })
            sr.insert(ignore_permissions=True)
            if DEBUG: print(f"[DEBUG] Inserted SR: {sr.name}")
            if DEBUG:
                if DEBUG: print(f"[DEBUG] DEBUG mode: leaving inbound SR {sr.name} as DRAFT (not submitted)")
                frappe.db.commit()  # persist draft
            else:
                sr.submit()
                frappe.db.commit()
                if DEBUG: print(f"[DEBUG] Submitted inbound SR: {sr.name}")
        except Exception:
            frappe.log_error(frappe.get_traceback(), "Inbound Stock Reconciliation Error")
            raise

# ──────────────────────────────────────────
# Orchestrator
# ──────────────────────────────────────────
def process_fba_inventory():
    try:  # ADDED: High-level wrap for entire function
        repo = AmazonRepository("q3opu7c5ac")
        settings = repo.amz_setting
        if DEBUG: print("[DEBUG] Starting FBA inventory sync...")

        # Pull and parse marketplace IDs from settings
        marketplace_ids = parse_marketplaces(settings.custom_marketplace)
        if DEBUG: print(f"[DEBUG] Fetching for marketplaces: {marketplace_ids}")

        # Aggregate across all marketplaces
        summaries = []
        

        for mkt_id in marketplace_ids:
            if DEBUG: print(f"[DEBUG] Querying marketplace: {mkt_id}")
            start_dt = (datetime.now(timezone.utc) - timedelta(days=540)).replace(microsecond=0)
            start_date = start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            base_qs = {
                "granularityType": "Marketplace",
                "granularityId": mkt_id,
                "marketplaceIds": mkt_id,
                "details": "true",
                "startDateTime": start_date  # UTC ISO format
            }
            if DEBUG: print(f"[DEBUG] Query parameters: {base_qs}")
            next_token = None
            page = 1
            while True:
                if next_token:
                    qs = dict(base_qs)
                    qs["nextToken"] = next_token
                    if DEBUG: print(f"[DEBUG] Updated qs with nextToken: {qs}")
                else:
                    qs = dict(base_qs)  # First page uses full filters + startDateTime

                if DEBUG: print(f"[DEBUG] Fetching page {page} for {mkt_id}...")
                try:  # ADDED: Wrap API call for logging
                    resp = _sp_get("/fba/inventory/v1/summaries", qs, settings, return_full=True)  # Added return_full=True
                except Exception:
                    frappe.log_error(frappe.get_traceback(), f"API Call Error for Marketplace {mkt_id}")
                    raise
                #print(f"[DEBUG] Full API response: {json.dumps(resp, indent=2)}")  # Uncomment if needed for verification
                page_summaries = resp.get("payload", {}).get("inventorySummaries", [])  # Extract from payload
                summaries.extend(page_summaries)
                if DEBUG: print(f"[DEBUG] Fetched {len(page_summaries)} summaries from page {page} for {mkt_id}")
                if len(page_summaries) == 0:
                    if DEBUG: print("[DEBUG] No summaries in this page - check if response has errors or warnings")
                next_token = resp.get("pagination", {}).get("nextToken")  # Extract from top-level pagination
                if not next_token:
                    if DEBUG: print(f"[DEBUG] No more pages for {mkt_id}")
                    break
                time.sleep(1)  # Throttle between pages
                page += 1
            time.sleep(2)  # Throttle between marketplaces to avoid rate limits

        if DEBUG: print(f"[DEBUG] Total summaries fetched: {len(summaries)}")
        if summaries:
            if DEBUG: print(f"[DEBUG] Sample summary: {summaries[0]}")  # Print first one for inspection
        else:
            if DEBUG: print("[DEBUG] No summaries fetched across all marketplaces - possible reasons: no FBA inventory in these marketplaces, missing 'Inventory' role in SP-API permissions, or try adding 'startDateTime' parameter for recent changes")

        # Collect all unique conditions for debugging
        conditions = set(s.get("condition", "UNKNOWN") for s in summaries)
        if DEBUG: print(f"[DEBUG] Unique conditions found in summaries: {conditions}")

        # Aggregate fulfillable and inbound qty by ASIN for new condition
        asin_fulfillable = defaultdict(int)
        asin_inbound = defaultdict(int)
        for s in summaries:
            cond = s.get("condition", "")  # Correct key per API docs
            asin = s.get("asin", "")
            fulfillable_qty = s.get("inventoryDetails", {}).get("fulfillableQuantity", 0)
            inbound_working = s.get("inventoryDetails", {}).get("inboundWorkingQuantity", 0)
            inbound_shipped = s.get("inventoryDetails", {}).get("inboundShippedQuantity", 0)
            inbound_receiving = s.get("inventoryDetails", {}).get("inboundReceivingQuantity", 0)
            inbound_qty = inbound_working + inbound_shipped + inbound_receiving
            if DEBUG: print(f"[DEBUG] Processing summary: ASIN={asin}, Condition={cond}, FulfillableQty={fulfillable_qty}, InboundQty={inbound_qty}")
            if cond != "NewItem":  # Filter to new condition (adjust if your data uses variants like "SELLABLE")
                if DEBUG: print(f"[DEBUG] Skipping non-new condition: {cond}")
                continue
            asin_fulfillable[asin] += fulfillable_qty
            asin_inbound[asin] += inbound_qty
            if DEBUG: print(f"[DEBUG] Added to asin_fulfillable: {asin} -> {asin_fulfillable[asin]}")
            if DEBUG: print(f"[DEBUG] Added to asin_inbound: {asin} -> {asin_inbound[asin]}")

        if DEBUG: print(f"[DEBUG] Aggregated asin_fulfillable: {dict(asin_fulfillable)}")
        if DEBUG: print(f"[DEBUG] Aggregated asin_inbound: {dict(asin_inbound)}")

        # Prepare Stock Reconciliation items for fulfillable
        wh = settings.afn_warehouse
        company = settings.company
        adjustment_account = settings.custom_amazon_inventory_adjustment_account  # Assume this custom field exists in settings; add if needed
        items_list = []
        for asin, new_qty in asin_fulfillable.items():
            item_code = frappe.db.get_value("Item", {"custom_asin": asin, "disabled": 0}, "name")
            if not item_code:
                if DEBUG: print(f"[DEBUG] No matching item_code found for ASIN: {asin}")
                continue

            # ADDED: Skip if not a stock item
            if not frappe.get_value("Item", item_code, "is_stock_item"):
                if DEBUG: print(f"[DEBUG] Skipping non-stock item: {item_code}")
                continue

            # Get current bin data
            bin_data = frappe.db.get_value(
                "Bin",
                {"item_code": item_code, "warehouse": wh},
                ["actual_qty", "valuation_rate"],
                as_dict=True
            ) or {}
            current_qty = bin_data.get("actual_qty", 0)
            if DEBUG: print(f"[DEBUG] Current qty in Bin: {current_qty} vs New qty: {new_qty} - {item_code}")
            if int(current_qty) == new_qty:
                continue  # No adjustment needed

            item_valuation_rate = frappe.get_value("Item", item_code, "valuation_rate") or 0
            item_dict = {
                "item_code": item_code,
                "warehouse": wh,
                "qty": new_qty,
            }
            if item_valuation_rate > 0:
                item_dict["valuation_rate"] = item_valuation_rate
            else:
                item_dict["valuation_rate"] = 0.01
            items_list.append(item_dict)

        # Fetch Amazon items in warehouse with positive qty not reported by Amazon, assume 0
        amazon_items_in_wh = frappe.db.sql("""
            SELECT i.name as item_code, i.custom_asin as asin, b.actual_qty, b.valuation_rate
            FROM `tabItem` i
            INNER JOIN `tabBin` b ON i.name = b.item_code
            WHERE b.warehouse = %s AND i.custom_asin IS NOT NULL AND b.actual_qty > 0 AND i.disabled = 0 AND i.is_stock_item = 1
        """, wh, as_dict=True)

        for row in amazon_items_in_wh:
            if row.asin in asin_fulfillable:
                continue  # Already handled if needed
            item_valuation_rate = frappe.get_value("Item", row.item_code, "valuation_rate") or 0
            item_dict = {
                "item_code": row.item_code,
                "warehouse": wh,
                "qty": 0,
            }
            if item_valuation_rate > 0:
                item_dict["valuation_rate"] = item_valuation_rate
            else:
                item_dict["valuation_rate"] = 0.01
            items_list.append(item_dict)

        if DEBUG: print(f"[DEBUG] Total items to reconcile: {len(items_list)}")
        if not items_list:
            if DEBUG: print("[DEBUG] No items to sync - exiting early")
        else:
            # Create and submit Stock Reconciliation
            if DEBUG: print("[DEBUG] Creating Stock Reconciliation...")
            try:  # ADDED: Wrap for error logging
                sr = frappe.get_doc({
                    "doctype": "Stock Reconciliation",
                    "company": company,
                    "posting_date": frappe.utils.today(),
                    "purpose": "Stock Reconciliation",
                    "expense_account": adjustment_account,  # For value adjustments
                    "items": items_list,
                })
                sr.insert(ignore_permissions=True)
                if DEBUG: print(f"[DEBUG] Inserted SR: {sr.name}")
                if DEBUG:
                    if DEBUG: print(f"[DEBUG] DEBUG mode: leaving FBA fulfillable SR {sr.name} as DRAFT (not submitted)")
                    frappe.db.commit()  # persist draft
                else:
                    sr.submit()
                    frappe.db.commit()
                    if DEBUG: print(f"[FBA_INV] Synced inventory via Stock Reconciliation {sr.name}")
            except Exception:
                frappe.log_error(frappe.get_traceback(), "Fulfillable Stock Reconciliation Error")
                raise

        # Process inbound inventory
        process_inbound_inventory(asin_inbound, settings)
    except Exception:
        frappe.log_error(frappe.get_traceback(), "FBA Inventory Process Error")
        raise

# ──────────────────────────────────────────
# Scheduler wrapper
# ──────────────────────────────────────────
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_sync_fba_inventory.run_daily_fba_inventory_sync")

NOTE:
You need to Manually Create Opening Stock Entries Before Running the Initial Sync
Go to Stock > Stock Transactions > Stock Entry > New
Set Stock Entry Type to "Material Receipt"
Set Target Warehouse to your relevant warehouses (Amazon FBA, Amazon FBA Inbound, Amazon FBA Prep Area
"""
@frappe.whitelist()
def run_daily_fba_inventory_sync():
    """Hourly scheduler entry: sync FBA inventory (only runs at 8 AM)."""
    
    pst_tz = ZoneInfo("America/Los_Angeles")
    now = datetime.now(pst_tz)
    if now.hour != 7 and DEBUG == False:
        return  # Only run at 7 AM in PST
    
    try:  # ADDED: Wrap scheduler call
        frappe.get_doc("Amazon SP API Settings", "q3opu7c5ac")  # Load to ensure active
        process_fba_inventory()
    except Exception:
        frappe.log_error(frappe.get_traceback(), "Daily FBA Inventory Sync Error")
        raise