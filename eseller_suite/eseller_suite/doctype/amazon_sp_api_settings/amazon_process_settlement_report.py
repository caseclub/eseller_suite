# amazon_process_settlement_report.py
# =========================================
#  Reconciles Amazon settlement reports
#  using ONLY the Reports API (no Finances).
# =========================================
from __future__ import annotations
import csv, io, json, base64, hashlib, requests
from datetime import datetime, timedelta, timezone, date
import time
import re
import frappe
from frappe.model.document import Document
from frappe.utils import flt, add_days, cint, getdate
from erpnext.accounts.party import get_party_account
from erpnext.accounts.utils import reconcile_against_document
from .amazon_repository import _sp_get, AmazonRepository
from requests.exceptions import HTTPError, RequestException
from urllib.parse import urlencode
from dateutil.parser import parse as dt_parse
import gzip
from io import BytesIO, StringIO
from collections import defaultdict
import pprint
from zoneinfo import ZoneInfo


# ──────────────────────────────────────────
# 0. — DB connection recovery
#
# This scheduled job performs long stretches of SP-API/network work. If the
# MariaDB connection is dropped while the job is outside the DB, the next
# Frappe query/commit can fail with pymysql InterfaceError(0, '').
#
# Reconnecting cannot restore a lost transaction, so these helpers are used
# only at transaction-safe boundaries (after read-only/network work, after an
# explicit rollback, or immediately before returning to Frappe).
# ──────────────────────────────────────────
def _is_db_connection_error(exc: Exception) -> bool:
    """Return True for a closed/lost MariaDB connection."""
    code = exc.args[0] if getattr(exc, "args", None) else None
    if code in (0, 2006, 2013, 2055):
        return True

    msg = str(exc).lower()
    return (
        "server has gone away" in msg
        or "lost connection" in msg
        or "broken pipe" in msg
        or "already closed" in msg
        or "interfaceerror" in msg
    )


def _reconnect_db() -> None:
    """Create a fresh DB connection and restore this job's session setting."""
    frappe.db.connect()
    frappe.db.sql("SET SESSION innodb_lock_wait_timeout = 300;")
    print("[SETT] MariaDB connection was dropped; reconnected")


def _ensure_db_connection() -> None:
    """Verify the DB socket and reconnect only when the connection is gone."""
    try:
        frappe.db.sql("SELECT 1")
    except Exception as exc:
        if not _is_db_connection_error(exc):
            raise
        _reconnect_db()


def _rollback_for_error() -> None:
    """Rollback if possible; reconnect if the old socket is already gone."""
    try:
        frappe.db.rollback()
    except Exception as exc:
        if not _is_db_connection_error(exc):
            raise
        _reconnect_db()


def _log_error_resilient(title: str, message: str) -> None:
    """Best-effort Error Log write even if the original DB socket was lost."""
    try:
        _ensure_db_connection()
        frappe.log_error(title=title[:140], message=message)
    except Exception as exc:
        print(f"[SETT] Could not write Error Log '{title}': {exc}")
        print(message)


def _sleep_with_db_check(seconds: float) -> None:
    """Sleep at a safe boundary, then verify the DB socket before continuing."""
    time.sleep(seconds)
    _ensure_db_connection()


# ──────────────────────────────────────────
# 1. — Helpers
# ──────────────────────────────────────────
def get_currency_accounts_map(settings):
    return {
        "USD": {
            "clearing": settings.custom_amazon_usd_clearing_account,
            "debtors": settings.custom_amazon_usd_debtors_account,
            "customer": settings.custom_amazon_fba_default_customer,
        },
        "CAD": {
            "clearing": settings.custom_amazon_cad_clearing_account,
            "debtors": settings.custom_amazon_cad_debtors_account,
            "customer": settings.custom_amazon_cad_fba_default_customer,
        },
        "MXN": {
            "clearing": settings.custom_amazon_mxn_clearing_account,
            "debtors": settings.custom_amazon_mxn_debtors_account,
            "customer": settings.custom_amazon_mxn_fba_default_customer,
        },
    }

# ──────────────────────────────────────────
# Updated: Get the name of the latest submitted non-return Sales Invoice (is_return=0) for the order
# ──────────────────────────────────────────
def get_sales_invoice(order_id: str) -> str | None:
    """
    Return the name of the latest submitted non-return Sales Invoice (is_return=0) linked to amazon_order_id,
    even if outstanding=0 (for refunds against closed invoices).
    """
    return frappe.db.get_value(
        "Sales Invoice",
        {"amazon_order_id": order_id, "docstatus": 1, "is_return": 0},
        "name",
        order_by="posting_date desc"  # Latest if multiple (rare)
    )

def get_open_sales_invoice(order_id: str) -> str | None:
    """
    Return the name of an open (submitted, outstanding > 0) Sales Invoice
    linked to the given amazon_order_id, or None if none exists.
    """
    return frappe.db.get_value(
        "Sales Invoice",
        {
            "amazon_order_id": order_id,
            "docstatus": 1,  # Submitted
            "is_return": 0,
            "outstanding_amount": [">", 0],  # Open/unpaid
        },
        "name",
        order_by="posting_date desc",
    )

def get_sales_order_context(order_id: str) -> frappe._dict:
    """Return the latest non-cancelled Amazon Sales Order customer/channel context."""
    # Cached per request: called once per AR line, per remark stamp and per CN.
    cache = frappe.local.amazon_so_ctx_cache = getattr(frappe.local, "amazon_so_ctx_cache", {})
    if order_id in cache:
        return cache[order_id]
    ctx = frappe.db.get_value(
        "Sales Order",
        {"amazon_order_id": order_id, "docstatus": ["!=", 2]},
        ["name", "customer", "fulfillment_channel"],
        as_dict=True,
        order_by="transaction_date desc, creation desc",
    ) or frappe._dict()
    cache[order_id] = ctx
    return ctx

def get_invoice_receivable_context(invoice_name: str | None) -> frappe._dict:
    """Return the accounting fields ERPNext requires when a JE references a Sales Invoice/Credit Note."""
    if not invoice_name:
        return frappe._dict()
    ctx = frappe.db.get_value(
        "Sales Invoice",
        invoice_name,
        [
            "name", "customer", "debit_to", "outstanding_amount",
            "conversion_rate", "currency", "is_return", "docstatus", "posting_date",
        ],
        as_dict=True,
    ) or frappe._dict()
    if ctx and ctx.debit_to:
        ctx.account_currency = _get_account_currency(ctx.debit_to)
    return ctx

def _ccy_mapping(settings, settlement_ccy: str) -> dict:
    """Return the configured Amazon currency map; account compatibility is validated before posting."""
    mapping = get_currency_accounts_map(settings)
    return mapping.get(settlement_ccy) or mapping["USD"]

def _get_account_currency(account: str | None, company: str | None = None) -> str:
    """Return Account.account_currency, treating a blank value as the company's base currency."""
    if not account:
        return ""
    account_ccy = frappe.get_cached_value("Account", account, "account_currency")
    if account_ccy:
        return str(account_ccy).upper()
    company = company or frappe.get_cached_value("Account", account, "company")
    if not company:
        return ""
    return (frappe.get_cached_value("Company", company, "default_currency") or "").upper()

def _account_matches_currency(account: str | None, settlement_ccy: str, company: str | None = None) -> bool:
    """
    True when a native settlement amount may be posted to this account.
    A blank Account.account_currency means company currency in ERPNext, so normalize
    that case before comparing against the settlement currency.
    """
    return _get_account_currency(account, company) == (settlement_ccy or "").upper()

def resolve_order_receivable_context(settings, order_id: str, settlement_ccy: str, invoice_name: str | None = None) -> frappe._dict:
    """
    Resolve AR party/account from the referenced invoice whenever possible.
    For an invoice that does not exist yet, use the imported Sales Order customer
    and ERPNext's party-account resolver. Only fall back to the legacy Amazon FBA
    customer/debtors mapping when neither document can identify the party.
    """
    invoice = get_invoice_receivable_context(invoice_name)
    order_ctx = get_sales_order_context(order_id)

    mapping = _ccy_mapping(settings, settlement_ccy)

    if invoice:
        return frappe._dict(
            account=invoice.debit_to,
            account_currency=(invoice.account_currency or "").upper(),
            customer=invoice.customer,
            currency=(invoice.currency or "").upper(),
            outstanding=abs(flt(invoice.outstanding_amount)),
            fulfillment_channel=(order_ctx.fulfillment_channel or "").upper(),
            invoice=invoice,
        )

    if order_ctx.customer:
        try:
            account = get_party_account("Customer", order_ctx.customer, settings.company)
        except Exception:
            account = None
            frappe.log_error(
                title=f"Amazon Settlement Party Account Resolution {order_id}"[:140],
                message=frappe.get_traceback(),
            )

        # get_party_account already falls back to the company default receivable, which is
        # in company currency. Reject it when it cannot carry the settlement currency and
        # use the currency-specific Amazon debtors account instead. The customer (which may
        # be a real FBM buyer) is never discarded just because the account had to change.
        if not _account_matches_currency(account, settlement_ccy, settings.company):
            account = mapping["debtors"]

        if not _account_matches_currency(account, settlement_ccy, settings.company):
            frappe.throw(
                f"Amazon settlement currency {settlement_ccy} requires a {settlement_ccy}-denominated "
                f"receivable account, but no compatible account is configured for Amazon order {order_id}."
            )

        return frappe._dict(
            account=account,
            account_currency=settlement_ccy,
            customer=order_ctx.customer,
            currency=settlement_ccy,
            outstanding=0.0,
            fulfillment_channel=(order_ctx.fulfillment_channel or "").upper(),
            invoice=frappe._dict(),
        )

    fallback_account = mapping["debtors"]
    if not _account_matches_currency(fallback_account, settlement_ccy, settings.company):
        frappe.throw(
            f"Amazon settlement currency {settlement_ccy} requires a {settlement_ccy}-denominated "
            "Amazon debtors account; check Amazon SP API Settings."
        )

    return frappe._dict(
        account=fallback_account,
        account_currency=settlement_ccy,
        customer=mapping["customer"],
        currency=settlement_ccy,
        outstanding=0.0,
        fulfillment_channel=(order_ctx.fulfillment_channel or "").upper(),
        invoice=frappe._dict(),
    )

def _append_ar_line(ar_lines: list, base: dict, amount: float, credit: bool):
    """
    Emit one AR line, flipping direction for negative amounts (e.g. a net-negative
    order_retrocharge) so the settlement total is never silently dropped from the JE.
    """
    if abs(flt(amount)) < 0.01:
        return
    line = dict(base)
    is_credit = credit if amount > 0 else not credit
    line["credit_in_account_currency" if is_credit else "debit_in_account_currency"] = abs(flt(amount))
    ar_lines.append(line)

def cancel_sales_invoice(inv_name: str) -> bool:
    """
    Idempotently cancel a Sales Invoice and its linked Sales Order(s).
    Returns True if the invoice ends up cancelled, False otherwise.
    Avoids noisy errors when already-cancelled or cancelled by a concurrent worker.
    """
    si = frappe.get_doc("Sales Invoice", inv_name)

    # Fast exits: nothing to do
    if si.docstatus == 2:
        #print(f"[SETT] Sales Invoice {inv_name} already cancelled; skipping")
        return True
    if si.docstatus != 1:
        #print(f"[SETT] Sales Invoice {inv_name} not submitted (docstatus={si.docstatus}); skipping")
        return False

    try:
        si.cancel()
        frappe.db.commit()
        #print(f"[SETT] Canceled Sales Invoice {inv_name} for refund")

        # Now cancel linked Sales Order(s)
        sales_orders = set(item.sales_order for item in si.items if item.sales_order)
        for so_name in sales_orders:
            so = frappe.get_doc("Sales Order", so_name)
            # Fast exits for SO
            if so.docstatus == 2:
                #print(f"[SETT] Sales Order {so_name} already cancelled; skipping")
                continue
            if so.docstatus != 1:
                #print(f"[SETT] Sales Order {so_name} not submitted (docstatus={so.docstatus}); skipping")
                continue

            try:
                so.cancel()
                frappe.db.commit()
                #print(f"[SETT] Canceled linked Sales Order {so_name} for refund")
            except Exception as so_e:
                frappe.db.rollback()
                current_so_status = frappe.db.get_value("Sales Order", so_name, "docstatus")
                if current_so_status == 2:
                    #print(f"[SETT] Sales Order {so_name} was cancelled concurrently; continuing")
                    continue

                # If links prevent cancellation, log as info and move on
                if isinstance(so_e, getattr(frappe, "LinkExistsError", Exception)):
                    frappe.get_logger("amazon_settlement").info(
                        f"Skip cancelling Sales Order {so_name}: linked records prevent cancel ({so_e})"
                    )
                    continue

                # Log other errors
                frappe.log_error(
                    f"Failed to cancel Sales Order {so_name}: {frappe.get_traceback()}",
                    "Amazon Settlement Refund SO Cancellation"
                )

        return True

    except Exception as e:
        # Possible race or legitimate block (payments/returns/etc).
        frappe.db.rollback()
        current_status = frappe.db.get_value("Sales Invoice", inv_name, "docstatus")

        if current_status == 2:
            # Someone else cancelled it between our read and cancel attempt.
            #print(f"[SETT] Sales Invoice {inv_name} was cancelled concurrently; continuing")
            return True

        # If links prevent cancellation, log as info (not an error) and move on.
        if isinstance(e, getattr(frappe, "LinkExistsError", Exception)):
            frappe.get_logger("amazon_settlement").info(
                f"Skip cancelling {inv_name}: linked records prevent cancel ({e})"
            )
            return False

        # Anything else is a real failure worth logging
        frappe.log_error(
            f"Failed to cancel {inv_name}: {frappe.get_traceback()}",
            "Amazon Settlement Refund Cancellation"
        )
        return False

        
def get_clearing_account(settings, ccy: str) -> str:
    map = get_currency_accounts_map(settings)
    return map.get(ccy, map.get("USD", {})).get("clearing")

def get_debtors_account(settings, ccy: str) -> str:
    map = get_currency_accounts_map(settings)
    return map.get(ccy, map.get("USD", {})).get("debtors")

def decrypt_aes_cbc_pkcs7(b64_key: str, b64_iv: str, blob: bytes) -> bytes:
    """Amazon encrypts report docs with AES-CBC + PKCS7."""
    from Crypto.Cipher import AES            # pycryptodome already in frappe env
    key = base64.b64decode(b64_key)
    iv  = base64.b64decode(b64_iv)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    decrypted = cipher.decrypt(blob)
    pad_len = decrypted[-1]
    return decrypted[:-pad_len]

# Get_exchange_rate import
try:
    # v14 / v15
    from erpnext.setup.doctype.currency_exchange.currency_exchange import get_exchange_rate
except ImportError:
    try:                                    # v13
        from erpnext.accounts.utils import get_exchange_rate
    except ImportError:                     # v12
        from erpnext.setup.utils import get_exchange_rate


def fx_rate(from_ccy: str, posting_date: str, to_ccy: str = "USD", max_retries=3, fallback_days=7) -> float:
    """Return ERPNext exchange rate with retries and fallbacks; 1 when currencies match."""
    from_ccy = (from_ccy or "").upper()
    to_ccy   = (to_ccy   or "").upper()

    if from_ccy == to_ccy:
        return 1.0

    # Check cache first
    cache_key = f"exchange_rate_{from_ccy}_{to_ccy}_{posting_date}"
    cached_rate = frappe.cache().get_value(cache_key)
    if cached_rate:
        return float(cached_rate)

    # Try to get rate with retries
    rate = None
    for attempt in range(max_retries):
        try:
            rate = get_exchange_rate(from_ccy, to_ccy, posting_date)
            if rate:
                break
        except (HTTPError, RequestException) as e:
            frappe.log_error(f"Exchange rate API failed (attempt {attempt+1}): {str(e)}", "Amazon Settlement FX Rate Fetch")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
            else:
                # On final failure, start fallback
                break

    # Fallback: Try previous days recursively
    if not rate and fallback_days > 0:
        prev_date = add_days(posting_date, -1)
        rate = fx_rate(from_ccy, prev_date, to_ccy, max_retries=1, fallback_days=fallback_days-1)  # Reduced retries for fallback

    # Ultimate fallback: Use latest DB rate or throw
    if not rate:
        # Query the most recent manual Currency Exchange record
        latest_rate = frappe.db.get_value(
            "Currency Exchange",
            {"from_currency": from_ccy, "to_currency": to_ccy},
            "exchange_rate",
            order_by="date desc"
        )
        if latest_rate:
            rate = float(latest_rate)
            frappe.log_error(
                f"Using latest manual rate ({rate}) as fallback for {from_ccy} → {to_ccy} on {posting_date}",
                "Amazon Settlement FX Fallback"
            )
        else:
            frappe.throw(
                f"Exchange rate {from_ccy} → {to_ccy} for {posting_date} (and fallbacks) is missing. "
                "Create it under Accounting ▸ Currency Exchange."
            )

    # Cache the rate for 24 hours
    frappe.cache().set_value(cache_key, rate, expires_in_sec=86400)

    return float(rate)

def list_latest_settlement_reports(settings, limit: int = 5, days_back: int = 90) -> list[dict]:
    """Fetch settlement reports created in the last `days_back` days, sort by dataEndTime descending, return top `limit`."""
    all_reports = []
    next_token = None
    
    # Use after_date if set, else fallback to days_back
    after_date = getattr(settings, 'after_date', None)
    current_dt = datetime.now(timezone.utc)
    
    if after_date:
        try:
            # Convert date object to aware datetime at midnight UTC
            after_dt = datetime.combine(after_date, datetime.min.time(), tzinfo=timezone.utc)
            # Clamp to no earlier than 90 days back to avoid API 400 error
            min_after_dt = current_dt - timedelta(days=90)
            after_dt = max(after_dt, min_after_dt)
            if after_dt > current_dt:
                return []  # No reports if after_date is in the future
            created_since = after_dt.isoformat()
        except (AttributeError, TypeError, ValueError) as e:
            # Fallback if after_date is not a valid date object
            frappe.log_error(f"Invalid after_date '{after_date}': {str(e)}", "Amazon Settlement Report Fetch")
            created_since = (current_dt - timedelta(days=days_back)).isoformat()
    else:
        created_since = (current_dt - timedelta(days=days_back)).isoformat()
    
    while True:
        qs_dict = {
            "reportTypes": "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2",
            "processingStatuses": "DONE",
            "pageSize": "100",  # Max for efficient pagination
            "createdSince": created_since,
        }
        if next_token:
            qs_dict["nextToken"] = next_token
        qs = urlencode(qs_dict)
        
        resp = _sp_get("/reports/2021-06-30/reports", qs, settings)
        reports = resp.get("reports", [])
        all_reports.extend(reports)
        
        next_token = resp.get("nextToken")
        if not next_token:
            break
        
        time.sleep(2)  # Short delay to avoid rate limits during pagination
    
    # Pagination is read-only/network work; reconnect here if MariaDB went idle.
    _ensure_db_connection()

    # Sort by dataEndTime (primary) or createdTime (fallback), newest first
    all_reports.sort(key=_report_sort_key, reverse=True)
    
    if not after_date:
        return all_reports[:limit]
    
    # Filter based on internal settlement-end-date
    filtered_reports = []
    for report in all_reports:
        rows = fetch_settlement_rows(settings, report)
        if not rows:
            continue
        end_str = rows[0].get("settlement-end-date", "").strip()
        if not end_str:
            continue
        try:
            # Parse and take only the date part for comparison
            end_dt = dt_parse(end_str)
            end_dt_date = datetime.combine(end_dt.date(), datetime.min.time(), tzinfo=timezone.utc)
            if end_dt_date >= after_dt:
                filtered_reports.append(report)
        except Exception as e:
            frappe.log_error(f"Failed to parse settlement-end-date '{end_str}' for report {report.get('reportId')}: {str(e)}", "Amazon Settlement Report Filter")
            continue
        if len(filtered_reports) >= limit:
            break

    return filtered_reports

def _report_sort_key(r: dict) -> datetime:
    """Return a comparable datetime for sorting newest-first."""
    for k in ("reportDate", "createdTime", "dataEndTime"):
        if k in r:
            return dt_parse(r[k])
    # If none of the expected keys exist, push it to the end
    return datetime.min.replace(tzinfo=timezone.utc)

def fetch_settlement_rows(settings, report: dict) -> list[dict]:
    """
    Download, decrypt / unzip (if needed) and parse the CSV.
    Always returns a list-of-dict with **lower-case keys**.
    Guarantees an “amount” (float) column even when Amazon exposes
    it as “total-amount”.
    """
    # 1) Get document metadata
    doc_id = report.get("reportDocumentId")
    meta   = _sp_get(f"/reports/2021-06-30/documents/{doc_id}", {}, settings)
    time.sleep(1.5)  # NEW: Match troubleshooting's delay to avoid rate limits
    url    = (meta.get("url")
              or meta.get("reportDocument", {}).get("url"))
    if not url:
        frappe.log_error("Document meta missing url", str(meta))
        return []

    # 2) Download the payload
    raw: bytes = requests.get(url, timeout=120).content

    # The report download can spend up to 120s with no DB traffic.
    _ensure_db_connection()

    # 3) Decrypt (AES-CBC/PKCS7) if Amazon gives us keys (kept as-is, but troubleshooting doesn't have this—remove if your reports aren't encrypted)
    if "encryptionDetails" in meta:
        ed  = meta["encryptionDetails"]
        raw = decrypt_aes_cbc_pkcs7(ed["key"], ed["initializationVector"], raw)

    # 4) Decompress (GZIP) when required (kept as-is, but troubleshooting doesn't have this—remove if not needed)
    if meta.get("compressionAlgorithm", "").upper() == "GZIP":
        raw = gzip.GzipFile(fileobj=BytesIO(raw)).read()

    # 5) CSV → rows (simplified to match troubleshooting's pd.read_csv style, but using csv for ERPNext compat)
    text = raw.decode("utf-8", errors="replace")
    first_line = text.splitlines()[0]
    dialect_delim = "\t" if "\t" in first_line else ","  # Matches troubleshooting's sep="\t" assumption

    rdr = csv.DictReader(StringIO(text), delimiter=dialect_delim)
    rows: list[dict] = []

    for r in rdr:
        # Lower-case *all* keys once
        r = {k.lower().strip(): v for k, v in r.items()}
        # Amazon sometimes exposes "amount type" (space) instead of "amount-type".
        if "amount type" in r and "amount-type" not in r:
            r["amount-type"] = r.pop("amount type").strip()
        if "amount description" in r and "amount-description" not in r:
            r["amount-description"] = r.pop("amount description").strip()

        # Normalise numeric field
        raw_amt = (r.get("amount") or r.get("total-amount") or "").strip()
        try:
            r["amount"] = float(raw_amt) if raw_amt else 0.0
        except ValueError:
            r["amount"] = 0.0

        rows.append(r)

    return rows

# ────────────────────────────────────────────────────────────────────
#  Journal-Entry builder  —  single net-deposit + optional fee lines
# ────────────────────────────────────────────────────────────────────
def get_open_credit_notes_for_order(order_id: str) -> list[str]:
    return frappe.db.get_all(
        "Sales Invoice",
        filters={
            "amazon_order_id": order_id,
            "is_return": 1,
            "docstatus": 1,
            "outstanding_amount": ["<", -0.01],
        },
        fields=["name"],
        order_by="posting_date asc, name asc",
        pluck="name",
    )

def stamp_marketplace_fields(
    dr: dict,
    cr: dict,
    marketplace_name: str,
    merchant_order_id: str,
    fulfillment_channel: str = "",
    is_refund: bool = False,
):
    """Stamp descriptive fields without assuming amazon.com means FBA."""
    suffix = "Refund" if is_refund else "Order"
    channel = (fulfillment_channel or "").upper()

    if marketplace_name == "non-amazon us":
        cleaned_id = re.sub(r'\D', '', merchant_order_id)
        dr["custom_merchant_order_id"] = cr["custom_merchant_order_id"] = cleaned_id
        dr["user_remark"] = cr["user_remark"] = f"Multi-Channel Fulfillment (MCF) {suffix}"
    elif marketplace_name == "amazon.com":
        dr["custom_merchant_order_id"] = cr["custom_merchant_order_id"] = ""
        if channel == "MFN":
            label = f"Fulfillment by Merchant (FBM/MFN) {suffix}"
        elif channel == "AFN":
            label = f"Fulfillment by Amazon (FBA) {suffix}"
        else:
            label = f"Amazon {suffix}"
        dr["user_remark"] = cr["user_remark"] = label

# ──────────────────────────────────────────
# Updated: Idempotency check now includes legacy "-adj" and docstatus=2 (cancelled) for stricter duplicate prevention
# ──────────────────────────────────────────
def is_already_referenced_by_report(rpt_id: str, reference_name: str) -> bool:
    if not reference_name:
        return False
    cheque_pattern = f"{rpt_id}%"
    order_id = frappe.db.get_value("Sales Invoice", reference_name, "amazon_order_id")  # Get from SI/CN
    if not order_id:
        return False
    exists = frappe.db.sql("""
        SELECT 1
        FROM `tabJournal Entry` je
        JOIN `tabJournal Entry Account` jea ON jea.parent = je.name
        WHERE je.docstatus = 1
          AND je.cheque_no LIKE %s
          AND jea.reference_type = 'Sales Invoice'
          AND jea.reference_name = %s
          AND jea.amazon_order_id = %s
        LIMIT 1
    """, (cheque_pattern, reference_name, order_id))
    return bool(exists)

# ──────────────────────────────────────────
# Helper: Get all submitted Credit Notes for an order (any outstanding, sorted asc)
# ──────────────────────────────────────────
def get_all_submitted_credit_notes_for_order(order_id: str) -> list[str]:
    return frappe.db.get_all(
        "Sales Invoice",
        filters={
            "amazon_order_id": order_id,
            "is_return": 1,
            "docstatus": 1,
        },
        fields=["name"],
        order_by="posting_date asc, name asc",
        pluck="name",
    )

# ──────────────────────────────────────────
# Helper: Look up account or create a new one
# ──────────────────────────────────────────
def get_account(settings, name: str) -> str:
    account_name = frappe.db.get_value("Account", {"account_name": f"Amazon {name}"})
    if not account_name:
        new_account = frappe.new_doc("Account")
        new_account.account_name = f"Amazon {name}"
        new_account.company = settings.company
        new_account.parent_account = settings.market_place_account_group
        new_account.insert(ignore_permissions=True)
        account_name = new_account.name
    return account_name

# ──────────────────────────────────────────
# Helper: Create and submit a Credit Note for partial/full refund (unchanged, but now called with non-return SI)
# ──────────────────────────────────────────
def create_credit_note_for_refund(settings, si_name: str, refund_amount: float, post_dt: str, order_id: str, marketplace_name: str, merchant_order_id: str, order_rows: list[dict], report_id: str) -> str | None:
    """
    Create a linked Credit Note (CN) for an Amazon refund from settlement data.
    
    Accounting logic:
    - Aggregates per-SKU principal refunds as negative-qty items (rate positive, qty negative).
    - Aggregates ALL non-principal refund components (e.g., shipping, taxes, promotions, commissions) 
      including any order-level (no-SKU) rows as 'Actual' taxes/charges lines with signs preserved 
      from the report (typically negative for refunds).
    - Maps each unique amount-description to an account via get_account().
    - Relies strictly on settlement report rows for totals; no rounding or adjustments applied.
      The CN grand_total should naturally match -refund_amount based on the rows provided.

    Idempotency: Skips if a matching CN (same return_against, order_id, grand_total) exists.
    No stock impact: Purely financial (update_stock=0).
    """
    try:
        si = frappe.get_doc("Sales Invoice", si_name)
        if si.is_return:
            frappe.throw("Cannot create Credit Note from another Credit Note.")
        
        # Filter to refund rows only (in case not pre-filtered)
        refund_rows = [r for r in order_rows if r.get('transaction-type', '').lower() == 'refund']
        if not refund_rows:
            print(f"[SETT] No refund rows for {order_id}; skipping CN creation")
            return None
        
        # Compute the actual refund magnitude from refund rows (positive value)
        computed_refund_amount = -sum(flt(r['amount']) for r in refund_rows)
        
        # Idempotency check: Skip if matching CN exists for this report_id
        if frappe.db.exists("Sales Invoice", {
            "return_against": si_name,
            "amazon_order_id": order_id,
            "company": si.company,
            "docstatus": 1,
            "is_return": 1,
            "custom_amazon_settlement_report_id": report_id  # NEW: Settlement-specific check
        }):
            existing_cn_name = frappe.db.get_value("Sales Invoice", {  # Get name for return
                "return_against": si_name,
                "amazon_order_id": order_id,
                "company": si.company,
                "docstatus": 1,
                "is_return": 1,
                "custom_amazon_settlement_report_id": report_id
            }, "name")
            print(f"[SETT] Existing CN {existing_cn_name} found for refund on {si_name} (order {order_id}, report {report_id}); skipping creation")
            return existing_cn_name
        
        # Group refund rows by SKU (including empty SKU for order-level if any)
        groups_by_sku = defaultdict(list)
        for r in refund_rows:
            sku = r.get('sku', '').strip()
            groups_by_sku[sku].append(r)  # Includes '' as a key for no-SKU rows
        
        if not groups_by_sku:
            print(f"[SETT] No grouped rows for {order_id}; skipping CN creation")
            return None
        
        # Create linked Credit Note
        cn = frappe.new_doc("Sales Invoice")
        cn.customer = si.customer
        cn.company = si.company
        cn.posting_date = post_dt
        cn.due_date = post_dt
        cn.currency = si.currency
        cn.conversion_rate = si.conversion_rate
        cn.is_return = 1
        cn.return_against = si_name  # Link to original SI
        cn.update_stock = 0
        
        # Collect all non-principal charges (doc-level aggregate, including no-SKU)
        charges = defaultdict(float)
        remark_details = []  # For per-SKU fee breakdown in user_remark
        
        # Per-SKU: Add items and collect per-SKU non-principal charges
        items_added = 0
        for sku, group_rows in groups_by_sku.items():
            if not sku:  # Skip empty SKU here; handle order-level separately below
                continue
            
            # Compute principal amount for this SKU (positive)
            principal_rows = [r for r in group_rows if 
                              ('principal' in r.get('amount-description', '').lower() or 'principal' in r.get('amount-type', '').lower())]
            principal_amount = -sum(flt(r['amount']) for r in principal_rows)  # Flip to positive
            if principal_amount <= 0:
                continue  # Skip zero/negative principal
            
            # Find matching item in SI by item_name == sku
            matching_item = next((item for item in si.items if item.item_name.strip() == sku), None)
            if not matching_item:
                frappe.log_error(f"No matching item in SI {si_name} for SKU {sku} (order {order_id}); skipping", "Amazon Settlement CN Item Match")
                continue
            
            # Compute refunded qty and rate, respecting UOM integer requirement
            original_rate = flt(matching_item.rate)
            positive_qty = principal_amount / original_rate if original_rate != 0 else 1.0  # Fallback to 1 if rate=0
            whole_number_required = frappe.db.get_value("UOM", matching_item.uom, "must_be_whole_number") or 0
            
            if whole_number_required:
                rounded_qty = round(positive_qty)  # Round to nearest integer
                if rounded_qty == 0 and principal_amount > 0:
                    rounded_qty = 1  # Handle tiny refunds
                adjusted_rate = principal_amount / rounded_qty if rounded_qty != 0 else principal_amount  # Fallback
                refunded_qty = -rounded_qty
                rate_to_use = adjusted_rate
            else:
                refunded_qty = -positive_qty
                rate_to_use = original_rate
            
            # Add item to CN
            cn.append("items", {
                "item_code": matching_item.item_code,
                "item_name": matching_item.item_name,
                "description": matching_item.description,
                "qty": refunded_qty,
                "uom": matching_item.uom,
                "rate": rate_to_use,  # Adjusted if needed
                "income_account": matching_item.income_account,
                "cost_center": matching_item.cost_center,
                "warehouse": matching_item.warehouse,
            })
            items_added += 1
            
            # Collect per-SKU non-principal charges
            sku_charges = defaultdict(float)
            for r in group_rows:
                if not ('principal' in r.get('amount-description', '').lower() or 'principal' in r.get('amount-type', '').lower()):
                    desc = r.get('amount-description', '').strip().upper()
                    amt = flt(r['amount'])
                    if abs(amt) < 0.01:  # Skip tiny noise
                        continue
                    sku_charges[desc] += amt
                    charges[desc] += amt  # Aggregate doc-level
            
            # Build per-SKU remark detail
            if sku_charges:
                sku_remark = f"Refund for SKU {sku}: " + ", ".join(f"{desc} {amt:.2f}" for desc, amt in sku_charges.items() if abs(amt) >= 0.01)
                remark_details.append(sku_remark)
        
        if items_added == 0:
            print(f"[SETT] No items added to CN for {order_id}; skipping creation")
            return None
        
        # Handle order-level (no-SKU) non-principal charges if any
        order_level_rows = groups_by_sku.get('', [])  # '' key for no-SKU
        for r in order_level_rows:
            if 'principal' in r.get('amount-description', '').lower() or 'principal' in r.get('amount-type', '').lower():
                continue  # Skip any misplaced principals (shouldn't happen)
            desc = r.get('amount-description', '').strip().upper()
            amt = flt(r['amount'])
            if abs(amt) < 0.01:  # Skip tiny noise
                continue
            charges[desc] += amt  # Add to doc-level aggregate
        
        # Add aggregated charges as taxes/charges (use SI cost_center if available)
        default_cost_center = si.items[0].cost_center if si.items else ""
        for desc, amt in charges.items():
            if abs(amt) < 0.01:
                continue
            account = get_account(settings, desc)
            cn.append("taxes", {
                "charge_type": "Actual",
                "account_head": account,
                "description": desc.title(),
                "included_in_print_rate": 0,
                "rate": 0,
                "tax_amount": amt,  # Preserve sign from report (negative)
                "cost_center": default_cost_center,
            })
        
        # Compute totals (no diff check or rounding; rely on report data)
        cn.calculate_taxes_and_totals()
               
        # Stamp fields. marketplace-name identifies the storefront, not who fulfilled it.
        cn.remarks = ""  # Initialize to empty string for safe appending
        fulfillment_channel = (get_sales_order_context(order_id).fulfillment_channel or "").upper()
        if marketplace_name == "non-amazon us":
            cn.custom_merchant_order_id = re.sub(r'\D', '', merchant_order_id)
            cn.remarks = "Multi-Channel Fulfillment (MCF) Order Refund"
        elif marketplace_name == "amazon.com":
            cn.custom_merchant_order_id = ""
            if fulfillment_channel == "MFN":
                cn.remarks = "Fulfillment by Merchant (FBM/MFN) Order Refund"
            elif fulfillment_channel == "AFN":
                cn.remarks = "Fulfillment by Amazon (FBA) Order Refund"
            else:
                cn.remarks = "Amazon Order Refund"
        cn.amazon_order_id = order_id
        cn.custom_amazon_settlement_report_id = report_id
        
        # Append per-SKU remark details
        if remark_details:
            cn.remarks += "\n" + "\n".join(remark_details)
        
        cn.insert(ignore_permissions=True)
        cn.submit()
        frappe.db.commit()
        print(f"[SETT] Created linked Credit Note {cn.name} for refund on {si_name} (order {order_id})")
        return cn.name
    except Exception as e:
        frappe.db.rollback()
        frappe.log_error(f"Failed to create CN for SI {si_name} (order {order_id}): {frappe.get_traceback()}", "Amazon Settlement CN Creation")
        return None

# ────────────────────────────────────────────────────────────────────
#  Journal-Entry builder  —  single net-deposit + optional fee lines
# ────────────────────────────────────────────────────────────────────
def build_je(
    repo: "AmazonRepository",
    report: dict,
    rows: list[dict],
    first_pass: bool,
) -> "frappe.model.document.Document | None":
    rpt_id = report["reportId"]
    post_dt = ((report.get("reportDate") or report.get("createdTime") or report.get("dataEndTime") or frappe.utils.now()))[:10] # keep YYYY-MM-DD
    # ──────────────────────────────────────────────
    # Diagnostics: Print build start and row details
    # ──────────────────────────────────────────────
    print(f"\n▶▶ BUILD_JE {rpt_id}")
    # ── DEBUG: show first 20 rows as–parsed ────────────────────────────
    #print("\n First 20 rows (post-normalisation):")
    #for i, r in enumerate(rows[:20], 1):
    # short = {k: r[k] for k in
    # ("transaction-type", "order-id", "amount-description",
    # "amount-type", "amount", "currency") if k in r}
    # print(f"{i:>2}.", short)
    #print("───────────────────────────────────────────────────────────────\n")
    print(f" rows: {len(rows)} first_pass: {first_pass}")
    # ──────────────────────────────────────────────
    # Initialize journal entry lines list
    # ──────────────────────────────────────────────
    je_lines: list[dict] = []
    # ──────────────────────────────────────────────
    # Common extraction: Identify net transfer row and calculate totals
    # ──────────────────────────────────────────────
    native_total = 0.0
    settlement_ccy = "USD"
    transfer_row = None
   
    # Extract settlement period dates from the first row (if available)
    start_date = ""
    end_date = ""
    if rows:
        start_str = rows[0].get("settlement-start-date", "").strip()
        end_str = rows[0].get("settlement-end-date", "").strip()
        # Take only the date part (DD.MM.YYYY), ignore time/UTC if present
        start_date = start_str.split(" ")[0] if " " in start_str else start_str
        end_date = end_str.split(" ")[0] if " " in end_str else end_str
    period_remark = f"Settlement Period: {start_date} - {end_date}" if start_date and end_date else "Settlement Period: Unknown"
   
    # Extract and parse deposit-date (for clearance_date)
    deposit_str = rows[0].get("deposit-date", "").strip() if rows else ""
    deposit_date = post_dt + " 00:00:00" # Fallback to posting date at midnight
    if deposit_str:
        parse_str = deposit_str
        if parse_str.endswith(" UTC"):
            parse_str = parse_str[:-4].strip() # Remove " UTC"
        try:
            naive_dt = dt_parse(parse_str)
            utc_dt = naive_dt.replace(tzinfo=timezone.utc)
            pst_tz = ZoneInfo("America/Los_Angeles")
            pst_dt = utc_dt.astimezone(pst_tz)
            deposit_date = pst_dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            frappe.log_error(f"Failed to parse/convert deposit date '{deposit_str}': {str(e)}", "Amazon Settlement Deposit Date Parsing")
    # ──────────────────────────────────────────────
    # Loop through rows to find the net transfer row (Amazon's net line)
    # ──────────────────────────────────────────────
    for r in rows:
        # Amazon’s net line always has an amount (positive for deposit, negative for withdrawal) and *no* order-id. Sometimes transaction-type == "Transfer"; csv may only show total-amount.
        t_type = (r.get("transaction-type") or "").strip().lower()
        desc = (r.get("amount-description") or "").strip().lower()
        looks_like_net = (
            (t_type == "transfer") or
            (desc in ("amazon proceeds", "transfer")) or
            (t_type == "" and desc == "")
        ) and not (r.get("order-id") or "").strip()
        if looks_like_net and abs(r["amount"]) > 0.0001:
            transfer_row = r
            settlement_ccy = (r["currency"] or "USD").upper()
            native_total = r["amount"]
            break
   
    # ──────────────────────────────────────────────
    # Early return if no valid net total amount found
    # ──────────────────────────────────────────────
    if abs(native_total) < 0.0001:
        print(f"[SETT] nothing to post for {rpt_id} (native_total = 0)")
        return None
    # ──────────────────────────────────────────────
    # CHANGE: Replace single net order_totals with separate sales_totals and refund_totals (positive magnitudes).
    # - sales_totals: sum positive "order" + "order_retrocharge" (treat retrocharge as sales adjustment, per original ORDER_NET_TYPES).
    # - refund_totals: -sum negative "refund" (positive magnitude for clarity in AR debit lines and CN creation).
    # - Still group rows for metadata/CN creation, but no netting.
    # - Reasoning: Allows separate AR lines for sales (credit) and refunds (debit), so SIs get paid even if refunds > sales in same report.
    # - Edge cases: Zero totals skipped; multiple SIs per order_id (rare, uses latest via get_sales_invoice); mixed currencies (preserved via per-line exchange_rate).
    # ──────────────────────────────────────────────
    SALES_TYPES = {"order", "order_retrocharge"}
    REFUND_TYPES = {"refund"}
    # Group order-level rows by order-id
    order_groups = defaultdict(list)
    for r in rows:
        t_type_lower = (r.get("transaction-type") or "").strip().lower()
        if t_type_lower in SALES_TYPES.union(REFUND_TYPES):
            order_id = (r.get("order-id") or "").strip()
            if order_id: # Only process rows with valid order IDs
                order_groups[order_id].append(r)
    # Calculate separate totals per order
    sales_totals = {}
    refund_totals = {}
    total_sales_native = 0.0
    total_refund_native = 0.0  # Positive magnitude
    for order_id, order_rows in order_groups.items():
        sales_total = sum(float(r["amount"]) for r in order_rows if (r.get("transaction-type") or "").strip().lower() in SALES_TYPES)
        refund_total = -sum(float(r["amount"]) for r in order_rows if (r.get("transaction-type") or "").strip().lower() in REFUND_TYPES)
        if abs(sales_total) >= 0.01:
            sales_totals[order_id] = sales_total
            total_sales_native += sales_total
        if abs(refund_total) >= 0.01:
            refund_totals[order_id] = refund_total
            total_refund_native += refund_total
    # CHANGE: Recompute order_net_native as sales - refunds for fee calc (preserves original fees_usd logic without change).
    order_net_native = total_sales_native - total_refund_native
    print(f"Sales total: {total_sales_native}")
    print(f"Refund total (positive): {total_refund_native}")
    print(f"Net (for fees): {order_net_native}")
   
    # ────────────────────────────────────────────────
    # Define reimbursement types whitelist
    # ─────────────────────────────────────────
    REIMBURSEMENT_WHITE_LIST = {
        # Amazon claw-back reversals & refunds
        "REVERSAL_REIMBURSEMENT", # generic reversal of a prior reimbursement
        "FREE_REPLACEMENT_REFUND_ITEMS", # they refunded you for free replacement items
        "WAREHOUSE_DAMAGE", # FBA reimbursement for damaged inventory
        "WAREHOUSE_LOST", # FBA reimbursement for lost inventory
        "COMPENSATED_CLAWBACK", # reversal of a clawback/liability
        "MISSING_FROM_INBOUND_CLAWBACK", # reversal of an inbound-shortage charge
        # Commission & shipping credits back on returns
        "REFUNDCOMMISSION", # Amazon gives back part of its commission
        "SHIPPINGCHARGEBACK", # Amazon refunds you shipping costs
        "MISSING_FROM_INBOUND",
    }
   
    # ──────────────────────────────────────────────
    # Calculate reimbursements: Sum positive reimbursement amounts
    # ──────────────────────────────────────────────
    reimb_native = sum(
        float(r["amount"])
        for r in rows
        if (desc := (r.get("amount-description") or "").strip().upper()) in REIMBURSEMENT_WHITE_LIST
        or "REIMBURSEMENT" in desc
        or "REIMBURSEMENT" in (r.get("amount-type") or "").strip().upper()
        and float(r["amount"]) > 0
    )
    # ───────────────────────────────────────────────
    # Define fee account mapping for special fees
    # ───────────────────────────────────────────────
    FEE_ACCOUNT_MAP = {
        "STORAGE FEE": repo.amz_setting.custom_amazon_storage_fee_account,
        "STORAGERENEWALBILLING": repo.amz_setting.custom_amazon_storage_renewal_billing_account,
        "FBA INBOUND PLACEMENT SERVICE FEE": repo.amz_setting.custom_amazon_inbound_placement_service_fee_account,
        "INBOUND TRANSPORTATION FEE": repo.amz_setting.custom_amazon_inbound_transportation_fee_account,
        "REMOVALCOMPLETE": repo.amz_setting.custom_amazon_removal_service_fee_account,
        "COMPENSATED_CLAWBACK": repo.amz_setting.custom_amazon_compensated_clawback_account,
        "DISPOSALCOMPLETE": repo.amz_setting.custom_amazon_disposal_service_fee_account,
        "LIQUIDATIONSBROKERAGEFEE": repo.amz_setting.custom_amazon_liquidation_brokerage_fee_account
    }
       
    # ──────────────────────────────────────────────
    # Calculate special fees: Sum negative non-order fees into buckets
    # ──────────────────────────────────────────────
    special_fee_native = defaultdict(float)
    for r in rows:
        amt = float(r["amount"])
        if amt >= 0: # fees are negative
            continue
        if (r.get("order-id") or "").strip(): # skip order-level rows
            continue
        desc = (r.get("amount-description") or "").strip().upper()
        if desc in FEE_ACCOUNT_MAP:
            special_fee_native[desc] += abs(amt) # store as positive

    if first_pass:
        rate = fx_rate(settlement_ccy, post_dt)
        # convert each bucket to USD
        special_fee_usd = {d: round(v * rate, 2) for d, v in special_fee_native.items()}
        special_fee_total_usd = sum(special_fee_usd.values())
   
        # ──────────────────────────────────────────────
        # Calculate USD equivalents for totals and fees
        # ──────────────────────────────────────────────
        usd_total = round(native_total * rate, 2)
        order_net_usd = round(order_net_native * rate, 2) # grand-total AR
        reimb_usd = round(reimb_native * rate, 2)
        fees_usd = round(
            (order_net_usd + reimb_usd) - (usd_total + special_fee_total_usd), 2
        )
        # ──────────────────────────────────────────────
        # Reset journal entry lines for building
        # ───────────────────────────────────────────────
        je_lines: list[dict] = []
        # ──────────────────────────────────────────────
        # FIRST-PASS Branch: Build initial journal entry with all lines
        # ──────────────────────────────────────────────
        non_ar_lines = []
        ar_lines = []
        # ──────────────────────────────────────────────
        # 1) Add clearing account line: Debit or Credit based on total
        # ──────────────────────────────────────────────
        clearing_line = {
            "account": get_clearing_account(repo.amz_setting, settlement_ccy),
            "exchange_rate": rate,
        }
        if native_total > 0:
            clearing_line.update({
                "debit_in_account_currency": native_total,
            })
        else:
            clearing_line.update({
                "credit_in_account_currency": -native_total,  # positive
            })
        non_ar_lines.append(clearing_line)
        # ──────────────────────────────────────────────
        # CHANGE: Process AR lines in two separate passes (sales credits, then refund debits).
        # - Sales: Add credit AR line if sales_total > 0, reference open SI if exists (else unreferenced → advance via _flag_unallocated_as_advance).
        # - Refunds: If refund_total > 0, create CN (passing only refund rows for this order_id), add debit AR line referencing CN.
        # - Reasoning: Ensures separate lines without netting; SIs get paid via credit allocation; CNs handle refunds financially (no stock).
        # - Edge cases: Partial payments (apply to open outstanding only); missing SI (treat as advance); refunds without SI (CN skipped, debit as advance); multiple CNs (idempotency skips duplicates).
        # ──────────────────────────────────────────────
        # Sales pass
        for order_id, sales_total_native in sales_totals.items():
            marketplace_name = ""
            merchant_order_id = ""
            order_rows = order_groups.get(order_id, [])
            if order_rows:
                first_row = order_rows[0]
                marketplace_name = (first_row.get("marketplace-name") or "").strip().lower()
                merchant_order_id = (first_row.get("merchant-order-id") or "").strip()

            si_name = get_sales_invoice(order_id)
            open_si_name = get_open_sales_invoice(order_id)
            ctx = resolve_order_receivable_context(
                repo.amz_setting, order_id, settlement_ccy, open_si_name or si_name
            )
            base_line = {
                "account": ctx.account,
                "exchange_rate": rate,
                "party_type": "Customer",
                "party": ctx.customer,
                "amazon_order_id": order_id,
            }
            stamp_marketplace_fields(
                base_line, {}, marketplace_name, merchant_order_id,
                ctx.fulfillment_channel, is_refund=False,
            )
            remaining = sales_total_native

            # Only reference an invoice denominated in the settlement currency; a native
            # amount posted against a differently-denominated AR account is not meaningful.
            ccy_ok = (
                (not ctx.currency or ctx.currency == settlement_ccy)
                and (not ctx.account_currency or ctx.account_currency == settlement_ccy)
            )
            if open_si_name and not ccy_ok:
                frappe.log_error(
                    title=f"Amazon Settlement Currency Mismatch {order_id}"[:140],
                    message=(
                        f"Settlement {rpt_id} currency {settlement_ccy} cannot be applied to Sales Invoice "
                        f"{open_si_name}: invoice currency={ctx.currency}, receivable account "
                        f"currency={ctx.account_currency}. Booking as an unreferenced settlement-currency line."
                    ),
                )
                # Do not post the native settlement amount to the invoice's differently
                # denominated debit_to account. Resolve a currency-compatible account while
                # retaining the real order customer (important for MFN/FBM).
                unref_ctx = resolve_order_receivable_context(
                    repo.amz_setting, order_id, settlement_ccy, invoice_name=None
                )
                base_line = {
                    "account": unref_ctx.account,
                    "exchange_rate": rate,
                    "party_type": "Customer",
                    "party": unref_ctx.customer,
                    "amazon_order_id": order_id,
                }
                stamp_marketplace_fields(
                    base_line, {}, marketplace_name, merchant_order_id,
                    unref_ctx.fulfillment_channel, is_refund=False,
                )

            # Reference only the amount ERPNext says is currently outstanding. Negative
            # sales totals (net retrocharges) are never referenced: a debit against an SI
            # would re-open an already-settled invoice.
            if open_si_name and ccy_ok and remaining > 0 and ctx.outstanding > 0:
                apply = min(remaining, ctx.outstanding)
                if apply >= 0.01:
                    ref_line = dict(base_line)
                    ref_line.update({
                        "reference_type": "Sales Invoice",
                        "reference_name": open_si_name,
                    })
                    _append_ar_line(ar_lines, ref_line, apply, credit=True)
                    remaining = round(remaining - apply, 2)

            # Preserve any excess/unmatched settlement amount (either sign) as an
            # unreferenced party line instead of over-allocating or dropping it.
            _append_ar_line(ar_lines, base_line, remaining, credit=True)

        # Refund pass
        for order_id, refund_total_native in refund_totals.items():
            marketplace_name = ""
            merchant_order_id = ""
            order_rows = order_groups.get(order_id, [])
            if order_rows:
                first_row = order_rows[0]
                marketplace_name = (first_row.get("marketplace-name") or "").strip().lower()
                merchant_order_id = (first_row.get("merchant-order-id") or "").strip()

            si_name = get_sales_invoice(order_id)
            cn_name = None
            refund_rows = [
                r for r in order_rows
                if (r.get("transaction-type") or "").strip().lower() in REFUND_TYPES
            ]
            # A negative refund total is a refund reversal, not a refund: no Credit Note.
            if si_name and refund_rows and refund_total_native > 0:
                cn_name = create_credit_note_for_refund(
                    repo.amz_setting, si_name, refund_total_native, post_dt, order_id,
                    marketplace_name, merchant_order_id, refund_rows, rpt_id
                )

            ctx = resolve_order_receivable_context(
                repo.amz_setting, order_id, settlement_ccy, cn_name or si_name
            )
            base_line = {
                "account": ctx.account,
                "exchange_rate": rate,
                "party_type": "Customer",
                "party": ctx.customer,
                "amazon_order_id": order_id,
            }
            stamp_marketplace_fields(
                base_line, {}, marketplace_name, merchant_order_id,
                ctx.fulfillment_channel, is_refund=True,
            )
            remaining = refund_total_native
            ccy_ok = (
                (not ctx.currency or ctx.currency == settlement_ccy)
                and (not ctx.account_currency or ctx.account_currency == settlement_ccy)
            )
            if (cn_name or si_name) and not ccy_ok:
                frappe.log_error(
                    title=f"Amazon Settlement Refund Currency Mismatch {order_id}"[:140],
                    message=(
                        f"Settlement {rpt_id} currency {settlement_ccy} cannot be applied to the linked "
                        f"Sales Invoice/Credit Note for Amazon order {order_id}: document currency={ctx.currency}, "
                        f"receivable account currency={ctx.account_currency}. Booking as an unreferenced "
                        "settlement-currency line."
                    ),
                )
                unref_ctx = resolve_order_receivable_context(
                    repo.amz_setting, order_id, settlement_ccy, invoice_name=None
                )
                base_line = {
                    "account": unref_ctx.account,
                    "exchange_rate": rate,
                    "party_type": "Customer",
                    "party": unref_ctx.customer,
                    "amazon_order_id": order_id,
                }
                stamp_marketplace_fields(
                    base_line, {}, marketplace_name, merchant_order_id,
                    unref_ctx.fulfillment_channel, is_refund=True,
                )

            if cn_name and ccy_ok and remaining > 0 and ctx.outstanding > 0:
                apply = min(remaining, ctx.outstanding)
                if apply >= 0.01:
                    ref_line = dict(base_line)
                    ref_line.update({
                        "reference_type": "Sales Invoice",
                        "reference_name": cn_name,
                    })
                    _append_ar_line(ar_lines, ref_line, apply, credit=False)
                    remaining = round(remaining - apply, 2)

            _append_ar_line(ar_lines, base_line, remaining, credit=False)
        # ──────────────────────────────────────────────
        # 3) Add reimbursement line if significant (unchanged)
        # ──────────────────────────────────────────────
        if abs(reimb_usd) >= 0.01:
            line = {
                "account": repo.amz_setting.custom_amazon_reimbursements_account,
                "exchange_rate": 1,
            }
            if reimb_usd > 0:
                line.update({"credit_in_account_currency": reimb_usd})
            else:
                line.update({"debit_in_account_currency": -reimb_usd})
            non_ar_lines.append(line)
        # ──────────────────────────────────────────────
        # 4) Add lines for each special fees (unchanged)
        # ──────────────────────────────────────────────
        for desc, amt_usd in special_fee_usd.items():
            if amt_usd < 0.009:
                continue
            non_ar_lines.append({
                "account": FEE_ACCOUNT_MAP[desc],
                "debit_in_account_currency": amt_usd,
                "exchange_rate": 1,
                "user_remark": desc.title(),
            })
        # ──────────────────────────────────────────────
        # 5) Add miscellaneous fees line if significant (unchanged)
        # ──────────────────────────────────────────────
        if abs(fees_usd) >= 0.01:
            line = {
                "account": repo.amz_setting.custom_amazon_miscellaneous_fulfillment_fees_account,
                "exchange_rate": 1,
            }
            if fees_usd > 0:
                line.update({"debit_in_account_currency": fees_usd})
            else:
                line.update({"credit_in_account_currency": -fees_usd})
            non_ar_lines.append(line)
        # ──────────────────────────────────────────────
        # Add rounding adjustment line if totals don't balance
        # ──────────────────────────────────────────────
        all_lines = non_ar_lines + ar_lines
        # Calculate in base currency (account_amount * exchange_rate)
        total_debit = sum(flt(line.get('debit_in_account_currency', 0)) * flt(line.get('exchange_rate', 1)) for line in all_lines)
        total_credit = sum(flt(line.get('credit_in_account_currency', 0)) * flt(line.get('exchange_rate', 1)) for line in all_lines)
        difference = round(total_debit - total_credit, 2)
        if abs(difference) > 1.00:
            #frappe.throw("Large imbalance detected in JE (base currency); manual review needed")
            print(f"Imbalance: debit={total_debit}, credit={total_credit}, diff={difference}")
        if abs(difference) > 0:
            rounding_account = repo.amz_setting.custom_round_off_account
            rounding_line = {
                "account": rounding_account,
                "exchange_rate": 1,
                "user_remark": "Rounding adjustment",
            }
            if difference > 0:
                rounding_line.update({"credit_in_account_currency": abs(difference), "credit": abs(difference)})
            else:
                rounding_line.update({"debit_in_account_currency": abs(difference), "debit": abs(difference)})
            non_ar_lines.append(rounding_line)
        # ──────────────────────────────────────────────
        # Build and return the Journal Entry document
        # ──────────────────────────────────────────────
        je_lines = non_ar_lines + ar_lines
        je = frappe.get_doc(
            {
                "doctype": "Journal Entry",
                "voucher_type": "Journal Entry",
                "company": repo.amz_setting.company,
                "posting_date": post_dt,
                "cheque_no": rpt_id,
                "cheque_date": post_dt,
                "multi_currency": 1,
                "accounts": je_lines,
                "user_remark": period_remark,
                "custom_deposit_date": deposit_date,
            }
        )
        _flag_unallocated_as_advance(je)  # Ensure unreferenced are advances
        return je
    # ──────────────────────────────────────────────
    # Non-first-pass: Handle late open invoices with adjustments
    # ──────────────────────────────────────────────
    else:
        print(f"[SETT] Non-first pass for {rpt_id}: Allocating late documents only")
        allocate_late_documents_for_settlement(rpt_id, repo, order_groups, settlement_ccy, post_dt)
        return None  # No JE created

def _resolve_advance_values():
    """Return correct values for is_advance depending on fieldtype (Check vs Select)."""
    try:
        meta = frappe.get_meta("Journal Entry Account")
        df = next((f for f in meta.fields if f.fieldname == "is_advance"), None)
        if df:
            if df.fieldtype == "Check":
                return 1, 0
            if df.fieldtype == "Select":
                return "Yes", "No"
    except Exception:
        pass
    return "Yes", "No"

_ADV_YES, _ADV_NO = _resolve_advance_values()

def _flag_unallocated_as_advance(je_doc):
    """
    Mark only legally oriented advances:
      - Customer: credit with no reference  -> advance
      - Supplier: debit  with no reference  -> advance
    Leave all other party lines as non-advance.
    """
    for row in je_doc.get("accounts", []):
        # BaseDocument: use getters/attribute assignment
        party_type = (row.get("party_type") or "").strip()
        party      = (row.get("party") or "").strip()
        has_party  = bool(party_type and party)
        has_ref    = bool(row.get("reference_name"))

        if not has_party or has_ref:
            # Referenced or non-party rows are never 'advance'
            if row.get("is_advance"):
                row.set("is_advance", _ADV_NO)
            continue

        # Amount polarity (company currency preferred)
        debit  = float(row.get("debit")  or 0) or float(row.get("debit_in_account_currency")  or 0)
        credit = float(row.get("credit") or 0) or float(row.get("credit_in_account_currency") or 0)

        if party_type == "Customer":
            # Only credits can be a customer advance
            if credit > 0:
                row.set("is_advance", _ADV_YES)
            else:
                # Avoid the "must be credit" validation by not marking it as advance
                row.set("is_advance", _ADV_NO)

        elif party_type == "Supplier":
            # Only debits can be a supplier advance
            if debit > 0:
                row.set("is_advance", _ADV_YES)
            else:
                row.set("is_advance", _ADV_NO)

        else:
            # Employees/Shareholders/etc.: safest default is non-advance
            row.set("is_advance", _ADV_NO)


            
def _enqueue_settlement_finalize(je_name: str, account_count: int | None = None) -> bool:
    """Idempotently enqueue the settlement finalize job for an existing draft JE."""
    from frappe.utils.background_jobs import enqueue

    job_name = f"Finalize Amazon Settlement {je_name}"
    if frappe.db.exists("RQ Job", {"job_name": job_name, "status": ["in", ["queued", "started"]]}):
        return False

    if account_count is None:
        account_count = frappe.db.count("Journal Entry Account", {"parent": je_name})
    enqueue(
        "eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_process_settlement_report.finalize_and_submit_settlement_je",
        queue="long" if account_count > 200 else "default",
        timeout=3600 if account_count > 200 else 300,
        job_name=job_name,
        je_name=je_name,
    )
    return True

# ──────────────────────────────────────────
# 4. — Orchestrator
# ──────────────────────────────────────────
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_process_settlement_report.process_settlements")
"""
def process_settlements(): # CHANGED: Default to 4
    """Pull newest settlement reports and book Journal Entries."""
   
    amz_settings = frappe.get_all(
        "Amazon SP API Settings",
        filters={"is_active": 1, "enable_sync": 1},
        pluck="name",
    )
    if not amz_settings:
        return
   
    repo = AmazonRepository("q3opu7c5ac")
    reports = list_latest_settlement_reports(repo.amz_setting, 4)
    _ensure_db_connection()
    print(f"[SETT] pulled {len(reports)} reports")
    for i, rpt in enumerate(reports):
        rpt_id = rpt["reportId"]
        submitted_je = frappe.db.get_value(
            "Journal Entry", {"cheque_no": rpt_id, "docstatus": 1}, "name", order_by="creation asc"
        )
        draft_je = frappe.db.get_value(
            "Journal Entry", {"cheque_no": rpt_id, "docstatus": 0}, "name", order_by="creation asc"
        )

        # Never build a second first-pass JE while an earlier draft for this exact report exists.
        # Re-enqueue the draft instead; finalize_and_submit_settlement_je is itself idempotent.
        if draft_je and not submitted_je:
            queued = _enqueue_settlement_finalize(draft_je)
            print(
                f"[SETT] {rpt_id} already has draft {draft_je}; "
                f"{'re-enqueued finalize' if queued else 'finalize already queued/running'}"
            )
            continue

        if draft_je and submitted_je:
            frappe.log_error(
                title="Amazon Settlement Duplicate Draft Manual Review",
                message=(
                    f"Settlement {rpt_id} already has submitted JE {submitted_je} but also draft JE {draft_je}. "
                    "The draft will not be submitted automatically; review/cancel it manually."
                ),
            )

        first_pass = not bool(submitted_je)
        try:
            rows = fetch_settlement_rows(repo.amz_setting, rpt)
            _ensure_db_connection()
           
            #save_settlement_csv(rpt_id, rows) #Save thet settlement reports for debugging
           
            je = build_je(repo, rpt, rows, first_pass)
            if not je:
                print(f"[SETT] nothing to post for {rpt_id}")
                continue
           
            # NEW: Set multi_currency=0 if base-only before insert
            company_currency = frappe.get_value("Company", repo.amz_setting.company, "default_currency")
            if is_base_currency_only(je, company_currency):
                je.multi_currency = 0
            
            _flag_unallocated_as_advance(je)
            _ensure_db_connection()
            frappe.db.sql("SET SESSION innodb_lock_wait_timeout = 300;")
           
            # Insert as draft (with retry)
            @_retry_locked()
            def _insert_draft():
                je.insert(ignore_permissions=True)
                frappe.db.commit()
           
            _insert_draft()
           
            _enqueue_settlement_finalize(je.name, len(je.accounts))
            print(f"[SETT] {rpt_id} ➜ {je.name} (draft inserted; finalize/submit queued)")
           
        except Exception:
            # Do not let a failed report leave a partial transaction that Frappe's
            # outer background-job commit could accidentally persist.
            _rollback_for_error()
            _log_error_resilient(
                f"Settlement sync failed {rpt_id}",
                frappe.get_traceback(),
            )
            continue

        # NEW: Match troubleshooting's delay between reports (skip after last one)
        if i < len(reports) - 1:
            print(f"⏳ Waiting 10s before next report …")
            _sleep_with_db_check(10)

    # execute_job() performs one final frappe.db.commit() after this function
    # returns. Hand it a live connection so that framework commit does not hit
    # an already-closed PyMySQL socket.
    _ensure_db_connection()

def is_base_currency_only(je_doc: Document, base_ccy: str) -> bool:
    """Check if all lines are in base currency with rate=1."""
    return all(
        (row.get("account_currency") or base_ccy) == base_ccy and
        flt(row.get("exchange_rate")) == 1.0
        for row in je_doc.get("accounts", [])
    )

# ──────────────────────────────────────────
# 6. — New method: Create payment entries for clearing transfers
# ──────────────────────────────────────────
def create_clearing_payment_entries():
    """
    Run hourly: Create Payment Entries to transfer from Amazon clearing accounts to Bank of America
    for settlement Journal Entries where deposit date has passed.
    """   
    amz_settings = frappe.get_all(
        "Amazon SP API Settings",
        filters={"is_active": 1, "enable_sync": 1},
        pluck="name",
    )

    if not amz_settings:
        return    
    
    settings = frappe.get_doc("Amazon SP API Settings", "q3opu7c5ac")
    
    clearing_accounts = [
        settings.custom_amazon_usd_clearing_account,
        settings.custom_amazon_cad_clearing_account,
        settings.custom_amazon_mxn_clearing_account,
    ]
    bank_account = settings.custom_default_bank_account
    mode_of_payment = settings.custom_bank_transfer_mode_of_payment
    
    ninety_days_ago = (datetime.today() - timedelta(days=90)).date().strftime("%Y-%m-%d")
    
    # Query JEs from last 90 days with clearing account lines
    query = """
        SELECT DISTINCT je.name, je.cheque_no, je.custom_deposit_date, je.company, je.posting_date
        FROM `tabJournal Entry` je
        INNER JOIN `tabJournal Entry Account` jea ON jea.parent = je.name
        WHERE je.docstatus = 1
        AND je.posting_date >= %(ninety_days_ago)s
        AND jea.account IN %(clearing_accounts)s
    """
    params = {
        "ninety_days_ago": ninety_days_ago,
        "clearing_accounts": tuple(clearing_accounts),
    }
    jes = frappe.db.sql(query, params, as_dict=True)
    
    for je_dict in jes:
        je_name = je_dict["name"]
        
        # Get the clearing line
        clearing_lines = frappe.db.get_all(
            "Journal Entry Account",
            filters={"parent": je_name, "account": ["in", clearing_accounts]},
            fields=[
                "account",
                "account_currency",
                "debit_in_account_currency",
                "credit_in_account_currency",
                "exchange_rate",
            ],
        )
        if not clearing_lines:
            continue
        
        cl = clearing_lines[0]  # Assume one per JE
        
        # Only handle positive deposits (debit to clearing > 0)
        if cl.debit_in_account_currency <= 0:
            continue
        
        amount = cl.debit_in_account_currency
        ccy = cl.account_currency
        clearing_account = cl.account
        original_exchange_rate = cl.exchange_rate  # Use this to match JE base amount
        
        # Parse custom_deposit_date and check if passed
        if not je_dict["custom_deposit_date"]:
            continue
        try:
            pst_tz = ZoneInfo("America/Los_Angeles")
            if isinstance(je_dict["custom_deposit_date"], str):
                dep_dt = datetime.strptime(je_dict["custom_deposit_date"], "%Y-%m-%d %H:%M:%S")
            else:
                dep_dt = je_dict["custom_deposit_date"]
            
            # Attach the Los Angeles timezone to dep_dt (it was previously a naive datetime with no timezone info)
            # This does not change the clock time — it simply tells Python that this time is in Los Angeles local time
            dep_dt = dep_dt.replace(tzinfo=pst_tz)
            if dep_dt > datetime.now(tz=pst_tz):
                continue
        except ValueError:
            frappe.log_error(f"Invalid custom_deposit_date in JE {je_name}", "Clearing Transfer")
            continue
        
        # Check if Payment Entry already exists with matching reference_no
        if frappe.db.exists(
            "Payment Entry",
            {"reference_no": je_dict["cheque_no"], "docstatus": 1},
        ):
            continue
        
        # Prepare Payment Entry
        pe_posting_date = dep_dt.date().strftime("%Y-%m-%d")
        reference_date = pe_posting_date
        company_currency = frappe.get_value("Company", je_dict["company"], "default_currency")
        
        is_multi_currency = ccy != company_currency
        source_exchange_rate = original_exchange_rate if is_multi_currency else 1.0
        received_amount = round(amount * source_exchange_rate, 2)
        
        pe = frappe.get_doc(
            {
                "doctype": "Payment Entry",
                "payment_type": "Internal Transfer",
                "company": je_dict["company"],
                "posting_date": pe_posting_date,
                "mode_of_payment": mode_of_payment,
                "paid_from": clearing_account,
                "paid_from_account_currency": ccy,
                "paid_to": bank_account,
                "paid_to_account_currency": company_currency,
                "paid_amount": amount,
                "received_amount": received_amount,
                "source_exchange_rate": source_exchange_rate,
                "reference_no": je_dict["cheque_no"],
                "reference_date": reference_date,
            }
        )
        
        try:
            pe.insert(ignore_permissions=True)
            pe.submit()
            frappe.db.commit()
            print(f"[CLEAR] Created Payment Entry {pe.name} for JE {je_name}")
        except Exception as e:
            frappe.log_error(
                f"Failed to create Payment Entry for JE {je_name}: {str(e)}",
                "Clearing Transfer Error"
            )


# Saves the settlement reports as csv files in the same directory as the program is running. (uncomment save_settlement_csv above to activate)
def save_settlement_csv(report_id: str, rows: list[dict], output_dir: str = None) -> str:
    """
    Save the settlement rows as a CSV file for debugging.
    Returns the full filepath of the saved CSV, or an empty string if no rows.
    """
    import os  # For path joining and directory handling
    
    if not rows:
        return ""
    
    # Default to the directory of this script file
    if output_dir is None:
        output_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    filename = f"settlement_{report_id}.csv"
    filepath = os.path.join(output_dir, filename)
    
    # Get all unique keys across rows (in case they vary slightly)
    all_keys = set()
    for row in rows:
        all_keys.update(row.keys())
    fieldnames = sorted(all_keys)  # Sort for consistent order
    
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"[DEBUG] Saved CSV for report {report_id} to {filepath}")
    return filepath

# [Hooked in hooks.py] This function trims the remarks field for all "GL Entries" and "Payment Ledger Entries", so they don't bloat the database. - As a reminder, erpnext natively copies over the remarks entry from journal entries to gl and payment ledger entries which are very long
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_process_settlement_report.shorten_remarks", doc="ACC-JV-2025-00165", method="on_submit")
"""
def shorten_remarks(doc, method):
    # If doc is a string (name), load the full document
    if isinstance(doc, str):
        doc = frappe.get_doc("Journal Entry", doc)
    
    if "Settlement Period" not in (doc.remark or ""):
        return

    # Parse remark for header details
    remark = doc.remark
    period_match = re.search(r"Settlement Period: ([\d./-]+ - [\d./-]+)", remark)
    period = period_match.group(1) if period_match else "Unknown"

    ref_match = re.search(r"Reference #([\d]+) dated ([\d-]+)", remark)
    ref_num = ref_match.group(1) if ref_match else "Unknown"
    ref_date = ref_match.group(2) if ref_match else "Unknown"

    # Build short remarks (header only, no invoice references)
    short_remarks = f"Note: Settlement Period: {period}\nReference #{ref_num} dated {ref_date}"

    # Update related GL Entries
    gl_entries = frappe.get_all("GL Entry", filters={"voucher_type": "Journal Entry", "voucher_no": doc.name}, fields=["name"])
    for gle in gl_entries:
        frappe.db.set_value("GL Entry", gle.name, "remarks", short_remarks, update_modified=False)

    # Update related Payment Ledger Entries if 'remarks' field exists
    if frappe.db.table_exists("Payment Ledger Entry") and frappe.db.has_column("Payment Ledger Entry", "remarks"):
        ple_entries = frappe.get_all("Payment Ledger Entry", filters={"voucher_type": "Journal Entry", "voucher_no": doc.name}, fields=["name"])
        for ple in ple_entries:
            frappe.db.set_value("Payment Ledger Entry", ple.name, "remarks", short_remarks, update_modified=False)
    
    # Toggle to also trim the Journal Entry remark (default: False)
    # (Currently untested)
    TRIM_JE = False
    if TRIM_JE:
        frappe.db.set_value("Journal Entry", doc.name, "remark", short_remarks, update_modified=False)
    
    # Commit changes to ensure they persist (useful for manual/bench runs)
    frappe.db.commit()

# ──────────────────────────────────────────
# 5. — Scheduler wrapper
# ──────────────────────────────────────────
@frappe.whitelist()
def run_daily_settlement_sync():
    """Daily scheduler entry: processes newest 4 reports."""
    setting = frappe.get_single("Amazon SP API Settings")
    process_settlements()

def _clear_je_invoice_reference(row):
    """Detach a stale Sales Invoice/Credit Note reference but preserve the accounting amount."""
    row.reference_type = None
    row.reference_name = None
    if hasattr(row, "reference_due_date"):
        row.reference_due_date = None
    if hasattr(row, "reference_detail_no"):
        row.reference_detail_no = None


def _append_unreferenced_residual(je, source_row, amount_field: str, amount: float):
    """Clone a JE account row as an unreferenced residual while preserving dimensions/metadata."""
    if flt(amount) < 0.01:
        return None

    excluded = {
        "name", "parent", "parentfield", "parenttype", "doctype", "docstatus", "idx",
        "reference_type", "reference_name", "reference_due_date", "reference_detail_no",
        "advance_voucher_type", "advance_voucher_no",
        "debit", "credit", "debit_in_account_currency", "credit_in_account_currency",
    }
    values = {}
    for fieldname in frappe.get_meta("Journal Entry Account").get_fieldnames_with_value():
        if fieldname in excluded:
            continue
        value = source_row.get(fieldname)
        if value is not None:
            values[fieldname] = value

    values.update({
        "reference_type": None,
        "reference_name": None,
        "debit_in_account_currency": 0,
        "credit_in_account_currency": 0,
        amount_field: flt(amount),
    })
    return je.append("accounts", values)


def _revalidate_draft_settlement_allocations(je) -> bool:
    """
    Re-read every referenced Sales Invoice/Credit Note immediately before submit.

    Settlement JEs are inserted as drafts and submitted later by a queued worker. Another
    settlement/payment can therefore change an invoice's outstanding amount after build_je()
    bounded the allocation. Lock referenced invoices, cap each draft reference to the live
    outstanding amount, and move any excess to an unreferenced row for the same party/account.
    """
    referenced_names = sorted({
        row.reference_name
        for row in je.accounts
        if row.reference_type == "Sales Invoice" and row.reference_name
    })
    if not referenced_names:
        return False

    # Serialize outstanding changes with other accounting transactions touching these invoices.
    for invoice_name in referenced_names:
        frappe.db.sql(
            "SELECT name FROM `tabSales Invoice` WHERE name=%s FOR UPDATE",
            (invoice_name,),
        )

    remaining_by_invoice = {}
    changed = False

    for row in list(je.accounts):
        if row.reference_type != "Sales Invoice" or not row.reference_name:
            continue

        invoice_name = row.reference_name
        inv = get_invoice_receivable_context(invoice_name)
        reason = None
        if not inv or inv.docstatus != 1:
            reason = "invoice is missing, cancelled, or no longer submitted"
        elif row.party != inv.customer or row.account != inv.debit_to:
            reason = "party/account no longer matches the referenced invoice"
        elif (inv.currency or "").upper() != (inv.account_currency or "").upper():
            reason = (
                f"invoice currency {inv.currency} differs from receivable account currency "
                f"{inv.account_currency}; automatic settlement allocation is unsafe"
            )

        is_return = bool(inv and cint(inv.is_return))
        amount_field = "debit_in_account_currency" if is_return else "credit_in_account_currency"
        opposite_field = "credit_in_account_currency" if is_return else "debit_in_account_currency"
        row_amount = flt(row.get(amount_field))

        if not reason and (row_amount <= 0 or flt(row.get(opposite_field)) > 0):
            reason = "reference direction is invalid for the invoice/credit-note type"

        if reason:
            _clear_je_invoice_reference(row)
            changed = True
            print(f"[SETT] Detached stale reference {invoice_name} from {je.name}: {reason}")
            continue

        if invoice_name not in remaining_by_invoice:
            outstanding = flt(inv.outstanding_amount)
            remaining_by_invoice[invoice_name] = (
                abs(outstanding) if is_return and outstanding < 0 else max(outstanding, 0)
            )

        available_outstanding = remaining_by_invoice[invoice_name]
        allowed = min(row_amount, available_outstanding)
        remaining_by_invoice[invoice_name] = max(available_outstanding - allowed, 0)

        if allowed + 0.000001 >= row_amount:
            continue

        residual = round(row_amount - allowed, 9)
        if allowed < 0.01:
            # The invoice was paid/closed after this JE was drafted. Keep the entire amount
            # as an unreferenced customer line rather than failing submission or overpaying.
            _clear_je_invoice_reference(row)
        else:
            row.set(amount_field, allowed)
            _append_unreferenced_residual(je, row, amount_field, residual)

        changed = True
        print(
            f"[SETT] Revalidated {je.name} -> {invoice_name}: live outstanding={available_outstanding}, "
            f"referenced={allowed}, residual_unreferenced={residual}"
        )

    if changed:
        _flag_unallocated_as_advance(je)
    return changed


def finalize_and_submit_settlement_je(je_name: str):
    """
    Queued job: Acquires lock, adds rounding if needed, saves, submits.
    Idempotent: Skips if already submitted or queued; avoids duplicate rounding.
    """
    if not frappe.db.exists("Journal Entry", je_name):
        frappe.log_error(f"JE {je_name} not found", "Settlement Finalize")
        return

    # Idempotency: Skip if already submitted
    docstatus = frappe.db.get_value("Journal Entry", je_name, "docstatus")
    if docstatus == 1:
        print(f"[SETT] JE {je_name} already submitted; skipping")
        return
    if docstatus == 2:
        print(f"[SETT] JE {je_name} cancelled; skipping")
        return

    # Acquire lock and proceed (retry on lock error)
    @_retry_locked()  # Retries Frappe document-lock conflicts
    def _finalize_and_submit():
        # The queued finalize worker is a different DB session from process_settlements().
        # Set the lock wait timeout here before taking potentially hundreds of invoice locks.
        frappe.db.sql("SET SESSION innodb_lock_wait_timeout = 300;")

        # Serialize duplicate finalize jobs for the same settlement JE. Keep this DB lock
        # until the final submit commit; do not commit between revalidation and submit.
        frappe.db.sql(
            "SELECT name FROM `tabJournal Entry` WHERE name=%s FOR UPDATE",
            (je_name,),
        )
        je = frappe.get_doc("Journal Entry", je_name)
        if je.docstatus != 0:
            print(f"[SETT] JE {je_name} no longer draft (docstatus={je.docstatus}); skipping")
            return

        # Close the draft->submit race: lock each referenced invoice and cap allocations
        # against the live outstanding immediately before submission.
        if _revalidate_draft_settlement_allocations(je):
            je.save(ignore_permissions=True)

        # Compute base difference after any allocation split/detach and validation.
        total_debit = sum(flt(row.debit) for row in je.accounts)
        total_credit = sum(flt(row.credit) for row in je.accounts)
        difference = total_debit - total_credit

        company_currency = frappe.get_value("Company", je.company, "default_currency")
        if is_base_currency_only(je, company_currency) and abs(total_debit - total_credit) < 0.01:
            # Already balanced and base-only; just submit
            pass  # Proceed to submit

        # NEW: Fetch system float_precision for robust threshold
        default_precision = cint(frappe.db.get_default("float_precision")) or 3
        threshold = 10 ** (-(default_precision + 1))  # e.g., 1e-4 for precision=3; safely below rounding unit

        if abs(difference) >= threshold:  # CHANGED: Skip tiny fp errors (was > 1e-9)
            # Get settings (assume repo.amz_setting is accessible or fetch)
            settings = frappe.get_doc("Amazon SP API Settings", "q3opu7c5ac")
            rounding_account = settings.custom_round_off_account
            
            # Idempotency: Check for existing rounding line
            rounding_line = next((row for row in je.accounts if row.account == rounding_account and row.user_remark == "Rounding adjustment for exchange rate variations"), None)
            
            if not rounding_line:
                rounding_line = je.append("accounts", {
                    "account": rounding_account,
                    "exchange_rate": 1,
                    "user_remark": "Rounding adjustment for exchange rate variations",
                })
            
            # Adjust to balance (use system precision for setting amount)
            adjusted_amount = round(abs(difference), default_precision + 3)  # Extra digits to avoid under-rounding
            if difference > 0:
                current_credit = flt(rounding_line.credit_in_account_currency or 0)
                rounding_line.credit_in_account_currency = flt(current_credit + adjusted_amount, default_precision + 3)
                rounding_line.credit = flt(rounding_line.credit_in_account_currency * rounding_line.exchange_rate, default_precision + 3)
                rounding_line.debit = 0
                rounding_line.debit_in_account_currency = 0
            else:
                current_debit = flt(rounding_line.debit_in_account_currency or 0)
                rounding_line.debit_in_account_currency = flt(current_debit + adjusted_amount, default_precision + 3)
                rounding_line.debit = flt(rounding_line.debit_in_account_currency * rounding_line.exchange_rate, default_precision + 3)
                rounding_line.credit = 0
                rounding_line.credit_in_account_currency = 0
            
            # NEW: If adjusted amount still rounds to zero in validation's flt, remove the line
            if flt(rounding_line.debit_in_account_currency) == 0 and flt(rounding_line.credit_in_account_currency) == 0:
                je.accounts.remove(rounding_line)
                print(f"[SETT] Skipped tiny rounding ({difference:.2e}) for {je_name} as it rounds to zero")
            else:
                # Save while retaining the transaction/row locks until submit.
                je.save(ignore_permissions=True)
                print(f"[SETT] Added/Adjusted rounding for difference {difference} in {je_name}")
        else:
            print(f"[SETT] Skipped negligible difference ({difference:.2e}) below threshold {threshold:.2e} for {je_name}")

        # Submit inline while the Journal Entry + Sales Invoice row locks are still held.
        # JournalEntry.submit() may queue itself when there are >100 account rows in ERPNext v15,
        # which would release these locks before the real submit. _submit() performs the submit now.
        je._submit()
        frappe.db.commit()
        print(f"[SETT] Submitted JE {je_name}")

    _finalize_and_submit()

def _is_retryable_lock_error(exc: Exception) -> bool:
    """Recognize Frappe locks plus MariaDB/MySQL lock-wait/deadlock OperationalErrors."""
    if isinstance(exc, frappe.exceptions.DocumentLockedError):
        return True
    code = exc.args[0] if getattr(exc, "args", None) else None
    if code in (1205, 1213):  # lock wait timeout / deadlock
        return True
    msg = str(exc).lower()
    return (
        "lock wait timeout" in msg
        or "deadlock found" in msg
        or "try restarting transaction" in msg
    )

def _retry_locked(tries=12, delay=2.0):
    def decorator(fn):
        def wrapper(*args, **kwargs):
            current_delay = delay
            for attempt in range(tries):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:
                    if not _is_retryable_lock_error(exc):
                        raise
                    # Release any locks/state from the failed transaction before retrying.
                    _rollback_for_error()
                    print(
                        f"[SETT] DB lock/deadlock on attempt {attempt+1}; "
                        f"retrying after {current_delay}s: {exc}"
                    )
                    _sleep_with_db_check(current_delay)
                    current_delay = min(current_delay * 1.5, 15.0)
            return fn(*args, **kwargs)
        return wrapper
    return decorator

def _reconcile_submitted_journal_line(
    je_name: str,
    line_name: str,
    against_voucher_type: str,
    against_voucher: str,
    allocated_amount: float,
) -> bool:
    """Use ERPNext's reconciliation engine instead of mutating a submitted JE row directly."""
    line = frappe.db.get_value(
        "Journal Entry Account",
        line_name,
        [
            "account", "party_type", "party", "exchange_rate", "is_advance",
            "debit_in_account_currency", "credit_in_account_currency",
        ],
        as_dict=True,
    )
    if not line:
        return False

    if flt(line.credit_in_account_currency) > 0:
        dr_or_cr = "credit_in_account_currency"
        available = flt(line.credit_in_account_currency)
    elif flt(line.debit_in_account_currency) > 0:
        dr_or_cr = "debit_in_account_currency"
        available = flt(line.debit_in_account_currency)
    else:
        return False

    apply = min(flt(allocated_amount), available)
    if apply < 0.01:
        return False

    args = frappe._dict({
        "voucher_type": "Journal Entry",
        "voucher_no": je_name,
        "voucher_detail_no": line_name,
        "against_voucher_type": against_voucher_type,
        "against_voucher": against_voucher,
        "account": line.account,
        "exchange_rate": flt(line.exchange_rate) or 1,
        "party_type": line.party_type,
        "party": line.party,
        "is_advance": line.is_advance,
        "dr_or_cr": dr_or_cr,
        "unreconciled_amount": available,
        "unadjusted_amount": available,
        "allocated_amount": apply,
        "difference_amount": 0,
    })

    try:
        reconcile_against_document([args])
        frappe.db.commit()
        return True
    except Exception:
        _rollback_for_error()
        _log_error_resilient(
            f"Amazon Settlement Reconciliation {je_name} -> {against_voucher}",
            frappe.get_traceback(),
        )
        return False
    finally:
        # reconcile_against_document sets this flag and only clears it on success.
        frappe.flags.ignore_party_validation = False

def _reclass_cheque_no(rpt_id: str, order_id: str) -> str:
    """Current deterministic cheque_no for one receivable reclassification per report+order."""
    return f"{rpt_id}-AR-{hashlib.sha1(order_id.encode()).hexdigest()[:10]}"

def _legacy_reclass_cheque_no(rpt_id: str, order_id: str) -> str:
    """Backward-compatible ID used by the earlier FBM-specific patch."""
    return f"{rpt_id}-FBM-{hashlib.sha1(order_id.encode()).hexdigest()[:10]}"

def _prefetch_reclassification_index(rpt_id: str) -> dict[str, frappe._dict]:
    """Fetch current and legacy reclassification JEs once per settlement report."""
    rows = frappe.db.sql(
        """
        SELECT name, cheque_no, docstatus
        FROM `tabJournal Entry`
        WHERE docstatus != 2
          AND (cheque_no LIKE %s OR cheque_no LIKE %s)
        """,
        (f"{rpt_id}-AR-%", f"{rpt_id}-FBM-%"),
        as_dict=True,
    )
    return {row.cheque_no: row for row in rows}

def _existing_reclassification(
    rpt_id: str, order_id: str, reclass_index: dict[str, frappe._dict] | None = None
) -> frappe._dict:
    """Return either the current -AR- JE or a legacy -FBM- JE, preferring the current key."""
    refs = (_reclass_cheque_no(rpt_id, order_id), _legacy_reclass_cheque_no(rpt_id, order_id))
    if reclass_index is not None:
        for ref in refs:
            if ref in reclass_index:
                return reclass_index[ref]
        return frappe._dict()
    row = frappe.db.get_value(
        "Journal Entry",
        {"cheque_no": ["in", list(refs)], "docstatus": ["!=", 2]},
        ["name", "cheque_no", "docstatus"],
        as_dict=True,
        order_by="creation asc",
    )
    return row or frappe._dict()

def _finish_pending_reclassification(
    rpt_id: str, je_name: str, order_id: str, reclass_index: dict[str, frappe._dict] | None = None
) -> bool:
    """
    Recover from a crash between reclassification-JE submission and reconciliation.

    The reclassification JE already closed the invoice, so the normal late-sales loop
    skips the order on the next run and would never retry. Detect the submitted
    reclassification JE plus a still-unreferenced settlement credit and finish the job.
    """
    reclass_row = _existing_reclassification(rpt_id, order_id, reclass_index)
    if not reclass_row or cint(reclass_row.docstatus) != 1:
        return False
    reclass_name = reclass_row.name

    line = frappe.db.get_value(
        "Journal Entry Account",
        {
            "parent": je_name,
            "amazon_order_id": order_id,
            "credit_in_account_currency": [">", 0],
            "reference_type": ["is", "not set"],
        },
        ["name", "account", "party", "credit_in_account_currency"],
        as_dict=True,
    )
    if not line:
        return False

    # Only consume as much as the reclassification JE actually took from this account/party.
    reclass_debit = flt(frappe.db.get_value(
        "Journal Entry Account",
        {
            "parent": reclass_name,
            "account": line.account,
            "party": line.party,
            "debit_in_account_currency": [">", 0],
            "reference_type": ["is", "not set"],
        },
        "debit_in_account_currency",
    ))
    apply = min(flt(line.credit_in_account_currency), reclass_debit)
    if apply < 0.01:
        return False

    print(f"[SETT] Resuming reconciliation of {je_name} -> {reclass_name} for {order_id}")
    return _reconcile_submitted_journal_line(je_name, line.name, "Journal Entry", reclass_name, apply)

def _reclassify_legacy_receivable_mismatch_sale(
    rpt_id: str,
    settlement_je: str,
    settlement_line: frappe._dict,
    si_name: str,
    order_id: str,
    apply: float,
    post_dt: str,
    marketplace_name: str,
    merchant_order_id: str,
    fulfillment_channel: str,
    reclass_index: dict[str, frappe._dict] | None = None,
) -> bool:
    """
    Repair a legacy settlement whose receivable party/account does not match the Sales Invoice.

    A new JE debits the old settlement customer and credits the real FBM invoice.
    The original settlement credit is then reconciled against the new JE's debit
    using ERPNext's reconciliation engine. This preserves the submitted settlement
    history while moving the receivable to the correct customer with a clear audit trail.
    """
    si = get_invoice_receivable_context(si_name)
    if not si or si.docstatus != 1:
        return False

    company = frappe.db.get_value("Journal Entry", settlement_je, "company")
    company_currency = (frappe.get_cached_value("Company", company, "default_currency") or "").upper()
    source_currency = _get_account_currency(settlement_line.account, company)
    target_currency = _get_account_currency(si.debit_to, company)
    if source_currency != target_currency:
        frappe.log_error(
            title="Amazon Settlement AR Reclassification Manual Review",
            message=(
                f"Cannot auto-reclassify Amazon order {order_id}: source AR currency {source_currency} "
                f"!= target AR currency {target_currency}. Settlement JE={settlement_je}, SI={si_name}."
            ),
        )
        return False

    # A foreign-currency AR reclassification carries an FX difference between the
    # settlement rate and the invoice's conversion_rate that this JE cannot book to
    # exchange gain/loss. Refuse it rather than leave an unexplained company-currency
    # residual in the receivable account.
    if (
        source_currency != company_currency
        and abs(flt(settlement_line.exchange_rate) - flt(si.conversion_rate)) > 0.000001
    ):
        frappe.log_error(
            title="Amazon Settlement AR Reclassification Manual Review",
            message=(
                f"Cannot auto-reclassify Amazon order {order_id}: settlement rate "
                f"{settlement_line.exchange_rate} != invoice conversion_rate {si.conversion_rate} on a "
                f"{source_currency} receivable. Use Payment Reconciliation so ERPNext books exchange gain/loss. "
                f"Settlement JE={settlement_je}, SI={si_name}."
            ),
        )
        return False

    reclass_ref = _reclass_cheque_no(rpt_id, order_id)
    existing_reclass = _existing_reclassification(rpt_id, order_id, reclass_index)
    reclass_name = existing_reclass.name if existing_reclass else None
    if existing_reclass and cint(existing_reclass.docstatus) == 0:
        # A previous current or legacy run left a draft behind; never create a second one.
        frappe.log_error(
            title="Amazon Settlement AR Reclassification Manual Review",
            message=(
                f"Draft reclassification JE {reclass_name} exists for {existing_reclass.cheque_no}; "
                "submit or delete it before retrying."
            ),
        )
        return False

    if not reclass_name:
        rate = flt(settlement_line.exchange_rate) or 1
        source_line = {
            "account": settlement_line.account,
            "party_type": "Customer",
            "party": settlement_line.party,
            "exchange_rate": rate,
            "debit_in_account_currency": apply,
            "amazon_order_id": order_id,
            "user_remark": "Reclassify legacy Amazon settlement from mismatched receivable party/account",
        }
        target_line = {
            "account": si.debit_to,
            "party_type": "Customer",
            "party": si.customer,
            "exchange_rate": rate,
            "credit_in_account_currency": apply,
            "reference_type": "Sales Invoice",
            "reference_name": si_name,
            "amazon_order_id": order_id,
        }
        stamp_marketplace_fields(
            target_line, {}, marketplace_name, merchant_order_id,
            fulfillment_channel, is_refund=False,
        )
        # The legacy case is "settlement arrived before the invoice", so post_dt is often
        # earlier than the SI. Posting before the invoice date can also land in a closed
        # accounting period; never post the repair earlier than the document it repairs.
        reclass_dt = max(getdate(post_dt), getdate(si.posting_date)).strftime("%Y-%m-%d")
        reclass = frappe.get_doc({
            "doctype": "Journal Entry",
            "voucher_type": "Journal Entry",
            "company": company,
            "posting_date": reclass_dt,
            "cheque_no": reclass_ref,
            "cheque_date": reclass_dt,
            "multi_currency": 1 if source_currency != company_currency else 0,
            "accounts": [source_line, target_line],
            "user_remark": f"Amazon settlement receivable reclassification for {order_id}; source settlement {settlement_je}",
        })
        try:
            reclass.insert(ignore_permissions=True)
            reclass.submit()
            frappe.db.commit()
            reclass_name = reclass.name
            if reclass_index is not None:
                reclass_index[reclass_ref] = frappe._dict(name=reclass_name, cheque_no=reclass_ref, docstatus=1)
            print(f"[SETT] Reclassified legacy Amazon settlement {order_id}: {settlement_je} -> {reclass_name} -> {si_name}")
        except Exception:
            frappe.db.rollback()
            frappe.log_error(
                title=f"Amazon Settlement AR Reclassification {order_id}"[:140],
                message=frappe.get_traceback(),
            )
            return False

    # Consume the original unallocated settlement credit against the reclassification JE.
    return _reconcile_submitted_journal_line(
        settlement_je, settlement_line.name, "Journal Entry", reclass_name, apply
    )

def allocate_late_documents_for_settlement(rpt_id: str, repo: AmazonRepository, order_groups: dict, settlement_ccy: str, post_dt: str):
    je_name = frappe.db.get_value("Journal Entry", {"cheque_no": rpt_id, "docstatus": 1}, "name")
    if not je_name:
        print(f"[SETT] No submitted first-pass JE for {rpt_id}; skipping allocation")
        return

    # One Journal Entry scan per report rather than one cheque_no lookup per order.
    # Include the earlier -FBM- prefix so a prior deployment cannot create a duplicate
    # -AR- reclassification for the same report/order.
    reclass_index = _prefetch_reclassification_index(rpt_id)

    SALES_TYPES = {"order", "order_retrocharge"}
    REFUND_TYPES = {"refund"}
    sales_totals = {}
    refund_totals = {}
    for order_id, order_rows in order_groups.items():
        sales_total = sum(float(r["amount"]) for r in order_rows if (r.get("transaction-type") or "").strip().lower() in SALES_TYPES)
        refund_total = -sum(float(r["amount"]) for r in order_rows if (r.get("transaction-type") or "").strip().lower() in REFUND_TYPES)
        if abs(sales_total) >= 0.01:
            sales_totals[order_id] = sales_total
        if abs(refund_total) >= 0.01:
            refund_totals[order_id] = refund_total

    # Late sales: use ERPNext reconciliation if party/account already match.
    # Historical receivable party/account mismatches are repaired with a reclassification JE.
    for order_id, sales_total_native in sales_totals.items():
        if sales_total_native < 0.01:
            # A net-negative order/retrocharge is not a payment against an open SI. It was
            # intentionally booked as an unreferenced debit on first pass.
            continue
        # Resume a reclassification that was submitted but never reconciled. Must run
        # before the open-invoice check, because that JE already closed the invoice.
        if _finish_pending_reclassification(rpt_id, je_name, order_id, reclass_index):
            continue

        si_name = get_open_sales_invoice(order_id)
        if not si_name or is_already_referenced_by_report(rpt_id, si_name):
            continue

        si = get_invoice_receivable_context(si_name)
        if not si or si.docstatus != 1:
            continue
        if (
            (si.currency or "").upper() != (settlement_ccy or "").upper()
            or (si.account_currency or "").upper() != (settlement_ccy or "").upper()
        ):
            frappe.log_error(
                title="Amazon Settlement Late Allocation Manual Review",
                message=(
                    f"Settlement {rpt_id} ({settlement_ccy}) cannot be allocated to Sales Invoice "
                    f"{si_name} for Amazon order {order_id}: invoice currency={si.currency}, "
                    f"receivable account currency={si.account_currency}."
                ),
            )
            continue
        outstanding = max(flt(si.outstanding_amount), 0)
        if outstanding < 0.01:
            continue

        line = frappe.db.get_value(
            "Journal Entry Account",
            {
                "parent": je_name,
                "amazon_order_id": order_id,
                "credit_in_account_currency": [">", 0],
                "reference_type": ["is", "not set"],
            },
            ["name", "account", "party_type", "party", "exchange_rate", "credit_in_account_currency"],
            as_dict=True,
        )
        if not line:
            continue

        apply = min(sales_total_native, outstanding, flt(line.credit_in_account_currency))
        if apply < 0.01:
            continue

        order_rows = order_groups.get(order_id, [])
        first_row = order_rows[0] if order_rows else {}
        marketplace_name = (first_row.get("marketplace-name") or "").strip().lower()
        merchant_order_id = (first_row.get("merchant-order-id") or "").strip()
        fulfillment_channel = (get_sales_order_context(order_id).fulfillment_channel or "").upper()

        if line.party == si.customer and line.account == si.debit_to:
            _reconcile_submitted_journal_line(je_name, line.name, "Sales Invoice", si_name, apply)
        else:
            _reclassify_legacy_receivable_mismatch_sale(
                rpt_id, je_name, line, si_name, order_id, apply, post_dt,
                marketplace_name, merchant_order_id, fulfillment_channel, reclass_index,
            )

    # Late refunds: the Credit Note must still be created (it is the revenue reversal and
    # is idempotent per report), but the submitted JE row is never rewritten with
    # frappe.db.set_value(). Allocation goes through ERPNext's reconciliation engine when
    # the settlement row already carries the right party/account, and is surfaced for
    # manual Payment Reconciliation otherwise.
    for order_id, refund_total_native in refund_totals.items():
        if refund_total_native < 0.01:
            continue

        order_rows = order_groups.get(order_id, [])
        first_row = order_rows[0] if order_rows else {}
        marketplace_name = (first_row.get("marketplace-name") or "").strip().lower()
        merchant_order_id = (first_row.get("merchant-order-id") or "").strip()
        refund_rows = [
            r for r in order_rows
            if (r.get("transaction-type") or "").strip().lower() in REFUND_TYPES
        ]

        source_si = get_sales_invoice(order_id)
        if source_si and refund_rows:
            create_credit_note_for_refund(
                repo.amz_setting, source_si, refund_total_native, post_dt, order_id,
                marketplace_name, merchant_order_id, refund_rows, rpt_id
            )

        for cn_name in get_open_credit_notes_for_order(order_id):
            if is_already_referenced_by_report(rpt_id, cn_name):
                continue
            cn = get_invoice_receivable_context(cn_name)
            if not cn or cn.docstatus != 1:
                continue
            if (
                (cn.currency or "").upper() != (settlement_ccy or "").upper()
                or (cn.account_currency or "").upper() != (settlement_ccy or "").upper()
            ):
                frappe.log_error(
                    title="Amazon Settlement Late Refund Manual Review",
                    message=(
                        f"Settlement {rpt_id} ({settlement_ccy}) cannot be allocated to Credit Note "
                        f"{cn_name} for Amazon order {order_id}: document currency={cn.currency}, "
                        f"receivable account currency={cn.account_currency}."
                    ),
                )
                continue
            outstanding = abs(flt(cn.outstanding_amount))
            if outstanding < 0.01:
                continue

            line = frappe.db.get_value(
                "Journal Entry Account",
                {
                    "parent": je_name,
                    "amazon_order_id": order_id,
                    "debit_in_account_currency": [">", 0],
                    "reference_type": ["is", "not set"],
                },
                ["name", "account", "party", "debit_in_account_currency"],
                as_dict=True,
            )
            if not line:
                break

            apply = min(refund_total_native, outstanding, flt(line.debit_in_account_currency))
            if apply < 0.01:
                break

            if line.party == cn.customer and line.account == cn.debit_to:
                _reconcile_submitted_journal_line(je_name, line.name, "Sales Invoice", cn_name, apply)
            else:
                # Legacy row booked to the FBA customer. A reclassification would have to
                # move a debit, which is the mirror of the sales case; leave it to a human.
                frappe.log_error(
                    title="Amazon Settlement Late Refund Manual Review",
                    message=(
                        f"Settlement {rpt_id} row {line.name} is booked to {line.party}/{line.account} but "
                        f"Credit Note {cn_name} belongs to {cn.customer}/{cn.debit_to} (Amazon order {order_id}, "
                        f"amount {apply}). Reconcile manually; the settlement job will not mutate submitted rows."
                    ),
                )
            refund_total_native = round(refund_total_native - apply, 2)
            if refund_total_native < 0.01:
                break
