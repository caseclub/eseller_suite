# Copyright (c) 2026, efeone and contributors
# For license information, please see license.txt
# /apps/eseller_suite/eseller_suite/eseller_suite/doctype/amazon_sp_api_settings/amazon_afn_fee_accounting.py
"""
AFN "Separate Fee JE" accounting support.

Problem: an Amazon-fulfilled (AFN) order whose seller fees exceed the sale cannot be a
normal Sales Order / Sales Invoice, because ERPNext 15 validates base_grand_total >= 0 for
every non-return document.  The historical "net invoice" model (seller fees as negative
Sales Taxes and Charges rows) therefore failed for those orders.

This module implements the narrow alternative path:

  * The SO/SI keep principal, buyer charges, marketplace-withheld tax and promotions.
  * The signed seller-fee components (`fees` + order-linked `service_fees`) are moved to a
    separate, deterministic Journal Entry (debit mapped fee accounts / credit the invoice
    receivable).  The fee set is frozen in a versioned snapshot on the Sales Order.
  * After the SI submits, the fee JE's unreferenced customer credit is reconciled through
    ERPNext's own `reconcile_against_document()` against the SI first, then against the
    matching negative-sale settlement debit of the same order.

Module boundaries (circular-import safe): only frappe/erpnext are imported at module load.
`amazon_repository` / `amazon_sp_api_settings` / `amazon_process_settlement_report` are
imported lazily inside the few functions that need them.
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
import time
from collections import defaultdict
from datetime import timedelta

import frappe
from frappe import _
from frappe.utils import add_days, cint, flt, getdate, now_datetime, nowdate

from erpnext.accounts.party import get_party_account
from erpnext.accounts.utils import get_currency_precision, reconcile_against_document

# ──────────────────────────────────────────
# Constants
# ──────────────────────────────────────────
FEE_ACCOUNTING_NET = "Net Invoice"
FEE_ACCOUNTING_SEPARATE = "Separate Fee JE"
FEE_ACCOUNTING_OPTIONS = f"\n{FEE_ACCOUNTING_NET}\n{FEE_ACCOUNTING_SEPARATE}"

FEE_SNAPSHOT_VERSION = 1
FEE_JE_CHEQUE_PREFIX = "AFN-FEE-"           # AFN-FEE-<amazon_order_id>
FEE_RECLASS_CHEQUE_PREFIX = "AFN-FEE-RC-"   # AFN-FEE-RC-<order>-<report hash>
FEE_JE_REMARK_PREFIX = "Amazon AFN seller fees for Order "
FEE_BUCKETS = ("fees", "service_fees")      # buckets moved off the SO/SI for this path

# Journal Entry Account line kinds (stamped on NEW rows only; never changes amounts).
LINE_KIND_SETTLEMENT_SALE = "settlement_sale"
LINE_KIND_SETTLEMENT_REFUND = "settlement_refund"
LINE_KIND_AFN_FEE = "afn_fee"
LINE_KIND_AFN_FEE_RECLASS = "afn_fee_reclass"

AFN_FEE_REVIEW_PREFIX = "AFN separate-fee manual review"
AFN_FEE_DEFER_PREFIX = "AFN separate-fee deferred"
AFN_FEE_RETRY_HORIZON_DAYS = 45      # bounded automatic retry window for stranded SOs
AFN_FEE_RETRY_BATCH_SIZE = 50
AFN_FEE_RETRY_INTERVAL_HOURS = 6     # cursor: a chain is retried at most this often
LOCK_RETRY_TRIES = 4
LOCK_RETRY_DELAY = 1.5

# cheque_no prefixes of vouchers that are NOT settlement reports; used to find settlement rows.
NON_SETTLEMENT_CHEQUE_PREFIXES = (FEE_JE_CHEQUE_PREFIX, "MFN-POST-", "MFN-FIN-")
NON_SETTLEMENT_CHEQUE_MARKERS = ("-AR-", "-FBM-")

_AMAZON_ORDER_ID_RE = re.compile(r"^\d{3}-\d{7}-\d{7}$")


class AmazonOrderFetchError(Exception):
    """Orders API did not return a usable payload (unavailable/throttled/forbidden)."""


# ──────────────────────────────────────────
# 1. Idempotent custom-field setup
# ──────────────────────────────────────────
def _custom_field_specs() -> dict[str, list[dict]]:
    """Every field this module relies on. Safe to re-run; existing fields are skipped."""
    return {
        "Sales Order": [
            dict(fieldname="custom_amazon_fee_accounting", label="Amazon Fee Accounting",
                 fieldtype="Select", options=FEE_ACCOUNTING_OPTIONS, read_only=1,
                 insert_after="amazon_order_amount" if _has_field("Sales Order", "amazon_order_amount") else None,
                 description="Blank = legacy net-invoice model."),
            dict(fieldname="custom_amazon_fee_snapshot", label="Amazon Fee Snapshot",
                 fieldtype="Code", options="JSON", read_only=1, hidden=0, no_copy=1,
                 insert_after="custom_amazon_fee_accounting"),
            dict(fieldname="custom_amazon_fee_je", label="Amazon Fee Journal Entry",
                 fieldtype="Data", read_only=1, allow_on_submit=1, no_copy=1,
                 insert_after="custom_amazon_fee_snapshot"),
            dict(fieldname="custom_amazon_fee_status", label="Amazon Fee Allocation Status",
                 fieldtype="Data", read_only=1, allow_on_submit=1, no_copy=1,
                 insert_after="custom_amazon_fee_je",
                 description="complete / pending: … / review: … (written by the allocator)"),
            dict(fieldname="custom_amazon_fee_last_retry", label="Amazon Fee Last Retry",
                 fieldtype="Datetime", read_only=1, allow_on_submit=1, no_copy=1,
                 insert_after="custom_amazon_fee_status"),
        ],
        "Sales Invoice": [
            dict(fieldname="custom_amazon_fee_accounting", label="Amazon Fee Accounting",
                 fieldtype="Select", options=FEE_ACCOUNTING_OPTIONS, read_only=1,
                 insert_after="amazon_order_amount" if _has_field("Sales Invoice", "amazon_order_amount") else None,
                 description="Blank = legacy net-invoice model."),
            dict(fieldname="custom_amazon_fee_je", label="Amazon Fee Journal Entry",
                 fieldtype="Data", read_only=1, allow_on_submit=1, no_copy=1,
                 insert_after="custom_amazon_fee_accounting"),
            dict(fieldname="custom_amazon_fee_fingerprint", label="Amazon Fee Fingerprint",
                 fieldtype="Data", read_only=1, allow_on_submit=1, no_copy=1,
                 insert_after="custom_amazon_fee_je"),
        ],
        "Journal Entry": [
            # amazon_order_id is already used by the integration; create only when absent.
            dict(fieldname="amazon_order_id", label="Amazon Order ID", fieldtype="Data",
                 read_only=0, insert_after="cheque_date"),
            dict(fieldname="custom_amazon_source_sales_order", label="Amazon Source Sales Order",
                 fieldtype="Data", read_only=1, no_copy=1, insert_after="amazon_order_id"),
            dict(fieldname="custom_amazon_fee_fingerprint", label="Amazon Fee Fingerprint",
                 fieldtype="Data", read_only=1, no_copy=1,
                 insert_after="custom_amazon_source_sales_order"),
            dict(fieldname="custom_amazon_settings", label="Amazon SP API Settings",
                 fieldtype="Data", read_only=1, no_copy=1,
                 insert_after="custom_amazon_fee_fingerprint"),
            dict(fieldname="custom_amazon_settlement_voucher", label="Amazon Settlement Voucher",
                 fieldtype="Data", read_only=1, no_copy=1,
                 insert_after="custom_amazon_settings",
                 description="Reclassification JEs: the settlement Journal Entry they offset"),
        ],
        "Journal Entry Account": [
            dict(fieldname="amazon_order_id", label="Amazon Order ID", fieldtype="Data",
                 insert_after="user_remark"),
            dict(fieldname="custom_amazon_line_kind", label="Amazon Line Kind", fieldtype="Data",
                 read_only=1, insert_after="amazon_order_id",
                 description="settlement_sale / settlement_refund / afn_fee / afn_fee_reclass"),
        ],
        "Amazon Failed Sync Record": [
            dict(fieldname="custom_resolution", label="Resolution", fieldtype="Small Text",
                 insert_after="remarks" if _has_field("Amazon Failed Sync Record", "remarks") else None),
            dict(fieldname="custom_resolved_on", label="Resolved On", fieldtype="Datetime",
                 insert_after="custom_resolution"),
        ],
    }


def _has_field(doctype: str, fieldname: str) -> bool:
    try:
        return bool(frappe.get_meta(doctype).get_field(fieldname))
    except Exception:
        return False


def setup_custom_fields(commit: bool = True) -> dict:
    """
    Idempotent field setup.  Run via:
        bench --site <site> execute eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_afn_fee_accounting.setup_custom_fields
    Existing fields (standard or custom) are left untouched; only missing ones are created.
    """
    from frappe.custom.doctype.custom_field.custom_field import create_custom_field

    created, skipped = [], []
    for doctype, specs in _custom_field_specs().items():
        if not frappe.db.exists("DocType", doctype):
            skipped.append(f"{doctype}: doctype missing")
            continue
        for spec in specs:
            spec = dict(spec)
            fieldname = spec["fieldname"]
            if _has_field(doctype, fieldname):
                skipped.append(f"{doctype}.{fieldname}")
                continue
            if not spec.get("insert_after"):
                spec.pop("insert_after", None)
            create_custom_field(doctype, spec, ignore_validate=True)
            frappe.clear_cache(doctype=doctype)
            created.append(f"{doctype}.{fieldname}")
    if commit:
        frappe.db.commit()
    result = {"created": created, "skipped_existing": skipped}
    print(f"[AFN-FEE] custom field setup: {json.dumps(result, indent=1)}", flush=True)
    return result


# ──────────────────────────────────────────
# 2. Small shared helpers
# ──────────────────────────────────────────
def _precision() -> int:
    try:
        return cint(get_currency_precision()) or 2
    except Exception:
        return 2


def _r(value, precision: int | None = None) -> float:
    return flt(value, precision if precision is not None else _precision())


def _is_canonical_order_id(value) -> bool:
    return bool(_AMAZON_ORDER_ID_RE.fullmatch(str(value or "").strip()))


def fee_je_cheque_no(order_id: str) -> str:
    return f"{FEE_JE_CHEQUE_PREFIX}{str(order_id).strip()}"


def fee_reclass_cheque_no(order_id: str, settlement_je: str) -> str:
    digest = hashlib.sha1(f"{order_id}|{settlement_je}".encode()).hexdigest()[:10]
    return f"{FEE_RECLASS_CHEQUE_PREFIX}{str(order_id).strip()}-{digest}"


def _company_currency(company: str) -> str:
    return (frappe.get_cached_value("Company", company, "default_currency") or "").upper()


def _account_currency(account: str | None, company: str | None = None) -> str:
    """Account.account_currency with ERPNext's 'blank means company currency' rule applied."""
    if not account:
        return ""
    ccy = frappe.get_cached_value("Account", account, "account_currency")
    if ccy:
        return str(ccy).upper()
    company = company or frappe.get_cached_value("Account", account, "company")
    return _company_currency(company) if company else ""


def _default_cost_center(company: str) -> str | None:
    try:
        import erpnext
        return erpnext.get_default_cost_center(company)
    except Exception:
        return frappe.get_cached_value("Company", company, "cost_center")


def _active_dimension_values(doc) -> dict:
    """Copy active accounting-dimension values from a source document (SO/SI)."""
    values = {}
    try:
        from erpnext.accounts.doctype.accounting_dimension.accounting_dimension import get_dimensions
        for dim in get_dimensions()[0]:
            value = doc.get(dim.fieldname) if doc else None
            if value:
                values[dim.fieldname] = value
    except Exception:
        pass
    return values


def record_review(order_id: str, reason: str, *, deferred: bool = False) -> None:
    """One durable, non-duplicating operator marker on Amazon Failed Sync Record."""
    prefix = AFN_FEE_DEFER_PREFIX if deferred else AFN_FEE_REVIEW_PREFIX
    remarks = f"{prefix}: {reason}"[:1000]
    print(f"[AFN-FEE] {order_id}: {remarks}", flush=True)
    try:
        if frappe.db.exists("Amazon Failed Sync Record", {"amazon_order_id": order_id, "remarks": remarks}):
            return
        row = frappe.new_doc("Amazon Failed Sync Record")
        row.amazon_order_id = order_id
        row.remarks = remarks
        row.save(ignore_permissions=True)
    except Exception:
        frappe.log_error(title=f"AFN fee review marker {order_id}"[:140], message=frappe.get_traceback())


def _is_retryable_lock_error(exc: Exception) -> bool:
    if isinstance(exc, getattr(frappe.exceptions, "DocumentLockedError", ())):
        return True
    code = exc.args[0] if getattr(exc, "args", None) else None
    if code in (1205, 1213):
        return True
    msg = str(exc).lower()
    return "lock wait timeout" in msg or "deadlock found" in msg or "try restarting transaction" in msg


def _with_lock_retry(fn, *args, **kwargs):
    """
    Retry only on lock/deadlock errors.  Every function passed here re-reads live state and
    commits per completed step, so a retry never replays an already-committed posting.
    """
    delay = LOCK_RETRY_DELAY
    for attempt in range(LOCK_RETRY_TRIES):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            if not _is_retryable_lock_error(exc) or attempt == LOCK_RETRY_TRIES - 1:
                raise
            try:
                frappe.db.rollback()
            except Exception:
                pass
            print(f"[AFN-FEE] lock conflict on attempt {attempt + 1}; retrying in {delay}s: {exc}", flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 10.0)


def _lock_rows(doctype: str, names) -> None:
    names = sorted({n for n in names if n})
    if not names:
        return
    placeholders = ",".join(["%s"] * len(names))
    frappe.db.sql(
        f"SELECT name FROM `tab{doctype}` WHERE name IN ({placeholders}) ORDER BY name FOR UPDATE",
        tuple(names),
    )


def _status(status: str, reason: str = "", **extra) -> frappe._dict:
    result = frappe._dict(status=status, reason=reason)
    result.update(extra)
    return result


# ──────────────────────────────────────────
# 3. Fee snapshot: build, fingerprint, parse, validate
# ──────────────────────────────────────────
def _component_record(bucket: str, component: dict) -> dict:
    return {
        "bucket": bucket,
        "account_head": (component.get("account_head") or "").strip(),
        "tax_amount": _r(component.get("tax_amount")),          # signed, native currency
        "description": (component.get("description") or "").strip(),
        "fee_type": (component.get("fee_type") or "").strip(),
        "sku": (component.get("sku") or "").strip(),
        "currency": (component.get("currency") or "").strip().upper(),   # Financial Events CurrencyCode
    }


def _fingerprint_payload(snapshot: dict) -> dict:
    components = sorted(
        (
            {
                "bucket": c.get("bucket"),
                "account_head": c.get("account_head"),
                "tax_amount": _r(c.get("tax_amount")),
                "description": c.get("description"),
                "currency": (c.get("currency") or "").upper(),
            }
            for c in snapshot.get("components") or []
        ),
        key=lambda c: (c["account_head"], c["tax_amount"], c["description"], c["bucket"]),
    )
    return {
        "version": snapshot.get("version"),
        "amazon_order_id": snapshot.get("amazon_order_id"),
        "company": snapshot.get("company"),
        "settings": snapshot.get("settings"),
        "currency": snapshot.get("currency"),
        "company_currency": snapshot.get("company_currency"),
        "conversion_rate": round(flt(snapshot.get("conversion_rate")), 9),
        "posting_date": str(snapshot.get("posting_date")),
        "customer": snapshot.get("customer"),
        "components": components,
        "fee_total": _r(snapshot.get("fee_total")),
        "sale_total": _r(snapshot.get("sale_total")),
        "projected_net_total": _r(snapshot.get("projected_net_total")),
    }


def compute_fingerprint(snapshot: dict) -> str:
    canonical = json.dumps(_fingerprint_payload(snapshot), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def build_fee_snapshot(
    so, order: dict, charges_and_fees: dict, fee_components: list[tuple[str, dict]],
    settings, *, projected_net_total: float, sale_total: float,
) -> dict:
    """
    Freeze the excluded seller-fee set from the SAME Financial Events response used for the
    SO economics.  Everything the fee JE needs later is here; no refetch is required.
    """
    company = so.company or settings.company
    components = [_component_record(bucket, comp) for bucket, comp in fee_components]
    components = [c for c in components if c["account_head"] and abs(c["tax_amount"]) >= 0.005]
    fee_total = _r(sum(c["tax_amount"] for c in components))
    summary = dict(charges_and_fees.get("financial_event_summary") or {})
    snapshot = {
        "version": FEE_SNAPSHOT_VERSION,
        "amazon_order_id": so.amazon_order_id,
        "company": company,
        "settings": settings.name,
        "marketplace_id": so.marketplace_id,
        "currency": (so.currency or "").upper(),
        "company_currency": _company_currency(company),
        "conversion_rate": flt(so.conversion_rate),
        "posting_date": str(so.transaction_date),
        "customer": so.customer,
        "fulfillment_channel": so.fulfillment_channel,
        "amazon_order_amount": flt(so.amazon_order_amount),
        "components": components,
        "fee_total": fee_total,                       # signed native, normally negative
        "expected_receivable_credit": _r(-fee_total),  # positive = credit the receivable
        "projected_net_total": _r(projected_net_total),   # net-invoice-model grand total
        "sale_total": _r(sale_total),                      # grand total with fees excluded
        "financial_event_summary": {
            k: summary.get(k) for k in (
                "shipment_event_count", "shipment_item_count", "fee_count", "fee_total",
                "service_fee_count", "service_fee_total", "principal_total", "charge_total",
                "withheld_tax_total", "promotion_total", "pagination_complete",
            )
        },
        "created_at": str(now_datetime()),
    }
    snapshot["fingerprint"] = compute_fingerprint(snapshot)
    return snapshot


def parse_snapshot(raw) -> dict | None:
    if not raw:
        return None
    if isinstance(raw, dict):
        return raw
    try:
        data = json.loads(raw)
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def verify_snapshot(snapshot: dict | None) -> tuple[bool, str]:
    if not snapshot:
        return False, "fee snapshot missing"
    if cint(snapshot.get("version")) != FEE_SNAPSHOT_VERSION:
        return False, f"unsupported fee snapshot version {snapshot.get('version')}"
    if not snapshot.get("components"):
        return False, "fee snapshot has no components"
    if (snapshot.get("fingerprint") or "") != compute_fingerprint(snapshot):
        return False, "fee snapshot fingerprint mismatch"
    expected = _r(-sum(flt(c.get("tax_amount")) for c in snapshot["components"]))
    if abs(expected - _r(snapshot.get("expected_receivable_credit"))) > 0.005:
        return False, "fee snapshot totals inconsistent"
    return True, "ok"


def snapshot_fee_accounts(snapshot: dict) -> dict[str, float]:
    """Signed native fee total per mapped fee account."""
    totals = defaultdict(float)
    for c in snapshot.get("components") or []:
        totals[c["account_head"]] += flt(c.get("tax_amount"))
    return {k: _r(v) for k, v in totals.items()}


def sales_order_snapshot(so) -> dict | None:
    return parse_snapshot(so.get("custom_amazon_fee_snapshot") if hasattr(so, "get") else None)


def _tax_rows_by_account(doc) -> dict[str, float]:
    totals = defaultdict(float)
    for row in doc.get("taxes") or []:
        account = (row.get("account_head") or "").strip()
        if account:
            totals[account] += flt(row.get("tax_amount"))
    return {k: _r(v) for k, v in totals.items()}


def document_excludes_snapshot_fees(doc, snapshot: dict) -> tuple[bool, str]:
    """
    A marked SO/SI must not still carry the snapshot's seller-fee rows.  A fee account may
    legitimately appear only if the snapshot does not own that amount (e.g. zero after
    offsets); the practical check is that no snapshot account carries its snapshot amount.
    """
    doc_taxes = _tax_rows_by_account(doc)
    for account, amount in snapshot_fee_accounts(snapshot).items():
        present = doc_taxes.get(account, 0.0)
        if abs(present) >= 0.005 and abs(present - amount) <= 0.005:
            return False, f"document still carries seller-fee rows for {account} ({present:.2f})"
    return True, "ok"


def _set_tax_rows(so, rows) -> None:
    """Replace so.taxes with the given row objects, renumber idx, and recalculate totals."""
    so.set("taxes", rows)
    for idx, row in enumerate(so.get("taxes") or [], start=1):
        row.idx = idx
    so.calculate_taxes_and_totals()


def plan_separate_fee_accounting(
    so, order: dict, charges_and_fees: dict, fee_components: list[tuple[str, dict]], settings,
) -> frappe._dict:
    """
    Decide whether a net-negative AFN order qualifies for the separate-fee path and, if so,
    strip the seller-fee rows from `so.taxes`, recalculate, and attach the frozen snapshot.

    `fee_components` is the list of (bucket, component dict) that create_sales_order placed
    on so.taxes from the `fees` / `service_fees` buckets of THIS Financial Events response.

    Returns frappe._dict(ok, reason, snapshot).  On failure the SO is left exactly as it was
    (net-invoice rows restored) so the caller can take its existing failure path.
    """
    precision = so.precision("grand_total") if hasattr(so, "precision") else _precision()
    summary = charges_and_fees.get("financial_event_summary") or {}
    projected_net_total = flt(so.grand_total, precision)

    if (so.fulfillment_channel or "").upper() != "AFN":
        return _status("not_applicable", "not an AFN order")
    for fieldname in ("custom_amazon_fee_accounting", "custom_amazon_fee_snapshot", "custom_amazon_fee_je"):
        if not _has_field("Sales Order", fieldname):
            return _status("rejected", f"Sales Order.{fieldname} is not installed; run setup_custom_fields() before this path can be used")
    if projected_net_total >= 0:
        return _status("not_applicable", "net-invoice total is not negative")
    if not fee_components:
        return _status("rejected", "net-negative total without seller-fee components; manual review")
    if summary.get("pagination_complete") is False:
        return _status("rejected", "Financial Events pagination was incomplete; not finalizing fee accounting")
    if int(summary.get("shipment_item_count") or 0) <= 0:
        return _status("rejected", "no shipment Financial Events; cannot freeze a fee snapshot")

    order_ccy = ((order.get("OrderTotal") or {}).get("CurrencyCode") or so.currency or "").upper()
    if order_ccy and (so.currency or "").upper() != order_ccy:
        return _status("rejected", f"currency mismatch: SO {so.currency} vs Orders API {order_ccy}")
    # Financial Events amounts must all be in the SO currency; never mix native amounts.
    seen = {str(c).upper() for c in (summary.get("currencies") or []) if c}
    for bucket in ("charges", "fees", "tds", "service_fees"):
        for component in charges_and_fees.get(bucket) or []:
            code = (component.get("currency") or "").strip().upper()
            if code:
                seen.add(code)
    foreign = sorted(c for c in seen if c != (so.currency or "").upper())
    if foreign:
        return _status("rejected", f"Financial Events components in {foreign} do not match SO currency {so.currency}")

    # Identify the fee rows by (account, amount, description); remove exactly one row per component.
    original_rows = list(so.get("taxes") or [])
    pending = [
        ((c.get("account_head") or "").strip(), _r(c.get("tax_amount")), (c.get("description") or "").strip())
        for _bucket, c in fee_components
    ]
    kept, removed = [], []
    for row in original_rows:
        key = ((row.get("account_head") or "").strip(), _r(row.get("tax_amount")), (row.get("description") or "").strip())
        if key in pending:
            pending.remove(key)
            removed.append(row)
        else:
            kept.append(row)
    if pending:
        return _status("rejected", f"could not locate {len(pending)} seller-fee row(s) on the Sales Order")

    _set_tax_rows(so, kept)
    sale_total = flt(so.grand_total, precision)
    if sale_total < 0:
        _set_tax_rows(so, original_rows)
        return _status(
            "rejected",
            f"excluding seller fees still leaves a negative sale document ({sale_total:.2f}); manual review",
        )

    snapshot = build_fee_snapshot(
        so, order, charges_and_fees, fee_components, settings,
        projected_net_total=projected_net_total, sale_total=sale_total,
    )
    ok, reason = verify_snapshot(snapshot)
    if not ok:
        _set_tax_rows(so, original_rows)
        return _status("rejected", f"snapshot validation failed: {reason}")

    so.custom_amazon_fee_accounting = FEE_ACCOUNTING_SEPARATE
    so.custom_amazon_fee_snapshot = json.dumps(snapshot, default=str)
    return _status("planned", "separate fee accounting", snapshot=snapshot, removed_rows=len(removed))


def is_validated_separate_fee_state(so) -> tuple[bool, str]:
    """True only for an explicit, fingerprint-verified Separate Fee JE state with no fee rows."""
    if (so.get("custom_amazon_fee_accounting") or "") != FEE_ACCOUNTING_SEPARATE:
        return False, "not marked"
    snapshot = sales_order_snapshot(so)
    ok, reason = verify_snapshot(snapshot)
    if not ok:
        return False, reason
    if (snapshot.get("amazon_order_id") or "") != (so.amazon_order_id or ""):
        return False, "snapshot order id mismatch"
    if (snapshot.get("company") or "") != (so.company or ""):
        return False, "snapshot company mismatch"
    if (snapshot.get("currency") or "") != (so.currency or "").upper():
        return False, "snapshot currency mismatch"
    if (snapshot.get("customer") or "") != (so.customer or ""):
        return False, f"snapshot customer {snapshot.get('customer')} != SO customer {so.customer}"
    if abs(flt(snapshot.get("conversion_rate")) - flt(so.conversion_rate)) > 1e-6:
        return False, f"snapshot conversion rate {snapshot.get('conversion_rate')} != SO {so.conversion_rate}"
    precision = so.precision("grand_total") if hasattr(so, "precision") else _precision()
    if abs(_r(snapshot.get("sale_total"), precision) - flt(so.grand_total, precision)) > 10 ** -precision / 2 + 1e-9:
        return False, f"snapshot sale total {snapshot.get('sale_total')} != SO grand total {so.grand_total}"
    if not snapshot.get("settings"):
        return False, "snapshot has no settings identity"
    ok, reason = document_excludes_snapshot_fees(so, snapshot)
    if not ok:
        return False, reason
    return True, "validated separate-fee state"


def validate_sales_order_fee_state(so) -> None:
    """Called from the Sales Order override's validate(); no DB writes, no commits."""
    if (so.get("custom_amazon_fee_accounting") or "") != FEE_ACCOUNTING_SEPARATE:
        return
    ok, reason = is_validated_separate_fee_state(so)
    if not ok:
        frappe.throw(_("Amazon separate-fee accounting state is invalid: {0}").format(reason))


# ──────────────────────────────────────────
# 4. Receivable resolution and fee Journal Entry construction
# ──────────────────────────────────────────
def find_sales_invoices_for_sales_order(so_name: str, *, docstatus=None, is_return: int | None = 0) -> list[frappe._dict]:
    """Sales Invoices linked to one SO through Sales Invoice Item.sales_order (lineage-safe)."""
    conditions = ["sii.sales_order = %(so)s"]
    params = {"so": so_name}
    if docstatus is not None:
        conditions.append("si.docstatus = %(docstatus)s")
        params["docstatus"] = docstatus
    else:
        conditions.append("si.docstatus < 2")
    if is_return is not None:
        conditions.append("si.is_return = %(is_return)s")
        params["is_return"] = is_return
    return frappe.db.sql(
        f"""
        SELECT DISTINCT si.name, si.docstatus, si.customer, si.debit_to, si.currency,
               si.conversion_rate, si.outstanding_amount, si.posting_date, si.company,
               si.is_return, si.grand_total, si.creation
        FROM `tabSales Invoice` si
        JOIN `tabSales Invoice Item` sii ON sii.parent = si.name
        WHERE {' AND '.join(conditions)}
        ORDER BY si.docstatus DESC, si.posting_date DESC, si.creation DESC
        """,
        params,
        as_dict=True,
    )


def resolve_fee_receivable(so, snapshot: dict) -> frappe._dict:
    """
    The receivable account/party the fee JE must credit: the invoice's debit_to when an SI
    exists (draft or submitted), else ERPNext's party-account resolver for the SO customer.
    Validates currency: the account must be in the order currency or the company currency.
    """
    company = so.company
    company_ccy = _company_currency(company)
    order_ccy = (snapshot.get("currency") or so.currency or company_ccy).upper()
    invoices = find_sales_invoices_for_sales_order(so.name)
    account, customer, source = None, so.customer, "party_account"
    if invoices:
        inv = invoices[0]
        account, customer, source = inv.debit_to, inv.customer, f"sales_invoice:{inv.name}"
        if (inv.currency or "").upper() != order_ccy:
            return _status("review", f"invoice {inv.name} currency {inv.currency} != order currency {order_ccy}")
    if not account:
        try:
            account = get_party_account("Customer", customer, company)
        except Exception:
            account = None
    if not account:
        return _status("review", f"no receivable account resolvable for customer {customer} in {company}")
    acct_ccy = _account_currency(account, company)
    if acct_ccy not in (order_ccy, company_ccy):
        return _status("review", f"receivable {account} currency {acct_ccy} is neither {order_ccy} nor {company_ccy}")
    return _status(
        "ok", "resolved", account=account, customer=customer, account_currency=acct_ccy,
        order_currency=order_ccy, company_currency=company_ccy, source=source,
    )


def _row_amounts(account: str, native_amount: float, order_ccy: str, company_ccy: str, rate: float, precision: int):
    """Return (amount_in_account_currency, exchange_rate) or (None, None) when unsupported."""
    acct_ccy = _account_currency(account)
    if acct_ccy == order_ccy:
        return flt(native_amount, precision), rate
    if acct_ccy == company_ccy:
        return flt(native_amount * rate, precision), 1.0
    return None, None


def build_fee_journal_entry_doc(so, snapshot: dict, receivable: frappe._dict, settings=None):
    """
    Construct (not insert) the fee Journal Entry from the frozen snapshot.

    * each signed component keeps its sign: negative fee -> debit fee account,
      positive fee credit/reversal -> credit fee account.
    * the receivable row carries the net: credit when fees are net negative.
    * conversion uses the SO's own conversion_rate (source accounting rate); only legitimate
      per-row rounding is absorbed by the configured round-off account inside a strict,
      precision-based tolerance.  Anything larger is a review case, never forced.
    """
    precision = _precision()
    company = so.company
    order_ccy = receivable.order_currency
    company_ccy = receivable.company_currency
    rate = flt(snapshot.get("conversion_rate")) or 1.0
    if order_ccy == company_ccy:
        rate = 1.0
    order_id = snapshot["amazon_order_id"]
    posting_date = snapshot.get("posting_date") or so.transaction_date
    cost_center = None
    for item in so.get("items") or []:
        if item.get("cost_center"):
            cost_center = item.cost_center
            break
    cost_center = cost_center or _default_cost_center(company)
    dimensions = _active_dimension_values(so)

    je = frappe.new_doc("Journal Entry")
    je.voucher_type = "Journal Entry"
    je.company = company
    je.posting_date = posting_date
    je.cheque_no = fee_je_cheque_no(order_id)
    je.cheque_date = posting_date
    je.user_remark = f"{FEE_JE_REMARK_PREFIX}{order_id}"
    je.amazon_order_id = order_id
    je.custom_amazon_source_sales_order = so.name
    je.custom_amazon_fee_fingerprint = snapshot.get("fingerprint")
    je.custom_amazon_settings = snapshot.get("settings") or (settings.name if settings else None)

    multi_currency = order_ccy != company_ccy
    base_debit = 0.0
    base_credit = 0.0

    for account, native in snapshot_fee_accounts(snapshot).items():
        if abs(native) < 0.005:
            continue
        amount, row_rate = _row_amounts(account, abs(native), order_ccy, company_ccy, rate, precision)
        if amount is None:
            return None, f"fee account {account} currency is neither {order_ccy} nor {company_ccy}"
        descriptions = sorted({c["description"] for c in snapshot["components"] if c["account_head"] == account and c.get("description")})
        row = {
            "account": account,
            "exchange_rate": row_rate,
            "cost_center": cost_center,
            "amazon_order_id": order_id,
            "custom_amazon_line_kind": LINE_KIND_AFN_FEE,
            "user_remark": ("; ".join(descriptions) or f"Amazon seller fee for {order_id}")[:140],
        }
        row.update(dimensions)
        if native < 0:
            row["debit_in_account_currency"] = amount
            base_debit += flt(amount * row_rate, precision)   # ERPNext rounds each row's base amount
        else:
            row["credit_in_account_currency"] = amount
            base_credit += flt(amount * row_rate, precision)
        if row_rate != 1.0:
            multi_currency = True
        je.append("accounts", row)

    net_credit = _r(snapshot.get("expected_receivable_credit"))
    amount, row_rate = _row_amounts(receivable.account, abs(net_credit), order_ccy, company_ccy, rate, precision)
    if amount is None:
        return None, f"receivable {receivable.account} currency unsupported"
    party_row = {
        "account": receivable.account,
        "party_type": "Customer",
        "party": receivable.customer,
        "exchange_rate": row_rate,
        "cost_center": cost_center,
        "amazon_order_id": order_id,
        "custom_amazon_line_kind": LINE_KIND_AFN_FEE,
        "user_remark": f"{FEE_JE_REMARK_PREFIX}{order_id}",
    }
    party_row.update(dimensions)
    if net_credit >= 0:
        party_row["credit_in_account_currency"] = amount
        base_credit += flt(amount * row_rate, precision)
    else:
        party_row["debit_in_account_currency"] = amount
        base_debit += flt(amount * row_rate, precision)
    if row_rate != 1.0:
        multi_currency = True
    je.append("accounts", party_row)

    # Per-row rounding tolerance: one currency unit of precision per converted row.
    difference = flt(base_debit - base_credit, precision)
    if abs(difference) > 0:
        tolerance = flt(len(je.accounts) * (10 ** -precision), precision)
        if abs(difference) > tolerance + 1e-9:
            return None, (
                f"fee JE base-currency imbalance {difference:.{precision}f} exceeds rounding tolerance "
                f"{tolerance:.{precision}f}; FX/rounding condition requires manual review"
            )
        round_off_account = getattr(settings, "custom_round_off_account", None) if settings else None
        if not round_off_account:
            round_off_account = frappe.get_cached_value("Company", company, "round_off_account")
        if not round_off_account:
            return None, "rounding difference present but no round-off account is configured"
        if _account_currency(round_off_account, company) != company_ccy:
            return None, f"round-off account {round_off_account} is not in company currency"
        round_row = {
            "account": round_off_account,
            "exchange_rate": 1.0,
            "cost_center": cost_center,
            "amazon_order_id": order_id,
            "custom_amazon_line_kind": LINE_KIND_AFN_FEE,
            "user_remark": "Currency rounding on Amazon AFN fee conversion",
        }
        if difference > 0:
            round_row["credit_in_account_currency"] = abs(difference)
        else:
            round_row["debit_in_account_currency"] = abs(difference)
        je.append("accounts", round_row)

    je.multi_currency = 1 if multi_currency else 0
    je.flags.ignore_mandatory = True
    return je, "ok"


def _je_rows(je_name: str) -> list[frappe._dict]:
    return frappe.get_all(
        "Journal Entry Account",
        filters={"parent": je_name},
        fields=[
            "name", "account", "party_type", "party", "exchange_rate", "idx", "user_remark",
            "debit_in_account_currency", "credit_in_account_currency", "debit", "credit",
            "reference_type", "reference_name", "amazon_order_id", "custom_amazon_line_kind",
        ],
        order_by="idx asc",
        parent_doctype="Journal Entry",
    )


def fee_journal_entry_matches_snapshot(je_name: str, so, snapshot: dict, receivable: frappe._dict) -> tuple[bool, str]:
    """Verify an existing fee JE by status-independent CONTENT (company, source SO, rows, party, amounts)."""
    je = frappe.db.get_value(
        "Journal Entry", je_name,
        ["company", "cheque_no", "custom_amazon_source_sales_order", "custom_amazon_fee_fingerprint", "amazon_order_id"],
        as_dict=True,
    )
    if not je:
        return False, "journal entry missing"
    if je.company != so.company:
        return False, f"company {je.company} != {so.company}"
    if je.cheque_no != fee_je_cheque_no(snapshot["amazon_order_id"]):
        return False, "cheque_no is not the deterministic fee key"
    if je.custom_amazon_source_sales_order and je.custom_amazon_source_sales_order != so.name:
        return False, f"posted for a different source Sales Order {je.custom_amazon_source_sales_order}"
    if je.custom_amazon_fee_fingerprint and je.custom_amazon_fee_fingerprint != snapshot.get("fingerprint"):
        return False, "fee fingerprint differs from the persisted snapshot"

    precision = _precision()
    order_ccy, company_ccy = receivable.order_currency, receivable.company_currency
    rate = flt(snapshot.get("conversion_rate")) or 1.0
    if order_ccy == company_ccy:
        rate = 1.0
    expected = snapshot_fee_accounts(snapshot)
    round_off_accounts = set()
    settings_name = snapshot.get("settings")
    if settings_name:
        acc = frappe.db.get_value("Amazon SP API Settings", settings_name, "custom_round_off_account")
        if acc:
            round_off_accounts.add(acc)
    company_round_off = frappe.get_cached_value("Company", so.company, "round_off_account")
    if company_round_off:
        round_off_accounts.add(company_round_off)

    actual = defaultdict(float)      # signed, in account currency (debit positive)
    actual_base = defaultdict(float) # signed, company currency (debit positive)
    party_net = 0.0                  # credit positive, in account currency
    party_net_base = 0.0             # credit positive, company currency
    party_seen = False
    rounding_total = 0.0
    tolerance = 0.5 * (10 ** -precision) + 1e-9

    def _expected_rate(account: str) -> float | None:
        acct_ccy = _account_currency(account, so.company)
        if acct_ccy == order_ccy:
            return rate
        if acct_ccy == company_ccy:
            return 1.0
        return None

    for row in _je_rows(je_name):
        signed = flt(row.debit_in_account_currency) - flt(row.credit_in_account_currency)
        signed_base = flt(row.debit) - flt(row.credit)
        if row.account == receivable.account and row.party_type == "Customer":
            if row.party != receivable.customer:
                return False, f"party {row.party} != expected {receivable.customer}"
            exp_rate = _expected_rate(row.account)
            if exp_rate is None or abs(flt(row.exchange_rate) - exp_rate) > 1e-6:
                return False, f"receivable row exchange rate {row.exchange_rate} != expected {exp_rate}"
            party_seen = True
            party_net += -signed
            party_net_base += -signed_base
        elif row.account in expected:
            exp_rate = _expected_rate(row.account)
            if exp_rate is None or abs(flt(row.exchange_rate) - exp_rate) > 1e-6:
                return False, f"fee row {row.account} exchange rate {row.exchange_rate} != expected {exp_rate}"
            actual[row.account] += signed
            actual_base[row.account] += signed_base
        elif row.account in round_off_accounts:
            if abs(flt(row.exchange_rate) - 1.0) > 1e-6 and abs(signed_base) >= 0.005:
                return False, f"rounding row {row.account} is not a company-currency row"
            rounding_total += signed_base
        else:
            return False, f"unexpected account {row.account} on fee JE"

    for account, native in expected.items():
        exp_amount, exp_rate = _row_amounts(account, -native, order_ccy, company_ccy, rate, precision)
        if exp_amount is None:
            return False, f"fee account {account} currency unsupported"
        if abs(flt(actual.get(account, 0.0), precision) - exp_amount) > tolerance:
            return False, f"fee account {account}: expected {exp_amount:.{precision}f}, found {actual.get(account, 0.0):.{precision}f}"
        exp_base = flt(exp_amount * exp_rate, precision)
        if abs(flt(actual_base.get(account, 0.0), precision) - exp_base) > tolerance:
            return False, f"fee account {account} base amount: expected {exp_base:.{precision}f}, found {actual_base.get(account, 0.0):.{precision}f}"
    if not party_seen:
        return False, "receivable row missing"
    exp_party, exp_party_rate = _row_amounts(receivable.account, _r(snapshot.get("expected_receivable_credit")), order_ccy, company_ccy, rate, precision)
    if abs(flt(party_net, precision) - exp_party) > tolerance:
        return False, f"receivable credit expected {exp_party:.{precision}f}, found {party_net:.{precision}f}"
    exp_party_base = flt(exp_party * exp_party_rate, precision)
    # Reconciliation splits the receivable row; each split is rounded, so allow one unit per split.
    party_rows = max(1, sum(1 for r in _je_rows(je_name) if r.account == receivable.account and r.party_type == "Customer"))
    if abs(flt(party_net_base, precision) - exp_party_base) > party_rows * (10 ** -precision) + 1e-9:
        return False, f"receivable base credit expected {exp_party_base:.{precision}f}, found {party_net_base:.{precision}f}"
    # Rounding row: only what per-row conversion legitimately needs, and it must balance the voucher.
    rounding_tolerance = (len(expected) + 2) * (10 ** -precision)
    if abs(rounding_total) > rounding_tolerance + 1e-9:
        return False, f"rounding row {rounding_total:.{precision}f} exceeds tolerance"
    expected_rounding = flt(sum(flt(actual_base.get(a, 0.0), precision) for a in expected) - flt(party_net_base, precision), precision)
    if abs(flt(-expected_rounding, precision) - flt(rounding_total, precision)) > party_rows * (10 ** -precision) + 1e-9:
        return False, f"rounding row {rounding_total:.{precision}f} does not balance the base-currency difference {expected_rounding:.{precision}f}"
    return True, "matches snapshot"


def _fee_journal_entries(order_id: str, company: str) -> list[frappe._dict]:
    return frappe.get_all(
        "Journal Entry",
        filters={"cheque_no": fee_je_cheque_no(order_id), "company": company},
        fields=["name", "docstatus", "custom_amazon_source_sales_order", "custom_amazon_fee_fingerprint", "creation"],
        order_by="creation asc",
    )


def _submit_and_confirm(je) -> tuple[bool, str]:
    """Submit and prove docstatus=1 from the database (submission may be queued/asynchronous)."""
    je.flags.ignore_mandatory = True
    je.submit()
    live = cint(frappe.db.get_value("Journal Entry", je.name, "docstatus"))
    if live == 1:
        return True, "submitted"
    return False, f"journal entry {je.name} submit did not become durable (docstatus={live}); retry later"


def ensure_fee_journal_entry(so_name: str, *, settings=None, allow_recreate_after_cancel: bool = False, commit: bool = True) -> frappe._dict:
    """
    Idempotently make sure the snapshot's fee Journal Entry exists and is submitted.

    Statuses: submitted | created | pending (transient; retry) | review (operator) | not_applicable.
    The Sales Order row is locked FOR UPDATE for the whole check-and-create so concurrent
    importers, invoice submitters and settlement workers serialize on the same order.
    """
    def _run():
        frappe.db.sql("SELECT name FROM `tabSales Order` WHERE name=%s FOR UPDATE", (so_name,))
        so = frappe.get_doc("Sales Order", so_name)
        if (so.get("custom_amazon_fee_accounting") or "") != FEE_ACCOUNTING_SEPARATE:
            return _status("not_applicable", "sales order is not marked Separate Fee JE")
        if cint(so.docstatus) != 1:
            return _status("pending", f"source Sales Order {so_name} is not submitted (docstatus={so.docstatus})")
        ok, reason = is_validated_separate_fee_state(so)
        if not ok:
            record_review(so.amazon_order_id, f"{so_name}: {reason}")
            return _status("review", reason)
        snapshot = sales_order_snapshot(so)
        order_id = snapshot["amazon_order_id"]
        receivable = resolve_fee_receivable(so, snapshot)
        if receivable.status != "ok":
            record_review(order_id, f"{so_name}: {receivable.reason}")
            return _status("review", receivable.reason)

        rows = _fee_journal_entries(order_id, so.company)
        submitted = [r for r in rows if cint(r.docstatus) == 1]
        drafts = [r for r in rows if cint(r.docstatus) == 0]
        cancelled = [r for r in rows if cint(r.docstatus) == 2]

        if len(submitted) > 1:
            reason = f"duplicate submitted fee JEs {[r.name for r in submitted]}"
            record_review(order_id, reason)
            return _status("review", reason, journal_entry=submitted[0].name)
        if submitted:
            je_name = submitted[0].name
            ok, reason = fee_journal_entry_matches_snapshot(je_name, so, snapshot, receivable)
            if not ok:
                record_review(order_id, f"submitted fee JE {je_name} does not match snapshot: {reason}")
                return _status("review", reason, journal_entry=je_name)
            if (so.get("custom_amazon_fee_je") or "") != je_name:
                frappe.db.set_value("Sales Order", so_name, "custom_amazon_fee_je", je_name, update_modified=False)
            return _status("submitted", "fee JE already submitted", journal_entry=je_name)

        if len(drafts) > 1:
            reason = f"multiple draft fee JEs {[r.name for r in drafts]}"
            record_review(order_id, reason)
            return _status("review", reason)
        if drafts:
            je_name = drafts[0].name
            ok, reason = fee_journal_entry_matches_snapshot(je_name, so, snapshot, receivable)
            if not ok:
                record_review(order_id, f"draft fee JE {je_name} does not match snapshot: {reason}; not submitted")
                return _status("review", reason, journal_entry=je_name)
            frappe.db.savepoint("afn_fee_je_finish_draft")
            try:
                je = frappe.get_doc("Journal Entry", je_name)
                ok, reason = _submit_and_confirm(je)
            except Exception:
                frappe.db.rollback(save_point="afn_fee_je_finish_draft")
                frappe.log_error(title=f"AFN fee JE draft submit {order_id}"[:140], message=frappe.get_traceback())
                return _status("pending", f"draft fee JE {je_name} could not be submitted; see Error Log")
            if not ok:
                return _status("pending", reason, journal_entry=je_name)
            frappe.db.set_value("Sales Order", so_name, "custom_amazon_fee_je", je_name, update_modified=False)
            return _status("created", "existing draft fee JE submitted", journal_entry=je_name)

        if cancelled and not allow_recreate_after_cancel:
            reason = (
                f"cancelled fee JE {[r.name for r in cancelled]} exists; automatic recreation is disabled. "
                "Re-run recovery with allow_recreate_after_cancel=True after confirming the cancellation was wrong."
            )
            record_review(order_id, reason)
            return _status("review", reason)

        je, reason = build_fee_journal_entry_doc(so, snapshot, receivable, settings or _settings_doc(snapshot.get("settings")))
        if je is None:
            record_review(order_id, f"{so_name}: {reason}")
            return _status("review", reason)
        frappe.db.savepoint("afn_fee_je_create")
        try:
            je.insert(ignore_permissions=True)
            ok, reason = _submit_and_confirm(je)
        except Exception:
            frappe.db.rollback(save_point="afn_fee_je_create")
            frappe.log_error(title=f"AFN fee JE posting {order_id}"[:140], message=frappe.get_traceback())
            return _status("pending", "fee JE failed to post; see Error Log (retried by the invoice gate)")
        if not ok:
            return _status("pending", reason, journal_entry=je.name)
        frappe.db.set_value("Sales Order", so_name, "custom_amazon_fee_je", je.name, update_modified=False)
        print(f"[AFN-FEE] posted fee JE {je.name} for {order_id} ({so_name})", flush=True)
        return _status("created", "fee JE posted", journal_entry=je.name)

    try:
        result = _with_lock_retry(_run)
    except Exception:
        try:
            frappe.db.rollback()
        except Exception:
            pass
        frappe.log_error(title=f"AFN fee JE lifecycle {so_name}"[:140], message=frappe.get_traceback())
        return _status("pending", "unexpected error; see Error Log")
    if commit:
        frappe.db.commit()
    return result


def _settings_doc(name: str | None):
    if name and frappe.db.exists("Amazon SP API Settings", name):
        return frappe.get_doc("Amazon SP API Settings", name)
    return None


# ──────────────────────────────────────────
# 5. Allocation of the fee credit (SI first, then negative-sale settlement debits)
# ──────────────────────────────────────────
def assert_fields_installed() -> None:
    missing = []
    for doctype, specs in _custom_field_specs().items():
        for spec in specs:
            if not _has_field(doctype, spec["fieldname"]):
                missing.append(f"{doctype}.{spec['fieldname']}")
    if missing:
        frappe.throw(
            _("AFN separate-fee custom fields are missing: {0}. Run setup_custom_fields() first.").format(", ".join(missing))
        )


def _unreferenced_credit_rows(je_name: str, account: str, party: str) -> list[frappe._dict]:
    """Live, unreferenced customer credit rows of a submitted JE for one account/party."""
    return frappe.db.sql(
        """
        SELECT jea.name, jea.idx, jea.exchange_rate, jea.is_advance, jea.cost_center,
               jea.credit_in_account_currency, jea.debit_in_account_currency, jea.account, jea.party
        FROM `tabJournal Entry Account` jea
        JOIN `tabJournal Entry` je ON je.name = jea.parent
        WHERE je.name = %s AND je.docstatus = 1
          AND jea.account = %s AND jea.party_type = 'Customer' AND jea.party = %s
          AND jea.credit_in_account_currency > 0
          AND IFNULL(jea.reference_type, '') = ''
        ORDER BY jea.idx ASC
        """,
        (je_name, account, party),
        as_dict=True,
    )


def _allocated_against_journal(against_je: str, order_id: str, account: str, party: str) -> float:
    """Credits already applied (by any submitted JE row of this order) against one JE 'invoice'."""
    value = frappe.db.sql(
        """
        SELECT IFNULL(SUM(jea.credit_in_account_currency - jea.debit_in_account_currency), 0)
        FROM `tabJournal Entry Account` jea
        JOIN `tabJournal Entry` je ON je.name = jea.parent
        WHERE je.docstatus = 1
          AND jea.reference_type = 'Journal Entry' AND jea.reference_name = %s
          AND jea.amazon_order_id = %s AND jea.account = %s AND jea.party = %s
        """,
        (against_je, order_id, account, party),
    )
    return flt(value[0][0]) if value else 0.0


def find_settlement_debit_rows(order_id: str, company: str) -> list[frappe._dict]:
    """
    Every still-unreferenced customer DEBIT row stamped with this order on a submitted
    settlement Journal Entry (any report, any age).  Fee/postage/repair/reclass vouchers and
    system exchange gain/loss journals are excluded by their deterministic keys.
    """
    rows = frappe.db.sql(
        """
        SELECT je.name AS je_name, je.cheque_no, je.posting_date, je.voucher_type,
               jea.name, jea.idx, jea.account, jea.party_type, jea.party, jea.exchange_rate,
               jea.debit_in_account_currency, jea.credit_in_account_currency,
               jea.user_remark, jea.custom_amazon_line_kind, jea.cost_center
        FROM `tabJournal Entry` je
        JOIN `tabJournal Entry Account` jea ON jea.parent = je.name
        WHERE je.docstatus = 1 AND je.company = %s
          AND jea.amazon_order_id = %s
          AND jea.party_type = 'Customer'
          AND jea.debit_in_account_currency > 0
          AND IFNULL(jea.reference_type, '') = ''
          AND IFNULL(je.voucher_type, '') != 'Exchange Gain Or Loss'
        ORDER BY je.posting_date ASC, je.creation ASC, jea.idx ASC
        """,
        (company, order_id),
        as_dict=True,
    )
    result = []
    for row in rows:
        cheque = str(row.cheque_no or "")
        if cheque.startswith(NON_SETTLEMENT_CHEQUE_PREFIXES) or any(m in cheque for m in NON_SETTLEMENT_CHEQUE_MARKERS):
            continue
        result.append(row)
    return result


def classify_settlement_debit_row(row: frappe._dict, report_rows: list[dict] | None = None,
                                  report_id: str | None = None) -> str:
    """
    'sale' (negative-sale residual), 'refund', or 'ambiguous'.

    Newly created rows carry custom_amazon_line_kind.  Legacy rows are classified from the
    stamped remark (settlement labels end in ' Order' / ' Refund') or, when the row belongs to
    `report_id`, from that report's own rows for the order.  Evidence from a different report
    is never applied, and nothing is guessed from order id + amount alone.
    """
    kind = (row.get("custom_amazon_line_kind") or "").strip()
    if kind == LINE_KIND_SETTLEMENT_SALE:
        return "sale"
    if kind == LINE_KIND_SETTLEMENT_REFUND:
        return "refund"
    remark = (row.get("user_remark") or "").strip()
    if remark.endswith(" Order"):
        return "sale"
    if remark.endswith(" Refund"):
        return "refund"
    row_report = str(row.get("cheque_no") or "").strip()
    if report_rows and report_id and row_report and row_report == str(report_id).strip():
        sale_total = sum(
            flt(r.get("amount")) for r in report_rows
            if (r.get("transaction-type") or "").strip().lower() in ("order", "order_retrocharge")
        )
        has_refund = any((r.get("transaction-type") or "").strip().lower() == "refund" for r in report_rows)
        if sale_total < -0.004 and not has_refund:
            return "sale"
        if has_refund and sale_total >= -0.004:
            return "refund"
    return "ambiguous"


def _gain_loss_posting_date(payment_posting_date, invoice_posting_date):
    setting = frappe.db.get_single_value("Accounts Settings", "exchange_gain_loss_posting_date", cache=True)
    if setting == "Invoice" and invoice_posting_date:
        return invoice_posting_date
    if setting == "Reconciliation Date":
        return nowdate()
    return payment_posting_date


def _build_reconcile_args(
    *, fee_je: str, credit_row: frappe._dict, against_type: str, against_name: str, apply: float,
    invoice_rate: float, invoice_posting_date, receivable: frappe._dict, company: str,
) -> tuple[frappe._dict | None, str]:
    """Same argument shape Payment Reconciliation hands to reconcile_against_document()."""
    precision = _precision()
    live_credit = flt(credit_row.credit_in_account_currency)
    difference_amount = 0.0
    difference_account = None
    payment_rate = flt(credit_row.exchange_rate) or 1.0
    invoice_rate = flt(invoice_rate) or 1.0
    if receivable.account_currency != receivable.company_currency and abs(payment_rate - invoice_rate) > 1e-9:
        difference_amount = flt(
            flt(payment_rate * flt(apply, precision), precision) - flt(invoice_rate * flt(apply, precision), precision),
            precision,
        )
        if abs(difference_amount) >= 10 ** -precision:
            difference_account = frappe.get_cached_value("Company", company, "exchange_gain_loss_account")
            if not difference_account:
                return None, (
                    f"exchange difference {difference_amount:.{precision}f} on {against_type} {against_name} needs "
                    f"Company.exchange_gain_loss_account; allocation left pending"
                )
        else:
            difference_amount = 0.0
    fee_posting_date = frappe.db.get_value("Journal Entry", fee_je, "posting_date")
    return frappe._dict({
        "voucher_type": "Journal Entry",
        "voucher_no": fee_je,
        "voucher_detail_no": credit_row.name,
        "against_voucher_type": against_type,
        "against_voucher": against_name,
        "account": receivable.account,
        "exchange_rate": invoice_rate,
        "party_type": "Customer",
        "party": receivable.customer,
        "is_advance": credit_row.is_advance,
        "dr_or_cr": "credit_in_account_currency",
        "unreconciled_amount": live_credit,
        "unadjusted_amount": live_credit,
        "allocated_amount": flt(apply, precision),
        "difference_amount": difference_amount,
        "difference_account": difference_account,
        "difference_posting_date": _gain_loss_posting_date(fee_posting_date, invoice_posting_date),
        "cost_center": credit_row.cost_center,
        "precision": precision,
    }), "ok"


def _reconcile(args: frappe._dict, commit: bool) -> tuple[bool, str]:
    frappe.db.savepoint("afn_fee_reconcile")
    try:
        reconcile_against_document([args])
    except Exception:
        try:
            frappe.db.rollback(save_point="afn_fee_reconcile")
        except Exception:
            frappe.db.rollback()
        frappe.log_error(
            title=f"AFN fee allocation {args.voucher_no} -> {args.against_voucher}"[:140],
            message=frappe.get_traceback(),
        )
        return False, f"reconciliation {args.voucher_no} -> {args.against_voucher_type} {args.against_voucher} failed; see Error Log"
    finally:
        frappe.flags.ignore_party_validation = False
    if commit:
        frappe.db.commit()
    return True, "ok"


def _live_settlement_remaining(je_name: str, order_id: str, account: str, party: str, report_id: str | None = None,
                               report_rows: list[dict] | None = None) -> float:
    """
    Live allocatable balance of this order's NEGATIVE-SALE debit rows on one settlement voucher
    (account/party): unreferenced sale-kind debit rows minus credits already referenced against
    the voucher for this order.  Re-read under lock before every allocation step.
    """
    rows = frappe.db.sql(
        """
        SELECT jea.name, jea.idx, jea.debit_in_account_currency, jea.user_remark, jea.custom_amazon_line_kind,
               je.cheque_no
        FROM `tabJournal Entry Account` jea
        JOIN `tabJournal Entry` je ON je.name = jea.parent
        WHERE je.name = %s AND je.docstatus = 1
          AND jea.amazon_order_id = %s AND jea.account = %s AND jea.party_type = 'Customer' AND jea.party = %s
          AND jea.debit_in_account_currency > 0
          AND IFNULL(jea.reference_type, '') = ''
        """,
        (je_name, order_id, account, party),
        as_dict=True,
    )
    debit_total = sum(
        flt(r.debit_in_account_currency) for r in rows
        if classify_settlement_debit_row(r, report_rows, report_id) == "sale"
    )
    allocated = _allocated_against_journal(je_name, order_id, account, party)
    return flt(debit_total - max(allocated, 0.0), _precision())


def _settlement_target_detail(je_name: str, order_id: str, account: str, party: str, report_id: str | None = None,
                              report_rows: list[dict] | None = None) -> frappe._dict:
    """Live sale-kind remainder plus the number of rows whose sale/refund identity is ambiguous."""
    rows = frappe.db.sql(
        """
        SELECT jea.name, jea.idx, jea.debit_in_account_currency, jea.user_remark, jea.custom_amazon_line_kind,
               je.cheque_no
        FROM `tabJournal Entry Account` jea
        JOIN `tabJournal Entry` je ON je.name = jea.parent
        WHERE je.name = %s AND je.docstatus = 1
          AND jea.amazon_order_id = %s AND jea.account = %s AND jea.party_type = 'Customer' AND jea.party = %s
          AND jea.debit_in_account_currency > 0
          AND IFNULL(jea.reference_type, '') = ''
        """,
        (je_name, order_id, account, party),
        as_dict=True,
    )
    kinds = [classify_settlement_debit_row(r, report_rows, report_id) for r in rows]
    sale_total = sum(flt(r.debit_in_account_currency) for r, k in zip(rows, kinds) if k == "sale")
    allocated = _allocated_against_journal(je_name, order_id, account, party)
    return frappe._dict(
        remaining=flt(sale_total - max(allocated, 0.0), _precision()),
        ambiguous=sum(1 for k in kinds if k == "ambiguous"),
        unreferenced_rows=len(rows),
    )


def _live_reclass_debit_remaining(reclass_je: str, order_id: str, account: str, party: str) -> float:
    """Open part of a reclassification JE's debit leg (invoice receivable/customer)."""
    rows = frappe.db.sql(
        """
        SELECT IFNULL(SUM(jea.debit_in_account_currency), 0)
        FROM `tabJournal Entry Account` jea JOIN `tabJournal Entry` je ON je.name = jea.parent
        WHERE je.name = %s AND je.docstatus = 1 AND jea.amazon_order_id = %s
          AND jea.account = %s AND jea.party_type = 'Customer' AND jea.party = %s
          AND jea.debit_in_account_currency > 0 AND IFNULL(jea.reference_type, '') = ''
        """,
        (reclass_je, order_id, account, party),
    )
    debit_total = flt(rows[0][0]) if rows else 0.0
    return flt(debit_total - max(_allocated_against_journal(reclass_je, order_id, account, party), 0.0), _precision())


def _apply_credit_rows(
    *, fee_je: str, receivable: frappe._dict, company: str, against_type: str, against_name: str,
    remaining_fn, invoice_rate: float, invoice_posting_date, commit: bool,
) -> tuple[float, str | None]:
    """
    Apply live, unreferenced credit rows of `fee_je` to one target until the target or the credit
    is exhausted.  `remaining_fn()` recomputes the target's live allocatable balance AFTER the
    row locks are (re-)acquired, so a per-step commit can never leave a stale target amount.
    """
    precision = _precision()
    applied_total = 0.0
    while True:
        # Re-establish row locks: a per-step commit releases them, and reconciliation splits rows.
        _lock_rows("Journal Entry", [fee_je] + ([against_name] if against_type == "Journal Entry" else []))
        if against_type == "Sales Invoice":
            _lock_rows("Sales Invoice", [against_name])
            if cint(frappe.db.get_value("Sales Invoice", against_name, "docstatus")) != 1:
                return applied_total, f"invoice {against_name} is no longer submitted"
        elif cint(frappe.db.get_value("Journal Entry", against_name, "docstatus")) != 1:
            return applied_total, f"journal entry {against_name} is no longer submitted"
        target_remaining = flt(remaining_fn(), precision)          # live, under lock
        if target_remaining < 10 ** -precision:
            break
        credit_rows = _unreferenced_credit_rows(fee_je, receivable.account, receivable.customer)  # refreshed: rows split on each allocation
        if not credit_rows:
            break
        row = credit_rows[0]
        apply = flt(min(target_remaining, flt(row.credit_in_account_currency)), precision)
        if apply < 10 ** -precision:
            break
        args, reason = _build_reconcile_args(
            fee_je=fee_je, credit_row=row, against_type=against_type, against_name=against_name,
            apply=apply, invoice_rate=invoice_rate, invoice_posting_date=invoice_posting_date,
            receivable=receivable, company=company,
        )
        if args is None:
            return applied_total, reason
        ok, reason = _reconcile(args, commit)
        if not ok:
            return applied_total, reason
        applied_total += apply
        print(f"[AFN-FEE] applied {apply:.{precision}f} from {fee_je} to {against_type} {against_name}", flush=True)
    return applied_total, None


def _invoice_remaining_fn(invoice_name: str):
    precision = _precision()
    return lambda: max(flt(frappe.db.get_value("Sales Invoice", invoice_name, "outstanding_amount"), precision), 0.0)


def _reclass_journal_entries(order_id: str, company: str) -> list[frappe._dict]:
    fields = ["name", "docstatus", "cheque_no", "posting_date"]
    if _has_field("Journal Entry", "custom_amazon_settlement_voucher"):
        fields.append("custom_amazon_settlement_voucher")
    return frappe.get_all(
        "Journal Entry",
        filters={"cheque_no": ["like", f"{FEE_RECLASS_CHEQUE_PREFIX}{order_id}-%"], "company": company},
        fields=fields, order_by="creation asc",
    )


def _finish_reclass_credit_leg(order_id: str, reclass: frappe._dict, company: str, company_ccy: str, order_ccy: str,
                               commit: bool, report_id: str | None = None,
                               report_rows: list[dict] | None = None) -> tuple[float, str | None]:
    """
    Resume leg 2 of a reclassification (reclass credit -> legacy settlement debit) from the
    reclass voucher itself, independent of whether the original fee credit still exists.
    Report evidence (`report_id`/`report_rows`) is forwarded so a legacy untagged debit that is
    identifiable only through its own report is still a valid target; an unidentifiable target
    returns an explicit reason instead of silently applying nothing.
    """
    settlement_je = reclass.get("custom_amazon_settlement_voucher")
    if not settlement_je:
        return 0.0, f"reclassification {reclass.name} does not record its settlement voucher; finish it manually"
    credit_rows = frappe.db.sql(
        """
        SELECT jea.account, jea.party, jea.exchange_rate
        FROM `tabJournal Entry Account` jea
        WHERE jea.parent = %s AND jea.credit_in_account_currency > 0 AND jea.party_type = 'Customer'
          AND IFNULL(jea.reference_type, '') = '' AND jea.amazon_order_id = %s
        ORDER BY jea.idx ASC LIMIT 1
        """,
        (reclass.name, order_id),
        as_dict=True,
    )
    if not credit_rows:
        return 0.0, None   # leg already finished
    row = credit_rows[0]
    legacy = frappe._dict(
        account=row.account, customer=row.party, account_currency=_account_currency(row.account, company),
        company_currency=company_ccy, order_currency=order_ccy,
    )
    settlement_rate = flt(frappe.db.get_value(
        "Journal Entry Account",
        {"parent": settlement_je, "amazon_order_id": order_id, "account": row.account, "party": row.party,
         "debit_in_account_currency": [">", 0]},
        "exchange_rate",
    )) or flt(row.exchange_rate) or 1.0
    applied, reason = _apply_credit_rows(
        fee_je=reclass.name, receivable=legacy, company=company, against_type="Journal Entry",
        against_name=settlement_je,
        remaining_fn=lambda: _live_settlement_remaining(settlement_je, order_id, row.account, row.party, report_id, report_rows),
        invoice_rate=settlement_rate, invoice_posting_date=None, commit=commit,
    )
    if reason:
        return applied, reason
    still_open = frappe.db.sql(
        """
        SELECT IFNULL(SUM(jea.credit_in_account_currency), 0) FROM `tabJournal Entry Account` jea
        WHERE jea.parent = %s AND jea.amazon_order_id = %s AND jea.party_type = 'Customer'
          AND jea.credit_in_account_currency > 0 AND IFNULL(jea.reference_type, '') = ''
        """,
        (reclass.name, order_id),
    )
    open_credit = flt(still_open[0][0]) if still_open else 0.0
    if open_credit >= 10 ** -_precision():
        detail = _settlement_target_detail(settlement_je, order_id, row.account, row.party, report_id, report_rows)
        if detail.ambiguous:
            return applied, (
                f"reclassification {reclass.name} credit {open_credit:.2f} is open: settlement {settlement_je} has "
                f"{detail.ambiguous} debit row(s) of ambiguous sale/refund identity for {order_id}; manual review"
            )
        return applied, (
            f"reclassification {reclass.name} credit {open_credit:.2f} is open but settlement {settlement_je} has no "
            f"identifiable negative-sale debit remaining for {order_id} ({row.account}/{row.party}); pending review"
        )
    return applied, None


def _reclassify_fee_credit_to_settlement_row(
    *, order_id: str, so, receivable: frappe._dict, fee_je: str, group: frappe._dict,
    remaining: float, settings, commit: bool, report_id: str | None = None,
    report_rows: list[dict] | None = None,
) -> tuple[float, str | None]:
    """
    Legacy party/account mismatch: the settlement debit sits on a different receivable
    account or party than the invoice.  Post one explicitly linked reclassification JE
    (debit invoice receivable/customer, credit settlement receivable/party) and reconcile
    both legs with ERPNext's engine.  Only same-currency, same-rate cases are automated.
    Both legs are resume-safe; the reclass voucher records its settlement voucher so an
    interrupted leg 2 can be finished later even when the fee credit is exhausted.
    """
    precision = _precision()
    company = so.company
    settlement_ccy = _account_currency(group.account, company)
    if settlement_ccy != receivable.account_currency:
        return 0.0, (
            f"settlement row {group.je_name} receivable currency {settlement_ccy} != invoice receivable "
            f"currency {receivable.account_currency}; manual reclassification required"
        )
    fee_rows = _unreferenced_credit_rows(fee_je, receivable.account, receivable.customer)
    if not fee_rows:
        return 0.0, None
    fee_rate = flt(fee_rows[0].exchange_rate) or 1.0
    if receivable.account_currency != receivable.company_currency and abs(fee_rate - flt(group.exchange_rate)) > 1e-9:
        return 0.0, (
            f"settlement {group.je_name} rate {group.exchange_rate} != fee JE rate {fee_rate} on a foreign-currency "
            "receivable with a party/account mismatch; use Payment Reconciliation manually so exchange gain/loss is booked"
        )
    apply = flt(min(remaining, sum(flt(r.credit_in_account_currency) for r in fee_rows)), precision)
    if apply < 10 ** -precision:
        return 0.0, None

    cheque = fee_reclass_cheque_no(order_id, group.je_name)
    existing = frappe.db.get_value(
        "Journal Entry", {"cheque_no": cheque, "company": company, "docstatus": ["!=", 2]},
        ["name", "docstatus"], as_dict=True, order_by="creation asc",
    )
    if existing and cint(existing.docstatus) == 0:
        return 0.0, f"draft reclassification JE {existing.name} exists; submit or delete it before retrying"
    reclass_name = existing.name if existing else None
    if not reclass_name:
        posting_date = max(getdate(group.posting_date), getdate(frappe.db.get_value("Journal Entry", fee_je, "posting_date")))
        cost_center = fee_rows[0].cost_center or _default_cost_center(company)
        je = frappe.new_doc("Journal Entry")
        je.voucher_type = "Journal Entry"
        je.company = company
        je.posting_date = posting_date
        je.cheque_no = cheque
        je.cheque_date = posting_date
        je.multi_currency = 1 if receivable.account_currency != receivable.company_currency else 0
        je.amazon_order_id = order_id
        je.custom_amazon_source_sales_order = so.name
        je.custom_amazon_settings = settings.name if settings else None
        je.custom_amazon_settlement_voucher = group.je_name
        je.user_remark = f"Amazon AFN fee credit reclassification for {order_id}: {fee_je} -> settlement {group.je_name}"
        je.append("accounts", {
            "account": receivable.account, "party_type": "Customer", "party": receivable.customer,
            "exchange_rate": fee_rate, "debit_in_account_currency": apply, "cost_center": cost_center,
            "amazon_order_id": order_id, "custom_amazon_line_kind": LINE_KIND_AFN_FEE_RECLASS,
            "user_remark": f"Move AFN fee credit for {order_id} to legacy settlement receivable",
        })
        je.append("accounts", {
            "account": group.account, "party_type": "Customer", "party": group.party,
            "exchange_rate": fee_rate, "credit_in_account_currency": apply, "cost_center": cost_center,
            "amazon_order_id": order_id, "custom_amazon_line_kind": LINE_KIND_AFN_FEE_RECLASS,
            "user_remark": f"Offset legacy settlement debit {group.je_name} for {order_id}",
        })
        frappe.db.savepoint("afn_fee_reclass")
        try:
            je.flags.ignore_mandatory = True
            je.insert(ignore_permissions=True)
            ok, reason = _submit_and_confirm(je)
            if not ok:
                return 0.0, reason
        except Exception:
            frappe.db.rollback(save_point="afn_fee_reclass")
            frappe.log_error(title=f"AFN fee reclassification {order_id}"[:140], message=frappe.get_traceback())
            return 0.0, "reclassification JE failed to post; see Error Log"
        if commit:
            frappe.db.commit()
        reclass_name = je.name
        print(f"[AFN-FEE] posted reclassification {reclass_name} for {order_id}", flush=True)

    # Leg 1: fee credit -> reclassification debit (live remaining of that debit leg).
    applied, reason = _apply_credit_rows(
        fee_je=fee_je, receivable=receivable, company=company, against_type="Journal Entry",
        against_name=reclass_name,
        remaining_fn=lambda: _live_reclass_debit_remaining(reclass_name, order_id, receivable.account, receivable.customer),
        invoice_rate=fee_rate, invoice_posting_date=None, commit=commit,
    )
    if reason:
        return applied, reason

    # Leg 2: reclassification credit -> settlement debit on the legacy account/party.
    reclass = frappe._dict(name=reclass_name, custom_amazon_settlement_voucher=group.je_name)
    _applied2, reason = _finish_reclass_credit_leg(
        order_id, reclass, company, receivable.company_currency, receivable.order_currency, commit,
        report_id=report_id, report_rows=report_rows,
    )
    if reason:
        return applied, reason
    return applied, None


def _set_fee_status(so_name: str, status: str, reason: str = "") -> None:
    """Allocation cursor/status on the Sales Order (db-level write; never touches economics)."""
    if not (_has_field("Sales Order", "custom_amazon_fee_status") and _has_field("Sales Order", "custom_amazon_fee_last_retry")):
        return
    text = status if not reason else f"{status}: {reason}"
    try:
        frappe.db.set_value("Sales Order", so_name, {
            "custom_amazon_fee_status": text[:140],
            "custom_amazon_fee_last_retry": now_datetime(),
        }, update_modified=False)
    except Exception:
        frappe.log_error(title=f"AFN fee status stamp {so_name}"[:140], message=frappe.get_traceback())


def allocate_fee_credit_for_order(order_id: str, *, company: str | None = None, settings=None,
                                  report_id: str | None = None, report_rows: list[dict] | None = None,
                                  commit: bool = True) -> frappe._dict:
    """
    Apply the fee JE's customer credit to (1) the order's submitted Sales Invoice, bounded by
    live outstanding and live credit, then (2) the matching negative-sale settlement debits
    of the same company/order/account/party, through reconcile_against_document(), and
    (3) finish any interrupted reclassification credit legs.

    Completion means the WHOLE voucher chain is closed: no unreferenced fee credit, no open
    reclassification leg, and every known sale-kind settlement debit of the order covered.
    Safe to repeat; every step re-reads live rows under lock and commits on its own.
    `report_rows` evidence is only used for legacy rows of `report_id`.
    """
    order_id = str(order_id or "").strip()
    assert_fields_installed()
    precision = _precision()

    def _run():
        filters = {"amazon_order_id": order_id, "docstatus": 1, "custom_amazon_fee_accounting": FEE_ACCOUNTING_SEPARATE}
        if company:
            filters["company"] = company
        so_rows = frappe.get_all("Sales Order", filters=filters, fields=["name", "company"], order_by="creation asc")
        if not so_rows:
            return _status("not_applicable", "no submitted Separate-Fee Sales Order for this order")
        if len(so_rows) > 1:
            reason = f"multiple submitted Separate-Fee Sales Orders {[r.name for r in so_rows]}"
            record_review(order_id, reason)
            return _status("review", reason)
        so_name = so_rows[0].name

        fee = ensure_fee_journal_entry(so_name, settings=settings, commit=False)
        if fee.status not in ("submitted", "created"):
            _set_fee_status(so_name, fee.status, fee.reason)
            return fee
        fee_je = fee.journal_entry
        so = frappe.get_doc("Sales Order", so_name)
        snapshot = sales_order_snapshot(so)
        receivable = resolve_fee_receivable(so, snapshot)
        if receivable.status != "ok":
            record_review(order_id, receivable.reason)
            _set_fee_status(so_name, "review", receivable.reason)
            return _status("review", receivable.reason, journal_entry=fee_je)

        settlement_rows = find_settlement_debit_rows(order_id, so.company)
        reclass_jes = [r for r in _reclass_journal_entries(order_id, so.company) if cint(r.docstatus) == 1]
        invoices = find_sales_invoices_for_sales_order(so_name, docstatus=1, is_return=0)
        draft_invoices = find_sales_invoices_for_sales_order(so_name, docstatus=0, is_return=0)
        # Lock order: Sales Order (already held) -> Journal Entries -> Sales Invoices.
        _lock_rows("Journal Entry", [fee_je] + [r.je_name for r in settlement_rows] + [r.name for r in reclass_jes])
        _lock_rows("Sales Invoice", [inv.name for inv in invoices])

        pending, review = [], []
        applied_invoice = applied_settlement = 0.0

        def _fee_credit_left() -> float:
            return sum(flt(r.credit_in_account_currency) for r in _unreferenced_credit_rows(fee_je, receivable.account, receivable.customer))

        # Step A: invoice(s) of this Sales Order (always first).
        if _fee_credit_left() >= 10 ** -precision:
            for inv in sorted(invoices, key=lambda i: (str(i.posting_date), i.creation)):
                if inv.debit_to != receivable.account or inv.customer != receivable.customer:
                    review.append(f"invoice {inv.name} is on {inv.customer}/{inv.debit_to}, fee JE on {receivable.customer}/{receivable.account}")
                    continue
                applied, reason = _apply_credit_rows(
                    fee_je=fee_je, receivable=receivable, company=so.company, against_type="Sales Invoice",
                    against_name=inv.name, remaining_fn=_invoice_remaining_fn(inv.name),
                    invoice_rate=flt(inv.conversion_rate) or 1.0, invoice_posting_date=inv.posting_date, commit=commit,
                )
                applied_invoice += applied
                if reason:
                    pending.append(reason)
                    _set_fee_status(so_name, "pending", reason)
                    return _status("pending", "; ".join(pending), journal_entry=fee_je, applied_to_invoice=applied_invoice,
                                   applied_to_settlement=applied_settlement, pending=pending)
            if not invoices:
                # The invoice always consumes the fee credit first; never let a settlement debit take
                # it while the sale document is still Draft (or missing after a failed SO hook).
                if draft_invoices:
                    reason = f"sales invoice {draft_invoices[0].name} is still Draft; settlement allocation waits for its submission"
                else:
                    reason = f"no Sales Invoice exists for {so_name}; settlement allocation waits (check the SO submit hook / Error Log)"
                _set_fee_status(so_name, "pending", reason)
                return _status("pending", reason, journal_entry=fee_je, applied_to_invoice=applied_invoice,
                               applied_to_settlement=applied_settlement, pending=[reason])

        # Step B: negative-sale settlement debits (grouped per settlement voucher/account/party).
        groups = {}
        for row in settlement_rows:
            kind = classify_settlement_debit_row(row, report_rows, report_id)
            if kind == "refund":
                continue
            if kind == "ambiguous":
                review.append(f"settlement {row.je_name} row #{row.idx} ({row.debit_in_account_currency}) sale/refund identity is ambiguous; not allocated")
                continue
            key = (row.je_name, row.account, row.party)
            groups.setdefault(key, frappe._dict(
                je_name=row.je_name, account=row.account, party=row.party, exchange_rate=row.exchange_rate,
                posting_date=row.posting_date, cheque_no=row.cheque_no,
            ))
        for group in sorted(groups.values(), key=lambda g: (str(g.posting_date), g.je_name)):
            remaining_fn = lambda g=group: _live_settlement_remaining(g.je_name, order_id, g.account, g.party, report_id, report_rows)
            remaining = remaining_fn()
            if remaining < 10 ** -precision or _fee_credit_left() < 10 ** -precision:
                continue
            if group.account == receivable.account and group.party == receivable.customer:
                applied, reason = _apply_credit_rows(
                    fee_je=fee_je, receivable=receivable, company=so.company, against_type="Journal Entry",
                    against_name=group.je_name, remaining_fn=remaining_fn,
                    invoice_rate=flt(group.exchange_rate) or 1.0, invoice_posting_date=group.posting_date, commit=commit,
                )
            else:
                applied, reason = _reclassify_fee_credit_to_settlement_row(
                    order_id=order_id, so=so, receivable=receivable, fee_je=fee_je, group=group,
                    remaining=remaining, settings=settings or _settings_doc(snapshot.get("settings")), commit=commit,
                    report_id=report_id, report_rows=report_rows,
                )
            applied_settlement += applied
            if reason:
                (review if ("manual" in reason or "Payment Reconciliation" in reason) else pending).append(reason)

        # Step C: finish interrupted reclassification credit legs (even when the fee credit is gone).
        for reclass in _reclass_journal_entries(order_id, so.company):
            if cint(reclass.docstatus) != 1:
                continue
            applied, reason = _finish_reclass_credit_leg(
                order_id, reclass, so.company, receivable.company_currency, receivable.order_currency, commit,
                report_id=report_id, report_rows=report_rows,
            )
            applied_settlement += applied
            if reason:
                (review if "manual" in reason else pending).append(reason)

        # Final chain state: re-read EVERYTHING live after the last step. Status is decided from
        # the whole voucher chain (fee credit, invoice outstanding, settlement debits, reclass
        # legs), never from an intermediate figure captured before Step B/C ran.
        leftover = _fee_credit_left()
        invoice_open = {}
        for inv in invoices:
            if inv.debit_to != receivable.account or inv.customer != receivable.customer:
                continue
            live = flt(frappe.db.get_value("Sales Invoice", inv.name, "outstanding_amount"), precision)
            if live >= 10 ** -precision:
                invoice_open[inv.name] = live
        settlement_open = {}
        for group in groups.values():
            live = _live_settlement_remaining(group.je_name, order_id, group.account, group.party, report_id, report_rows)
            if live >= 10 ** -precision:
                settlement_open[group.je_name] = live
        open_reclass_legs = []
        for r in _reclass_journal_entries(order_id, so.company):
            if cint(r.docstatus) != 1:
                continue
            open_credit = frappe.db.sql(
                """SELECT IFNULL(SUM(credit_in_account_currency), 0) FROM `tabJournal Entry Account`
                   WHERE parent = %s AND amazon_order_id = %s AND credit_in_account_currency > 0
                     AND IFNULL(reference_type, '') = '' AND party_type = 'Customer'""",
                (r.name, order_id),
            )
            if open_credit and flt(open_credit[0][0]) >= 10 ** -precision:
                open_reclass_legs.append(r.name)

        if open_reclass_legs and not any("reclassification" in p for p in pending + review):
            pending.append(f"reclassification credit leg(s) still open: {open_reclass_legs}")
        if invoice_open and leftover < 10 ** -precision:
            pending.append(
                f"invoice outstanding {sum(invoice_open.values()):.{precision}f} remains ({list(invoice_open)}) after the fee "
                "credit was exhausted; waiting for a settlement credit or manual review"
            )
        if settlement_open and leftover < 10 ** -precision:
            review.append(
                f"settlement debit {sum(settlement_open.values()):.{precision}f} remains for {order_id} "
                f"({list(settlement_open)}) but the fee JE credit is exhausted; a later fee event/retrocharge or refund "
                "changed the economics; review manually"
            )
        for reason in review:
            record_review(order_id, reason)
        if review:
            status = "review"
        elif pending or leftover >= 10 ** -precision or invoice_open or settlement_open or open_reclass_legs:
            status = "pending"
            if not pending:
                targets = []
                if invoice_open:
                    targets.append(f"invoice {list(invoice_open)}")
                if settlement_open:
                    targets.append(f"settlement {list(settlement_open)}")
                pending.append(
                    f"fee credit {leftover:.{precision}f} remains unallocated"
                    + (f" with open targets {', '.join(targets)}" if targets else " (settlement debit not yet posted)")
                )
        else:
            status = "complete"
        reason_text = "; ".join(review + pending) if (review or pending) else "allocated"
        _set_fee_status(so_name, status, "" if status == "complete" else reason_text)
        return _status(status, reason_text, journal_entry=fee_je, applied_to_invoice=applied_invoice,
                       applied_to_settlement=applied_settlement, leftover_fee_credit=leftover,
                       invoice_open=invoice_open, settlement_open=settlement_open, pending=pending, review=review,
                       open_reclass_legs=open_reclass_legs)

    try:
        result = _with_lock_retry(_run)
    except Exception:
        try:
            frappe.db.rollback()
        except Exception:
            pass
        frappe.log_error(title=f"AFN fee allocation {order_id}"[:140], message=frappe.get_traceback())
        # Stamp the retry cursor even on an unexpected failure (after the rollback) so a chain
        # that keeps failing cannot monopolise the oldest retry slots; committed separately.
        so_filters = {"amazon_order_id": order_id, "docstatus": 1}
        if company:
            so_filters["company"] = company
        for row in frappe.get_all("Sales Order", filters=so_filters, fields=["name"]):
            _set_fee_status(row.name, "pending", "unexpected error; see Error Log")
        if commit:
            frappe.db.commit()
        return _status("pending", "unexpected error; see Error Log")
    if commit:
        frappe.db.commit()
    return result


def retry_afn_fee_lifecycle(sales_orders=None, *, amz_setting_name: str | None = None,
                            limit: int = AFN_FEE_RETRY_BATCH_SIZE, horizon_days: int = AFN_FEE_RETRY_HORIZON_DAYS,
                            retry_interval_hours: int = AFN_FEE_RETRY_INTERVAL_HOURS) -> list[frappe._dict]:
    """
    Bounded retry for already-submitted Separate-Fee Sales Orders whose chain is not complete.

    Candidate selection is a cursor: chains marked `complete` are excluded, chains marked
    `review:` wait for an operator (recovery can force them), and every attempt stamps
    custom_amazon_fee_last_retry so a batch never re-selects the same orders before the
    retry interval elapses.  Draft invoices are left to enq_si_submit(), which gates and
    submits them and then calls the same allocation.
    """
    for fieldname in ("custom_amazon_fee_accounting", "custom_amazon_fee_status", "custom_amazon_fee_last_retry"):
        if not _has_field("Sales Order", fieldname):
            return []  # fields not installed yet: nothing can be marked, nothing to retry
    params = {"marker": FEE_ACCOUNTING_SEPARATE, "limit": int(limit)}
    conditions = ["docstatus = 1", "custom_amazon_fee_accounting = %(marker)s",
                  "IFNULL(custom_amazon_fee_status, '') NOT LIKE 'complete%%'",
                  "IFNULL(custom_amazon_fee_status, '') NOT LIKE 'review%%'"]
    if sales_orders:
        names = list(dict.fromkeys(sales_orders))
        params["names"] = tuple(names)
        conditions.append("name IN %(names)s")
    else:
        params["cutoff"] = add_days(nowdate(), -horizon_days)
        params["retry_before"] = now_datetime() - timedelta(hours=retry_interval_hours)
        conditions.append("transaction_date >= %(cutoff)s")
        conditions.append("(custom_amazon_fee_last_retry IS NULL OR custom_amazon_fee_last_retry <= %(retry_before)s)")
    if amz_setting_name:
        company = frappe.db.get_value("Amazon SP API Settings", amz_setting_name, "company")
        if company:
            params["company"] = company
            conditions.append("company = %(company)s")
    candidates = frappe.db.sql(
        f"""SELECT name, amazon_order_id, company, custom_amazon_fee_je FROM `tabSales Order`
            WHERE {' AND '.join(conditions)}
            ORDER BY custom_amazon_fee_last_retry IS NOT NULL, custom_amazon_fee_last_retry ASC, creation ASC
            LIMIT %(limit)s""",
        params, as_dict=True,
    )
    settings = _settings_doc(amz_setting_name)
    results = []
    for row in candidates:
        try:
            fee = ensure_fee_journal_entry(row.name, settings=settings)
            if fee.status in ("submitted", "created"):
                alloc = allocate_fee_credit_for_order(row.amazon_order_id, company=row.company, settings=settings)
            else:
                _set_fee_status(row.name, fee.status, fee.reason)
                frappe.db.commit()
                alloc = fee
            results.append(frappe._dict(sales_order=row.name, amazon_order_id=row.amazon_order_id, status=alloc.status, reason=alloc.reason))
        except Exception:
            frappe.db.rollback()
            frappe.log_error(title=f"AFN fee retry {row.amazon_order_id}"[:140], message=frappe.get_traceback())
            # Always advance the cursor for an attempted chain, including failure paths.
            _set_fee_status(row.name, "error", "see Error Log")
            frappe.db.commit()
            results.append(frappe._dict(sales_order=row.name, amazon_order_id=row.amazon_order_id, status="error", reason="see Error Log"))
    return results


# ──────────────────────────────────────────
# 6. Refunds and cancellations for marked invoices
# ──────────────────────────────────────────
def marked_invoice_context(si_name: str) -> frappe._dict:
    """Marker + snapshot + fee JE state for a Sales Invoice (through its source SO lineage)."""
    marker = frappe.db.get_value("Sales Invoice", si_name, "custom_amazon_fee_accounting") if _has_field("Sales Invoice", "custom_amazon_fee_accounting") else None
    so_names = frappe.db.sql_list(
        "SELECT DISTINCT sales_order FROM `tabSales Invoice Item` WHERE parent=%s AND IFNULL(sales_order,'')!=''",
        (si_name,),
    )
    so = None
    for name in so_names:
        row = frappe.db.get_value(
            "Sales Order", name,
            ["name", "docstatus", "amazon_order_id", "custom_amazon_fee_accounting", "custom_amazon_fee_snapshot", "custom_amazon_fee_je", "company"],
            as_dict=True,
        )
        if row and (row.custom_amazon_fee_accounting == FEE_ACCOUNTING_SEPARATE or marker == FEE_ACCOUNTING_SEPARATE):
            so = row
            break
    if not so and (marker or "") != FEE_ACCOUNTING_SEPARATE:
        return frappe._dict(marked=False)
    snapshot = parse_snapshot(so.custom_amazon_fee_snapshot) if so else None
    fee_je = None
    if so:
        rows = _fee_journal_entries(so.amazon_order_id, so.company)
        submitted = [r for r in rows if cint(r.docstatus) == 1]
        fee_je = submitted[0].name if len(submitted) == 1 else None
    return frappe._dict(marked=True, sales_order=so, snapshot=snapshot, fee_je=fee_je)


def validate_refund_for_marked_invoice(si_doc, charges_by_account: dict[str, float], refunded_items: list[tuple[str, float, float]]) -> tuple[bool, str]:
    """
    Narrow pre-check for a settlement Credit Note against a Separate-Fee invoice.

    * native return rules: refunded qty / principal may not exceed what the SI billed;
    * fee reversal accounting: a positive (reversal) amount on a snapshot fee account may not
      exceed the magnitude booked by the fee JE; the fee JE itself must be submitted.
    Everything else (buyer tax/shipping refunds, new refund-side fees) keeps the existing CN
    behavior, which credits/debits the component accounts without touching the fee JE.
    """
    ctx = marked_invoice_context(si_doc.name)
    if not ctx.marked:
        return True, "not marked"
    if not ctx.snapshot:
        return False, "Separate-Fee invoice has no fee snapshot on its source Sales Order; refund deferred for review"
    if not ctx.fee_je:
        return False, "fee Journal Entry for this invoice is not submitted; refund deferred until fee accounting completes"
    billed = {}
    for item in si_doc.items:
        key = (item.item_name or "").strip()
        billed[key] = (flt(billed.get(key, (0, 0))[0]) + flt(item.qty), flt(billed.get(key, (0, 0))[1]) + flt(item.amount))
    for sku, qty, principal in refunded_items:
        bq, ba = billed.get(sku, (0.0, 0.0))
        if abs(qty) > bq + 1e-6:
            return False, f"refund qty {abs(qty)} for {sku} exceeds billed qty {bq}"
        if principal > ba + 0.005:
            return False, f"refund principal {principal:.2f} for {sku} exceeds billed amount {ba:.2f}"
    booked = snapshot_fee_accounts(ctx.snapshot)
    already = _reversed_fee_by_account(si_doc.name)
    for account, amount in charges_by_account.items():
        if account in booked and amount > 0.005:
            entitlement = _r(abs(booked[account]) - already.get(account, 0.0))
            if amount > entitlement + 0.005:
                return False, (
                    f"fee reversal {amount:.2f} on {account} exceeds the remaining entitlement {entitlement:.2f} "
                    f"(booked {abs(booked[account]):.2f} by fee JE {ctx.fee_je}, already reversed {already.get(account, 0.0):.2f} "
                    "on earlier credit notes); possible duplicate reversal, deferred for review"
                )
    return True, "ok"


def _reversed_fee_by_account(si_name: str) -> dict[str, float]:
    """Positive (reversal) tax amounts already posted on submitted Credit Notes against this invoice."""
    rows = frappe.db.sql(
        """
        SELECT stc.account_head, IFNULL(SUM(stc.tax_amount), 0) AS total
        FROM `tabSales Taxes and Charges` stc
        JOIN `tabSales Invoice` si ON si.name = stc.parent
        WHERE si.return_against = %s AND si.is_return = 1 AND si.docstatus = 1 AND stc.tax_amount > 0
        GROUP BY stc.account_head
        """,
        (si_name,),
        as_dict=True,
    )
    return {r.account_head: _r(r.total) for r in rows}


def note_source_cancellation(doc) -> None:
    """Sales Order / Sales Invoice on_cancel hook helper (no commits): flag the fee voucher chain."""
    if (doc.get("custom_amazon_fee_accounting") or "") != FEE_ACCOUNTING_SEPARATE:
        return
    order_id = doc.get("amazon_order_id") or ""
    fee_je = doc.get("custom_amazon_fee_je") or ""
    status = frappe.db.get_value("Journal Entry", fee_je, "docstatus") if fee_je else None
    record_review(
        order_id,
        f"{doc.doctype} {doc.name} was cancelled; fee JE {fee_je or '(none)'} docstatus={status}. "
        "Review and cancel/repost the fee JE and its allocations manually; automation will not treat "
        "cancelled vouchers as valid.",
    )


# ──────────────────────────────────────────
# 7. Recovery of failed orders (dry_run=True by default)
# ──────────────────────────────────────────
def read_order_ids_from_failed_sync_csv(path: str) -> list[str]:
    """Order ids from an 'Amazon Failed Sync Record' export (column 'Amazon Order ID')."""
    ids = []
    with open(path, newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        columns = {(c or "").strip().lower(): c for c in (reader.fieldnames or [])}
        column = columns.get("amazon order id") or columns.get("amazon_order_id")
        if not column:
            raise ValueError("CSV has no 'Amazon Order ID' column")
        for row in reader:
            value = str(row.get(column) or "").strip()
            if _is_canonical_order_id(value) and value not in ids:
                ids.append(value)
    return ids


def fetch_order_strict(repo, order_id: str) -> dict | None:
    """
    Fetch and UNWRAP one Orders API order object.  Returns None only when Amazon explicitly
    returns an empty Orders list; raises AmazonOrderFetchError when the call failed/degraded.
    """
    from eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_repository import _list_orders

    payload = _list_orders(repo.amz_setting, amazon_order_ids=order_id)
    if not payload:
        raise AmazonOrderFetchError(f"Orders API returned no payload for {order_id} (unavailable/throttled/forbidden)")
    if "Orders" not in payload and "payload" in payload:
        payload = payload.get("payload") or {}
    orders = payload.get("Orders")
    if orders is None:
        raise AmazonOrderFetchError(f"Orders API envelope for {order_id} has no Orders list: {list(payload.keys())[:8]}")
    for order in orders:
        if (order.get("AmazonOrderId") or "").strip() == order_id:
            return order
    return None


def inspect_order(order_id: str, company: str | None = None) -> frappe._dict:
    """Read-only inventory of everything that exists for one Amazon order."""
    so_filters = {"amazon_order_id": order_id}
    if company:
        so_filters["company"] = company
    so_fields = ["name", "docstatus", "amazon_order_status", "fulfillment_channel", "customer", "currency",
                 "grand_total", "company", "transaction_date", "temporary_stock_tranfer_id"]
    for f in ("custom_amazon_fee_accounting", "custom_amazon_fee_je"):
        if _has_field("Sales Order", f):
            so_fields.append(f)
    sales_orders = frappe.get_all("Sales Order", filters=so_filters, fields=so_fields, order_by="creation asc")
    invoices = []
    for so in sales_orders:
        invoices.extend(find_sales_invoices_for_sales_order(so.name, docstatus=None, is_return=None))
    direct = frappe.get_all("Sales Invoice", filters={"amazon_order_id": order_id}, fields=["name", "docstatus", "is_return", "outstanding_amount", "customer", "debit_to", "currency", "posting_date"])
    seen = {inv.name for inv in invoices}
    invoices.extend(inv for inv in direct if inv.name not in seen)
    fee_jes = frappe.get_all("Journal Entry", filters={"cheque_no": fee_je_cheque_no(order_id)},
                             fields=["name", "docstatus", "company", "posting_date", "custom_amazon_source_sales_order"] if _has_field("Journal Entry", "custom_amazon_source_sales_order") else ["name", "docstatus", "company", "posting_date"])
    settlement_rows = frappe.db.sql(
        """
        SELECT je.name AS je_name, je.cheque_no AS report_id, je.posting_date, je.docstatus,
               jea.idx, jea.account, jea.party, jea.debit_in_account_currency, jea.credit_in_account_currency,
               jea.reference_type, jea.reference_name, jea.user_remark, jea.exchange_rate
        FROM `tabJournal Entry` je
        JOIN `tabJournal Entry Account` jea ON jea.parent = je.name
        WHERE jea.amazon_order_id = %s AND je.docstatus != 2
        ORDER BY je.posting_date ASC, je.name ASC, jea.idx ASC
        """,
        (order_id,),
        as_dict=True,
    )
    settlement_rows = [
        r for r in settlement_rows
        if not str(r.report_id or "").startswith(NON_SETTLEMENT_CHEQUE_PREFIXES)
        and not any(m in str(r.report_id or "") for m in NON_SETTLEMENT_CHEQUE_MARKERS)
    ]
    stock_entries = []
    for so in sales_orders:
        if so.temporary_stock_tranfer_id:
            se = frappe.db.get_value("Stock Entry", so.temporary_stock_tranfer_id, ["name", "docstatus"], as_dict=True)
            if se:
                stock_entries.append(se)
    failed_fields = ["name", "remarks", "creation"]
    if _has_field("Amazon Failed Sync Record", "custom_resolution"):
        failed_fields.append("custom_resolution")
    failed_records = frappe.get_all("Amazon Failed Sync Record", filters={"amazon_order_id": order_id}, fields=failed_fields, order_by="creation asc")
    return frappe._dict(
        order_id=order_id, sales_orders=sales_orders, invoices=invoices, fee_journal_entries=fee_jes,
        settlement_rows=settlement_rows, stock_entries=stock_entries, failed_sync_records=failed_records,
    )


def _plan_for(state: frappe._dict) -> tuple[str, list[str]]:
    """Concise per-order plan derived from what exists; mutation happens only in apply mode."""
    plan = []
    submitted_sos = [s for s in state.sales_orders if cint(s.docstatus) == 1]
    draft_sos = [s for s in state.sales_orders if cint(s.docstatus) == 0]
    submitted_sis = [i for i in state.invoices if cint(i.docstatus) == 1 and not cint(i.is_return)]
    draft_sis = [i for i in state.invoices if cint(i.docstatus) == 0 and not cint(i.is_return)]
    open_debits = [r for r in state.settlement_rows if flt(r.debit_in_account_currency) > 0 and not r.reference_type and cint(r.docstatus) == 1]
    if len(submitted_sos) > 1:
        return "review", [f"multiple submitted Sales Orders {[s.name for s in submitted_sos]}; manual review"]
    if submitted_sos:
        so = submitted_sos[0]
        marker = so.get("custom_amazon_fee_accounting") or ""
        if marker != FEE_ACCOUNTING_SEPARATE:
            plan.append(f"submitted legacy Sales Order {so.name} (marker={marker or 'blank'}); submitted economics are never rewritten")
            if submitted_sis:
                plan.append(f"submitted invoice(s) {[i.name for i in submitted_sis]} left unchanged")
            return "legacy_submitted", plan
        plan.append(f"Sales Order {so.name} is submitted with Separate Fee JE: ensure fee JE, submit draft SI {[i.name for i in draft_sis]}, allocate credit")
    elif draft_sos:
        plan.append(f"draft Sales Order {draft_sos[0].name}: fetch order, rebuild with separate-fee accounting (rebuild guard bypassed), submit")
    else:
        plan.append("no Sales Order: fetch order, create with separate-fee accounting, submit")
    if open_debits:
        plan.append(f"{len(open_debits)} unreferenced settlement debit row(s) found in reports {[r.report_id for r in open_debits]}; reconcile fee credit against them (no new payout)")
    else:
        plan.append("no unreferenced settlement debit found yet; settlement allocation waits for the report")
    cancelled_jes = [j.name for j in state.fee_journal_entries if cint(j.docstatus) == 2]
    if cancelled_jes:
        plan.append(f"cancelled fee JE {cancelled_jes} exists; recreation only with allow_recreate_after_cancel=True")
    return "recoverable", plan


def _annotate_failed_sync_records(state: frappe._dict, note: str) -> int:
    if not _has_field("Amazon Failed Sync Record", "custom_resolution"):
        return 0
    count = 0
    for rec in state.failed_sync_records:
        if (rec.get("custom_resolution") or "").strip():
            continue
        frappe.db.set_value("Amazon Failed Sync Record", rec.name, {
            "custom_resolution": note[:2000],
            "custom_resolved_on": now_datetime(),
        }, update_modified=False)
        count += 1
    return count


def recover_failed_afn_orders(amz_setting_name: str, order_ids=None, csv_path: str | None = None,
                              dry_run: bool = True, allow_recreate_after_cancel: bool = False) -> list[dict]:
    """
    Recovery entry point.

        bench --site <site> execute eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_afn_fee_accounting.recover_failed_afn_orders \
            --kwargs "{'amz_setting_name': '<settings>', 'csv_path': '/path/Amazon Failed Sync Record.csv', 'dry_run': True}"

    dry_run=True prints the per-order plan and mutates nothing.  dry_run=False applies the
    plan one order at a time with a commit after each order; it is safe to repeat.
    """
    from eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_repository import AmazonRepository

    ids = [str(o).strip() for o in (order_ids or []) if str(o).strip()]
    if csv_path:
        ids.extend(read_order_ids_from_failed_sync_csv(csv_path))
    ids = list(dict.fromkeys(ids))
    if not ids:
        raise ValueError("no Amazon order ids supplied")

    settings = frappe.get_doc("Amazon SP API Settings", amz_setting_name)
    if not dry_run:
        assert_fields_installed()
    repo = AmazonRepository(settings)
    results = []
    for order_id in ids:
        state = inspect_order(order_id, settings.company)
        status, plan = _plan_for(state)
        result = {"order_id": order_id, "plan_status": status, "plan": plan, "actions": [], "final_status": status}
        print(f"[AFN-RECOVER] {order_id}: {status}\n  - " + "\n  - ".join(plan), flush=True)
        if dry_run or status != "recoverable":
            results.append(result)
            continue
        try:
            result["final_status"] = _apply_recovery(repo, settings, order_id, state, result, allow_recreate_after_cancel)
            frappe.db.commit()
        except AmazonOrderFetchError as exc:
            frappe.db.rollback()
            result["actions"].append(f"API unavailable: {exc}; nothing deleted or changed")
            result["final_status"] = "api_unavailable"
        except Exception:
            frappe.db.rollback()
            frappe.log_error(title=f"AFN recovery {order_id}"[:140], message=frappe.get_traceback())
            result["actions"].append("unexpected error; see Error Log")
            result["final_status"] = "error"
        print(f"[AFN-RECOVER] {order_id}: {result['final_status']} :: {' | '.join(result['actions'])}", flush=True)
        results.append(result)
    return results


def _apply_recovery(repo, settings, order_id: str, state: frappe._dict, result: dict, allow_recreate_after_cancel: bool) -> str:
    from eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_sp_api_settings import enq_si_submit

    submitted_sos = [s for s in state.sales_orders if cint(s.docstatus) == 1]
    so_name = submitted_sos[0].name if submitted_sos else None

    if not so_name:
        order = fetch_order_strict(repo, order_id)
        if order is None:
            result["actions"].append("Orders API returned an empty Orders list; existing draft kept, nothing deleted")
            return "order_not_found_in_api"
        if (order.get("FulfillmentChannel") or "").upper() != "AFN":
            result["actions"].append(f"order is {order.get('FulfillmentChannel')}; this recovery handles AFN only")
            return "not_afn"
        so_name = repo.create_sales_order(order, force_rebuild=True)
        frappe.db.commit()
        if not so_name:
            result["actions"].append("create_sales_order() produced no Sales Order; see Amazon Failed Sync Record / Error Log")
            return "sales_order_not_created"
        docstatus = cint(frappe.db.get_value("Sales Order", so_name, "docstatus"))
        marker = frappe.db.get_value("Sales Order", so_name, "custom_amazon_fee_accounting")
        result["actions"].append(f"Sales Order {so_name} docstatus={docstatus} marker={marker}")
        if docstatus != 1:
            return "sales_order_not_submitted"

    fee = ensure_fee_journal_entry(so_name, settings=settings, allow_recreate_after_cancel=allow_recreate_after_cancel)
    result["actions"].append(f"fee JE: {fee.status} {fee.get('journal_entry') or ''} {fee.reason}")
    if fee.status not in ("submitted", "created"):
        return f"fee_je_{fee.status}"

    enq_si_submit(sales_orders=[so_name], amz_setting_name=settings.name)
    invoices = find_sales_invoices_for_sales_order(so_name, is_return=0)
    submitted = [i for i in invoices if cint(i.docstatus) == 1]
    result["actions"].append(f"invoices: {[(i.name, i.docstatus) for i in invoices]}")
    if not submitted:
        failure = frappe.db.get_value("Amazon Failed Invoice Record", {"invoice_id": ["in", [i.name for i in invoices] or [""]]}, "error")
        result["actions"].append(f"invoice not submitted: {failure}")
        return "invoice_not_submitted"

    alloc = allocate_fee_credit_for_order(order_id, company=settings.company, settings=settings)
    result["actions"].append(
        f"allocation: {alloc.status}; invoice={alloc.get('applied_to_invoice', 0)} settlement={alloc.get('applied_to_settlement', 0)} "
        f"leftover={alloc.get('leftover_fee_credit', 0)} {alloc.reason}"
    )
    # Distinct outcomes: documents exist (SO/SI/JE) -> allocation pending/review -> verified.
    if alloc.status == "review":
        return "allocation_review"
    if alloc.status != "complete":
        return "allocation_pending"
    verification = verify_order_accounting(order_id, settings.company)
    if verification["problems"]:
        result["actions"].append(f"verification problems: {verification['problems']}")
        return "allocation_complete_verification_failed"
    state = inspect_order(order_id, settings.company)
    note = (
        f"Recovered via Separate Fee JE on {nowdate()}: SO {so_name}, SI {[i.name for i in submitted]}, "
        f"fee JE {fee.get('journal_entry')}, allocation complete, GL/PLE verification passed "
        f"({len(verification['checks'])} checks)"
    )
    annotated = _annotate_failed_sync_records(state, note)
    result["actions"].append(f"verified; annotated {annotated} Amazon Failed Sync Record(s)")
    return "verified"


# ──────────────────────────────────────────
# 8. Read-only staging verification (bench execute; mutates nothing)
# ──────────────────────────────────────────
def verify_order_accounting(order_id: str, company: str | None = None) -> dict:
    """
    Read-only checks for one Separate-Fee order, suitable for staging validation:

        bench --site <site> execute eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_afn_fee_accounting.verify_order_accounting --args "['113-6478413-9291458']"

    Reports revenue/fee GL postings, invoice outstanding, unallocated fee credit, settlement
    debit allocation and the order-specific receivable position (sum of this order's own
    vouchers in the payment ledger), and flags anything that does not reconcile.
    """
    order_id = str(order_id or "").strip()
    state = inspect_order(order_id, company)
    checks = []
    problems = []

    def check(ok: bool, text: str):
        checks.append(("PASS" if ok else "FAIL", text))
        if not ok:
            problems.append(text)

    submitted_sos = [s for s in state.sales_orders if cint(s.docstatus) == 1]
    check(len(submitted_sos) == 1, f"exactly one submitted Sales Order (found {len(submitted_sos)})")
    if not submitted_sos:
        return {"order_id": order_id, "checks": checks, "problems": problems}
    so = frappe.get_doc("Sales Order", submitted_sos[0].name)
    marker = so.get("custom_amazon_fee_accounting") or ""
    check(marker == FEE_ACCOUNTING_SEPARATE, f"SO {so.name} marker is '{marker}'")
    snapshot = sales_order_snapshot(so)
    ok, reason = verify_snapshot(snapshot)
    check(ok, f"snapshot verification: {reason}")
    if not ok:
        return {"order_id": order_id, "checks": checks, "problems": problems}
    precision = _precision()

    invoices = [i for i in find_sales_invoices_for_sales_order(so.name, docstatus=1, is_return=0)]
    check(len(invoices) >= 1, f"submitted Sales Invoice(s): {[i.name for i in invoices]}")
    for inv in invoices:
        live = flt(frappe.db.get_value("Sales Invoice", inv.name, "outstanding_amount"), precision)
        check(abs(live) < 10 ** -precision, f"invoice {inv.name} outstanding is {live:.{precision}f}")
        check(abs(flt(inv.grand_total, precision) - _r(snapshot.get("sale_total"))) <= 10 ** -precision,
              f"invoice {inv.name} grand total {inv.grand_total} == snapshot sale total {snapshot.get('sale_total')}")
        si_marker = frappe.db.get_value("Sales Invoice", inv.name, "custom_amazon_fee_accounting")
        check(si_marker == FEE_ACCOUNTING_SEPARATE, f"invoice {inv.name} marker is '{si_marker}'")

    receivable = resolve_fee_receivable(so, snapshot)
    check(receivable.status == "ok", f"receivable resolution: {receivable.reason}")
    fee_rows = _fee_journal_entries(order_id, so.company)
    submitted_fee = [r for r in fee_rows if cint(r.docstatus) == 1]
    check(len(submitted_fee) == 1, f"exactly one submitted fee JE (found {[(r.name, r.docstatus) for r in fee_rows]})")
    if submitted_fee and receivable.status == "ok":
        je_name = submitted_fee[0].name
        ok, reason = fee_journal_entry_matches_snapshot(je_name, so, snapshot, receivable)
        check(ok, f"fee JE {je_name} matches snapshot: {reason}")
        leftover = sum(flt(r.credit_in_account_currency) for r in _unreferenced_credit_rows(je_name, receivable.account, receivable.customer))
        check(leftover < 10 ** -precision, f"unallocated fee credit on {je_name}: {leftover:.{precision}f}")
        gl = frappe.db.sql(
            """SELECT account, SUM(debit - credit) AS net FROM `tabGL Entry`
               WHERE voucher_type='Journal Entry' AND voucher_no=%s AND is_cancelled=0 GROUP BY account""",
            (je_name,), as_dict=True,
        )
        gl_by_account = {g.account: flt(g.net) for g in gl}
        rate = flt(snapshot.get("conversion_rate")) or 1.0
        if (snapshot.get("currency") or "").upper() == (snapshot.get("company_currency") or "").upper():
            rate = 1.0
        for account, native in snapshot_fee_accounts(snapshot).items():
            expected_base = flt(-native * rate, precision)   # GL is company currency; sign: fee charged = debit
            got = flt(gl_by_account.get(account, 0.0), precision)
            check(abs(got - expected_base) <= 2 * 10 ** -precision,
                  f"GL fee expense {account}: {got:.{precision}f} (expected ≈ {expected_base:.{precision}f} company currency)")

    debits = find_settlement_debit_rows(order_id, so.company)
    for group_key in sorted({(d.je_name, d.account, d.party) for d in debits}):
        je_name, account, party = group_key
        rows = [d for d in debits if (d.je_name, d.account, d.party) == group_key]
        kinds = {classify_settlement_debit_row(d) for d in rows}
        if "ambiguous" in kinds:
            checks.append(("INFO", f"settlement {je_name} has legacy debit row(s) of ambiguous sale/refund identity; not evaluated"))
        sale_total = sum(flt(d.debit_in_account_currency) for d in rows if classify_settlement_debit_row(d) == "sale")
        if sale_total < 10 ** -precision:
            continue
        allocated = _allocated_against_journal(je_name, order_id, account, party)
        check(abs(sale_total - allocated) < 10 ** -precision,
              f"settlement {je_name} negative-sale debit {sale_total:.{precision}f} allocated {allocated:.{precision}f} ({account}/{party})")
    for reclass in _reclass_journal_entries(order_id, so.company):
        if cint(reclass.docstatus) != 1:
            continue
        open_credit = frappe.db.exists(
            "Journal Entry Account",
            {"parent": reclass.name, "credit_in_account_currency": [">", 0], "reference_type": ["is", "not set"], "amazon_order_id": order_id},
        )
        check(not open_credit, f"reclassification {reclass.name} credit leg is closed")

    # Order-specific receivable position in the payment ledger: the invoice(s), the fee JE and
    # any reclassification JE of this order (settlement vouchers are shared across orders and
    # are checked per debit group above instead).
    own_vouchers = [i.name for i in invoices] + [r.name for r in submitted_fee]
    own_vouchers += frappe.get_all(
        "Journal Entry", filters={"cheque_no": ["like", f"{FEE_RECLASS_CHEQUE_PREFIX}{order_id}-%"], "docstatus": 1}, pluck="name",
    )
    if own_vouchers:
        placeholders = ",".join(["%s"] * len(own_vouchers))
        ple = frappe.db.sql(
            f"""SELECT IFNULL(SUM(amount_in_account_currency), 0), IFNULL(SUM(amount), 0) FROM `tabPayment Ledger Entry`
                WHERE delinked = 0 AND account_type = 'Receivable' AND against_voucher_no IN ({placeholders})""",
            tuple(own_vouchers),
        )
        position_acc = flt(ple[0][0] if ple else 0, precision)
        position_base = flt(ple[0][1] if ple else 0, precision)
        check(abs(position_acc) < 10 ** -precision,
              f"payment-ledger balance (account currency) against this order's own vouchers {own_vouchers}: {position_acc:.{precision}f}")
        check(abs(position_base) < 10 ** -precision,
              f"payment-ledger balance (company currency, incl. exchange gain/loss journals) against own vouchers: {position_base:.{precision}f}")

    for status_text, text in checks:
        print(f"[AFN-VERIFY] {order_id} {status_text}: {text}", flush=True)
    return {"order_id": order_id, "checks": checks, "problems": problems}


def verify_orders(order_ids=None, csv_path: str | None = None, company: str | None = None) -> dict:
    """Run verify_order_accounting for several orders (or the ids in a Failed Sync Record CSV)."""
    ids = [str(o).strip() for o in (order_ids or []) if str(o).strip()]
    if csv_path:
        ids.extend(read_order_ids_from_failed_sync_csv(csv_path))
    results = {order_id: verify_order_accounting(order_id, company) for order_id in dict.fromkeys(ids)}
    failed = {k: v["problems"] for k, v in results.items() if v["problems"]}
    print(f"[AFN-VERIFY] {len(results) - len(failed)}/{len(results)} orders reconcile; problems: {json.dumps(failed, indent=1)}", flush=True)
    return results
