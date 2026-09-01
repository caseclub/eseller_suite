# Copyright (c) 2024, efeone and contributors
# For license information, please see license.txt
#/apps/eseller_suite/eseller_suite/eseller_suite/doctype/amazon_sp_api_settings

from datetime import datetime, timedelta

import frappe
from frappe import _
from frappe.model.document import Document
from frappe.utils import add_days, cint, getdate, now_datetime, today, get_date_str
import pytz


class AmazonSPAPISettings(Document):
    def validate(self):
        self.validate_after_date()

        if self.is_active == 0:
            self.enable_sync = 0

        if not self.max_retry_limit:
            self.max_retry_limit = 1
        elif self.max_retry_limit and self.max_retry_limit > 5:
            frappe.throw(frappe._("Value for <b>Max Retry Limit</b> must be less than or equal to 5."))

    def save(self):
        super(AmazonSPAPISettings, self).save()

        # if not self.is_old_data_migrated:
        #     self.db_set("is_old_data_migrated", 1)

    def validate_after_date(self):
        if datetime.strptime(add_days(today(), -60), "%Y-%m-%d") > datetime.strptime(
            get_date_str(self.after_date), "%Y-%m-%d"
        ):
            frappe.throw(_("The date must be within the last 60 days."))

    @frappe.whitelist()
    def get_order_details(self):
        from eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_repository import (get_orders)
        if self.is_active == 1:
            job_name = f"Get Amazon Orders - {self.name}"

            if frappe.db.get_all("RQ Job", {"job_name": job_name, "status": ["in", ["queued", "started"]]}):
                return frappe.msgprint(_("The order details are currently being fetched in the background."))

            frappe.enqueue(
                job_name=job_name,
                method=get_orders,
                amz_setting_name=self.name,
                last_updated_after=self.after_date,
                sync_selected_date_only=self.sync_selected_date_only,
                timeout=6000,
                now=frappe.flags.in_test,
            )

            frappe.msgprint(_("Order details will be fetched in the background."))
        else:
            frappe.msgprint(
                _("Please enable the Amazon SP API Settings {0}.").format(frappe.bold(self.name))
            )


# Called via a hook in every hour. Pulls all orders from the day (going back to the last midnight)
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_sp_api_settings.schedule_get_order_details")
"""
def schedule_get_order_details():
    current_datetime = now_datetime()

    # 2. Midnight guard (so the daily job handles that window)
    yesterday_23 = (current_datetime - timedelta(days=1)).replace(
        hour=23, minute=0, second=0, microsecond=0
    )
    today_01 = current_datetime.replace(hour=1, minute=0, second=0, microsecond=0)

    if yesterday_23 <= current_datetime < today_01:
        return

    # 3. Prepare the SP-API date filter
    system_timezone = frappe.db.get_single_value("System Settings", "time_zone")
    local_tz = pytz.timezone(system_timezone)
    gmt_tz = pytz.timezone("GMT")

    local_datetime = local_tz.localize(current_datetime)
    gmt_datetime = local_datetime.astimezone(gmt_tz)
    current_date = gmt_datetime.strftime("%Y-%m-%d")
      
    # 4. Pull your active, enabled Amazon settings with after_date
    amz_settings = frappe.get_all(
        "Amazon SP API Settings",
        filters={"is_active": 1, "enable_sync": 1},
        fields=["name", "after_date"],
    )

    # 5. For each account, enqueue a background job (non-blocking!)
    from eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_repository import get_orders

    for setting in amz_settings:
        after_date_dt = getdate(setting.after_date)
        current_dt = getdate(current_date)
        if after_date_dt > current_dt:
            continue  # Skip if after_date is future

        #job_name = f"Hourly Amazon Order Sync - {setting.name}" # With setting name. Beneficial for more than one instance
        job_name = f"Hourly Amazon Order Sync"

        # 5a. Skip enqueue if one is already queued or running for this setting
        if frappe.db.exists(
            "RQ Job",
            {"job_name": job_name, "status": ["in", ["queued", "started"]]}
        ):
            continue
        
        # 5b. Hand off to a long-queue worker, using current_date (>= after_date)
        frappe.enqueue(
            method=get_orders,
            queue="long",
            job_name=job_name,
            amz_setting_name=setting.name,
            last_updated_after=current_date,
            sync_selected_date_only=0,
            timeout=6000,
        )


"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_sp_api_settings.schedule_get_order_details_daily")
"""
# Called via a hook every day to sync data of the previous day.
def schedule_get_order_details_daily():
    from eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_repository import get_orders, reprocess_draft_orders_func  # Adjusted import to include the new wrapper
    
    today_dt = getdate()
    amz_settings = frappe.get_all(
        "Amazon SP API Settings",
        filters={"is_active": 1, "enable_sync": 1},
        fields=["name", "after_date"],
    )
    for amz_setting in amz_settings:
        after_date_dt = getdate(amz_setting.after_date)
        if after_date_dt > today_dt:
            continue  # Skip if after_date is future
        seven_days_ago_dt = add_days(today_dt, -7)
        from_date_dt = max(after_date_dt, seven_days_ago_dt)
        from_date = from_date_dt.strftime("%Y-%m-%d")
        
        # Enqueue get_orders
        get_orders_job_name = f"Daily Amazon Order Sync – (from {from_date})"
        
        # Skip if a similar job is already queued/running for this setting
        # Enqueue get_orders
        
        if not frappe.db.exists("RQ Job", {"job_name": get_orders_job_name, "status": ["in", ["queued","started"]]}):
            frappe.enqueue(
                method=get_orders,
                queue="long",
                job_name=get_orders_job_name,
                amz_setting_name=amz_setting.name,
                last_updated_after=from_date,
                sync_selected_date_only=0,  # Adjust if needed
                timeout=10800,  # 3 hours; increase if necessary
            )
        
        # Enqueue reprocess_draft_orders_func
        age_days = 7
        reprocess_job_name = f"Reprocess Draft Amazon Orders Older Than {age_days} Days"

        # Enqueue reprocess_draft_orders_func (independent gate)
        if not frappe.db.exists("RQ Job", {"job_name": reprocess_job_name, "status": ["in", ["queued","started"]]}):
            frappe.enqueue(
                method=reprocess_draft_orders_func,
                queue="long",
                job_name=reprocess_job_name,
                amz_setting_name=amz_setting.name,
                age_days=7,  # Explicitly pass your desired value
                timeout=10800,  # 3 hours; increase if necessary
            )
    
    # Optionally enqueue enq_si_submit afterward (unchanged; runs once after loop)
    frappe.enqueue(
        method="eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_sp_api_settings.enq_si_submit",
        queue="long",
        job_name="Submit Sales Invoices for Amazon Orders",
    )

AMAZON_INVOICE_AMOUNT_TOLERANCE = 0.02
MFN_ACCOUNTING_FINAL_STATUSES = {"Shipped", "InvoiceUnconfirmed"}
MFN_INVOICE_FINANCE_RETRY_HORIZON_DAYS = 7
MFN_INVOICE_FINANCE_RETRY_BATCH_SIZE = 25
# Operational bound on the unscoped catch-up scan; not an accounting rule.
MFN_INVOICE_UNSCOPED_CANDIDATE_LIMIT = 500
MFN_INVOICE_REVIEW_PREFIX = "MFN accounting finalization manual review"
# Terminal marker for invoices that keep failing GL submission; stops unbounded retry.
AMAZON_INVOICE_SUBMIT_REVIEW_PREFIX = "Amazon Sales Invoice submission manual review"


def _record_amazon_invoice_failure(invoice_name: str, message: str) -> None:
    """Persist/update one concise review record without creating duplicates on every retry."""
    print(f"[AMZ-SI] {invoice_name}: {message}", flush=True)
    existing = frappe.db.get_value(
        "Amazon Failed Invoice Record", {"invoice_id": invoice_name}, ["name", "error"], as_dict=True
    )
    if existing:
        existing_error = str(existing.error or "")
        # Never overwrite a terminal review marker with a later transient error.
        if existing_error.startswith((MFN_INVOICE_REVIEW_PREFIX, AMAZON_INVOICE_SUBMIT_REVIEW_PREFIX)):
            return
        if existing_error != message:
            frappe.db.set_value(
                "Amazon Failed Invoice Record", existing.name, "error", message, update_modified=False
            )
        return
    frappe.get_doc({
        "doctype": "Amazon Failed Invoice Record",
        "invoice_id": invoice_name,
        "error": message,
    }).insert(ignore_permissions=True)


def _invoice_terminal_review_reason(invoice_name: str) -> str | None:
    """Return the terminal marker text, if this invoice already reached a terminal state."""
    error = str(frappe.db.get_value(
        "Amazon Failed Invoice Record", {"invoice_id": invoice_name}, "error"
    ) or "")
    for prefix in (MFN_INVOICE_REVIEW_PREFIX, AMAZON_INVOICE_SUBMIT_REVIEW_PREFIX):
        if error.startswith(prefix):
            return error
    return None


def _mfn_invoice_terminal_review_exists(invoice_name: str) -> bool:
    return _invoice_terminal_review_reason(invoice_name) is not None


def _record_mfn_invoice_validation_failure(si, reason: str) -> None:
    cutoff = add_days(today(), -MFN_INVOICE_FINANCE_RETRY_HORIZON_DAYS)
    terminal = getdate(si.creation) < getdate(cutoff)
    if terminal:
        message = f"{MFN_INVOICE_REVIEW_PREFIX}: {reason}"
    else:
        message = f"MFN accounting validation deferred: {reason}"
    _record_amazon_invoice_failure(si.name, message)


def _draft_amazon_sales_invoices(sales_orders=None) -> list[str]:
    """Return unique Draft Amazon Sales Invoices, including legacy rows missing SI.amazon_order_id."""
    sales_orders = list(dict.fromkeys(sales_orders or []))
    if sales_orders:
        # docstatus lives on the child row too; filter in SQL instead of loading submitted
        # and cancelled invoice documents only to discard them in the caller.
        rows = frappe.db.get_all(
            "Sales Invoice Item",
            filters={"sales_order": ["in", sales_orders], "docstatus": 0},
            fields=["parent"],
            order_by="parent asc",
        )
        candidates = [row.parent for row in rows]
    else:
        # The old catch-up query required Sales Invoice.amazon_order_id to be populated, which
        # made exactly the affected MFN invoices invisible. Include lineage through the source SO.
        rows = frappe.db.sql(
            """
            SELECT DISTINCT si.name, si.creation
            FROM `tabSales Invoice` si
            LEFT JOIN `tabSales Invoice Item` sii ON sii.parent = si.name
            LEFT JOIN `tabSales Order` so ON so.name = sii.sales_order
            WHERE si.docstatus = 0
              AND (
                    IFNULL(si.amazon_order_id, '') != ''
                    OR IFNULL(so.amazon_order_id, '') != ''
                  )
            ORDER BY si.creation ASC, si.name ASC
            LIMIT %s
            """,
            (MFN_INVOICE_UNSCOPED_CANDIDATE_LIMIT,),
            as_dict=True,
        )
        candidates = [row.name for row in rows]

    # Child-table lookups can return the same invoice once per item.
    return list(dict.fromkeys(candidates))


def _amazon_source_sales_orders(invoice_name: str) -> list[frappe._dict]:
    """Resolve Amazon Sales Order lineage for one Draft Sales Invoice."""
    return frappe.db.sql(
        """
        SELECT DISTINCT
               so.name, so.amazon_order_id, so.fulfillment_channel,
               so.amazon_order_status, so.docstatus, so.company, so.marketplace_id,
               so.customer, so.currency, so.grand_total, so.discount_amount,
               so.replaced_order_id
        FROM `tabSales Invoice Item` sii
        JOIN `tabSales Order` so ON so.name = sii.sales_order
        WHERE sii.parent = %s
          AND IFNULL(so.amazon_order_id, '') != ''
        ORDER BY so.name ASC
        """,
        (invoice_name,),
        as_dict=True,
    )


def _setting_marketplace_ids(raw) -> set[str]:
    return {part.strip() for part in str(raw or "").split(",") if part.strip()}


def _resolve_amazon_setting_for_so(
    so, supplied_setting_name: str | None = None
) -> tuple[str | None, str]:
    """Resolve the credential/settings record without guessing across multiple Amazon accounts."""
    if supplied_setting_name:
        settings = frappe.get_doc("Amazon SP API Settings", supplied_setting_name)
        if settings.company != so.company:
            return None, (
                f"settings {supplied_setting_name} company {settings.company} "
                f"!= SO company {so.company}"
            )
        configured = _setting_marketplace_ids(getattr(settings, "custom_marketplace", ""))
        if so.marketplace_id and configured and so.marketplace_id not in configured:
            return None, (
                f"settings {supplied_setting_name} does not contain marketplace {so.marketplace_id}"
            )
        return settings.name, "explicit_settings_match"

    settings_rows = frappe.get_all(
        "Amazon SP API Settings",
        filters={"is_active": 1, "company": so.company},
        fields=["name", "custom_marketplace"],
        order_by="name asc",
    )
    if not settings_rows:
        return None, f"no active Amazon SP API Settings found for company {so.company}"

    if so.marketplace_id:
        marketplace_matches = [
            row for row in settings_rows
            if so.marketplace_id in _setting_marketplace_ids(row.custom_marketplace)
        ]
        if len(marketplace_matches) == 1:
            return marketplace_matches[0].name, "marketplace_match"
        if len(marketplace_matches) > 1:
            return None, f"multiple settings match marketplace {so.marketplace_id}"

    if len(settings_rows) == 1:
        return settings_rows[0].name, "single_company_setting"
    return None, f"cannot uniquely resolve Amazon settings for SO {so.name}"


def _tax_amounts_by_account(doc) -> dict[str, float]:
    totals = {}
    for row in getattr(doc, "taxes", None) or []:
        account = (getattr(row, "account_head", None) or "").strip()
        amount = float(getattr(row, "tax_amount", 0) or 0)
        if not account or abs(amount) < 0.005:
            continue
        totals[account] = round(totals.get(account, 0.0) + amount, 2)
    return totals


def _amount_maps_match(
    expected: dict[str, float], actual: dict[str, float]
) -> tuple[bool, str]:
    for account in sorted(set(expected) | set(actual)):
        exp = round(float(expected.get(account, 0) or 0), 2)
        got = round(float(actual.get(account, 0) or 0), 2)
        if abs(exp - got) > AMAZON_INVOICE_AMOUNT_TOLERANCE:
            return False, f"account {account}: expected {exp:.2f}, found {got:.2f}"
    return True, "match"


def _mfn_source_so_matches_current_finances(repo, so_doc) -> tuple[bool, str]:
    """
    Revalidate a submitted MFN SO against current posted Financial Events before SI submission.

    This is especially important for pre-patch SOs that may have been submitted while Unshipped
    with an incomplete Financial Events snapshot. Submitted SOs are never mutated here.
    """
    order_id = (so_doc.amazon_order_id or "").strip()
    if not order_id:
        return False, "source Sales Order has no amazon_order_id"
    if so_doc.docstatus != 1:
        return False, f"source Sales Order is not submitted (docstatus={so_doc.docstatus})"

    if (so_doc.fulfillment_channel or "").upper() != "MFN":
        return True, "not_mfn"

    is_zero_replacement = bool(
        abs(float(so_doc.grand_total or 0)) < 0.01 and (so_doc.replaced_order_id or "")
    )
    if is_zero_replacement:
        return True, "zero_value_replacement"

    if so_doc.amazon_order_status not in MFN_ACCOUNTING_FINAL_STATUSES:
        return False, (
            f"source MFN SO status is {so_doc.amazon_order_status!r}; "
            "only Shipped/InvoiceUnconfirmed may finalize accounting"
        )

    order = repo._fetch_order_by_id(order_id)
    if not order:
        return False, "Orders API returned no current order payload"
    if (order.get("OrderStatus") or "") not in MFN_ACCOUNTING_FINAL_STATUSES:
        return False, f"current Amazon order status is {order.get('OrderStatus')!r}"

    charges = repo.get_charges_and_fees(order_id)
    gate_items = [{"amount": float(item.amount or 0)} for item in so_doc.items]
    ready, reason = repo._mfn_financial_events_ready(order, gate_items, charges)
    if not ready:
        return False, f"Financial Events not ready: {reason}"

    summary = charges.get("financial_event_summary") or {}
    expected_principal = round(float(summary.get("principal_total") or 0), 2)
    so_principal = round(sum(float(item.amount or 0) for item in so_doc.items), 2)
    if abs(expected_principal - so_principal) > AMAZON_INVOICE_AMOUNT_TOLERANCE:
        return False, (
            f"source SO principal mismatch: Financial Events={expected_principal:.2f}, "
            f"SO items={so_principal:.2f}"
        )

    postage_account = getattr(repo.amz_setting, "mfn_postage_fee_account_head", None)
    expected_taxes = {}
    postage_components = []
    for bucket in ("charges", "fees", "tds", "service_fees"):
        for component in charges.get(bucket) or []:
            account = (component.get("account_head") or "").strip()
            amount = float(component.get("tax_amount") or 0)
            if not account or abs(amount) < 0.005:
                continue
            is_separate_postage = (
                bucket == "service_fees"
                and postage_account
                and account == postage_account
                and not so_doc.replaced_order_id
            )
            if is_separate_postage:
                postage_components.append(component)
                continue
            expected_taxes[account] = round(expected_taxes.get(account, 0.0) + amount, 2)

    taxes_ok, taxes_reason = _amount_maps_match(
        expected_taxes, _tax_amounts_by_account(so_doc)
    )
    if not taxes_ok:
        return False, f"source SO financial rows are stale/incomplete: {taxes_reason}"

    expected_discount = round(-float(charges.get("additional_discount") or 0), 2)
    actual_discount = round(float(so_doc.discount_amount or 0), 2)
    if abs(expected_discount - actual_discount) > AMAZON_INVOICE_AMOUNT_TOLERANCE:
        return False, (
            f"source SO promotion mismatch: Financial Events discount={expected_discount:.2f}, "
            f"SO discount={actual_discount:.2f}"
        )

    expected_grand = round(
        expected_principal + sum(expected_taxes.values()) - expected_discount, 2
    )
    actual_grand = round(float(so_doc.grand_total or 0), 2)
    if abs(expected_grand - actual_grand) > AMAZON_INVOICE_AMOUNT_TOLERANCE:
        return False, (
            f"source SO grand total mismatch: Financial Events reconstruction={expected_grand:.2f}, "
            f"SO={actual_grand:.2f}"
        )

    # A configured MFN postage service fee is intentionally owned by a separate JE. If the
    # repository's post-submit attempt failed, retry it here using the same idempotent helper
    # before allowing the SI to become posted accounting.
    if postage_components:
        remark = f"Amazon MFN Postage Fee for Order {order_id}"
        cheque_no = f"MFN-POST-{order_id}"
        for component in postage_components:
            account = (component.get("account_head") or "").strip()
            amount = abs(float(component.get("tax_amount") or 0))
            if not repo._mfn_postage_je_exists(order_id, account, remark, cheque_no):
                repo._post_mfn_postage_service_fee(so_doc, component)
            if not repo._mfn_postage_je_exists(order_id, account, remark, cheque_no):
                return False, (
                    f"separate MFN postage JE is missing for {account} amount {amount:.2f}"
                )

    return True, "source_so_matches_current_financial_events"


def _item_amounts_by_code(doc) -> tuple[dict[str, float], dict[str, float]]:
    """Aggregate per item_code so legitimate row splitting/reordering still compares equal."""
    amounts, quantities = {}, {}
    for row in getattr(doc, "items", None) or []:
        code = (getattr(row, "item_code", None) or "").strip()
        amounts[code] = round(amounts.get(code, 0.0) + float(getattr(row, "amount", 0) or 0), 2)
        quantities[code] = round(quantities.get(code, 0.0) + float(getattr(row, "qty", 0) or 0), 4)
    return amounts, quantities


def _draft_si_matches_source_so(si, so_doc) -> tuple[bool, str]:
    """Fail closed if the Draft SI did not copy the verified source SO economics."""
    if si.customer != so_doc.customer:
        return False, f"customer mismatch SI={si.customer} SO={so_doc.customer}"
    if (si.currency or "").upper() != (so_doc.currency or "").upper():
        return False, f"currency mismatch SI={si.currency} SO={so_doc.currency}"
    if si.company != so_doc.company:
        return False, f"company mismatch SI={si.company} SO={so_doc.company}"

    # A different conversion rate silently changes every base-currency GL amount even when the
    # transaction-currency totals agree, so it must be compared explicitly.
    si_rate = round(float(si.conversion_rate or 0), 9)
    so_rate = round(float(so_doc.conversion_rate or 0), 9)
    if so_rate and abs(si_rate - so_rate) > 0.000001:
        return False, f"conversion rate mismatch SI={si_rate} SO={so_rate}"

    # Aggregate totals alone cannot detect a substituted item or a shifted qty/rate split.
    si_amounts, si_qty = _item_amounts_by_code(si)
    so_amounts, so_qty = _item_amounts_by_code(so_doc)
    for code in sorted(set(si_amounts) | set(so_amounts)):
        exp_amt = round(so_amounts.get(code, 0.0), 2)
        got_amt = round(si_amounts.get(code, 0.0), 2)
        if abs(exp_amt - got_amt) > AMAZON_INVOICE_AMOUNT_TOLERANCE:
            return False, f"item {code!r} amount mismatch SI={got_amt:.2f} SO={exp_amt:.2f}"
        exp_qty = round(so_qty.get(code, 0.0), 4)
        got_qty = round(si_qty.get(code, 0.0), 4)
        if abs(exp_qty - got_qty) > 0.0001:
            return False, f"item {code!r} qty mismatch SI={got_qty} SO={exp_qty}"

    si_item_total = round(sum(float(item.amount or 0) for item in si.items), 2)
    so_item_total = round(sum(float(item.amount or 0) for item in so_doc.items), 2)
    if abs(si_item_total - so_item_total) > AMAZON_INVOICE_AMOUNT_TOLERANCE:
        return False, f"item total mismatch SI={si_item_total:.2f} SO={so_item_total:.2f}"

    taxes_ok, taxes_reason = _amount_maps_match(
        _tax_amounts_by_account(so_doc), _tax_amounts_by_account(si)
    )
    if not taxes_ok:
        return False, f"tax/fee rows did not copy from SO: {taxes_reason}"

    si_discount = round(float(si.discount_amount or 0), 2)
    so_discount = round(float(so_doc.discount_amount or 0), 2)
    if abs(si_discount - so_discount) > AMAZON_INVOICE_AMOUNT_TOLERANCE:
        return False, f"discount mismatch SI={si_discount:.2f} SO={so_discount:.2f}"

    si_grand = round(float(si.grand_total or 0), 2)
    so_grand = round(float(so_doc.grand_total or 0), 2)
    if abs(si_grand - so_grand) > AMAZON_INVOICE_AMOUNT_TOLERANCE:
        return False, f"grand total mismatch SI={si_grand:.2f} SO={so_grand:.2f}"

    return True, "draft_si_matches_source_so"


def enq_si_submit(sales_orders=None, amz_setting_name: str | None = None):
    """
    Submit Draft Amazon Sales Invoices only after their accounting lineage is safe.

    AFN keeps the historical submission behavior, with amazon_order_id backfilled from its SO.
    MFN additionally revalidates current posted Financial Events against the submitted source SO
    and verifies that the Draft SI copied those verified SO economics before GL submission.
    """
    scoped_sales_orders = bool(sales_orders)
    sales_invoices = _draft_amazon_sales_invoices(sales_orders)
    if not sales_invoices:
        return

    unscoped_mfn_finance_checks = 0
    for invoice_name in sales_invoices:
        si = frappe.get_doc("Sales Invoice", invoice_name)
        if si.docstatus in [1, 2]:
            continue
        terminal_reason = _invoice_terminal_review_reason(si.name)
        if terminal_reason:
            print(f"[AMZ-SI] {si.name}: terminal review already recorded; skipping", flush=True)
            continue
        source_rows = _amazon_source_sales_orders(si.name)
        source_order_id = None

        if source_rows:
            order_ids = {
                str(row.amazon_order_id).strip()
                for row in source_rows
                if row.amazon_order_id
            }
            if len(order_ids) != 1 or len(source_rows) != 1:
                _record_amazon_invoice_failure(
                    si.name,
                    f"Amazon SI must map to exactly one Amazon Sales Order; "
                    f"found SOs={[row.name for row in source_rows]} order_ids={sorted(order_ids)}",
                )
                frappe.db.commit()
                continue

            source_order_id = next(iter(order_ids))
            source_so = frappe.get_doc("Sales Order", source_rows[0].name)
            existing_si_order_id = (si.amazon_order_id or "").strip()
            if existing_si_order_id and existing_si_order_id != source_order_id:
                _record_amazon_invoice_failure(
                    si.name,
                    f"amazon_order_id conflict: SI={existing_si_order_id}, source SO={source_order_id}",
                )
                frappe.db.commit()
                continue

            if (source_so.fulfillment_channel or "").upper() == "MFN":
                if (
                    not scoped_sales_orders
                    and unscoped_mfn_finance_checks >= MFN_INVOICE_FINANCE_RETRY_BATCH_SIZE
                ):
                    print(
                        f"[AMZ-SI] Unscoped MFN finance validation batch limit "
                        f"{MFN_INVOICE_FINANCE_RETRY_BATCH_SIZE} reached; remaining drafts retry later",
                        flush=True,
                    )
                    continue
                if not scoped_sales_orders:
                    unscoped_mfn_finance_checks += 1

                setting_name, setting_reason = _resolve_amazon_setting_for_so(
                    source_so, amz_setting_name
                )
                if not setting_name:
                    _record_mfn_invoice_validation_failure(
                        si, f"settings resolution failed: {setting_reason}"
                    )
                    frappe.db.commit()
                    continue

                # Local import avoids a module-load circular dependency: amazon_repository
                # imports AmazonSPAPISettings from this module.
                from eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_repository import AmazonRepository

                repo = AmazonRepository(setting_name)
                finance_ok, finance_reason = _mfn_source_so_matches_current_finances(
                    repo, source_so
                )
                if not finance_ok:
                    _record_mfn_invoice_validation_failure(si, finance_reason)
                    frappe.db.commit()
                    continue

                invoice_ok, invoice_reason = _draft_si_matches_source_so(si, source_so)
                if not invoice_ok:
                    _record_mfn_invoice_validation_failure(
                        si, f"Draft SI did not copy verified source SO economics: {invoice_reason}"
                    )
                    frappe.db.commit()
                    continue

        elif not (si.amazon_order_id or "").strip():
            # Neither direct ID nor SO lineage: this is not safely identifiable as an Amazon SI.
            continue

        # Preserve the legacy AFN/standalone path when no source SO lineage exists. MFN invoices
        # created from Sales Orders take the strict validation path above.
        frappe.db.sql("start transaction")
        frappe.db.savepoint("before_testing_si_submit")
        try:
            # Serialize with any other worker holding the same Draft invoice, then re-read the
            # authoritative docstatus so two workers cannot both attempt submission.
            frappe.db.sql("SELECT name FROM `tabSales Invoice` WHERE name=%s FOR UPDATE", (si.name,))
            live_docstatus = frappe.db.get_value("Sales Invoice", si.name, "docstatus")
            if cint(live_docstatus) != 0:
                frappe.db.commit()
                continue

            if source_order_id and not (si.amazon_order_id or "").strip():
                si.amazon_order_id = source_order_id
                si.save(ignore_permissions=True)
            si.submit()
            frappe.db.commit()
            print(
                f"[AMZ-SI] Submitted {si.name}"
                + (f" for Amazon order {source_order_id}" if source_order_id else ""),
                flush=True,
            )
        except Exception as e:
            frappe.db.rollback(save_point="before_testing_si_submit")
            # Bound the retry: an invoice that has failed GL submission past the horizon gets a
            # terminal marker instead of being re-attempted (and re-polling Amazon) forever.
            cutoff = add_days(today(), -MFN_INVOICE_FINANCE_RETRY_HORIZON_DAYS)
            if getdate(si.creation) < getdate(cutoff):
                message = f"{AMAZON_INVOICE_SUBMIT_REVIEW_PREFIX}: {e}"
            else:
                message = str(e)
            _record_amazon_invoice_failure(si.name, message)
            frappe.db.commit()
