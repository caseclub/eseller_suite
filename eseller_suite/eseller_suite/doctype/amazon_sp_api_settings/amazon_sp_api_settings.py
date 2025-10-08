# Copyright (c) 2024, efeone and contributors
# For license information, please see license.txt
#/apps/eseller_suite/eseller_suite/eseller_suite/doctype/amazon_sp_api_settings

from datetime import datetime, timedelta

import frappe
from frappe import _
from frappe.model.document import Document
from frappe.utils import add_days, getdate, now_datetime, today, get_date_str
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

def enq_si_submit(sales_orders = []):
    if not sales_orders:
        sales_invoices = frappe.db.get_all("Sales Invoice", {"docstatus":0, "amazon_order_id":["is", "set"]}, pluck="name")
    else:
        sales_invoices = frappe.db.get_all("Sales Invoice Item", {"sales_order":["in", sales_orders]}, pluck="parent")
    for sales_invoice in sales_invoices:
        sales_invoice = frappe.get_doc("Sales Invoice", sales_invoice)
        if sales_invoice.docstatus in [1, 2]:
            continue
        frappe.db.sql("start transaction")
        frappe.db.savepoint("before_testing_si_submit")
        try:
            sales_invoice.submit()
            frappe.db.commit()
        except Exception as e:
            frappe.db.rollback(save_point="before_testing_si_submit")
            if not frappe.db.exists("Amazon Failed Invoice Record", {"invoice_id":sales_invoice.name}):
                frappe.get_doc({"doctype":"Amazon Failed Invoice Record", "invoice_id":sales_invoice.name, "error":str(e)}).insert()
            frappe.db.commit()