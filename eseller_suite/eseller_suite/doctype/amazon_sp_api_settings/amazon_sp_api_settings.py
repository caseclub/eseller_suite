# Copyright (c) 2024, efeone and contributors
# For license information, please see license.txt


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
		# 	self.db_set("is_old_data_migrated", 1)

	def validate_after_date(self):
		if datetime.strptime(add_days(today(), -30), "%Y-%m-%d") > datetime.strptime(
			get_date_str(self.after_date), "%Y-%m-%d"
		):
			frappe.throw(_("The date must be within the last 60 days."))

	@frappe.whitelist()
	def get_order_details(self):
		from eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_repository import (
			get_orders,
		)
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


# Called via a hook in every hour.
def schedule_get_order_details():
	current_datetime = now_datetime()

	yesterday_23 = (current_datetime - timedelta(days=1)).replace(
		hour=23, minute=0, second=0, microsecond=0
	)
	today_01 = current_datetime.replace(hour=1, minute=0, second=0, microsecond=0)

	if yesterday_23 <= current_datetime < today_01: # Makes it so that the hourly scheduler won't work at midnight and the daily one will
		return

	from eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_repository import (
		get_orders,
	)

	system_timezone = frappe.db.get_single_value("System Settings", "time_zone")

	local_tz = pytz.timezone(system_timezone)
	gmt_tz = pytz.timezone("GMT")

	local_datetime = local_tz.localize(current_datetime)
	gmt_datetime = local_datetime.astimezone(gmt_tz)
	current_date = gmt_datetime.strftime("%Y-%m-%d")

	amz_settings = frappe.get_all(
		"Amazon SP API Settings",
		filters={"is_active": 1, "enable_sync": 1},
		fields=["name"],
	)

	for amz_setting in amz_settings:
		get_orders(amz_setting_name=amz_setting.name, last_updated_after=current_date)

# Called via a hook every day to sync data of the previous day.
def schedule_get_order_details_daily():
	from eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_repository import (
		get_orders,
	)

	from_date = add_days(getdate(), -1).strftime("%Y-%m-%d")

	amz_settings = frappe.get_all(
		"Amazon SP API Settings",
		filters={"is_active": 1, "enable_sync": 1},
		fields=["name"],
	)

	for amz_setting in amz_settings:
		get_orders(amz_setting_name=amz_setting.name, last_updated_after=from_date)
		frappe.enqueue("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_sp_api_settings.enq_si_submit", queue="long")

def enq_si_submit(sales_orders = []):
	if not sales_orders:
		sales_invoices = frappe.db.get_all("Sales Invoice", {"docstatus":0, "amazon_order_id":["is", "set"]}, pluck="name")
	else:
		sales_invoices = frappe.db.get_all("Sales Invoice Item", {"sales_order":["in", sales_orders]}, pluck="parent")
	for sales_invoice in sales_invoices:
		sales_invoice = frappe.get_doc("Sales Invoice", sales_invoice)
		frappe.db.savepoint("before_testing_si_submit")
		try:
			sales_invoice.submit()
		except Exception as e:
			frappe.db.rollback(save_point="before_testing_si_submit")
			if not frappe.db.exists("Amazon Failed Invoice Record", {"invoice_id":sales_invoice.name}):
				frappe.get_doc({"doctype":"Amazon Failed Invoice Record", "invoice_id":sales_invoice.name, "error":e}).insert()
