# Copyright (c) 2024, efeone and contributors
# For license information, please see license.txt


from datetime import datetime

import frappe
from frappe import _
from frappe.custom.doctype.custom_field.custom_field import create_custom_fields
from frappe.model.document import Document
from frappe.utils import add_days, today


class AmazonSPAPISettings(Document):
	def validate(self):
		self.validate_after_date()

		if self.is_active == 1:
			setup_custom_fields()
		else:
			self.enable_sync = 0

		if not self.max_retry_limit:
			self.max_retry_limit = 1
		elif self.max_retry_limit and self.max_retry_limit > 5:
			frappe.throw(frappe._("Value for <b>Max Retry Limit</b> must be less than or equal to 5."))

	def save(self):
		super(AmazonSPAPISettings, self).save()

		if not self.is_old_data_migrated:
			migrate_old_data()
			self.db_set("is_old_data_migrated", 1)

	def validate_after_date(self):
		if datetime.strptime(add_days(today(), -30), "%Y-%m-%d") > datetime.strptime(
			self.after_date, "%Y-%m-%d"
		):
			frappe.throw(_("The date must be within the last 30 days."))

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
				created_after=self.after_date,
				timeout=4000,
				now=frappe.flags.in_test,
			)

			frappe.msgprint(_("Order details will be fetched in the background."))
		else:
			frappe.msgprint(
				_("Please enable the Amazon SP API Settings {0}.").format(frappe.bold(self.name))
			)
 

# Called via a hook in every hour.
def schedule_get_order_details():
	from eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_repository import (
		get_orders,
	)

	amz_settings = frappe.get_all(
		"Amazon SP API Settings",
		filters={"is_active": 1, "enable_sync": 1},
		fields=["name", "after_date"],
	)

	for amz_setting in amz_settings:
		get_orders(amz_setting_name=amz_setting.name, created_after=amz_setting.after_date)


def setup_custom_fields():
	custom_fields = {
		"Sales Order": [
			dict(
				fieldname="amazon_order_id",
				label="Amazon Order ID",
				fieldtype="Data",
				insert_after="title",
				read_only=1,
				print_hide=1,
			)
		],
	}

	create_custom_fields(custom_fields)


def migrate_old_data():
	column_exists = frappe.db.has_column("Item", "amazon_item_code")

	if column_exists:
		item = frappe.qb.DocType("Item")
		items = (frappe.qb.from_(item).select("*").where(item.amazon_item_code.notnull())).run(
			as_dict=True
		)

		for item in items:
			if not frappe.db.exists("Ecommerce Item", {"erpnext_item_code": item.name}):
				ecomm_item = frappe.new_doc("Ecommerce Item")
				ecomm_item.integration = "Amazon"
				ecomm_item.erpnext_item_code = item.name
				ecomm_item.integration_item_code = item.amazon_item_code
				ecomm_item.has_variants = 0
				ecomm_item.sku = item.amazon_item_code
				ecomm_item.flags.ignore_mandatory = True
				ecomm_item.save(ignore_permissions=True)
