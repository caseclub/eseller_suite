import frappe
from frappe.model.mapper import get_mapped_doc
from erpnext.selling.doctype.sales_order.sales_order import SalesOrder
from erpnext.setup.doctype.item_group.item_group import get_item_group_defaults
from erpnext.stock.doctype.item.item import get_item_defaults
from frappe.contacts.doctype.address.address import get_company_address
from frappe.model.mapper import get_mapped_doc
from frappe.model.utils import get_fetch_values
from erpnext.accounts.party import get_party_account
from frappe.utils import flt, cint

def validate(doc, method):
	if not doc.is_return and doc.update_stock:
		for item in doc.items:
			if frappe.db.get_value("Item", item.item_code, "has_serial_no"):
				item.use_serial_batch_fields = 1
				serial_nos = get_serial_nos(item.warehouse, item.item_code, item.qty)
				item.serial_no = "\n".join(serial_nos)

def on_cancel(doc, method):
	'''
		Method which get trgiggered in on_cancel event
	'''
	if doc.is_return:
		for item in doc.items:
			if item.sales_invoice_item and frappe.db.exists('Sales Invoice Item', item.sales_invoice_item):
				frappe.db.set_value('Sales Invoice Item', item.sales_invoice_item, 'refunded', 0)

def get_serial_nos(warehouse, item_code, qty):
	"""
		Fetch serial numbers using FIFO for the given item and quantity.
	"""
	serial_no_list = frappe.get_all("Serial No",
									filters={
										"warehouse": warehouse,
										"item_code": item_code,
										"status": "Active"
									},
									fields=["name"],
									order_by="creation asc",
									limit=qty)
	if len(serial_no_list) < qty:
		frappe.throw(f"Not enough serial numbers available for item {item_code}.")
	return [serial_no.name for serial_no in serial_no_list]
