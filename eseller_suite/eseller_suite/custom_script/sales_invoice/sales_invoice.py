import frappe
from eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_repository import create_stock_entry

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

def before_submit(doc, method):
	'''
        Method which get trgiggered in before_submit event
	'''
	if doc.replaced_order_id and doc.amazon_order_id:
		create_stock_entry(doc.name)

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
