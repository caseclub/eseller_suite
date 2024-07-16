import json
import frappe
from frappe.model.mapper import get_mapped_doc
from erpnext.selling.doctype.sales_order.sales_order import make_sales_invoice

def on_submit(doc, method):
    sales_invoice = make_sales_invoice(source_name=doc.name, target_doc=None, ignore_permissions=True)
    sales_invoice.insert(ignore_permissions=True)
    sales_invoice.submit()
