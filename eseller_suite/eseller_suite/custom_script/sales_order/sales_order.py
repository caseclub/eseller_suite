import json
import frappe
from frappe.model.mapper import get_mapped_doc
from erpnext.selling.doctype.sales_order.sales_order import make_sales_invoice

def on_submit(doc, method):
    sales_invoice = make_sales_invoice(source_name=doc.name, target_doc=None, ignore_permissions=True)
    sales_invoice.flags.ignore_validate = True
    sales_invoice.insert(ignore_permissions=True)
    sales_invoice.submit()

def after_insert(doc, method):
    total_qty = 0
    total = 0
    total_taxes_and_charges = 0
    grand_total = 0
    for item in doc.items:
        qty = int(item.qty)
        rate = float(item.rate)
        amount = rate*qty
        item.base_rate = rate
        item.amount = amount
        item.base_amount = amount
        item.uom = item.stock_uom
        if doc.delivery_date:
            item.delivery_date = doc.delivery_date
        total_qty += qty
        total += amount

    for tax_row in doc.taxes:
        if tax_row.tax_amount:
            total_taxes_and_charges += float(tax_row.tax_amount)

    doc.total = total
    doc.total_qty = total_qty
    doc.total_taxes_and_charges = total_taxes_and_charges
    doc.grand_total = total + total_taxes_and_charges
    doc.base_grand_total = total + total_taxes_and_charges
    doc.save()
