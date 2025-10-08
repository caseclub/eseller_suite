#/apps/eseller_suite/eseller_suite/eseller_suite/custom_script/sales_order
import frappe
from erpnext.accounts.party import get_party_account
from erpnext.selling.doctype.sales_order.sales_order import SalesOrder
from erpnext.setup.doctype.item_group.item_group import get_item_group_defaults
from erpnext.stock.doctype.item.item import get_item_defaults
from frappe.contacts.doctype.address.address import get_company_address
from frappe.model.mapper import get_mapped_doc
from frappe.model.utils import get_fetch_values
from frappe.utils import cint, flt
from erpnext.stock.stock_ledger import get_stock_balance


class SalesOrderOverride(SalesOrder):
    def custom_validate(self):
        total_qty = 0
        total = 0
        total_taxes_and_charges = 0
        for item in self.items:
            qty = int(item.qty)
            rate = float(item.rate)
            amount = rate*qty
            #item.base_rate = rate
            item.amount = amount
            #item.base_amount = amount
            item.uom = item.stock_uom
            if self.delivery_date:
                item.delivery_date = self.delivery_date
            total_qty += qty
            total += amount

        for tax_row in self.taxes:
            if tax_row.tax_amount:
                total_taxes_and_charges += float(tax_row.tax_amount)

        self.total = total
        self.total_qty = total_qty
        self.total_taxes_and_charges = total_taxes_and_charges
        self.grand_total = total + total_taxes_and_charges
        self.base_grand_total = total + total_taxes_and_charges

    def validate(self):
        self.custom_validate()
        recall_order_prefixes = ['S']
        super(SalesOrderOverride, self).validate()
        if self.amazon_order_status != 'Canceled' and not self.amazon_order_amount and self.amazon_order_id and self.amazon_order_id[0] not in recall_order_prefixes:
            self.amazon_order_amount = self.total
        if self.amazon_order_id and self.amazon_order_id[0] in recall_order_prefixes:
            self.amazon_order_amount =  0
        if self.amazon_order_status == 'Canceled' or self.replaced_order_id:
            self.amazon_order_amount =  0

    def on_submit(self):
        #print("on_submit ------- create sales invoice!")
        super(SalesOrderOverride, self).on_submit()
        
        sales_invoice = make_sales_invoice(source_name=self.name, target_doc=None, ignore_permissions=True)
        if self.fulfillment_channel == "AFN":
            sales_invoice.update_stock = 0
        elif self.fulfillment_channel == "MFN":
            sales_invoice.update_stock = 1
        else:
            # fallback if you have other channels
            sales_invoice.update_stock = 0
        sales_invoice.insert(ignore_permissions=True)
        #sales_invoice.submit() #This is not native and never used. Probably not needed

    def on_update(self):
        if self.amazon_order_status == "Canceled" and self.temporary_stock_tranfer_id:
            if frappe.db.exists("Stock Entry", {"name":self.temporary_stock_tranfer_id, "docstatus":["!=", 2]}):
                temp_stock_transfer_doc = frappe.get_doc("Stock Entry", self.temporary_stock_tranfer_id)
                self.temporary_stock_tranfer_id = ""
                self.save(ignore_permissions=True)
                temp_stock_transfer_doc.cancel()
                temp_stock_transfer_doc.delete()

    def after_insert(self):
        amz_setting = frappe.get_last_doc("Amazon SP API Settings", {"is_active":1})
        if amz_setting.temporary_stock_transfer_required and self.amazon_order_id and self.amazon_order_status != "Canceled":
            self.create_temporary_stock_transfer()

    def after_delete(self):
        """method deletes the temporary stock entry if it exists"""
        if self.temporary_stock_tranfer_id:
            if frappe.db.exists("Stock Entry", {"name":self.temporary_stock_tranfer_id, "docstatus":["!=", 2]}):
                temp_stock_transfer_doc = frappe.get_doc("Stock Entry", self.temporary_stock_tranfer_id)
                if temp_stock_transfer_doc.docstatus == 1:
                    temp_stock_transfer_doc.cancel()
                temp_stock_transfer_doc.delete()

    def create_temporary_stock_transfer(self):
        """method creates a stock entry to the temporary warehouse when a sales order is screated
        """
        temp_stock_entry = frappe.new_doc("Stock Entry")
        temp_stock_entry.stock_entry_type = "Material Transfer"
        temp_stock_entry.set_posting_time = 1
        temp_stock_entry.posting_date = self.transaction_date
        temp_stock_entry.posting_time = self.transaction_time
        amz_setting = frappe.db.exists("Amazon SP API Settings", {"is_active":1})
        warehouse = frappe.db.get_value("Amazon SP API Settings", amz_setting, "warehouse")
        if self.fulfillment_channel:
            if self.fulfillment_channel=='AFN':
                warehouse = frappe.db.get_value("Amazon SP API Settings", amz_setting, "afn_warehouse")
        for item in self.items:
            temp_stock_entry.append("items", {
                "s_warehouse": warehouse,
                "t_warehouse": frappe.db.get_value("Amazon SP API Settings", amz_setting, "temporary_order_warehouse"),
                "item_code": item.item_code,
                "qty": item.qty,
                "allow_zero_valuation_rate": 1,
            })
        temp_stock_entry.insert(ignore_permissions=True)
        self.temporary_stock_tranfer_id = temp_stock_entry.name
        self.save(ignore_permissions=True)
        frappe.db.savepoint("before_temp_stock_entry_submit")
        try:
            temp_stock_entry.submit()
        except Exception as e:
            frappe.db.rollback(save_point="before_temp_stock_entry_submit")
            failed_sync_record = frappe.new_doc('Amazon Failed Sync Record')
            failed_sync_record.amazon_order_id = self.amazon_order_id
            failed_sync_record.remarks = "Failed to create temporary stock entry\n" + str(e)
            failed_sync_record.save(ignore_permissions=True)

@frappe.whitelist()
def make_sales_invoice(source_name, target_doc=None, ignore_permissions=False):
    def postprocess(source, target):
        set_missing_values(source, target)
        # Get the advance paid Journal Entries in Sales Invoice Advance
        if target.get("allocate_advances_automatically"):
            target.set_advances()

    def set_missing_values(source, target):
        target.flags.ignore_permissions = True
        target.run_method("set_missing_values")
        target.run_method("set_po_nos")
        target.run_method("calculate_taxes_and_totals")
        target.run_method("set_use_serial_batch_fields")

        if source.company_address:
            target.update({"company_address": source.company_address})
        else:
            # set company address
            target.update(get_company_address(target.company))

        if target.company_address:
            target.update(get_fetch_values("Sales Invoice", "company_address", target.company_address))

        # set the redeem loyalty points if provided via shopping cart
        if source.loyalty_points and source.order_type == "Shopping Cart":
            target.redeem_loyalty_points = 1

        target.debit_to = get_party_account("Customer", source.customer, source.company)
        target.set_posting_time = 1

    def update_item(source, target, source_parent):
        target.allow_zero_valuation_rate = 1
        target.amount = flt(source.amount) - flt(source.billed_amt)
        target.base_amount = target.amount * flt(source_parent.conversion_rate)
        target.qty = (
            target.amount / flt(source.rate)
            if (source.rate and source.billed_amt)
            else source.qty - source.returned_qty
        )
        target.total_order_value = source.total_order_value

        if source_parent.project:
            target.cost_center = frappe.db.get_value("Project", source_parent.project, "cost_center")
        if target.item_code:
            item = get_item_defaults(target.item_code, source_parent.company)
            item_group = get_item_group_defaults(target.item_code, source_parent.company)
            cost_center = item.get("selling_cost_center") or item_group.get("selling_cost_center")

            if cost_center:
                target.cost_center = cost_center

    doclist = get_mapped_doc(
        "Sales Order",
        source_name,
        {
            "Sales Order": {
                "doctype": "Sales Invoice",
                "field_map": {
                    "party_account_currency": "party_account_currency",
                    "payment_terms_template": "payment_terms_template",
                    "transaction_date": "posting_date",
                    "transaction_time": "posting_time",
                    "delivery_date": "due_date",
                    "amazon_order_id": "amazon_order_id",
                    "amazon_order_status": "amazon_order_status",
                    "amazon_customer_type": "amazon_customer_type",
                    "fulfillment_channel": "fulfillment_channel",
                    "replaced_order_id": "replaced_order_id",
                    "amazon_order_amount": "amazon_order_amount",
                },
                "field_no_map": ["payment_terms_template"],
                "validation": {"docstatus": ["=", 1]},
            },
            "Sales Order Item": {
                "doctype": "Sales Invoice Item",
                "field_map": {
                    "name": "so_detail",
                    "parent": "sales_order",
                },
                "postprocess": update_item,
                "condition": lambda doc: doc.qty
                and (doc.base_amount == 0 or abs(doc.billed_amt) < abs(doc.amount)),
            },
            "Sales Taxes and Charges": {"doctype": "Sales Taxes and Charges", "add_if_empty": True},
            "Sales Team": {"doctype": "Sales Team", "add_if_empty": True},
        },
        target_doc,
        postprocess,
        ignore_permissions=ignore_permissions,
    )

    automatically_fetch_payment_terms = cint(
        frappe.db.get_single_value("Accounts Settings", "automatically_fetch_payment_terms")
    )
    if automatically_fetch_payment_terms:
        doclist.set_payment_schedule()

    return doclist

def enq_si_submit(sales_invoice):
    insufficient_stock = False
    error_records = []

    # Collect stock levels for all items first (Assumption: Bulk fetching is possible)
    stock_levels = {item.item_code: get_stock_balance(item.item_code, item.warehouse, sales_invoice.posting_date)
                    for item in sales_invoice.items}

    for item in sales_invoice.items:
        stock_qty = stock_levels.get(item.item_code, 0)
        if item.qty > stock_qty:
            insufficient_stock = True
            error_records.append({
                "doctype": "Amazon Failed Invoice Record",
                "invoice_id": sales_invoice.name,
                "error": f"Insufficient stock for item {item.item_code} as of {sales_invoice.posting_date}. "
                        f"Available: {stock_qty}, Required: {item.qty}"
            })

    # Insert all error records in batch
    if error_records:
        for record in error_records:
            frappe.get_doc(record).insert()

    if not insufficient_stock:
        sales_invoice.flags.ignore_links = True
        sales_invoice.submit()
