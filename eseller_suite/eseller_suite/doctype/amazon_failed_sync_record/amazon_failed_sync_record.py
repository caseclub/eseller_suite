# Copyright (c) 2024, efeone and contributors
# For license information, please see license.txt

import frappe
import json
from frappe.model.document import Document
from frappe.utils import get_url_to_form
from eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_repository import get_order

class AmazonFailedSyncRecord(Document):
	@frappe.whitelist()
	def retry_fetching(self):
		so = []
		if self.amazon_order_id:
			if frappe.db.exists('Amazon SP API Settings', { 'is_active':1 }):
				amz_setting_name = frappe.db.get_value('Amazon SP API Settings', { 'is_active':1 })
				try:
					so = get_order(amz_setting_name=amz_setting_name, amazon_order_ids=self.amazon_order_id)
				except Exception as e:
					print(e)
		if so:
			return so
		else:
			return 0

	@frappe.whitelist()
	def create_replaced_so(self):
		if self.payload and not self.replaced_so:
			data = json.loads(self.payload)
			so_doc = frappe.get_doc(data)
			so_doc.taxes = []
			so_doc.flags.ignore_mandatory = True
			so_doc.disable_rounded_total = 1
			so_doc.custom_validate()
			so_doc.save(ignore_permissions=True)
			if so_doc.amazon_order_status == 'Shipped':
				so_doc.submit()
			frappe.db.set_value(self.doctype, self.name, 'replaced_so', so_doc.name)
			frappe.msgprint('Sales Order Created: <a href="{0}">{1}</a>'.format(get_url_to_form(so_doc.doctype, so_doc.name), so_doc.name), alert=True, indicator='green')

	@frappe.whitelist()
	def create_replaced_jv(self):
		if self.payload and not self.replaced_jv and self.grand_total:
			data = json.loads(self.payload)
			jv_doc = frappe.new_doc('Journal Entry')
			jv_doc.voucher_type = 'Journal Entry'
			jv_doc.posting_date = self.posting_date
			jv_doc.user_remark = 'Adjustment Entry for Replaced Order'
			jv_doc.amazon_order_id = self.amazon_order_id
			if data.get('taxes'):
				for row in data.get('taxes'):
					tax_amount = abs(float(row.get('tax_amount')))
					jv_row = jv_doc.append('accounts')
					jv_row.account = row.get('account_head')
					jv_row.debit = tax_amount
					jv_row.debit_in_account_currency = tax_amount
					jv_row.user_remark = row.get('description')
					jv_row.amazon_order_id = self.amazon_order_id
			if data.get('customer') and data.get('company'):
				default_receivable_account = frappe.db.get_value('Company', data.get('company'), 'default_receivable_account')
				jv_row = jv_doc.append('accounts')
				jv_row.credit = abs(self.grand_total)
				jv_row.credit_in_account_currency = abs(self.grand_total)
				jv_row.user_remark = 'Adjustment Entry for Replaced Order'
				jv_row.amazon_order_id = self.amazon_order_id
				jv_row.party_type = 'Customer'
				jv_row.party = data.get('customer')
				jv_row.account = default_receivable_account
				jv_doc.flags.ignore_mandatory = True
				jv_doc.save(ignore_permissions=True)
				jv_doc.submit()
				frappe.db.set_value(self.doctype, self.name, 'replaced_jv', jv_doc.name)
				frappe.msgprint('Journal Entry Created: <a href="{0}">{1}</a>'.format(get_url_to_form(jv_doc.doctype, jv_doc.name), jv_doc.name), alert=True, indicator='green')