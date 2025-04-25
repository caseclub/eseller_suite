# Copyright (c) 2024, efeone and contributors
# For license information, please see license.txt

import os
import csv
import xlrd
import frappe
from datetime import datetime
from frappe.model.document import Document
from frappe.utils import get_link_to_form, get_url_to_form
from eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_repository import get_order
from charset_normalizer import from_path

class AmazonPaymentEntry(Document):
	def validate(self):
		if not self.payment_details:
			self.process_payment_data()
		if not self.amazon_reserve_fund_account:
			amazon_reserve_fund = frappe.db.get_single_value('eSeller Settings', 'amazon_reserve_fund')
			if amazon_reserve_fund:
				self.amazon_reserve_fund_account = amazon_reserve_fund
			else:
				frappe.throw('Please configure the `Default Amazon Customer` in {0}'.format(get_link_to_form('eSeller Settings', 'eSeller Settings')))

	def on_submit(self):
		frappe.db.set_value(self.doctype, self.name, 'in_progress', 1)
		self.create_journal_entry()

	def process_payment_data(self):
		if self.payment_file:
			attached_file = frappe.get_doc("File", {"file_url": self.payment_file})
			file_path = frappe.get_site_path("private", "files", attached_file.file_name)
			frappe.logger().debug(f"Processing file at path: {file_path}")
			if not os.path.exists(file_path):
				frappe.throw(f"File not found at path: {file_path}")
			if attached_file.file_url.endswith(".csv"):
				self.process_csv(file_path)
			else:
				frappe.throw("Unsupported file format. Only CSV files are supported.")
		else:
			self.payment_details = []

	def process_csv(self, file_path):
		try:
			detected = from_path(file_path).best()
			encoding = detected.encoding

			with open(file_path, "r", encoding=encoding) as file:
				csv_reader = csv.DictReader(file)
				for row in csv_reader:
					self.save_payment_details(row)
		except Exception as e:
			frappe.throw("Error Processing the file: {0}".format(e))

	def parse_date(self, date_str):
		try:
			parsed_date = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
			return parsed_date
		except ValueError:
			frappe.throw(f"Invalid date format: {date_str}")

	def save_payment_details(self, row):
		key_mapping = {
		'Transaction type': "transaction_type",
		'Product Details': "product_details",
		'Order ID': "order_id",
		'Total (INR)': "total"
		}
		if row.get('\ufeff"Date"'):
			key_mapping['\ufeff"Date"'] = "date"
		else:
			key_mapping['Date'] = "date"
		payment_detail = {}
		is_blank_row = True
		for csv_key, internal_key in key_mapping.items():
			value = row.get(csv_key)
			if value:
				is_blank_row = False
				if internal_key == "date":
					value = self.parse_date(value)
				payment_detail[internal_key] = value
		if not is_blank_row and  payment_detail.get('transaction_type'):
			self.append("payment_details", payment_detail)
	
	@frappe.whitelist()
	def fetch_invoice_details(self):
		'''
			Method to get fetch Invoice and Customer details against each Amazon Order IDs
		'''
		has_changes = False
		total_pending_count = frappe.db.get_all('Amazon Payment Entry Item', { 'parent':self.name, 'ready_to_process':0 })
		i = 0
		for row in self.payment_details:
			if not row.ready_to_process:
				i += 1
				frappe.publish_realtime("fetch_invoice_details", dict(progress=i, total=len(total_pending_count)))
				if row.order_id and row.transaction_type in ['Order Payment', 'Amazon Easy Ship Charges', 'Fulfillment Fee Refund', 'Refund', 'Other']:
					invoice_details = get_invoice_details(row.order_id, is_return=0)
					return_invoice_details = None
					is_return = False
					if row.transaction_type in ['Fulfillment Fee Refund', 'Refund'] or row.product_details == 'Weight Handling Fees Reversal':
						is_return = True
						return_invoice_details = get_invoice_details(row.order_id, is_return=1)
					if invoice_details.get('sales_invoice'):
						row.sales_invoice = invoice_details.get('sales_invoice')
					if invoice_details.get('customer'):
						row.customer = invoice_details.get('customer')
						if not is_return:
							row.ready_to_process = 1
						has_changes = True
					if return_invoice_details:
						if return_invoice_details.get('sales_invoice'):
							row.return_sales_invoice = return_invoice_details.get('sales_invoice')
							row.ready_to_process = 1
							has_changes = True
				elif row.transaction_type in ["Unavailable balance", "Previous statement's unavailable balance"]:
					row.ready_to_process = 1
					has_changes = True
				elif row.transaction_type == 'Service Fees':
					if row.product_details:
						service_type_details = get_amazon_service_type_details(row.product_details)
						if service_type_details.get('service_type'):
							row.amazon_service_type = service_type_details.get('service_type')
						if service_type_details.get('expense_account'):
							row.amazon_expense_account = service_type_details.get('expense_account')
							row.ready_to_process = 1
							has_changes = True
				if row.transaction_type == 'Order Payment' and row.total and row.order_id:
					if float(row.total) < 0:
						row.ready_to_process = 0
						replaced_jv = get_replaced_jv(row.order_id)
						if replaced_jv:
							row.journal_entry = replaced_jv
							row.ready_to_process = 1
							has_changes = True
				if row.transaction_type in ['Other', 'Inventory Reimbursement'] and row.product_details in ['FBA Inventory Reimbursement', 'FBA Reversed Reimbursement'] and row.order_id == '---':
					if float(row.total) < 0:
						inventory_reimbursement_account = frappe.db.get_single_value('eSeller Settings', 'inventory_reimbursement_account')
						if inventory_reimbursement_account:
							row.ready_to_process = 1
							row.amazon_expense_account = inventory_reimbursement_account
							has_changes = True
					elif float(row.total) > 0:
						inventory_reimbursement_income_account = frappe.db.get_single_value('eSeller Settings', 'inventory_reimbursement_income_account')
						if inventory_reimbursement_income_account:
							row.ready_to_process = 1
							row.amazon_expense_account = inventory_reimbursement_income_account
							has_changes = True
				if row.transaction_type == 'Inventory Reimbursement' and row.product_details == 'FBA Inventory Reimbursement' and row.order_id:
					if float(row.total) > 0:
						inventory_reimbursement_income_account = frappe.db.get_single_value('eSeller Settings', 'inventory_reimbursement_income_account')
						if inventory_reimbursement_income_account:
							row.ready_to_process = 1
							row.amazon_expense_account = inventory_reimbursement_income_account
							has_changes = True
				if row.transaction_type == 'Other' and row.product_details == 'Others' and row.order_id == '---':
					if float(row.total) < 0:
						other_expenses_account = frappe.db.get_single_value('eSeller Settings', 'other_expenses_account')
						if other_expenses_account:
							row.ready_to_process = 1
							row.amazon_expense_account = other_expenses_account
							has_changes = True
					elif float(row.total) > 0:
						other_income_account = frappe.db.get_single_value('eSeller Settings', 'other_income_account')
						if other_income_account:
							row.ready_to_process = 1
							row.amazon_expense_account = other_income_account
							has_changes = True
				if row.transaction_type.strip() == 'Cancellation' and row.product_details == 'Order Cancellation Charge' and row.total and row.order_id:
					if float(row.total) < 0:
						order_cancellation_account = frappe.db.get_single_value('eSeller Settings', 'order_cancellation_account')
						if order_cancellation_account:
							row.ready_to_process = 1
							row.amazon_expense_account = order_cancellation_account
							has_changes = True
		if has_changes:
			self.save()
		return 1
	
	@frappe.whitelist()
	def create_journal_entry(self):
		'''
			Method to create Journal Entry against payment details table row
		'''
		jv_doc = frappe.new_doc('Journal Entry')
		jv_doc.voucher_type = 'Journal Entry'
		jv_doc.posting_date = self.posting_date
		jv_doc.cheque_date = self.posting_date
		jv_doc.cheque_no = self.name
		jv_doc.title = self.name
		total_debit = 0
		total_credit = 0
		for row in self.payment_details:
			if row.ready_to_process and row.total:
				jv_row = jv_doc.append('accounts')
				if row.transaction_type in ["Previous statement's unavailable balance", "Unavailable balance"]:
					jv_row.account = self.amazon_reserve_fund_account
					jv_row.user_remark = row.product_details
				if row.order_id:
					jv_row.amazon_order_id = row.order_id
				if row.customer:
					jv_row.party_type = 'Customer'
					jv_row.party = row.customer
					jv_row.account = self.default_receivable_account
				sales_invoice_ref = False
				if row.sales_invoice:
					sales_invoice_ref = row.sales_invoice
				if row.return_sales_invoice:
					sales_invoice_ref = row.return_sales_invoice
				if sales_invoice_ref:
					jv_row.reference_type = 'Sales Invoice'
					outstanding_amount = float(frappe.db.get_value('Sales Invoice', sales_invoice_ref, 'outstanding_amount'))
					if outstanding_amount >= abs(float(row.total)):
						jv_row.reference_name = sales_invoice_ref
					else:
						remarks = 'Invoice outstanding_amount = {0}'.format(outstanding_amount)
						jv_row.user_remark = remarks
				if row.journal_entry:
					jv_row.reference_type = ''
					jv_row.reference_name = ''
				if row.amazon_expense_account:
					if row.amazon_service_type:
						jv_row.user_remark = row.amazon_service_type
					else:
						jv_row.user_remark = row.product_details
					jv_row.account = row.amazon_expense_account

				if float(row.total) > 0:
					jv_row.credit = abs(float(row.total))
					jv_row.credit_in_account_currency = abs(float(row.total))
					total_credit += abs(float(row.total))
				else:
					jv_row.debit = abs(float(row.total))
					jv_row.debit_in_account_currency = abs(float(row.total))
					total_debit += abs(float(row.total))
			elif frappe.db.get_single_value("eSeller Settings", "use_reserve_lines_in_amazon_payment_entry"):
				reserve_jv_row = jv_doc.append('accounts')
				reserve_jv_row.user_remark = row.product_details
				if float(row.total) > 0:
					reserve_jv_row.account = frappe.db.get_single_value("eSeller Settings", "amazon_reserve_income_account")
					reserve_jv_row.credit = abs(float(row.total))
					reserve_jv_row.credit_in_account_currency = abs(float(row.total))
					total_credit += abs(float(row.total))
				else:
					reserve_jv_row.account = frappe.db.get_single_value("eSeller Settings", "amazon_reserve_expense_account")
					reserve_jv_row.debit = abs(float(row.total))
					reserve_jv_row.debit_in_account_currency = abs(float(row.total))
					total_debit += abs(float(row.total))
				if row.order_id:
					reserve_jv_row.amazon_order_id = row.order_id
				if row.customer:
					reserve_jv_row.party_type = 'Customer'
					reserve_jv_row.party = row.customer



		difference_amount = total_debit-total_credit
		jv_row = jv_doc.append('accounts')
		jv_row.account = self.payment_account
		if difference_amount>0:
			jv_row.credit = difference_amount
			jv_row.credit_in_account_currency = difference_amount
		else:
			jv_row.debit = abs(difference_amount)
			jv_row.debit_in_account_currency = abs(difference_amount)
		jv_doc.flags.ignore_mandatory = True
		jv_doc.save(ignore_permissions=True)
		jv_doc.submit()
		frappe.msgprint('Journal Entry Created: <a href="{0}">{1}</a>'.format(get_url_to_form(jv_doc.doctype, jv_doc.name), jv_doc.name), alert=True, indicator='green')

	@frappe.whitelist()
	def get_missing_sales_orders(self):
		frappe.db.set_value(self.doctype, self.name, 'in_progress', 1)
		frappe.db.commit()
		if frappe.db.exists('Amazon SP API Settings', { 'is_active':1 }):
			amz_setting_name = frappe.db.get_value('Amazon SP API Settings', { 'is_active':1 })
			max_invoice_count = frappe.db.get_single_value('eSeller Settings', 'max_invoice_count') or 25
			total_invoice_to_fetch = len(frappe.db.get_all('Amazon Payment Entry Item', { 'parent':self.name, 'ready_to_process':0 }))
			max_threshold = total_invoice_to_fetch if total_invoice_to_fetch <= max_invoice_count else max_invoice_count
			i = 0
			for row in self.payment_details:
				if row.order_id and row.order_id != '---' and not row.ready_to_process and i<max_threshold:
					i += 1
					frappe.publish_realtime("get_missing_sales_orders", dict(progress=i, total=max_threshold))
					try:
						get_order(amz_setting_name=amz_setting_name, amazon_order_ids=row.order_id)
					except Exception as e:
						print(e)
		frappe.db.set_value(self.doctype, self.name, 'in_progress', 0)

def get_invoice_details(amazon_order_id, is_return=0):
	'''
		This method will return the Invoice ID and Customer
	'''
	invoice_details = {}
	si = frappe.db.exists('Sales Invoice', {'amazon_order_id':amazon_order_id, 'is_return':is_return, 'docstatus':1})
	if si:
		customer = frappe.db.get_value('Sales Invoice', si, 'customer')
		invoice_details['sales_invoice'] = si
		invoice_details['customer'] = customer
	return invoice_details
	
def get_amazon_service_type_details(service_type):
	'''
		This method will return the Amazon Service Type and expense account
	'''
	service_type_details = {}
	if frappe.db.exists('Amazon Service Type', service_type):
		expense_account = frappe.db.get_value('Amazon Service Type', service_type, 'expense_account')
		service_type_details['service_type'] = service_type
		service_type_details['expense_account'] = expense_account
	return service_type_details

def get_replaced_jv(amazon_order_id):
	jv_reference = False
	if frappe.db.exists('Journal Entry', { 'docstatus':1, 'amazon_order_id':amazon_order_id }):
		jv_reference = frappe.db.get_value('Journal Entry', { 'docstatus':1, 'amazon_order_id':amazon_order_id })
	return jv_reference