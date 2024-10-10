# Copyright (c) 2024, efeone and contributors
# For license information, please see license.txt

import os
import csv
import xlrd
import frappe
from datetime import datetime
from frappe.model.document import Document
from frappe.utils import getdate
from erpnext.accounts.doctype.sales_invoice.sales_invoice import get_bank_cash_account

class AmazonPaymentEntry(Document):
	def validate(self):
		self.process_payment_data()
		
	def on_submit(self):
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
		with open(file_path, "r") as file:
			csv_reader = csv.DictReader(file)
			for row in csv_reader:
				self.save_payment_details(row)

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
		if not is_blank_row and not any(
			pd.transaction_type == payment_detail["transaction_type"] and
			pd.order_id == payment_detail["order_id"]
			for pd in self.payment_details
		):
			if payment_detail.get('transaction_type'):
				self.append("payment_details", payment_detail)
	
	@frappe.whitelist()
	def fetch_invoice_details(self):
		'''
			Method to get fetch Invoice and Customer details against each Amazon Order IDs
		'''
		has_changes = False
		for row in self.payment_details:
			if not row.ready_to_process:
				if row.transaction_type != 'Service Fees':
					if row.order_id:
						invoice_details = get_invoice_details(row.order_id, row.transaction_type)
						if invoice_details.get('sales_invoice'):
							row.sales_invoice = invoice_details.get('sales_invoice')
						if invoice_details.get('customer'):
							row.customer = invoice_details.get('customer')
							row.ready_to_process = 1
							has_changes = True
				else:
					if row.product_details:
						service_type_details = get_amazon_service_type_details(row.product_details)
						if service_type_details.get('service_type'):
							row.amazon_service_type = service_type_details.get('service_type')
						if service_type_details.get('expense_account'):
							row.amazon_expense_account = service_type_details.get('expense_account')
							row.ready_to_process = 1
							has_changes = True
		if has_changes:
			self.save()
		return 1
	
	def create_journal_entry(self):
		'''
			Method to create Journal Entry against payment details table row
		'''
		pass

def get_invoice_details(amazon_order_id, transaction_type):
	'''
		This method will return the Invoice ID and Customer
		If transaction_type is `Refund` or `Fulfillment Fee Refund` then  will return Credit Note Invoice ID
		Else will return Invoice ID
	'''
	is_return = 0
	invoice_details = {}
	if transaction_type in ['Fulfillment Fee Refund', 'Refund']:
		is_return = 1
	if frappe.db.exists('Sales Invoice', { 'amazon_order_id':amazon_order_id, 'is_return':is_return }):
		si = frappe.db.get_value('Sales Invoice', { 'amazon_order_id':amazon_order_id, 'is_return':is_return })
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