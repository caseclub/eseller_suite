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


class AmazonPaymentTool(Document):
	def validate(self):
		self.process_payment_data()

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
        '\ufeff"Date"': "date",
        'Transaction type': "transaction_type",
        'Order ID': "order_id",
        'Total (INR)': "total"
        }
		payment_detail = {}
		for csv_key, internal_key in key_mapping.items():
			value = row.get(csv_key)
			if internal_key == "date":
				value = self.parse_date(value)
			payment_detail[internal_key] = value

		if not any(
            pd.transaction_type == payment_detail["transaction_type"] and
            pd.order_id == payment_detail["order_id"]
            for pd in self.payment_details
        ):
			if payment_detail.get('transaction_type'):
				self.append("payment_details", payment_detail)

	@frappe.whitelist()
	def create_payments(self):
		'''
            Method to Mark Payments against each Order IDs
		'''
		for row in self.payment_details:
			if row.order_id and row.order_id != '---' and not row.payment_created:
				create_payment_against_order_id(row.name)

def create_payment_against_order_id(row_id):
	'''
        Method to create payment entry against Order ID
	'''
	order_id, amount, date = frappe.db.get_value('Amazon Payment Tool Item', row_id, fieldname=['order_id', 'total', 'date'])
	posting_date = getdate(date)
	payment_amount = float(amount)
	if frappe.db.exists('Sales Order', { 'amazon_order_id':order_id }):
		so, customer = frappe.db.get_value('Sales Order', { 'amazon_order_id':order_id }, fieldname=['name', 'customer'])
		pay_doc = frappe.new_doc('Payment Entry')
		pay_doc.party_type = 'Customer'
		pay_doc.party = customer
		pay_doc.posting_date = posting_date
		pay_doc.mode_of_payment = 'Cash'
		pay_doc.received_amount = payment_amount
		pay_doc.source_exchange_rate = 1
		pay_doc.target_exchange_rate = 1
		mode_of_payment_account = get_bank_cash_account(mode_of_payment=pay_doc.mode_of_payment, company=pay_doc.company)
		if payment_amount<0:
			pay_doc.payment_type = 'Pay'
			pay_doc.paid_from = mode_of_payment_account.get('account')
			pay_doc.paid_amount = - payment_amount
		else:
			pay_doc.payment_type = 'Receive'
			pay_doc.paid_to = mode_of_payment_account.get('account')
			pay_doc.paid_amount = payment_amount
		pay_doc.setup_party_account_field()
		pay_doc.set_missing_values()
		pay_doc.flags.ignore_mandatory = True
		pay_doc.save(ignore_permissions=True)
		pay_doc.set_missing_values()
		pay_doc.submit()
		frappe.db.set_value('Amazon Payment Tool Item', row_id, 'payment_created', 1)
		frappe.msgprint("Payment Entry Created : {0}".format(pay_doc.name), alert=True, indicator='green')