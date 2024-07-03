# Copyright (c) 2024, efeone and contributors
# For license information, please see license.txt

import frappe
import requests
import json
from datetime import datetime
from frappe.utils import get_datetime, add_to_date
from frappe.model.document import Document
DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT = "%H:%M:%S.%f"
DATETIME_FORMAT = f"{DATE_FORMAT} {TIME_FORMAT}"


class AmazonSellerCentralSettings(Document):
	def on_update(self):
		if self.enable:
			generate_access_token()

def get_authorisation_token():
	'''
		Method to get authorisation_token from the Settings
	'''
	return frappe.utils.password.get_decrypted_password(
		"Amazon Seller Central Settings", "Amazon Seller Central Settings", "authorisation_token"
	)


def get_access_token():
	'''
		Method to get access_token from the Settings, if exprired generate and return new token
	'''
	if frappe.db.get_single_value("Amazon Seller Central Settings", "access_token"):
		token_exprire_on = frappe.db.get_single_value("Amazon Seller Central Settings", "token_exprire_on")
		if token_exprire_on and get_datetime()> get_datetime(token_exprire_on):
			return frappe.utils.password.get_decrypted_password(
				"Amazon Seller Central Settings", "Amazon Seller Central Settings", "access_token"
			)
	return generate_access_token()

def generate_access_token():
	'''
		Method to generate access_token
	'''
	endpoint = "https://api.amazon.com/auth/o2/token"
	authorisation_token = get_authorisation_token()
	client_id = frappe.db.get_single_value("Amazon Seller Central Settings", "client_id")
	client_secret = frappe.db.get_single_value("Amazon Seller Central Settings", "client_secret")

	response_data = {
		"grant_type": "refresh_token",
		"refresh_token": authorisation_token,
		"client_secret": client_secret,
		"client_id": client_id
	}

	response = requests.post(
		endpoint,
		json=response_data,
		headers={
			"Content-Type": "application/json",
		},
	)
	if response.ok:
		response_json = response.json()
		expires_in = 3600
		if response_json.get('expires_in'):
			expires_in = response_json.get('expires_in')
		if response_json.get('access_token'):
			access_token = response_json.get('access_token')
			token_generated_on = get_datetime()
			token_exprire_on = add_to_date(token_generated_on, seconds=expires_in)
			frappe.db.set_single_value("Amazon Seller Central Settings", "access_token", access_token)
			frappe.db.set_single_value("Amazon Seller Central Settings", "token_generated_on", get_datetime(token_generated_on).strftime(DATETIME_FORMAT))
			frappe.db.set_single_value("Amazon Seller Central Settings", "token_exprire_on", get_datetime(token_exprire_on).strftime(DATETIME_FORMAT))
			return access_token

@frappe.whitelist()
def get_orders(docname):
    token = get_access_token()
    url = "https://sellingpartnerapi-eu.amazon.com/orders/v0/orders"
    response_data = {
        "MarketplaceIds": "A21TJRUUN4KGV",
        "CreatedAfter": "2024-06-25"
    }

    headers = {
        "x-amz-access-token": token,
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers, params=response_data)
    if response.ok:
        response_json = response.json()
        if response_json.get('payload'):
            payload = response_json.get('payload')
            if payload.get('Orders'):
                orders = payload.get('Orders')[:5]
                for order in orders:
                    if order.get('AmazonOrderId'):
                        order_id = order.get('AmazonOrderId')
                        order_items = get_order_items(order_id)
                        create_sales_invoices(docname, order, order_items)

@frappe.whitelist()
def get_order_items(order_id):
    '''
    Method to get order Items
    '''
    token = get_access_token()
    api_base_url = "https://sellingpartnerapi-eu.amazon.com"
    endpoint = f"{api_base_url}/orders/v0/orders/{order_id}/orderItems"

    headers = {
        "x-amz-access-token": token,
        "Content-Type": "application/json"
    }

    response = requests.get(endpoint, headers=headers)
    if response.ok:
        response_json = response.json()
        print('\n\n order_id : ', order_id)
        print(response_json)
        if response_json.get('payload'):
            payload = response_json.get('payload')
            return payload.get('OrderItems', [])
    return []

@frappe.whitelist()
def create_sales_invoices(docname, order, order_items):
    '''
    Method to Create Sales invoices.
    '''
    amazon_payment_central_settings = frappe.get_doc('Amazon Seller Central Settings', docname)
    default_amazon_customer = frappe.db.get_single_value('eSeller Settings', 'default_amazon_customer')
    default_pos_profile = frappe.db.get_single_value('eSeller Settings', 'default_pos_profile')

    if not default_amazon_customer:
        frappe.throw('Please configure the `Default Amazon Customer` in {0}'.format(get_link_to_form('eSeller Settings', 'eSeller Settings')))
    if not default_pos_profile:
        frappe.throw('Please configure the `Default POS Profile` in {0}'.format(get_link_to_form('eSeller Settings', 'eSeller Settings')))

    sales_invoice_count = 0
    for item in order_items:
        new_sales_invoice = frappe.new_doc('Sales Invoice')
        new_sales_invoice.custom_amazon_order_id = order.get('AmazonOrderId')
        new_sales_invoice.custom_transaction_type = order.get('OrderType')
        purchase_date = order.get('PurchaseDate')
        if purchase_date:
	        posting_date = datetime.strptime(purchase_date, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d')
	        new_sales_invoice.posting_date = posting_date
        new_sales_invoice.customer = default_amazon_customer
        item_code = item.get('SellerSKU')
        item_price = item.get('ItemPrice', {}).get('Amount', 0)
        new_sales_invoice.append('items', {
            'item_code': item_code,
            'qty': 1,
            'rate': item_price,
            'amount': item_price,
            'custom_amazon_fees': item_price,
            'allow_zero_valuation_rate': 1,
            'income_account': 'Sales - E',
        })
        new_sales_invoice.update_stock = 1
        new_sales_invoice.is_pos = 1
        new_sales_invoice.pos_profile = default_pos_profile
        new_sales_invoice.flags.ignore_mandatory = True
        new_sales_invoice.flags.ignore_validate = True
        new_sales_invoice.set_missing_values()
        new_sales_invoice.calculate_taxes_and_totals()
        new_sales_invoice.outstanding_amount = 0
        new_sales_invoice.disable_rounded_total = 1
        new_sales_invoice.save()
        sales_invoice_count += 1

        amazon_payment_central_settings.sales_invoice_created = 1
        amazon_payment_central_settings.save()
        frappe.msgprint(
	        f"{sales_invoice_count} Sales Invoices Created.",
	        indicator="green",
	        alert=True,
	    )
