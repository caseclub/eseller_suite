# Copyright (c) 2024, efeone and contributors
# For license information, please see license.txt

import frappe
import requests
import json
from datetime import datetime
from frappe.utils import add_days, today, get_link_to_form
from frappe.model.document import Document
from frappe import _

class AmazonSellerCentralSettings(Document):
     def validate(self):
           self.validate_after_date()

     def validate_after_date(self):
        if datetime.strptime(add_days(today(), -30), "%Y-%m-%d") > datetime.strptime(
            self.after_date, "%Y-%m-%d"
        ):
            frappe.throw(_("The date must be within the last 30 days."))

def get_authorisation_token():
	'''
		Method to get authorisation_token from the Settings
	'''
	return frappe.utils.password.get_decrypted_password(
		"Amazon Seller Central Settings", "Amazon Seller Central Settings", "authorisation_token"
	)

def get_access_token():
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
		if response_json.get('access_token'):
			access_token = response_json.get('access_token')
			return access_token

@frappe.whitelist()
def get_orders(max_results=10, created_after=None, next_token=None):
    if not created_after:
        created_after = today()
    token = get_access_token()
    url = "https://sellingpartnerapi-eu.amazon.com/orders/v0/orders"
    response_data = {
        "MarketplaceIds": "A21TJRUUN4KGV",
        "MaxResultsPerPage": max_results,
        "CreatedAfter": created_after,
        "NextToken": next_token
    }

    headers = {
        "x-amz-access-token": token,
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers, params=response_data)
    response_json = response.json()
    print("response_json ", response_json)
    if response.ok:
        if response_json.get('payload'):
            payload = response_json.get('payload')
            if payload.get('NextToken'):
                next_token = payload.get('NextToken')
                frappe.db.set_single_value('Amazon Seller Central Settings', 'next_token', next_token)
            if payload.get('Orders'):
                orders = payload.get('Orders')
                for order in orders:
                    if order.get('AmazonOrderId'):
                        order_id = order.get('AmazonOrderId')
                        order_items = get_order_items(order_id)
                        # get_financial_events(order_id)
                        create_sales_invoice(order, order_items)

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
def get_financial_events(order_id):
    '''
        Method to get order Items
    '''
    token = get_access_token()
    api_base_url = "https://sellingpartnerapi-eu.amazon.com"
    endpoint = f"{api_base_url}/finances/v0/orders/{order_id}/financialEvents"

    headers = {
        "x-amz-access-token": token,
        "Content-Type": "application/json"
    }

    response = requests.get(endpoint, headers=headers)
    response_json = response.json()
    print("\n\n\n\n\n\n\n")
    print("response_json : ", response_json)
    if response.ok:
        if response_json.get('payload'):
            payload = response_json.get('payload')
            return payload.get('FinancialEvents', [])
    return []

@frappe.whitelist()
def create_sales_invoice(order, order_items):
    '''
    Method to Create Sales invoices.
    '''
    default_amazon_customer = frappe.db.get_single_value('eSeller Settings', 'default_amazon_customer')
    default_pos_profile = frappe.db.get_single_value('eSeller Settings', 'default_pos_profile')
    create_items_if_not_exists = frappe.db.get_single_value("Amazon Seller Central Settings", "create_items_if_not_exists")

    if not default_amazon_customer:
        frappe.throw('Please configure the `Default Amazon Customer` in {0}'.format(get_link_to_form('eSeller Settings', 'eSeller Settings')))
    if not default_pos_profile:
        frappe.throw('Please configure the `Default POS Profile` in {0}'.format(get_link_to_form('eSeller Settings', 'eSeller Settings')))

    new_sales_invoice = frappe.new_doc('Sales Invoice')
    new_sales_invoice.custom_amazon_order_id = order.get('AmazonOrderId')
    new_sales_invoice.custom_transaction_type = order.get('OrderType')
    purchase_date = order.get('PurchaseDate')
    if purchase_date:
        posting_date = datetime.strptime(purchase_date, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d')
        new_sales_invoice.posting_date = posting_date
    new_sales_invoice.customer = default_amazon_customer
    for item in order_items:
        amazon_item_code = item.get('SellerSKU')
        qty = item.get('QuantityOrdered') or 1
        item_code = None
        if frappe.db.exists('Item', { 'custom_amazon_item_code': amazon_item_code }):
            item_code = frappe.db.get_value('Item', { 'custom_amazon_item_code': amazon_item_code })
        else:
            if create_items_if_not_exists:
                default_item_group = frappe.db.get_single_value("Amazon Seller Central Settings", "default_item_group")
                item_doc = frappe.new_doc('Item')
                item_doc.item_code = amazon_item_code
                item_doc.item_name = amazon_item_code 
                item_doc.description = item.get('Title') or '' 
                item_doc.item_group = default_item_group
                item_doc.custom_amazon_item_code = amazon_item_code
                item_doc.flags.ignore_mandatory = True
                item_doc.flags.ignore_validate = True
                item_doc.save()
                item_code = item_doc.name
        item_price = item.get('ItemPrice', {}).get('Amount', 0)
        if item_code:
            new_sales_invoice.append('items', {
                'item_code': item_code,
                'qty': qty,
                'rate': item_price,
                'amount': item_price,
                'allow_zero_valuation_rate': 1
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
    frappe.msgprint(
        f"Sales Invoices {new_sales_invoice.name} Created.",
        indicator="green",
        alert=True,
    )
