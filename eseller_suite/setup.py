import frappe
from frappe import _
from frappe.custom.doctype.custom_field.custom_field import create_custom_fields

def after_install():
	create_custom_fields(get_item_custom_fields(), ignore_validate=True)
	create_custom_fields(get_sales_order_custom_fields(), ignore_validate=True)
	create_custom_fields(get_sales_invoice_custom_fields(), ignore_validate=True)
	create_custom_fields(get_purchase_invoice_custom_fields(), ignore_validate=True)

def after_migrate():
	after_install()

def before_uninstall():
	delete_custom_fields(get_item_custom_fields())
	delete_custom_fields(get_sales_order_custom_fields())
	delete_custom_fields(get_sales_invoice_custom_fields())
	delete_custom_fields(get_purchase_invoice_custom_fields())

def delete_custom_fields(custom_fields: dict):
	'''
		Method to Delete custom fields
		args:
			custom_fields: a dict like `{'Sales Order': [{fieldname: 'amazon_order_id', ...}]}`
	'''
	for doctype, fields in custom_fields.items():
		frappe.db.delete(
			"Custom Field",
			{
				"fieldname": ("in", [field["fieldname"] for field in fields]),
				"dt": doctype,
			},
		)
		frappe.clear_cache(doctype=doctype)

def get_item_custom_fields():
	'''
        eSeller Suite specific custom fields in Item
    '''
	return {
		"Item": [
			{
				"fieldname": "amazon_item_code",
				"fieldtype": "Data",
				"label": "Amazon Item Code",
				"insert_after": "item_code",
				"in_standard_filter": 1,
				"unique": 1,
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "flipkart_item_code",
				"fieldtype": "Data",
				"label": "Flipkart Item Code",
				"insert_after": "amazon_item_code",
				"in_standard_filter": 1,
				"unique": 1,
				"read_only": 1,
				"no_copy": 1
			}
		]
	}

def get_sales_order_custom_fields():
	'''
        eSeller Suite specific custom fields in Sales Order
    '''
	return {
		"Sales Order": [
			{
				"fieldname": "amazon_order_id",
				"fieldtype": "Data",
				"label": "Amazon Order ID",
				"insert_after": "title",
				"in_standard_filter": 1,
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "amazon_transaction_type",
				"fieldtype": "Data",
				"label": "Amazon Transaction Type",
				"insert_after": "amazon_order_id",
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "amazon_order_status",
				"fieldtype": "Select",
				"label": "Amazon Order Status",
				"insert_after": "amazon_transaction_type",
				"options": "\nShipped\nInvoiceUnconfirmed\nCanceled\nUnfulfillable\nPending\nUnshipped",
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "marketplace_id",
				"fieldtype": "Data",
				"label": "Marketplace ID",
				"insert_after": "amazon_order_status",
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "amazon_customer_type",
				"fieldtype": "Select",
				"label": "Customer Type",
				"insert_after": "marketplace_id",
				"read_only": 1,
				"no_copy": 1,
				"options": "\nB2B\nB2C"
			},
			{
				"fieldname": "fulfillment_channel",
				"fieldtype": "Select",
				"label": "Fulfillment Channel",
				"insert_after": "delivery_date",
				"read_only": 1,
				"no_copy": 1,
				"options": "\nAFN\nMFN"
			},
			{
				"fieldname": "replaced_order_id",
				"fieldtype": "Data",
				"label": "Replaced Order ID",
				"insert_after": "is_export_with_gst",
				"read_only": 1,
				"no_copy": 1,
			},
			{
				"fieldname": "amazon_order_amount",
				"fieldtype": "Currency",
				"label": "Amazon Order Amount",
				"insert_after": "base_in_words",
				"read_only": 1,
				"no_copy": 1,
			}
		]
	}

def get_sales_invoice_custom_fields():
	'''
        eSeller Suite specific custom fields in Sales Invoice
    '''
	return {
		"Sales Invoice": [
			{
				"fieldname": "amazon_order_id",
				"fieldtype": "Data",
				"label": "Amazon Order ID",
				"insert_after": "due_date",
				"in_standard_filter": 1,
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "amazon_transaction_type",
				"fieldtype": "Data",
				"label": "Amazon Transaction Type",
				"insert_after": "amazon_order_id",
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "amazon_order_status",
				"fieldtype": "Select",
				"label": "Amazon Order Status",
				"insert_after": "amazon_transaction_type",
				"options": "\nShipped\nInvoiceUnconfirmed\nCanceled\nUnfulfillable\nPending\nUnshipped",
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "amazon_customer_type",
				"fieldtype": "Select",
				"label": "Customer Type",
				"insert_after": "amazon_order_status",
				"read_only": 1,
				"no_copy": 1,
				"options": "\nB2B\nB2C"
			},
            {
				"fieldname": "fulfillment_channel",
				"fieldtype": "Select",
				"label": "Fulfillment Channel",
				"insert_after": "amazon_customer_type",
				"read_only": 1,
				"no_copy": 1,
				"options": "\nAFN\nMFN"
			},
			{
				"fieldname": "replaced_order_id",
				"fieldtype": "Data",
				"label": "Replaced Order ID",
				"insert_after": "is_return",
				"read_only": 1,
				"no_copy": 1,
			},
			{
				"fieldname": "amazon_order_amount",
				"fieldtype": "Currency",
				"label": "Amazon Order Amount",
				"insert_after": "base_in_words",
				"read_only": 1,
				"no_copy": 1,
			}
		],
		"Sales Invoice Item": [
			{
				"fieldname": "amazon_details",
				"fieldtype": "Section Break",
				"label": "Amazon Details",
				"insert_after": "customer_item_code",
			},
			{
				"fieldname": "total_product_charges",
				"fieldtype": "Currency",
				"label": "Total Product Charges",
				"insert_after": "amazon_details",
			},
			{
				"fieldname": "total_promotional_rebates",
				"fieldtype": "Currency",
				"label": "Total Promotional Rebates",
				"insert_after": "total_product_charges",
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "other",
				"fieldtype": "Currency",
				"label": "Other",
				"insert_after": "amazon_details",
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "refunded",
				"fieldtype": "Check",
				"label": "Refunded",
				"insert_after": "other",
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "cb_1",
				"fieldtype": "Column Break",
				"insert_after": "refunded"
			},
			{
				"fieldname": "amazon_fees",
				"fieldtype": "Currency",
				"label": "Amazon Fees",
				"insert_after": "cb_1",
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "total_inr",
				"fieldtype": "Currency",
				"label": "Total INR",
				"insert_after": "amazon_fees",
				"read_only": 1,
				"no_copy": 1
			},
		]
	}

def get_purchase_invoice_custom_fields():
	'''
        eSeller Suite specific custom fields in Purchase Invoice
    '''
	return {
		"Purchase Invoice": [
			{
				"fieldname": "flipkart_transaction_id",
				"fieldtype": "Data",
				"label": "Flipkart Transaction ID",
				"insert_after": "due_date",
				"in_standard_filter": 1,
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "flipkart_transaction_type",
				"fieldtype": "Data",
				"label": "Flipkart Transaction Type",
				"insert_after": "flipkart_transaction_id",
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "wallet_redeem",
				"fieldtype": "Data",
				"label": "Wallet Redeem",
				"insert_after": "flipkart_transaction_type",
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "wallet_topup",
				"fieldtype": "Data",
				"label": "Wallet Topup",
				"insert_after": "wallet_redeem",
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "wallet_refund",
				"fieldtype": "Data",
				"label": "Wallet Refund",
				"insert_after": "wallet_topup",
				"read_only": 1,
				"no_copy": 1
			}
		],
		"Purchase Invoice Item": [
			{
				"fieldname": "flipkart_payment_details",
				"fieldtype": "Section Break",
				"label": "Flipkart Payment Details",
				"insert_after": "customer_item_code",
			},
			{
				"fieldname": "wallet_redeem_reversal",
				"fieldtype": "Data",
				"label": "Wallet Redeem Reversal",
				"insert_after": "flipkart_payment_details",
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "gst_on_ads_fees",
				"fieldtype": "Data",
				"label": "GST on Ads Fees",
				"insert_after": "wallet_redeem_reversal",
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "settlement_value",
				"fieldtype": "Data",
				"label": "Settlement Value",
				"insert_after": "gst_on_ads_fees",
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "cb_1",
				"fieldtype": "Column Break",
				"insert_after": "settlement_value"
			},
			{
				"fieldname": "wallet_redeem",
				"fieldtype": "Data",
				"label": "Wallet Redeem",
				"insert_after": "cb_1",
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "wallet_topup",
				"fieldtype": "Data",
				"label": "Wallet Topup",
				"insert_after": "wallet_redeem",
				"read_only": 1,
				"no_copy": 1
			},
			{
				"fieldname": "wallet_refund",
				"fieldtype": "Data",
				"label": "Wallet Refund",
				"insert_after": "wallet_topup",
				"read_only": 1,
				"no_copy": 1
			}
        ]
	}

def create_property_setters(property_setter_datas):
	'''
		Method to create custom property setters
		args:
			property_setter_datas : list of dict of property setter obj
	'''
	for property_setter_data in property_setter_datas:
		if frappe.db.exists("Property Setter", property_setter_data):
			continue
		property_setter = frappe.new_doc("Property Setter")
		property_setter.update(property_setter_data)
		property_setter.flags.ignore_permissions = True
		property_setter.insert()