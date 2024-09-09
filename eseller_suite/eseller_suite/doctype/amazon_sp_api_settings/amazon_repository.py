# Copyright (c) 2024, efeone and contributors
# For license information, please see license.txt


import time
import urllib

import dateutil
import frappe
from frappe import _

from eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_sp_api import (
	CatalogItems,
	Finances,
	Orders,
	SPAPIError,
)
from eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_sp_api_settings import (
	AmazonSPAPISettings,
)
from frappe.utils.data import getdate


class AmazonRepository:
	def __init__(self, amz_setting: str | AmazonSPAPISettings) -> None:
		if isinstance(amz_setting, str):
			amz_setting = frappe.get_doc("Amazon SP API Settings", amz_setting)

		self.amz_setting = amz_setting
		self.instance_params = dict(
			client_id=self.amz_setting.client_id,
			client_secret=self.amz_setting.get_password("client_secret"),
			refresh_token=self.amz_setting.refresh_token,
			country_code=self.amz_setting.country,
		)

	def return_as_list(self, input) -> list:
		if isinstance(input, list):
			return input
		else:
			return [input]

	def call_sp_api_method(self, sp_api_method, **kwargs) -> dict:
		errors = {}
		max_retries = self.amz_setting.max_retry_limit

		for x in range(max_retries):
			try:
				result = sp_api_method(**kwargs)
				return result.get("payload")
			except SPAPIError as e:
				if e.error not in errors:
					errors[e.error] = e.error_description

				time.sleep(1)
				continue

		for error in errors:
			msg = f"<b>Error:</b> {error}<br/><b>Error Description:</b> {errors.get(error)}"
			frappe.msgprint(msg, alert=True, indicator="red")
			frappe.log_error(
				message=f"{error}: {errors.get(error)}", title=f'Method "{sp_api_method.__name__}" failed',
			)

		self.amz_setting.enable_sync = 0
		self.amz_setting.save()

		frappe.throw(
			_("Scheduled sync has been temporarily disabled because maximum retries have been exceeded!")
		)

	def get_finances_instance(self) -> Finances:
		return Finances(**self.instance_params)

	def get_account(self, name) -> str:
		account_name = frappe.db.get_value("Account", {"account_name": "Amazon {0}".format(name)})

		if not account_name:
			new_account = frappe.new_doc("Account")
			new_account.account_name = "Amazon {0}".format(name)
			new_account.company = self.amz_setting.company
			new_account.parent_account = self.amz_setting.market_place_account_group
			new_account.insert(ignore_permissions=True)
			account_name = new_account.name

		return account_name

	def get_charges_and_fees(self, order_id) -> dict:
		finances = self.get_finances_instance()
		financial_events_payload = self.call_sp_api_method(
			sp_api_method=finances.list_financial_events_by_order_id, order_id=order_id
		)

		charges_and_fees = {"charges": [], "fees": [], "tds": [], "principal_amounts":{}, "additional_discount": 0}
  
		if not (
				financial_events_payload
				and len(financial_events_payload.get("FinancialEvents", {}))
			):
				return charges_and_fees

		while True:
			shipment_event_list = financial_events_payload.get("FinancialEvents", {}).get(
				"ShipmentEventList", []
			)
			service_fee_event_list = financial_events_payload.get("FinancialEvents", {}).get(
				"ServiceFeeEventList", []
			)
			next_token = financial_events_payload.get("NextToken")
			principal_amounts = {}
			promotion_discount = 0
			for shipment_event in shipment_event_list:
				if shipment_event:
					for shipment_item in shipment_event.get("ShipmentItemList", []):
						promotion_list = shipment_item.get("PromotionList", [])
						seller_sku = shipment_item.get("SellerSKU")
						qty = shipment_item.get("QuantityShipped")
						charges = shipment_item.get("ItemChargeList", [])
						fees = shipment_item.get("ItemFeeList", [])
						tds_list = shipment_item.get("ItemTaxWithheldList", [])
						tdss = []
						if tds_list:
							tdss = tds_list[0].get("TaxesWithheld", [])

						for charge in charges:
							charge_type = charge.get("ChargeType")
							amount = charge.get("ChargeAmount", {}).get("CurrencyAmount", 0)

							if charge_type != "Principal" and float(amount) != 0:
								charge_account = self.get_account(charge_type)
								charges_and_fees.get("charges").append(
									{
										"charge_type": "Actual",
										"account_head": charge_account,
										"tax_amount": amount,
										"description": charge_type + " for " + seller_sku,
									}
								)
							if charge_type == 'Principal':
								principal_amounts[seller_sku] = round((float(amount)/qty), 2)

						for fee in fees:
							fee_type = fee.get("FeeType")
							amount = fee.get("FeeAmount", {}).get("CurrencyAmount", 0)

							if float(amount) != 0:
								fee_account = self.get_account(fee_type)
								charges_and_fees.get("fees").append(
									{
										"charge_type": "Actual",
										"account_head": fee_account,
										"tax_amount": amount,
										"description": fee_type + " for " + seller_sku,
									}
								)

						for tds in tdss:
							tds_type = tds.get("ChargeType")
							amount = tds.get("ChargeAmount", {}).get("CurrencyAmount", 0)
							if float(amount) != 0:
								tds_account = self.get_account(tds_type)
								charges_and_fees.get("tds").append(
									{
										"charge_type": "Actual",
										"account_head": tds_account,
										"tax_amount": amount,
										"description": tds_type + " for " + seller_sku,
									}
								)

						for promotion in promotion_list:
							amount = promotion.get("PromotionAmount", {}).get("CurrencyAmount", 0)
							promotion_discount += float(amount)

			charges_and_fees["principal_amounts"] = principal_amounts
			charges_and_fees["additional_discount"] = promotion_discount

			for service_fee in service_fee_event_list:
				if service_fee:
					for service_fee_item in service_fee.get("FeeList", []):
						fee_type = service_fee_item.get("FeeType")
						amount = service_fee_item.get("FeeAmount", {}).get("CurrencyAmount", 0)
						if float(amount) != 0:
							fee_account = self.get_account(fee_type)
							charges_and_fees.get("fees").append(
                                {
                                    "charge_type": "Actual",
                                    "account_head": fee_account,
                                    "tax_amount": amount,
                                    "description": fee_type + " for " + seller_sku,
                                }
                            )

			if not next_token:
				break

			financial_events_payload = self.call_sp_api_method(
				sp_api_method=finances.list_financial_events_by_order_id,
				order_id=order_id,
				next_token=next_token,
			)

		return charges_and_fees

	def get_orders_instance(self) -> Orders:
		return Orders(**self.instance_params)

	def create_item(self, order_item) -> str:
		def create_item_group(amazon_item) -> str:
			if not amazon_item:
				return self.amz_setting.parent_item_group
			if not amazon_item.get("AttributeSets"):
				return self.amz_setting.parent_item_group
			item_group_name = amazon_item.get("AttributeSets")[0].get("ProductGroup")

			if item_group_name:
				item_group = frappe.db.get_value("Item Group", filters={"item_group_name": item_group_name})

				if not item_group:
					new_item_group = frappe.new_doc("Item Group")
					new_item_group.item_group_name = item_group_name
					new_item_group.parent_item_group = self.amz_setting.parent_item_group
					new_item_group.insert()
					return new_item_group.item_group_name
				return item_group

			raise (KeyError("ProductGroup"))

		def create_brand(amazon_item) -> str:
			if not amazon_item:
				return
			if not amazon_item.get("AttributeSets"):
				return

			brand_name = amazon_item.get("AttributeSets")[0].get("Brand")

			if not brand_name:
				return

			existing_brand = frappe.db.get_value("Brand", filters={"brand": brand_name})

			if not existing_brand:
				brand = frappe.new_doc("Brand")
				brand.brand = brand_name
				brand.insert()
				return brand.brand
			return existing_brand

		def create_manufacturer(amazon_item) -> str:
			if not amazon_item:
				return
			if not amazon_item.get("AttributeSets"):
				return
	  
			manufacturer_name = amazon_item.get("AttributeSets")[0].get("Manufacturer")

			if not manufacturer_name:
				return

			existing_manufacturer = frappe.db.get_value(
				"Manufacturer", filters={"short_name": manufacturer_name}
			)

			if not existing_manufacturer:
				manufacturer = frappe.new_doc("Manufacturer")
				manufacturer.short_name = manufacturer_name
				manufacturer.insert()
				return manufacturer.short_name
			return existing_manufacturer

		def create_item_price(amazon_item, item_code) -> None:
			if not amazon_item:
				return
			if not amazon_item.get("AttributeSets"):
				return
	  
			item_price = frappe.new_doc("Item Price")
			item_price.price_list = self.amz_setting.price_list
			item_price.price_list_rate = (
				amazon_item.get("AttributeSets")[0].get("ListPrice", {}).get("Amount") or 0
			)
			item_price.item_code = item_code
			item_price.insert()

		catalog_items = self.get_catalog_items_instance()
		amazon_item = catalog_items.get_catalog_item(order_item["ASIN"])["payload"]

		item = frappe.new_doc("Item")
		item.item_group = create_item_group(amazon_item)
		item.brand = create_brand(amazon_item)
		item.manufacturer = create_manufacturer(amazon_item)
		item.amazon_item_code = order_item["SellerSKU"]
		item.item_code = order_item["SellerSKU"]
		item.item_name = order_item["SellerSKU"]
		item.description = order_item["Title"]
		item.insert(ignore_permissions=True)

		create_item_price(amazon_item, item.item_code)

		return item.name

	def get_item_code(self, order_item) -> str:
		if frappe.db.exists('Item', { 'amazon_item_code': order_item['SellerSKU']}):
			return frappe.db.get_value('Item', { 'amazon_item_code': order_item['SellerSKU']})

		item_code = self.create_item(order_item)
		return item_code

	def get_order_items(self, order_id) -> list:
		orders = self.get_orders_instance()
		order_items_payload = self.call_sp_api_method(
			sp_api_method=orders.get_order_items, order_id=order_id
		)

		final_order_items = []
		warehouse = self.amz_setting.warehouse

		while True:
			order_items_list = order_items_payload.get("OrderItems")
			next_token = order_items_payload.get("NextToken")

			for order_item in order_items_list:
				if order_item.get("QuantityOrdered") > 0:
					item_rate = order_item.get("ItemPrice", {}).get("Amount", 0)
					item_qty = order_item.get("QuantityOrdered")
					final_order_items.append(
						{
							"item_code": self.get_item_code(order_item),
							"item_name": order_item.get("SellerSKU"),
							"description": order_item.get("Title"),
							"rate": item_rate,
							"base_rate": item_rate,
							"qty": item_qty,
							"amount": item_rate*item_qty,
							"base_amount": item_rate*item_qty,
							"uom": "Nos",
							"stock_uom": "Nos",
							"warehouse": warehouse,
							"conversion_factor": 1.0,
							"allow_zero_valuation_rate": 1
						}
					)

			if not next_token:
				break

			order_items_payload = self.call_sp_api_method(
				sp_api_method=orders.get_order_items, order_id=order_id, next_token=next_token,
			)

		return final_order_items

	def create_sales_order(self, order) -> str | None:
		def create_customer(order) -> str:
			order_customer_name = ""
			buyer_info = order.get("BuyerInfo")

			if buyer_info and buyer_info.get("BuyerEmail"):
				order_customer_name = buyer_info.get("BuyerEmail")
			else:
				order_customer_name = f"Buyer - {order.get('AmazonOrderId')}"

			existing_customer_name = frappe.db.get_value(
				"Customer", filters={"name": order_customer_name}, fieldname="name"
			)

			if existing_customer_name:
				filters = [
					["Dynamic Link", "link_doctype", "=", "Customer"],
					["Dynamic Link", "link_name", "=", existing_customer_name],
					["Dynamic Link", "parenttype", "=", "Contact"],
				]

				existing_contacts = frappe.get_list("Contact", filters)

				if not existing_contacts:
					new_contact = frappe.new_doc("Contact")
					new_contact.first_name = order_customer_name
					new_contact.append(
						"links", {"link_doctype": "Customer", "link_name": existing_customer_name},
					)
					new_contact.insert()

				return existing_customer_name
			else:
				new_customer = frappe.new_doc("Customer")
				new_customer.customer_name = order_customer_name
				new_customer.customer_group = self.amz_setting.customer_group
				new_customer.territory = self.amz_setting.territory
				new_customer.customer_type = self.amz_setting.customer_type
				new_customer.save()

				new_contact = frappe.new_doc("Contact")
				new_contact.first_name = order_customer_name
				new_contact.append("links", {"link_doctype": "Customer", "link_name": new_customer.name})

				new_contact.insert()

				return new_customer.name

		def create_address(order, customer_name) -> str | None:
			shipping_address = order.get("ShippingAddress")

			if not shipping_address:
				return
			else:
				make_address = frappe.new_doc("Address")
				make_address.address_line1 = shipping_address.get("AddressLine1", "Not Provided")
				make_address.city = shipping_address.get("City", "Not Provided")
				amazon_state = shipping_address.get("StateOrRegion")
				if frappe.db.exists("Amazon State Mapping", {"amazon_state": amazon_state}):
					make_address.state = frappe.db.get_value("Amazon State Mapping", {"amazon_state": amazon_state}, "state")
				else:
					failed_sync_record = frappe.new_doc('Amazon Failed Sync Record')
					failed_sync_record.amazon_order_id = order_id
					failed_sync_record.remarks = 'No State Mapping found for {0}'.format(amazon_state)
					failed_sync_record.save(ignore_permissions=True)
					return
				make_address.pincode = shipping_address.get("PostalCode")

				filters = [
					["Dynamic Link", "link_doctype", "=", "Customer"],
					["Dynamic Link", "link_name", "=", customer_name],
					["Dynamic Link", "parenttype", "=", "Address"],
				]
				existing_address = frappe.get_list("Address", filters)

				for address in existing_address:
					address_doc = frappe.get_doc("Address", address["name"])
					if (
						address_doc.address_line1 == make_address.address_line1
						and address_doc.pincode == make_address.pincode
					):
						return address

				make_address.append("links", {"link_doctype": "Customer", "link_name": customer_name})
				make_address.address_type = "Shipping"
				make_address.insert()

		def get_refunds(self, order_id) -> dict:
			finances = self.get_finances_instance()
			financial_events_payload = self.call_sp_api_method(
				sp_api_method=finances.list_financial_events_by_order_id, order_id=order_id
			)
   
			if not (
				financial_events_payload
				and financial_events_payload.get("FinancialEvents")
				and financial_events_payload["FinancialEvents"].get("RefundEventList")
			):
				return []

   
			refund_events = []

			while True:
				refund_event_list = financial_events_payload.get("FinancialEvents", {}).get("RefundEventList", [])
				service_fee_event_list = financial_events_payload.get("FinancialEvents", {}).get("ServiceFeeEventList", [])
				next_token = financial_events_payload.get("NextToken")

				charges_and_fees = {"posting_date": "", "items":[], "charges": [], "fees": []}

				for refund_event in refund_event_list:
					if refund_event:
						charges_and_fees["posting_date"] = refund_event.get("PostedDate")
						for refund_item in refund_event.get("ShipmentItemAdjustmentList", []):
							charges = refund_item.get("ItemChargeAdjustmentList", [])
							fees = refund_item.get("ItemFeeAdjustmentList", [])
							seller_sku = refund_item.get("SellerSKU")
							item_code = None
							if frappe.db.exists('Item', { 'amazon_item_code': seller_sku }):
								item_code =  frappe.db.get_value('Item', { 'amazon_item_code': seller_sku })

							for charge in charges:
								charge_type = charge.get("ChargeType")
								amount = charge.get("ChargeAmount", {}).get("CurrencyAmount", 0)

								if charge_type != "Principal" and float(amount) != 0:
									charge_account = self.get_account(charge_type)
									charges_and_fees.get("charges").append(
										{
											"charge_type": "Actual",
											"account_head": charge_account,
											"tax_amount": amount,
											"description": charge_type + " refund for " + seller_sku,
										}
									)
								else:
									charges_and_fees.get("items").append(
										{
											"item_code": item_code,
											"qty": refund_item.get("QuantityShipped"),
											"amount": charge.get("ChargeAmount", {}).get("CurrencyAmount", 0)
										}
									)

							for fee in fees:
								fee_type = fee.get("FeeType")
								amount = fee.get("FeeAmount", {}).get("CurrencyAmount", 0)

								if float(amount) != 0:
									fee_account = self.get_account(fee_type)
									charges_and_fees.get("fees").append(
										{
											"charge_type": "Actual",
											"account_head": fee_account,
											"tax_amount": amount,
											"description": fee_type + " refund for " + seller_sku,
										}
									)

				for service_fee in service_fee_event_list:
					if service_fee:
						for service_fee_item in service_fee.get("FeeList", []):
							fee_type = service_fee_item.get("FeeType")
							amount = service_fee_item.get("FeeAmount", {}).get("CurrencyAmount", 0)
							if float(amount) != 0:
								fee_account = self.get_account(fee_type)
								charges_and_fees.get("fees").append(
                                    {
                                    "charge_type": "Actual",
                                    "account_head": fee_account,
                                    "tax_amount": amount,
                                    "description": fee_type + " for " + seller_sku,
                                    }
                                )

				refund_events.append(charges_and_fees)

				if not next_token:
					break

				financial_events_payload = self.call_sp_api_method(
					sp_api_method=finances.list_financial_events_by_order_id,
					order_id=order_id,
					next_token=next_token,
				)

			return refund_events

		order_id = order.get("AmazonOrderId")
		so_id = None
		so_docstatus = 0
		refunds = get_refunds(self, order_id)
		if frappe.db.exists("Sales Order", {"amazon_order_id": order_id}):
			so_id, so_docstatus = frappe.db.get_value("Sales Order", filters={"amazon_order_id": order_id}, fieldname=["name", "docstatus"])

		if so_id and refunds and so_docstatus:
			for refund in refunds:
				if not frappe.db.exists("Sales Invoice", { "amazon_order_id": order_id, "docstatus":1, "is_return":0 }):
					failed_sync_record = frappe.new_doc('Amazon Failed Sync Record')
					failed_sync_record.amazon_order_id = order_id
					failed_sync_record.remarks = 'Failed to create return Sales Invoice, Not able to find any Sales Invoice with this Amazon Order ID. Sales Order ID : {0}'.format(so)
					failed_sync_record.payload = refund
					failed_sync_record.save(ignore_permissions=True)
					break

				return_created = False
				si = frappe.db.get_value("Sales Invoice", { "amazon_order_id": order_id, "docstatus":1, "is_return":0  })
				return_si = frappe.new_doc("Sales Invoice")
				return_si.is_return = 1
				return_si.update_stock = 1
				return_si.return_against = si
				return_si.customer = frappe.db.get_value("Sales Invoice", si, "customer")
				for item in refund.get("items", []):
					if frappe.db.exists("Sales Invoice Item", {"parent": si, "item_code": item.get('item_code'), "refunded":0 }):
						return_si.append("items", {
                            "item_code": item.get('item_code'),
                            "qty": -1 * float(item.get('qty')),
                            "rate": abs(float(item.get('amount'))/float(item.get('qty'))),
                            "sales_order": so_id,
                            "sales_invoice_item": frappe.db.get_value("Sales Invoice Item", {"parent": si, "item_code": item.get('item_code')}, "name")
                        })
						frappe.db.set_value("Sales Invoice Item", {"parent": si, "item_code": item.get('item_code')}, "refunded", 1)
						return_created = True

				if return_created:
					for charge in refund.get("charges", []):
						return_si.append("taxes", charge)

					for fee in refund.get("fees", []):
						return_si.append("taxes", fee)

					return_si.amazon_order_id = frappe.db.get_value("Sales Invoice", si, "amazon_order_id")
					return_si.disable_rounded_total = 1
					return_si.update_outstanding_for_self = 0
					return_si.insert(ignore_permissions=True)
					return_si.submit()

			return so_id

		else:
			if so_docstatus:
				return
			if not so_id and refunds:
				failed_sync_record = frappe.new_doc('Amazon Failed Sync Record')
				failed_sync_record.amazon_order_id = order_id
				failed_sync_record.remarks = 'Failed to create Sales Order for Order ID : {0}. It has refund events in it.'.format(order_id)
				failed_sync_record.payload = refunds[0]
				failed_sync_record.save(ignore_permissions=True)
				return
			if not so_id:
				so = frappe.new_doc("Sales Order")
			else:
				so = frappe.get_doc('Sales Order', so_id)

			customer_name = create_customer(order)
			create_address(order, customer_name)

			delivery_date = dateutil.parser.parse(order.get("LatestShipDate")).strftime("%Y-%m-%d")
			transaction_date = dateutil.parser.parse(order.get("PurchaseDate")).strftime("%Y-%m-%d")

			so.amazon_order_id = order_id
			so.marketplace_id = order.get("MarketplaceId")
			so.amazon_order_status = order.get("OrderStatus")
			so.fulfillment_channel = order.get("FulfillmentChannel")
			so.customer = customer_name
			so.delivery_date = delivery_date if getdate(delivery_date) > getdate(transaction_date) else transaction_date
			so.transaction_date = transaction_date
			so.company = self.amz_setting.company
			warehouse = self.amz_setting.warehouse
			if so.fulfillment_channel:
				if so.fulfillment_channel=='AFN':
					warehouse = self.amz_setting.afn_warehouse
			if order.get("IsBusinessOrder"):
				so.amazon_customer_type = 'B2B'
			else:
				so.amazon_customer_type = 'B2C'
			so.set_warehouse = warehouse

			items = self.get_order_items(order_id)

			if not items:
				if not so_id:
					return
				else:
					so.flags.ignore_mandatory = True
					so.disable_rounded_total = 1
					so.custom_validate()
					if so.grand_total>=0:
						so.save(ignore_permissions=True)
					else:
						failed_sync_record = frappe.new_doc('Amazon Failed Sync Record')
						failed_sync_record.amazon_order_id = order_id
						failed_sync_record.remarks = 'Failed to create Sales Order for {0}. Sales Order grand Total = {1}'.format(order_id, so.grand_total)
						failed_sync_record.payload = so.as_dict()
						failed_sync_record.posting_date = so.transaction_date
						failed_sync_record.grand_total = so.grand_total
						failed_sync_record.save(ignore_permissions=True)
					return

			so.items = []
			so.taxes = []
			so.taxes_and_charges = ''

			for item in items:
				item["warehouse"] = warehouse
				so.append("items", item)

			taxes_and_charges = self.amz_setting.taxes_charges

			if taxes_and_charges:
				charges_and_fees = self.get_charges_and_fees(order_id)
				if charges_and_fees.get("principal_amounts"):
					principal_amounts = charges_and_fees.get("principal_amounts")
					for item_row in so.items:
						if item_row.item_name and principal_amounts.get(item_row.item_name):
							pricipal_amount = float(principal_amounts.get(item_row.item_name)) or 0
							qty = item_row.qty
							if pricipal_amount:
								item_row.rate = pricipal_amount
								item_row.base_rate = pricipal_amount
								item_row.amount = pricipal_amount*qty
								item_row.base_amount = pricipal_amount*qty

				for charge in charges_and_fees.get("charges"):
					if charge:
						so.append("taxes", charge)

				for fee in charges_and_fees.get("fees"):
					if fee:
						so.append("taxes", fee)

				for tds in charges_and_fees.get("tds"):
					if tds:
						so.append("taxes", tds)

				if charges_and_fees.get("additional_discount"):
					so.discount_amount = float(charges_and_fees.get("additional_discount")) * -1

			so.flags.ignore_mandatory = True
			so.disable_rounded_total = 1
			so.custom_validate()
			if so.grand_total>=0:
				# so.flags.ignore_validate = True
				so.save(ignore_permissions=True)

				so.amazon_order_status = order.get("OrderStatus")
	
				order_statuses = [
					"Shipped",
					"InvoiceUnconfirmed",
					"Canceled",
					"Unfulfillable",
				]

				if order.get("OrderStatus") in order_statuses:
					so.submit()
			else:
				failed_sync_record = frappe.new_doc('Amazon Failed Sync Record')
				failed_sync_record.amazon_order_id = order_id
				failed_sync_record.remarks = 'Failed to create Sales Order for {0}. Sales Order grand Total = {1}'.format(order_id, so.grand_total)
				failed_sync_record.posting_date = so.transaction_date
				failed_sync_record.grand_total = so.grand_total
				if not so_id:
					failed_sync_record.payload = so.as_dict()
				failed_sync_record.save(ignore_permissions=True)

			return so.name

	def get_orders(self, last_updated_after, amazon_order_ids=None) -> list:
		orders = self.get_orders_instance()
		order_statuses = [
			"Shipped",
			"InvoiceUnconfirmed",
			"Canceled",
			"Unfulfillable",
		]
		fulfillment_channels = ["FBA", "SellerFulfilled"]
		orders_payload = self.call_sp_api_method(
            sp_api_method=orders.get_orders,
            last_updated_after=last_updated_after,
            order_statuses=order_statuses,
            fulfillment_channels=fulfillment_channels,
            max_results=50,
        )

		sales_orders = []

		while True:
			orders_list = orders_payload.get("Orders")
			next_token = orders_payload.get("NextToken")

			if not orders_list or len(orders_list) == 0:
				break

			for order in orders_list:
				sales_order = self.create_sales_order(order)
				if sales_order:
					sales_orders.append(sales_order)

			if not next_token:
				break
			if amazon_order_ids:
				orders_payload = self.call_sp_api_method(
                    sp_api_method=orders.get_orders, last_updated_after=last_updated_after, next_token=next_token, amazon_order_ids=amazon_order_ids,
                )
			else:
				orders_payload = self.call_sp_api_method(
                    sp_api_method=orders.get_orders, last_updated_after=last_updated_after, next_token=next_token,
                )

		return sales_orders

	def get_order(self, amazon_order_ids) -> list:
		orders = self.get_orders_instance()
		fulfillment_channels = ["FBA", "SellerFulfilled"]
		orders_payload = self.call_sp_api_method(
            sp_api_method=orders.get_orders,
            fulfillment_channels=fulfillment_channels,
            amazon_order_ids=amazon_order_ids,
        )

		sales_orders = []

		orders_list = orders_payload.get("Orders")
		if orders_list:
			for order in orders_list:
				sales_order = self.create_sales_order(order)
				if sales_order:
					sales_orders.append(sales_order)
			orders_payload = self.call_sp_api_method(
                sp_api_method=orders.get_orders, amazon_order_ids=amazon_order_ids,
            )
		return sales_orders

	def get_catalog_items_instance(self) -> CatalogItems:
		return CatalogItems(**self.instance_params)

def get_orders(amz_setting_name, last_updated_after) -> list:
	ar = AmazonRepository(amz_setting_name)
	return ar.get_orders(last_updated_after)

@frappe.whitelist()
def get_order(amz_setting_name, amazon_order_ids) -> list:
	ar = AmazonRepository(amz_setting_name)
	return ar.get_order(amazon_order_ids)