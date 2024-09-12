# Copyright (c) 2024, efeone and contributors
# For license information, please see license.txt

import frappe
from frappe.utils import getdate

def execute(filters=None):
	columns, data = get_columns(filters), get_data(filters)
	return columns, data

def get_columns(filters):
    '''
        Method to get columns for the report
    '''
    group_based_on = filters.get("group_based_on")
    columns = [
        {
            "label": "Date",
            "fieldname": "amazon_order_date",
            "fieldtype": "Date",
            "width": 150
        }
    ]
    if group_based_on != 'Date':
        date_columns = [
            {
                "label": "Order ID",
                "fieldname": "amazon_order_id",
                "fieldtype": "Data",
                "width": 200
            }
        ]
        columns.extend(date_columns)
    columns_common = [
        {
            "label": "Amazon Order Amount",
            "fieldname": "amazon_order_amount",
            "fieldtype": "Currency",
            "width": 180
        },
        {
            "label": "Order Amount",
            "fieldname": "order_amount",
            "fieldtype": "Currency",
            "width": 150
        }
    ]
    columns.extend(columns_common)
    return columns

def get_data(filters):
    '''
        Method to get data for report
    '''
    data  = []
    from_date = getdate(filters.get("from_date"))
    to_date = getdate(filters.get("to_date"))
    group_based_on = filters.get("group_based_on")
    if group_based_on == 'Order ID':
        group_based_on = 'amazon_order_id'
    else:
        group_based_on = 'amazon_order_date'
    so_query = '''
        SELECT
            distinct amazon_order_id,
            name,
            amazon_order_date,
            SUM(amazon_order_amount) as amazon_order_amount,
            SUM(grand_total) as order_amount
        FROM
            `tabAmazon Failed Sync Record`
        WHERE
            amazon_order_date BETWEEN %(from_date)s AND %(to_date)s
    '''
    so_query += '''
        GROUP BY
            {0}
        ORDER BY
            amazon_order_date
    '''.format(group_based_on)
    results = frappe.db.sql(so_query, {
        'group_based_on':group_based_on,
        'from_date':from_date,
        'to_date':to_date,
        'customer_type':filters.get("customer_type"),
        'fulfillment_channel':filters.get("fulfillment_channel")
    }, as_dict=True)
    return results