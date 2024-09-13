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
    order_query = '''
        SELECT
            DISTINCT amazon_order_id,
            name,
            amazon_order_date,
            amazon_order_amount,
            grand_total as order_amount
        FROM
            `tabAmazon Failed Sync Record`
        WHERE
            amazon_order_date BETWEEN %(from_date)s AND %(to_date)s
        GROUP BY
            amazon_order_id
        ORDER BY
            amazon_order_date
    '''
    so_query = '''
        SELECT
            amazon_order_id,
            name,
            amazon_order_date,
            SUM(amazon_order_amount) as amazon_order_amount,
            SUM(order_amount) as order_amount
        FROM
        (
            {0}
        ) AS distinct_orders
        GROUP BY
            amazon_order_date
    '''.format(order_query)
    if group_based_on == 'Order ID':
         results = frappe.db.sql(order_query, {
            'from_date':from_date,
            'to_date':to_date
        }, as_dict=True)
    else:
        results = frappe.db.sql(so_query, {
            'from_date':from_date,
            'to_date':to_date
        }, as_dict=True)
    return results