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
            "fieldname": "transaction_date",
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
                "width": 150
            },
            {
                "label": "Customer Type",
                "fieldname": "customer_type",
                "fieldtype": "Select",
                "options": "\nB2B\nB2C",
                "width": 150
            },
            {
                "label": "Amazon Order Status",
                "fieldname": "amazon_order_status",
                "fieldtype": "Select",
                "options": "\nShipped\nInvoiceUnconfirmed\nCanceled\nUnfulfillable\nPending\nUnshipped",
                "width": 150
            },
            {
                "label": "Fulfillment Channel",
                "fieldname": "fulfillment_channel",
                "fieldtype": "Select",
                "options": "\nAFN\nMFN",
                "width": 150
            }
        ]
        columns.extend(date_columns)
    columns_common = [
        {
            "label": "Amazon Order Amount",
            "fieldname": "amazon_order_amount",
            "fieldtype": "Currency",
            "width": 150
        },
        {
            "label": "Order Amount",
            "fieldname": "order_amount",
            "fieldtype": "Currency",
            "width": 150
        },
        {
            "label": "Invoice Amount",
            "fieldname": "invoice_amount",
            "fieldtype": "Currency",
            "width": 150
        },
        {
            "label": "Return Amount",
            "fieldname": "return_amount",
            "fieldtype": "Currency",
            "width": 180
        },
        {
            "label": "Cancelled Amount",
            "fieldname": "cancelled_amount",
            "fieldtype": "Currency",
            "width": 150
        },
        {
            "label": "Total Amount",
            "fieldname": "total_amount",
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
        group_based_on = 'transaction_date'
    so_query = '''
        SELECT
            name,
            transaction_date,
            amazon_order_id,
            amazon_customer_type as customer_type,
            amazon_order_status,
            fulfillment_channel,
            SUM(amazon_order_amount) as amazon_order_amount,
            SUM(grand_total) as order_amount
        FROM
            `tabSales Order`
        WHERE
            transaction_date BETWEEN %(from_date)s AND %(to_date)s
    '''
    if filters.get("customer_type"):
        so_query += '''
            AND amazon_customer_type = %(customer_type)s
        '''
    if filters.get("fulfillment_channel"):
        so_query += '''
            AND fulfillment_channel = %(fulfillment_channel)s
        '''
    so_query += '''
        GROUP BY
            {0}
        ORDER BY
            transaction_date
    '''.format(group_based_on)
    results = frappe.db.sql(so_query, {
        'group_based_on':group_based_on,
        'from_date':from_date,
        'to_date':to_date,
        'customer_type':filters.get("customer_type"),
        'fulfillment_channel':filters.get("fulfillment_channel")
    }, as_dict=True)
    for row in results:
        print("\n group_based_on : ", group_based_on)
        print("row : ", row)
        if group_based_on == 'amazon_order_id':
            row['invoice_amount'] = get_invoice_amount(amazon_order_id=row.get(group_based_on))
            row['return_amount'] = get_total_returns(amazon_order_id=row.get(group_based_on))
            row['cancelled_amount'] = get_total_cancels(amazon_order_id=row.get(group_based_on))
        else:
            row['invoice_amount'] = get_invoice_amount(transaction_date=row.get(group_based_on))
            row['return_amount'] = get_total_returns(transaction_date=row.get(group_based_on))
            row['cancelled_amount'] = get_total_cancels(transaction_date=row.get(group_based_on))
        row['total_amount'] = row['order_amount'] - row['return_amount'] -  row['cancelled_amount']
        data.append(row)
    return data

def get_invoice_amount(amazon_order_id=None, transaction_date=None):
    '''
        Method to get Total Invoiced amount with Amazon Order ID or Date
    '''
    print("get_invoice_amount")
    if amazon_order_id:
        print("amazon_order_id : ", amazon_order_id)
    if transaction_date:
        print("transaction_date : ", transaction_date)
    total_invoice_amount = 0
    if transaction_date or amazon_order_id:
        query = '''
            SELECT
                IFNULL(SUM(grand_total), 0) as total
            FROM
                `tabSales Invoice`
            WHERE
                is_return = 0 AND
                docstatus != 2
        '''
        if amazon_order_id:
            query += '''
                AND amazon_order_id = %(amazon_order_id)s
            GROUP BY
                amazon_order_id
            '''
        if transaction_date:
            query += '''
                AND posting_date = %(transaction_date)s
            GROUP BY
                posting_date
            '''
        output = frappe.db.sql(query, { 'transaction_date':transaction_date, 'amazon_order_id':amazon_order_id } ,as_dict=True)
        if output:
            if output[0] and output[0].get('total'):
                total_invoice_amount = output[0].get('total', 0)
    return total_invoice_amount

def get_total_returns(amazon_order_id=None, transaction_date=None):
    '''
        Method to get Total Returns and Cancelled amount with Amazon Order ID or Date
    '''
    total_cancels = 0
    total_returns = 0
    if transaction_date or amazon_order_id:
        #Refunded Orders
        query = '''
            SELECT
                IFNULL(SUM(grand_total), 0) as total
            FROM
                `tabSales Invoice`
            WHERE
                is_return = 1
        '''
        if amazon_order_id:
            query += '''
                AND amazon_order_id = %(amazon_order_id)s
            GROUP BY
                amazon_order_id
            '''
        if transaction_date:
            query += '''
                AND posting_date = %(transaction_date)s
            GROUP BY
                posting_date
            '''
        output = frappe.db.sql(query, { 'transaction_date':transaction_date, 'amazon_order_id':amazon_order_id } ,as_dict=True)
        if output:
            if output[0] and output[0].get('total'):
                total_returns = output[0].get('total', 0)
        if total_returns<0:
            total_returns *= -1
    return total_returns

def get_total_cancels(amazon_order_id=None, transaction_date=None):
    '''
        Method to get Total Returns and Cancelled amount with Amazon Order ID or Date
    '''
    total_cancels = 0
    if transaction_date or amazon_order_id:
        #Cancelled Orders
        query = '''
            SELECT
                IFNULL(SUM(grand_total), 0) as total
            FROM
                `tabSales Order`
            WHERE
                amazon_order_status = 'Canceled'
        '''
        if amazon_order_id:
            query += '''
                AND amazon_order_id = %(amazon_order_id)s
            GROUP BY
                amazon_order_id
            '''
        if transaction_date:
            query += '''
                AND transaction_date = %(transaction_date)s
            GROUP BY
                transaction_date
            '''
        output = frappe.db.sql(query, { 'transaction_date':transaction_date, 'amazon_order_id':amazon_order_id } ,as_dict=True)
        if output:
            if output[0] and output[0].get('total'):
                total_cancels = output[0].get('total', 0)
        if total_cancels<0:
            total_cancels *= -1
    return total_cancels