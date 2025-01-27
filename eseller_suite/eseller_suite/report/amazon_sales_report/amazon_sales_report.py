# Copyright (c) 2024, efeone and contributors
# For license information, please see license.txt

import frappe
from frappe.utils import getdate


def execute(filters=None):
    columns, data = get_columns(filters), get_data(filters)
    return columns, data


def get_columns(filters):
    """
    Method to get columns for the report.
    """
    group_based_on = filters.get("group_based_on")
    columns = [
        {
            "label": "Date",
            "fieldname": "transaction_date",
            "fieldtype": "Date",
            "width": 110,
        }
    ]

    if group_based_on != "Date":
        date_columns = [
            {
                "label": "Order ID",
                "fieldname": "amazon_order_id",
                "fieldtype": "Data",
                "width": 200,
            },
            {
                "label": "Customer Type",
                "fieldname": "customer_type",
                "fieldtype": "Select",
                "options": "\nB2B\nB2C",
                "width": 130,
            },
            {
                "label": "Amazon Status",
                "fieldname": "amazon_order_status",
                "fieldtype": "Select",
                "options": "\nShipped\nInvoiceUnconfirmed\nCanceled\nUnfulfillable\nPending\nUnshipped",
                "width": 150,
            },
            {
                "label": "Fulfillment Channel",
                "fieldname": "fulfillment_channel",
                "fieldtype": "Select",
                "options": "\nAFN\nMFN",
                "width": 160,
            },
        ]
        columns.extend(date_columns)

    columns_common = [
        {
            "label": "Amazon Amount",
            "fieldname": "amazon_order_amount",
            "fieldtype": "Currency",
            "width": 150,
        },
        {
            "label": "SO Amount",
            "fieldname": "order_amount",
            "fieldtype": "Currency",
            "width": 120,
        },
        {
            "label": "Invoice Amount",
            "fieldname": "invoice_amount",
            "fieldtype": "Currency",
            "width": 150,
        },
        {
            "label": "Return Amount",
            "fieldname": "return_amount",
            "fieldtype": "Currency",
            "width": 150,
        },
        {
            "label": "Cancelled Amount",
            "fieldname": "cancelled_amount",
            "fieldtype": "Currency",
            "width": 150,
        },
        {
            "label": "Total Invoice Amount",
            "fieldname": "total_amount",
            "fieldtype": "Currency",
            "width": 180,
        },
        {
            "label": "Total Order Amount",
            "fieldname": "total_order_amount",
            "fieldtype": "Currency",
            "width": 160,
        },
    ]
    columns.extend(columns_common)
    return columns


def get_data(filters):
    """
    Method to get data for the report.
    """
    data = []
    from_date = getdate(filters.get("from_date"))
    to_date = getdate(filters.get("to_date"))
    group_based_on = filters.get("group_based_on", "transaction_date")

    if group_based_on == "Order ID":
        group_based_on = "amazon_order_id"
    else:
        group_based_on = "transaction_date"

    # Optimized query with joins for invoice, return, and cancel sums
    so_query = """
        SELECT
            s.name,
            s.transaction_date,
            s.amazon_order_id,
            s.amazon_customer_type AS customer_type,
            s.amazon_order_status,
            s.fulfillment_channel,
            SUM(s.amazon_order_amount) AS amazon_order_amount,
            SUM(s.grand_total) AS order_amount,
            IFNULL(SUM(i.grand_total), 0) AS invoice_amount,
            IFNULL(SUM(r.grand_total), 0) AS return_amount,
            IFNULL(SUM(c.grand_total), 0) AS cancelled_amount
        FROM
            `tabSales Order` s
        LEFT JOIN
            `tabSales Invoice` i ON i.amazon_order_id = s.amazon_order_id AND i.is_return = 0 AND i.docstatus != 2
        LEFT JOIN
            `tabSales Invoice` r ON r.amazon_order_id = s.amazon_order_id AND r.is_return = 1 AND r.docstatus != 2
        LEFT JOIN
            `tabSales Order` c ON c.amazon_order_id = s.amazon_order_id AND c.amazon_order_status = 'Canceled' AND c.docstatus != 2
        WHERE
            s.transaction_date BETWEEN %(from_date)s AND %(to_date)s
    """
    if filters.get("customer_type"):
        so_query += """
            AND s.amazon_customer_type = %(customer_type)s
        """
    if filters.get("fulfillment_channel"):
        so_query += """
            AND s.fulfillment_channel = %(fulfillment_channel)s
        """

    so_query += """
        GROUP BY
            {0}
        ORDER BY
            s.transaction_date
    """.format(
        group_based_on
    )

    results = frappe.db.sql(
        so_query,
        {
            "from_date": from_date,
            "to_date": to_date,
            "customer_type": filters.get("customer_type"),
            "fulfillment_channel": filters.get("fulfillment_channel"),
        },
        as_dict=True,
    )

    for row in results:
        row["total_order_amount"] = (
            row["amazon_order_amount"] - row["return_amount"] - row["cancelled_amount"]
        )
        row["total_amount"] = (
            row["order_amount"] - row["return_amount"] - row["cancelled_amount"]
        )
        data.append(row)

    return data
