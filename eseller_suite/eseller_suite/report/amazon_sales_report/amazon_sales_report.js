// Copyright (c) 2024, efeone and contributors
// For license information, please see license.txt

frappe.query_reports["Amazon Sales Report"] = {
    "filters": [
        {
            "fieldname": "group_based_on",
            "label": __("Group By"),
            "fieldtype": "Select",
            "options": "Order ID\nDate",
            "default": "Order ID",
            "reqd": 1
        },
        {
            "fieldname": "from_date",
            "label": __("From Date"),
            "fieldtype": "Date",
            "default": frappe.datetime.add_days(frappe.datetime.nowdate(), -6),
            "reqd": 1
        },
        {
            "fieldname": "to_date",
            "label": __("To Date"),
            "fieldtype": "Date",
            "default": frappe.datetime.nowdate(),
            "reqd": 1
        },
        {
            "fieldname": "customer_type",
            "label": __("Customer Type"),
            "fieldtype": "Select",
            "options": "\nB2B\nB2C"
        },
        {
            "fieldname": "fulfillment_channel",
            "label": __("Fulfillment Channel"),
            "fieldtype": "Select",
            "options": "\nAFN\nMFN"
        }
    ]
};
