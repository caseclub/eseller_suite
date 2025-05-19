import frappe


def execute():
    print("Deleting unlinked temporary stock transfers...")

    frappe.db.set_value("Stock Settings", "Stock Settings", "allow_negative_stock", 1)

    is_negative_stock_allowed = frappe.db.get_single_value("Stock Settings", "allow_negative_stock")

    if not is_negative_stock_allowed:
        print("Negative stock is not allowed. Exiting.")
        return
    
    query = """
        SELECT se.name
        FROM `tabStock Entry` AS se
        LEFT JOIN `tabStock Entry Detail` AS sed ON sed.parent = se.name
        WHERE sed.t_warehouse = %s
        AND se.name NOT IN (
            SELECT temporary_stock_tranfer_id
            FROM `tabSales Order`
            WHERE temporary_stock_tranfer_id IS NOT NULL
        )
    """

    warehouse = "Temporary warehouse  - HEL"

    # Fetch as list of tuples, then extract just the names
    results = frappe.db.sql(query, (warehouse,), as_list=True)
    names = [row[0] for row in results]

    for name in names:
        print(f"Cancelling & Deleting stock entry: {name}")
        se_doc = frappe.get_doc("Stock Entry", name)
        se_doc.flags.ignore_validate = True
        se_doc.cancel()
        frappe.delete_doc("Stock Entry", name)
        print(f"Deleted stock entry: {name}")

    frappe.db.set_value("Stock Settings", "Stock Settings", "allow_negative_stock", 0)
    
    print("Unlinked temporary stock transfers deleted successfully.")
