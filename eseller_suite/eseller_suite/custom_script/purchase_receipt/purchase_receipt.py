import frappe
from frappe.defaults import get_defaults


def create_barcodes(doc, method=None):
    """
    Create serial numbers for the items in a purchase receipt.

    Args:
        doc (object): The purchase receipt document.
        method (str, optional): The triggered method. Defaults to None.
    """
    company = get_defaults().get("company")

    if not company:
        frappe.throw("Company default is not set. Please configure your defaults.")

    serial_numbers_processed = False

    for item in doc.items:
        if not item.barcode_no:
            continue

        barcodes = item.barcode_no.split("\n")
        duplicates = find_duplicates(barcodes)
        if duplicates:
            duplicate_message = ", ".join(duplicates)
            frappe.throw(
                f"Duplicate barcodes found: {duplicate_message}. Please check and resolve."
            )

        qty = 0

        for barcode in barcodes:
            if not barcode:
                continue
            filters = {}
            filters["serial_no"] = barcode
            if doc.doctype == "Purchase Receipt":
                filters["purchase_document_no"] = ["!=", doc.name]
                existing_serial_no = frappe.db.exists(
                    "eSeller Serial No",
                    filters,
                )
                if existing_serial_no:
                    duplicate_item_code = frappe.db.get_value(
                        "eSeller Serial No", existing_serial_no, "item_code"
                    )
                    frappe.throw(
                        title="Duplicate Entry",
                        msg=f"Barcode No. {barcode} already exists for Item {duplicate_item_code}.",
                    )
            if doc.doctype == "Stock Entry":
                filters["transfer_document_no"] = ["not in", [doc.name, ""]]
                existing_serial_no = frappe.db.exists(
                    "eSeller Serial No",
                    filters,
                )
                if existing_serial_no:
                    stock_entry_ref = frappe.db.get_value(
                        "eSeller Serial No", existing_serial_no, "transfer_document_no"
                    )
                    frappe.throw(
                        title="Duplicate Entry",
                        msg=f"Barcode No. {barcode} already created against {stock_entry_ref}.",
                    )

            already_created_filters = {}
            already_created_filters["serial_no"] = barcode
            if doc.doctype == "Purchase Receipt":
                already_created_filters["purchase_document_no"] = doc.name
            if frappe.db.exists(
                "eSeller Serial No",
                already_created_filters,
            ):
                qty += 1
                continue

            serial_no = frappe.get_doc(
                {
                    "doctype": "eSeller Serial No",
                    "serial_no": barcode,
                    "item_code": item.item_code,
                    "purchase_rate": (
                        item.rate
                        if doc.doctype == "Purchase Receipt"
                        else item.basic_rate
                    ),
                    "company": company,
                    "status": "Inactive",
                }
            )
            if doc.doctype == "Purchase Receipt":
                serial_no.purchase_document_no = doc.name
                serial_no.purchase_rate = item.rate
            elif doc.doctype == "Stock Entry":
                serial_no.transfer_document_no = doc.name
                serial_no.purchase_rate = item.basic_rate
            serial_no.insert(ignore_permissions=True)

            qty += 1
            serial_numbers_processed = True

        item.qty = qty
        if doc.doctype == "Purchase Receipt":
            item.received_qty = qty

    if serial_numbers_processed:
        frappe.msgprint(
            msg="Serial Numbers Processed.",
            alert=True,
            indicator="green"
        )


def activate_barcodes(doc, method=None):
    """method activates the barcodes on submit

    Args:
        doc (_type_): _description_
        method (_type_, optional): _description_. Defaults to None.
    """
    serial_numbers_activated = False

    for item in doc.items:
        if not item.barcode_no:
            continue

        barcodes = item.barcode_no.split("\n")
        for barcode in barcodes:
            existing_serial_no = frappe.db.exists(
                "eSeller Serial No",
                {"serial_no": barcode, "purchase_document_no": doc.name},
            )
            if existing_serial_no:
                serial_doc = frappe.get_doc("eSeller Serial No", existing_serial_no)
                serial_doc.status = "Active"
                serial_doc.warehouse = item.warehouse
                serial_doc.save()
                serial_numbers_activated = True

    if serial_numbers_activated:
        frappe.msgprint(
            "Serial Numbers are now Active",
            alert=True,
        )


def find_duplicates(lst):
    seen = set()
    duplicates = set()
    for item in lst:
        if item in seen:
            duplicates.add(item)
        else:
            seen.add(item)
    cleaned_duplicates = [item for item in duplicates if item.strip()]
    return cleaned_duplicates

def delete_barcodes(doc, method=None):
    """
    Delete barcodes from the purchase receipt.
    """
    for item in doc.items:
        if not item.barcode_no:
            continue

        barcodes = item.barcode_no.split("\n")
        for barcode in barcodes:
            if not barcode:
                continue
            existing_serial_no = frappe.db.exists(
                "eSeller Serial No",
                {"serial_no": barcode, "purchase_document_no": doc.name},
            )
            if existing_serial_no:
                serial_doc = frappe.get_doc("eSeller Serial No", existing_serial_no)
                serial_doc.delete()
