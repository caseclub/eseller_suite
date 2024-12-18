import frappe

def transfer_barcodes(doc, method=None):
    """method transfers the barcodes on submit

    Args:
        doc (_type_): _description_
        method (_type_, optional): _description_. Defaults to None.
    """
    for item in doc.items:
        if not item.barcode_no:
            continue

        barcodes = item.barcode_no.split("\n")
        for barcode in barcodes:
            existing_serial_no = frappe.db.exists(
                "eSeller Serial No",
                {"serial_no": barcode},
            )
            if existing_serial_no:
                serial_doc = frappe.get_doc("eSeller Serial No", existing_serial_no)
                serial_doc.status = "Transferred"
                serial_doc.warehouse = item.t_warehouse
                serial_doc.transfer_document_no = doc.name
                serial_doc.save()
                frappe.msgprint(
                    f"{existing_serial_no} of {item.item_code} is now Transferred",
                    alert=True,
                )
