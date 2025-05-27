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
        "Serial Numbers are now Transferred",
        alert=True,
    )

def before_insert_custom(doc, method=None):
    """method corrects warehouse in stock returns from returns & replaced orders if temporary stock transfer is on"""
    if doc.from_return_invoice:
        amz_settings = frappe.get_last_doc("Amazon SP API Settings")
        if amz_settings.temporary_stock_transfer_required:
            si = doc.sales_invoice_no
            return_warehouse = amz_settings.warehouse
            si_fulfillment_channel = frappe.db.get_value("Sales Invoice", si, "fulfillment_channel")
        
            if si_fulfillment_channel:
                if si_fulfillment_channel == 'AFN':
                    return_warehouse = amz_settings.afn_warehouse
        
            doc.to_warehouse = return_warehouse
            for row in doc.items:
                row.t_warehouse = return_warehouse
