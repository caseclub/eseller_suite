#bench functions
import frappe
from frappe.utils import now
#-------------------------------------------------------------------------------------------------------------------------------------------------------Accounting
#--------------------------------------------
#Delete all general ledger entries
#-------------------------------------------- 
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.delete_all_gl_entries")
"""
def delete_all_gl_entries():
    """Deletes every GL Entry in the system with periodic commits."""
    # Fetch every GL Entry name
    gle_names = frappe.get_all("GL Entry", pluck="name")
    total = len(gle_names)

    if not total:
        print("No GL Entries found.")
        return

    print(f"Found {total} GL Entries; deleting now…")

    commit_interval = 10000

    # Loop through and delete each one
    for idx, name in enumerate(gle_names, start=1):
        try:
            frappe.delete_doc(
                "GL Entry",
                name,
                force=True,            # bypass Submitted / linked-doc checks
                ignore_permissions=True
            )
            print(f"[{idx}/{total}] Deleted GL Entry {name}")
        except Exception as e:
            print(f"[{idx}/{total}] FAILED to delete {name}: {e}")

        if idx % commit_interval == 0:
            frappe.db.commit()
            print(f"Committed after {idx} deletions…")

    # Final commit for any remaining changes
    frappe.db.commit()
    print(f"Finished. Attempted deletion of {total} GL Entries.")


#--------------------------------------------
#Delete specific General Ledger Entry
#--------------------------------------------    
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.delete_gl_entry", gle_name="f51b4f0e63")
"""
def delete_gl_entry(gle_name):
    """Deletes a single GL Entry by name."""
    frappe.delete_doc("GL Entry", gle_name,
                      force=True,
                      ignore_permissions=True)
    frappe.db.commit()
    return f"GL Entry {gle_name} deleted."


#--------------------------------------------
#Delete specific Payment Ledger Entry
#--------------------------------------------   
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.delete_payment_ledger_entries", prefix="p58ghddlhn")
"""
def delete_payment_ledger_entries(prefix):
    """Deletes all Payment Ledger Entry documents whose name starts with `prefix`."""
    # Find all matching names
    names = frappe.get_all(
        "Payment Ledger Entry",
        filters={ "name": ["like", f"{prefix}%"] },
        pluck="name"
    )

    # Delete each one
    for ple_name in names:
        frappe.delete_doc(
            "Payment Ledger Entry",
            ple_name,
            force=True,
            ignore_permissions=True
        )

    frappe.db.commit()
    return f"Deleted {len(names)} Payment Ledger Entry rows matching '{prefix}'."


#--------------------------------------------
#Delete all Payment Ledger Entries
#--------------------------------------------
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.delete_all_payment_ledger_entries")
"""
def delete_all_payment_ledger_entries():
    """Deletes every Payment Ledger Entry in the system."""
    # Fetch every Payment Ledger Entry name
    ple_names = frappe.get_all("Payment Ledger Entry", pluck="name")
    total = len(ple_names)

    if not total:
        return "No Payment Ledger Entry rows found."

    # Loop and delete with periodic commits
    commit_interval = 10000
    for idx, name in enumerate(ple_names, start=1):
        frappe.delete_doc(
            "Payment Ledger Entry",
            name,
            force=True,           # bypass Submitted / linked-doc checks
            ignore_permissions=True
        )
        print(f"[{idx}/{total}] Deleted Payment Ledger Entry {name}")
        if idx % commit_interval == 0:
            frappe.db.commit()

    # Final commit for any remaining changes
    frappe.db.commit()
    return f"Deleted {total} Payment Ledger Entry rows."


#--------------------------------------------
#Delete all Journal Entries
#--------------------------------------------
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.delete_all_journal_entries")
"""
def delete_all_journal_entries():
    entries = frappe.get_all('Journal Entry', fields=['name'])  # , filters={'owner': frappe.session.user}

    for entry in entries:
        name = entry['name']
        try:
            # Forcefully delete the document, even if submitted (skips cancel step)
            # Since you've already deleted GL and payment ledger entries, this assumes no need for reversal
            frappe.delete_doc('Journal Entry', name, force=True, ignore_permissions=True)
            
            print(f"Successfully deleted Journal Entry: {name}")
        except Exception as e:
            print(f"Error deleting Journal Entry {name}: {str(e)}")

    # Commit all changes to the database
    frappe.db.commit()

    print("All Journal Entries deleted.")


#--------------------------------------------
#Cancel a specific General Ledger Entry
#--------------------------------------------    
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.cancel_journal_entry", jv_name="ACC-JV-2025-00124")
"""
def cancel_journal_entry(jv_name):
    # Load the journal entry document
    jv = frappe.get_doc('Journal Entry', jv_name)

    # Optionally, ignore permissions if the UI failure is related to role checks
    jv.flags.ignore_permissions = True

    # Cancel the document (this will reverse GL entries, update status, etc.)
    jv.cancel()

    # Commit changes to the database
    frappe.db.commit()

    print(f"Journal Entry {jv_name} has been cancelled.")

#--------------------------------------------
# Delete all payment entries
#--------------------------------------------    
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.delete_all_payment_entries")
"""
def delete_all_payment_entries():
    # Get all Payment Entries
    payment_entries = frappe.get_all("Payment Entry", fields=["name"])
    total = len(payment_entries)

    # Set all to draft (docstatus=0)
    frappe.db.sql("UPDATE `tabPayment Entry` SET docstatus = 0")
    frappe.db.commit()

    # Delete each one with progress
    for i, pe in enumerate(payment_entries, 1):
        name = pe.name
        try:
            frappe.delete_doc("Payment Entry", name, force=1, ignore_permissions=True)
            frappe.db.commit()
            print(f"Deleted {name} ({i}/{total})")
        except Exception as e:
            print(f"Error deleting {name}: {str(e)} ({i}/{total})")

#--------------------------------------------
#Force a submitted journal entry back into draft state (makes deleting easier for large scale journal entries)
#--------------------------------------------    
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.force_unsubmit_journal_entry", jv_name="ACC-JV-2025-00120")
"""
def force_unsubmit_journal_entry(jv_name):
    # Optional: Handle linked GL Entries first
    frappe.db.delete("GL Entry", {"voucher_type": "Journal Entry", "voucher_no": jv_name})
    
    # Force docstatus to 0 (draft)
    frappe.db.sql("""UPDATE `tabJournal Entry` SET docstatus = 0 WHERE name = %s""", jv_name)
    frappe.db.commit()
    
    # Reload the document to reflect changes
    jv = frappe.get_doc("Journal Entry", jv_name)
    jv.reload()
    print(f"Journal Entry {jv_name} set to draft.")

#--------------------------------------------
#Force a all submitted journal entries back into draft state
#--------------------------------------------    
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.force_unsubmit_all_journal_entries")
"""
def force_unsubmit_all_journal_entries():
    # Get all submitted Journal Entries (docstatus=1)
    jv_names = frappe.db.get_all("Journal Entry", filters={"docstatus": 1}, fields=["name"], pluck="name")
    
    for jv_name in jv_names:
        # Force docstatus to 0 (draft)
        frappe.db.sql("""UPDATE `tabJournal Entry` SET docstatus = 0 WHERE name = %s""", jv_name)
        frappe.db.commit()
        
        # Reload the document to reflect changes
        jv = frappe.get_doc("Journal Entry", jv_name)
        jv.reload()
        print(f"Journal Entry {jv_name} set to draft.")
    
#--------------------------------------------
#Delete all journal entries, payment entries and general ledger entries
#--------------------------------------------    
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.delete_all_journal_entries_and_data")
"""
def delete_all_journal_entries_and_data():
    delete_all_payment_ledger_entries()
    delete_all_gl_entries()
    force_unsubmit_all_journal_entries()
    delete_all_journal_entries()
    


        
#--------------------------------------------
 #Delete custom field by ID
#--------------------------------------------    
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.delete_custom_field", field_id="Shipment-shipstation_order_id")
"""
def delete_custom_field(field_id):
    """
    Deletes a Custom Field by its unique ID (name).
    
    Args:
        field_id (str): The name of the Custom Field, e.g., 'Sales Order-shipstation_order_id'
    """
    if not frappe.db.exists("Custom Field", field_id):
        print(f"Custom Field '{field_id}' does not exist.")
        return
    
    try:
        frappe.delete_doc("Custom Field", field_id, ignore_permissions=True, force=True)
        frappe.db.commit()
        print(f"Successfully deleted Custom Field '{field_id}'.")
    except Exception as e:
        print(f"Error deleting Custom Field '{field_id}': {str(e)}")

#-------------------------------------------------------------------------------------------------------------------------------------------------------Inventory
#--------------------------------------------
# Delete all bin entries
#--------------------------------------------    
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.delete_all_bin_entries")
"""
def delete_all_bin_entries():
    """
    Delete every Bin document in the system, showing progress as (deleted/total).
    """
    # Count how many Bin records exist before we start
    total_bins = frappe.db.count("Bin")
    if total_bins == 0:
        print("No Bin entries found.")
        return

    batch_size = 100
    deleted_so_far = 0

    while True:
        # Fetch a batch of Bin names
        bin_names = frappe.db.sql(
            "SELECT name FROM `tabBin` LIMIT %s",
            batch_size,
            as_dict=True
        )
        if not bin_names:
            break

        for b in bin_names:
            frappe.delete_doc(
                "Bin",
                b.name,
                ignore_permissions=True,
                force=True
            )
            deleted_so_far += 1
            print(f"Deleted Bin: {b.name} ({deleted_so_far}/{total_bins})")

        frappe.db.commit()  # commit after each batch
        
#--------------------------------------------
# Delete all stock ledger entries
#--------------------------------------------    
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.delete_all_stock_ledger_entries")
"""
def delete_all_stock_ledger_entries():
    # Get the total count once, before we start deleting
    total_sle = frappe.db.count("Stock Ledger Entry")
    if total_sle == 0:
        print("No Stock Ledger Entries found.")
        return

    batch_size = 100
    deleted_so_far = 0

    while True:
        # Grab a batch of names to delete
        sle_names = frappe.db.sql(
            "SELECT name FROM `tabStock Ledger Entry` LIMIT %s",
            batch_size,
            as_dict=True
        )
        if not sle_names:
            break

        for sle in sle_names:
            frappe.delete_doc(
                "Stock Ledger Entry",
                sle.name,
                ignore_permissions=True,
                force=True
            )
            deleted_so_far += 1
            print(f"Deleted Stock Ledger Entry: {sle.name} ({deleted_so_far}/{total_sle})")

        frappe.db.commit()  # commit after each batch

#--------------------------------------------
# Delete all stock entries
#--------------------------------------------    
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.delete_all_stock_entries")
"""
def delete_all_stock_entries():
    batch_size = 100
    while True:
        # Fetch a batch of Stock Entry names
        se_names = frappe.db.sql("SELECT name FROM `tabStock Entry` LIMIT %s", batch_size, as_dict=True)
        if not se_names:
            break
        for se in se_names:
            frappe.db.set_value("Stock Entry", se.name, "docstatus", 0)
            frappe.delete_doc("Stock Entry", se.name, ignore_permissions=True, force=True)
            print(f"Deleted Stock Entry: {se.name}")
        frappe.db.commit()  # Commit after each batch

#--------------------------------------------
# Delete a specific stock entry
#--------------------------------------------    
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.delete_stock_entry", stock_entry_name="MAT-STE-2025-00042")
"""
def delete_stock_entry(stock_entry_name):
    frappe.db.sql("""UPDATE `tabStock Entry` SET docstatus = 0 WHERE name = %s""", stock_entry_name)
    frappe.delete_doc("Stock Entry", stock_entry_name)
    print(f"Deleted {stock_entry_name}")
    frappe.db.commit()

#-------------------------------------------------------------------------------------------------------------------------------------------------------Work Orders
#--------------------------------------------
# Delete all job cards
#--------------------------------------------    
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.delete_all_job_cards")
"""
def delete_all_job_cards():
    batch_size = 100
    while True:
        # Fetch a batch of Job Card names
        jc_names = frappe.db.sql("SELECT name FROM `tabJob Card` LIMIT %s", batch_size, as_dict=True)
        if not jc_names:
            break
        for jc in jc_names:
            frappe.db.set_value("Job Card", jc.name, "docstatus", 0)
            frappe.delete_doc("Job Card", jc.name, ignore_permissions=True, force=True)
            print(f"Deleted Job Card: {jc.name}")
        frappe.db.commit()  # Commit after each batch
        
#--------------------------------------------
 #Delete all work orders
#--------------------------------------------    
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.delete_all_work_orders")
"""
def delete_all_work_orders():
    work_orders = frappe.get_all("Work Order", fields=["name"])
    total = len(work_orders)
    for i, wo in enumerate(work_orders, 1):
        frappe.db.sql("""UPDATE `tabWork Order` SET docstatus = 0 WHERE name = %s""", wo.name)
        frappe.delete_doc("Work Order", wo.name)
        print(f"Deleted {wo.name} ({i}/{total})")
    frappe.db.commit()

#-------------------------------------------------------------------------------------------------------------------------------------------------------Sales Orders
#--------------------------------------------
# Delete all sales orders
#--------------------------------------------    
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.delete_all_sales_orders")
"""
def delete_all_sales_orders():
    # Get all Sales Orders
    sales_orders = frappe.get_all("Sales Order", fields=["name"])
    total = len(sales_orders)

    # Set all to draft (docstatus=0)
    frappe.db.sql("UPDATE `tabSales Order` SET docstatus = 0")
    frappe.db.commit()

    # Delete each one with progress
    for i, so in enumerate(sales_orders, 1):
        name = so.name
        try:
            frappe.delete_doc("Sales Order", name, force=1, ignore_permissions=True)
            frappe.db.commit()
            print(f"Deleted {name} ({i}/{total})")
        except Exception as e:
            print(f"Error deleting {name}: {str(e)} ({i}/{total})")
    
            
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_sp_api_settings.print_all_sales_orders")
"""
def print_all_sales_orders():
    # Fetch all Sales Orders with all fields
    sales_orders = frappe.get_all("Sales Order", fields=["*"], order_by="name")
    
    if not sales_orders:
        print("No Sales Orders found.")
        return
    
    for so in sales_orders:
        # Print the basic details
        print(f"\nSales Order: {so.name}")
        print("-" * 40)
        for key, value in so.items():
            print(f"{key}: {value}")
        print("-" * 40)
        
        # Optionally, fetch and print child tables if needed (e.g., items, taxes)
        full_doc = frappe.get_doc("Sales Order", so.name)
        if full_doc.items:
            print("Items:")
            for item in full_doc.items:
                print(f"  - Item Code: {item.item_code}, Qty: {item.qty}, Amount: {item.amount}")
        
        if full_doc.taxes:
            print("Taxes:")
            for tax in full_doc.taxes:
                print(f"  - Account: {tax.account_head}, Amount: {tax.tax_amount}")
            
#-------------------------------------------------------------------------------------------------------------------------------------------------------Sales Invoices
#--------------------------------------------
# Delete all sales invoices
#--------------------------------------------    
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.delete_all_sales_invoices")
"""
def delete_all_sales_invoices():
    # Get all s
    sales_invoices = frappe.get_all("Sales Invoice", fields=["name"])
    total = len(sales_invoices)

    # Set all to draft (docstatus=0)
    frappe.db.sql("UPDATE `tabSales Invoice` SET docstatus = 0")
    frappe.db.commit()

    # Delete each one with progress
    for i, si in enumerate(sales_invoices, 1):
        name = si.name
        try:
            frappe.delete_doc("Sales Invoice", name, force=1, ignore_permissions=True)
            print(f"Deleted {name} ({i}/{total})")
        except Exception as e:
            print(f"Error deleting {name}: {str(e)} ({i}/{total})")
            
#--------------------------------------------
 #Delete all sales invoices that are in status "Return"
#--------------------------------------------    
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.delete_all_sales_invoices_return")
"""
def delete_all_sales_invoices_return():
    """Deletes every Sales Invoice in the system with status 'Return' after setting to Draft."""
    # Fetch every Sales Invoice name with status 'Return'
    si_names = frappe.get_all("Sales Invoice", filters={"status": "Return"}, pluck="name")
    total = len(si_names)
    if not total:
        return "No Sales Invoice rows with status 'Return' found."
    # Loop and process with periodic commits
    commit_interval = 10000
    for idx, name in enumerate(si_names, start=1):
        # Set Sales Invoice to Draft
        frappe.db.set_value("Sales Invoice", name, "docstatus", 0)
        frappe.delete_doc(
            "Sales Invoice",
            name,
            force=True,  # bypass Submitted / linked-doc checks
            ignore_permissions=True
        )
        print(f"[{idx}/{total}] Set to Draft and Deleted Sales Invoice {name}")
        if idx % commit_interval == 0:
            frappe.db.commit()
    # Final commit for any remaining changes
    frappe.db.commit()
    return f"Deleted {total} Sales Invoice rows with status 'Return' after setting to Draft."



#--------------------------------------------
# Delete all repost item valuation entries
#--------------------------------------------    
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.delete_all_repost_item_valuation_entries")
"""
def delete_all_repost_item_valuation_entries():
    batch_size = 100
    while True:
        # Fetch a batch of Repost Item Valuation names
        riv_names = frappe.db.sql("SELECT name FROM `tabRepost Item Valuation` LIMIT %s", batch_size, as_dict=True)
        if not riv_names:
            break
        for riv in riv_names:
            frappe.db.set_value("Repost Item Valuation", riv.name, "docstatus", 0)
            frappe.delete_doc("Repost Item Valuation", riv.name, ignore_permissions=True, force=True)
            print(f"Deleted Repost Item Valuation: {riv.name}")
        frappe.db.commit()  # Commit after each batch

#--------------------------------------------
 #Delete all inventory from specific warehouse
#--------------------------------------------    
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.delete_inventory_from_warehouse", warehouse="Main Warehouse - CC")
"""
def delete_inventory_from_warehouse(warehouse):
    # Get all unique items in the warehouse
    items = frappe.db.get_list("Bin", filters={"warehouse": warehouse}, fields=["item_code"], pluck="item_code")
    total = len(items)
    for i, item_code in enumerate(items, 1):
        # Delete Stock Ledger Entries for this item and warehouse
        frappe.db.sql("""DELETE FROM `tabStock Ledger Entry` WHERE warehouse = %s AND item_code = %s""", (warehouse, item_code))
        # Delete Bin for this item and warehouse
        bin_name = frappe.db.get_value("Bin", {"warehouse": warehouse, "item_code": item_code})
        if bin_name:
            frappe.db.delete("Bin", bin_name)
        print(f"Deleted {item_code} ({i}/{total})")
    frappe.db.commit()

"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.hrms_temp")
"""
def hrms_temp():
    frappe.db.delete("Module Def", {"name":["in",["HR","Payroll"]]})
    frappe.db.commit()

#-------------------------------------------------------------------------------------------------------------------------------------------------------Bill of Material (BOM)
#--------------------------------------------
#Change BOM status to draft
#-------------------------------------------- 
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.bench_functions.force_set_bom_to_draft", bom_name="BOM-FO-CC30CALPIS1")
"""
def force_set_bom_to_draft(bom_name: str) -> None:

    """
    Forcefully set a BOM's docstatus to Draft (0), regardless of current state.
    Prints step-by-step operations and exits cleanly if already draft.

    WARNING:
        This bypasses normal ERPNext workflow (submit/cancel/amend).
        Use with caution and only if you know the implications for linked records.
    """
    print(f"[start] Preparing to force-set BOM '{bom_name}' to Draft…")

    # Fetch the BOM
    try:
        bom = frappe.get_doc("BOM", bom_name)
    except frappe.DoesNotExistError:
        print(f"[error] BOM '{bom_name}' not found.")
        return

    print(f"[info] Current status for '{bom.name}': docstatus={bom.docstatus} "
          f"(0=Draft, 1=Submitted, 2=Cancelled)")

    # If already draft, nothing to do
    if bom.docstatus == 0:
        print(f"[skip] BOM '{bom.name}' is already Draft. No changes made.")
        return

    # Force-flip using a DB write (bypasses all validations/workflows)
    print(f"[action] Forcing docstatus -> 0 (Draft) via DB update…")
    frappe.db.sql(
        """UPDATE `tabBOM`
           SET docstatus = 0, modified = %s, modified_by = %s
           WHERE name = %s""",
        (now(), frappe.session.user if frappe.session.user else "Administrator", bom.name),
    )

    # Commit and verify
    print(f"[commit] Committing transaction…")
    frappe.db.commit()

    bom_reloaded = frappe.get_doc("BOM", bom.name)
    print(f"[verify] New status for '{bom_reloaded.name}': docstatus={bom_reloaded.docstatus}")

    if bom_reloaded.docstatus == 0:
        print(f"[done] Success. BOM '{bom_reloaded.name}' is now Draft.")
    else:
        print(f"[warn] Expected docstatus=0, but got {bom_reloaded.docstatus}. Check DB/permissions.")
