//After updating:
//bench build
//bench --site erp.caseclub.com clear-cache
//bench restart
frappe.ui.form.on('Sales Order', {
    refresh(frm) {
        if (frm.doc.amazon_order_id && frm.doc.fulfillment_channel !== 'MFN') {
            frm.disable_form();
            frm.disable_save();
            $('.custom-actions').hide()
        }
    }
})