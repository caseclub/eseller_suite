frappe.ui.form.on('Sales Order', {
    refresh(frm) {
        if (frm.doc.amazon_order_id) {
            frm.disable_form();
            frm.disable_save();
            $('.custom-actions').hide()
        }
    }
})