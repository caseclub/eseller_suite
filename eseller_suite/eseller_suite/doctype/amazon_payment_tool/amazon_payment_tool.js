// Copyright (c) 2024, efeone and contributors
// For license information, please see license.txt

frappe.ui.form.on("Amazon Payment Tool", {
    refresh(frm) {
        handle_custom_buttons(frm);
        if (frm.doc.started) {
            frm.disable_save()
        }
    },
});

function handle_custom_buttons(frm) {
    if (!frm.is_new()) {
        if (!frm.doc.payments_created) {
            frm.add_custom_button('Payments', () => {
                add_payments(frm);
            }, 'Create');
        }
        if (!frm.doc.jv_created) {
            frm.add_custom_button('Journal Entry', () => {
                add_journal_entries(frm);
            }, 'Create');
        }
    }
}

function add_payments(frm) {
    console.log("add_payments")
    frm.call('create_payments').then(r => {
        if (r.message) {
            console.log(r.message);
        }
    })
}

function add_journal_entries(frm) {
    console.log("add_journal_entries")
}
