// Copyright (c) 2024, efeone and contributors
// For license information, please see license.txt

frappe.ui.form.on("Amazon Failed Sync Record", {
    refresh(frm) {
        frm.disable_save();
        frm.disable_form();
        if (!frm.is_new()) {
            handle_custom_buttons(frm);
        }
    }
});

function handle_custom_buttons(frm) {
    if (!frm.is_new()) {
        if (frm.doc.amazon_order_id && !frm.doc.replaced_order_id) {
            frm.add_custom_button('Retry', () => {
                retry_fetching(frm);
            }).addClass("btn-primary");
        }
        if (frm.doc.replaced_order_id) {
            if (!frm.doc.replaced_so) {
                frm.add_custom_button('Sales Order', () => {
                    create_replaced_so(frm);
                }, 'Create');
            }
            if (!frm.doc.replaced_jv) {
                frm.add_custom_button('Journal Entry', () => {
                    create_replaced_jv(frm);
                }, 'Create');
            }
        }
    }
}

function retry_fetching(frm) {
    if (frm.doc.amazon_order_id) {
        frm.call({
            method: "retry_fetching",
            doc: frm.doc,
            freeze: true,
            freeze_message: __("Syncing Sales Order.."),
            callback: (r) => {
                if (r && r.message) {
                    frappe.show_alert({
                        message: __('Sales Orders created/updated successfully'),
                        indicator: 'green'
                    }, 5);
                }
                else {
                    frappe.show_alert({
                        message: __('Failed to create/update Sales Order. Please check Amazon Failed Sync Record'),
                        indicator: 'red'
                    }, 5);
                }
            }
        });
    }
    else {
        frappe.throw(__('Amazon Order ID is required'))
    }
}

function create_replaced_so(frm) {
    frm.call({
        method: "create_replaced_so",
        doc: frm.doc,
        freeze: true,
        freeze_message: __("Creating Replaced Sales Order.."),
        callback: (r) => {
            frm.reload_doc();
        }
    });
}

function create_replaced_jv(frm) {
    frm.call({
        method: "create_replaced_jv",
        doc: frm.doc,
        freeze: true,
        freeze_message: __("Creating Adjustment Journal Entry.."),
        callback: (r) => {
            frm.reload_doc();
        }
    });
}