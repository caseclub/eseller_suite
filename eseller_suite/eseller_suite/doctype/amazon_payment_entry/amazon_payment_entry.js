// Copyright (c) 2024, efeone and contributors
// For license information, please see license.txt

frappe.ui.form.on("Amazon Payment Entry", {
    refresh(frm) {
        if (!frm.is_new() && frm.doc.docstatus === 0) {
            handle_custom_buttons(frm);
        }
    },
    mode_of_payment(frm) {
        if (frm.doc.mode_of_payment) {
            if (!frm.doc.company) {
                frm.set_value('mode_of_payment',);
                frappe.throw('Company is required before Mode of Payment selection')
            }
            else {
                fetch_mode_of_payment_account(frm);
            }
        }
        else {
            frm.set_value('payment_account',);
        }
    }
});

function handle_custom_buttons(frm) {
    if (!frm.is_new() && frm.doc.docstatus === 0) {
        if (!frm.doc.invoice_details_fetched) {
            frm.add_custom_button('Invoice Details', () => {
                fetch_invoice_details(frm);
            }, 'Fetch');
        }
    }
}

function fetch_mode_of_payment_account(frm) {
    if (frm.doc.mode_of_payment && frm.doc.company) {
        frappe.call({
            method: "erpnext.accounts.doctype.sales_invoice.sales_invoice.get_bank_cash_account",
            args: {
                'mode_of_payment': frm.doc.mode_of_payment,
                'company': frm.doc.company
            },
            callback: function (r) {
                if (r && r.message && r.message.account) {
                    frm.set_value('payment_account', r.message.account)
                }
            },
            error: function (exe) {
                frm.set_value('mode_of_payment',);
            },
            freeze: true,
            freeze_message: __('Fetching Mode of Payment Account...')
        });
    }
}

function fetch_invoice_details(frm) {
    frm.call({
        method: "fetch_invoice_details",
        doc: frm.doc,
        callback: function (r) {
            frappe.show_alert({
                message: __("Invoice Details fetched.."),
                indicator: "green",
            });
            frm.reload_doc();
        },
        freeze: true,
        freeze_message: __('Fetching Invoice Details...')
    });
}