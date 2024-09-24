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
    frm.call({
        method: "create_payments",
        doc: frm.doc,
        callback: function (r) {
            if (r.message) {
                if (r.message.length > 0) {
                    frappe.show_alert({
                        message: __("Payment Entries Created: {0}", [
                            r.message.map(function (d) {
                                return repl(
                                    '<a href="/app/payment-entry/%(name)s">%(name)s</a>',
                                    { name: d }
                                );
                            }).join(", "),
                        ]),
                        indicator: "green",
                    });
                    frm.reload_doc();
                }
                else {
                    frappe.show_alert({
                        message: __("Failed to Create Payments"),
                        indicator: "red",
                    });
                }
            }
            else {
                frappe.show_alert({
                    message: __("Failed to Create Payments"),
                    indicator: "red",
                });
            }
        },
        freeze: true,
        freeze_message: __('Creating Payment Entries...')
    });
}

function add_journal_entries(frm) {
    add_journal_entry_popup(frm);
}

function add_journal_entry_popup(frm) {
    let d = new frappe.ui.Dialog({
        title: 'Create Journal Entry',
        fields: [
            {
                label: 'Transaction Type',
                fieldname: 'transaction_type',
                fieldtype: 'Select',
                options: 'Service Fees',
                default: 'Service Fees',
                read_only: 1
            },
            {
                label: 'Credit Account',
                fieldname: 'credit_account',
                fieldtype: 'Link',
                options: 'Account',
                only_select: 1,
                get_query: function () {
                    return {
                        filters: {
                            is_group: 0
                        }
                    }
                },
            },
            {
                label: 'Debit Account',
                fieldname: 'debit_account',
                fieldtype: 'Link',
                options: 'Account',
                only_select: 1,
                get_query: function () {
                    return {
                        filters: {
                            is_group: 0
                        }
                    }
                },
            }
        ],
        primary_action_label: 'Create JV',
        primary_action(values) {
            create_journal_entries(frm, values)
            d.hide();
        }
    });
    d.show();
}

function create_journal_entries(frm, values) {
    frm.call({
        method: "create_journal_entries",
        doc: frm.doc,
        args: {
            'credit_account': values.credit_account,
            'debit_account': values.debit_account,
            'transaction_type': values.transaction_type,
        },
        callback: function (r) {
            if (r.message) {
                if (r.message.length > 0) {
                    frappe.show_alert({
                        message: __("Journal Entries Created: {0}", [
                            r.message.map(function (d) {
                                return repl(
                                    '<a href="/app/journal-entry/%(name)s">%(name)s</a>',
                                    { name: d }
                                );
                            }).join(", "),
                        ]),
                        indicator: "green",
                    });
                    frm.reload_doc();
                }
                else {
                    frappe.show_alert({
                        message: __("Failed to Create Journal Entries"),
                        indicator: "red",
                    });
                }
            }
            else {
                frappe.show_alert({
                    message: __("Failed to Create Journal Entries"),
                    indicator: "red",
                });
            }
        },
        freeze: true,
        freeze_message: __('Creating Journal Entries...')
    });
}
