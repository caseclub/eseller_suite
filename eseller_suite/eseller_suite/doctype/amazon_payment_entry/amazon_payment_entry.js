// Copyright (c) 2024, efeone and contributors
// For license information, please see license.txt

frappe.ui.form.on("Amazon Payment Entry", {
    onload(frm) {
        if (frm.is_new()) {
            frappe.db.get_single_value('eSeller Settings', 'default_mode_of_payment').then(default_mode_of_payment => {
                frm.set_value('mode_of_payment', default_mode_of_payment);
            });
        }
        frm.set_df_property('payment_details', 'cannot_add_rows', true)
    },
    refresh(frm) {
        if (frm.doc.in_progress) {
            frm.set_intro('Background syncing is in progress, Please wait and reload again', 'orange');
            frm.disable_form();
            frm.disable_save();
        }
        if (!frm.is_new() && frm.doc.docstatus === 0 && frm.doc.in_progress === 0) {
            handle_custom_buttons(frm);
        }
        frappe.realtime.on("fetch_invoice_details", (data) => {
            frappe.hide_msgprint(true);
            frappe.show_progress('Fetching Invoice Details...', data.progress, data.total, __("Fetching {0} of {1} invoices", [data.progress, data.total]), true);
            if (data.progress === data.total) {
                frm.reload_doc();
            }
        });
        frappe.realtime.on("get_missing_sales_orders", (data) => {
            frappe.hide_msgprint(true);
            frappe.show_progress('Syncing Sales Order..', data.progress, data.total, __("Fetching {0} of {1} invoices", [data.progress, data.total]), true);
            if (data.progress === data.total) {
                frm.reload_doc();
            }
        });
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
    },
    reset_progress(frm) {
        frm.set_value('in_progress', 0);
        frm.refresh_fields();
        frm.save();
    }
});

function handle_custom_buttons(frm) {
    if (!frm.is_new() && frm.doc.docstatus === 0) {
        if (!frm.doc.invoice_details_fetched) {
            frm.add_custom_button('Invoice Details', () => {
                fetch_invoice_details(frm);
            }, 'Fetch');

            frm.add_custom_button('Missing Sales Orders', () => {
                get_missing_sales_orders(frm);
            }, 'Fetch');

            // Button for debug purposes only for Administrator
            if (frappe.user.has_role('System Manager')) {
                frm.add_custom_button('Unset Ready to Process', () => {
                    unset_ready_to_process(frm);
                }, 'Fetch');
            }
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

/**
 * Function to fetch missing sales orders based on the payment details
 * and sync them using the Amazon SP API Settings.
 * It limits the number of invoices fetched based on the max_invoice_count setting.
 */
async function get_missing_sales_orders(frm) {
    let amazon_order_ids = [];
    let count = 0;

    const max_invoice_count = await frappe.db.get_single_value(
        "eSeller Settings",
        "max_invoice_count"
    );

    for (const row of frm.doc.payment_details) {
        if (
            row.order_id &&
            row.ready_to_process == 0 &&
            row.order_id.trim() !== "" &&
            count < max_invoice_count &&
            !amazon_order_ids.includes(row.order_id.trim())
        ) {
            amazon_order_ids.push(row.order_id.trim());
            count++;
        } else if (count >= max_invoice_count) {
            break;
        }
    }

    const records = await frappe.db.get_list("Amazon SP API Settings", {
        fields: ["name"],
        filters: { is_active: 1 },
    });

    const amz_setting_name = records.at(-1)?.name;

    if (!amz_setting_name) {
        frappe.msgprint("No active Amazon SP API Settings found.");
        return;
    }

    for (let i = 0; i < amazon_order_ids.length; i++) {
        frappe.show_progress(
            "Syncing Sales Order..",
            i + 1,
            amazon_order_ids.length,
            __("Fetching {0} of {1} invoices", [i + 1, amazon_order_ids.length])
        );

        try {
            await frappe.call({
                method:
                    "eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_repository.get_order",
                args: {
                    amz_setting_name,
                    amazon_order_ids: amazon_order_ids[i],
                },
                freeze: true,
                freeze_message: __("Syncing Sales Order.."),
            });
        } catch (err) {
            console.error("Error fetching order:", err);
        }
    }

    frappe.hide_progress(); // hide after loop
}

/**
 * function to uncheck ready to process checks in all the lines in the table
 */
function unset_ready_to_process(frm) {
    frm.call({
        method: "unset_ready_to_process",
        doc: frm.doc,
        freeze: true,
        freeze_message: __("Removing Ready to Process.."),
        callback: (r) => {
            if (r && r.message) {
                frappe.show_alert({
                    message: __('Ready to Process removed successfully'),
                    indicator: 'green'
                }, 5);
            }
            frm.reload_doc();
        }
    });
}
