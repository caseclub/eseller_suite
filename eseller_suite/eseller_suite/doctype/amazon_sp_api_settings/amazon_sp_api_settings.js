// Copyright (c) 2024, efeone and contributors
// For license information, please see license.txt

frappe.ui.form.on('Amazon SP API Settings', {
    refresh(frm) {
        frm.trigger("set_queries");
        hanlde_retry_btn(frm);
    },
    set_queries(frm) {
        frm.set_query("warehouse", () => {
            return {
                filters: {
                    "is_group": 0,
                    "company": frm.doc.company,
                }
            };
        });

        frm.set_query("market_place_account_group", () => {
            return {
                filters: {
                    "is_group": 1,
                    "company": frm.doc.company,
                }
            };
        });
    }
});

function hanlde_retry_btn(frm) {
    frm.add_custom_button('Get Order', () => {
        let d = new frappe.ui.Dialog({
            title: 'Sync by Order ID',
            fields: [
                {
                    label: 'Amazon SP API Settings',
                    fieldname: 'sp_api_settings',
                    fieldtype: 'Link',
                    options: 'Amazon SP API Settings',
                    reqd: 1,
                    default: frm.doc.name,
                    hidden: 1
                },
                {
                    label: 'Amazon Order ID',
                    fieldname: 'amazon_order_id',
                    fieldtype: 'Data',
                    reqd: 1,
                },
            ],
            primary_action_label: 'Sync',
            primary_action(values) {
                d.hide();
                frappe.call({
                    method: 'eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_repository.get_order',
                    args: {
                        amz_setting_name: values.sp_api_settings,
                        amazon_order_ids: values.amazon_order_id
                    },
                    freeze: true,
                    freeze_message: __("Syncing Sales Order.."),
                    callback: (r) => {
                        if (r && r.message) {
                            frappe.show_alert({
                                message: __('Sales Orders created/updated successfully'),
                                indicator: 'green'
                            }, 5);
                        }
                    }
                })
            }
        });
        d.show();
    })

    frm.add_custom_button('Get Orders', () => {
        let d = new frappe.ui.Dialog({
            title: 'Sync by Order ID',
            fields: [
                {
                    label: 'Amazon SP API Settings',
                    fieldname: 'sp_api_settings',
                    fieldtype: 'Link',
                    options: 'Amazon SP API Settings',
                    reqd: 1,
                    default: frm.doc.name,
                    hidden: 1
                },
                {
                    label: 'Amazon Order IDs',
                    fieldname: 'amazon_order_ids',
                    fieldtype: 'Small Text',
                    reqd: 1,
                },
            ],
            primary_action_label: 'Sync',
            primary_action(values) {
                d.hide();
                let amazon_order_ids = (values.amazon_order_ids).split("\n");
                for (let i = 0; i < amazon_order_ids.length; i++) {
                    frappe.call({
                        method: 'eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_repository.get_order',
                        args: {
                            amz_setting_name: values.sp_api_settings,
                            amazon_order_ids: amazon_order_ids[i]
                        },
                        freeze: true,
                        freeze_message: __("Syncing Sales Order.."),
                        callback: (r) => {
                            if (r && r.message) {
                                frappe.show_alert({
                                    message: __('Sales Orders created/updated successfully'),
                                    indicator: 'green'
                                }, 5);
                            }
                        }
                    })
                }
            }
        });
        d.show();
    });
}
