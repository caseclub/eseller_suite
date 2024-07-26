// Copyright (c) 2024, efeone and contributors
// For license information, please see license.txt

frappe.ui.form.on("Amazon Failed Sync Record", {
    refresh(frm) {
        frm.disable_save();
        frm.disable_form();
    },
});
