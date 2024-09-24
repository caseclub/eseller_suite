// Copyright (c) 2024, efeone and contributors
// For license information, please see license.txt

frappe.ui.form.on("APR Log", {
    refresh(frm) {
        frm.disable_form();
        frm.disable_save();
    }
});
