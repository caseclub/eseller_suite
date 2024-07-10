// Copyright (c) 2024, efeone and contributors
// For license information, please see license.txt

frappe.ui.form.on("Amazon Seller Central Settings", {
	sync_orders(frm) {
		frappe.call({
			method: 'eseller_suite.eseller_suite.doctype.amazon_seller_central_settings.amazon_seller_central_settings.get_orders',
			args: {
				created_after: frm.doc.after_date,
				next_token: frm.doc.next_token
			},
			freeze: true,
			callback: (r) => {
				frm.reload_doc();
			}
		});
	},
});
