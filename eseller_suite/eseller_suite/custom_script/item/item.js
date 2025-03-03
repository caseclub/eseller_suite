frappe.ui.form.on("Item", {
    refresh: function (frm) {
        const stock_exists = frm.doc.__onload && frm.doc.__onload.stock_exists ? 1 : 0;
        ["is_actual_item", "is_stock_item", "has_serial_no", "has_batch_no", "has_variants"].forEach((fieldname) => {
            frm.set_df_property(fieldname, "read_only", stock_exists);
        });
        frm.set_query('actual_item', () => {
            return {
                filters: {
                    is_actual_item: 1
                }
            }
        })
    },
    is_actual_item: function (frm) {
        frm.set_value('is_stock_item', frm.doc.is_actual_item);
        frm.set_value('is_sales_item', frm.doc.is_actual_item);
        frm.set_value('is_purchase_item', frm.doc.is_actual_item);
    }
})