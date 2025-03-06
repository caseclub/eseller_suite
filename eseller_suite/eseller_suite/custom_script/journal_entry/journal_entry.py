import frappe
from erpnext.accounts.deferred_revenue import get_deferred_booking_accounts
from erpnext.accounts.doctype.invoice_discounting.invoice_discounting import (
    get_party_account_based_on_invoice_discounting,
)
from erpnext.accounts.doctype.journal_entry.journal_entry import JournalEntry
from frappe import _, scrub
from frappe.utils import cstr, flt


class JournalEntryOverride(JournalEntry):
    def validate_reference_doc(self):
        """Validates reference document"""
        field_dict = {
            "Sales Invoice": ["Customer", "Debit To"],
            "Purchase Invoice": ["Supplier", "Credit To"],
            "Sales Order": ["Customer"],
            "Purchase Order": ["Supplier"],
        }

        self.reference_totals = {}
        self.reference_types = {}
        self.reference_accounts = {}

        for d in self.get("accounts"):
            if not d.reference_type:
                d.reference_name = None
            if not d.reference_name:
                d.reference_type = None
            if (
                d.reference_type
                and d.reference_name
                and (d.reference_type in list(field_dict))
            ):
                dr_or_cr = (
                    "credit_in_account_currency"
                    if d.reference_type in ("Sales Order", "Sales Invoice")
                    else "debit_in_account_currency"
                )

                # check debit or credit type Sales / Purchase Order
                if d.reference_type == "Sales Order" and flt(d.debit) > 0:
                    frappe.throw(
                        _("Row {0}: Debit entry can not be linked with a {1}").format(
                            d.idx, d.reference_type
                        )
                    )

                if d.reference_type == "Purchase Order" and flt(d.credit) > 0:
                    frappe.throw(
                        _("Row {0}: Credit entry can not be linked with a {1}").format(
                            d.idx, d.reference_type
                        )
                    )

                # set totals
                if d.reference_name not in self.reference_totals:
                    self.reference_totals[d.reference_name] = 0.0

                if self.voucher_type not in ("Deferred Revenue", "Deferred Expense"):
                    self.reference_totals[d.reference_name] = round(
                        self.reference_totals[d.reference_name] + flt(d.get(dr_or_cr)),
                        2,
                    )
                    if d.reference_name == "ACC-SINV-2025-09583":
                        print(d.get(dr_or_cr))

                self.reference_types[d.reference_name] = d.reference_type
                self.reference_accounts[d.reference_name] = d.account

                against_voucher = frappe.db.get_value(
                    d.reference_type,
                    d.reference_name,
                    [scrub(dt) for dt in field_dict.get(d.reference_type)],
                )

                if not against_voucher:
                    frappe.throw(
                        _("Row {0}: Invalid reference {1}").format(
                            d.idx, d.reference_name
                        )
                    )

                # check if party and account match
                if d.reference_type in ("Sales Invoice", "Purchase Invoice"):
                    if (
                        self.voucher_type in ("Deferred Revenue", "Deferred Expense")
                        and d.reference_detail_no
                    ):
                        debit_or_credit = "Debit" if d.debit else "Credit"
                        party_account = get_deferred_booking_accounts(
                            d.reference_type, d.reference_detail_no, debit_or_credit
                        )
                        against_voucher = ["", against_voucher[1]]
                    else:
                        if d.reference_type == "Sales Invoice":
                            party_account = (
                                get_party_account_based_on_invoice_discounting(
                                    d.reference_name
                                )
                                or against_voucher[1]
                            )
                        else:
                            party_account = against_voucher[1]

                    if (
                        against_voucher[0] != cstr(d.party)
                        or party_account != d.account
                    ) and self.voucher_type != "Exchange Gain Or Loss":
                        frappe.throw(
                            _(
                                "Row {0}: Party / Account does not match with {1} / {2} in {3} {4}"
                            ).format(
                                d.idx,
                                field_dict.get(d.reference_type)[0],
                                field_dict.get(d.reference_type)[1],
                                d.reference_type,
                                d.reference_name,
                            )
                        )

                # check if party matches for Sales / Purchase Order
                if d.reference_type in ("Sales Order", "Purchase Order"):
                    # set totals
                    if against_voucher != d.party:
                        frappe.throw(
                            _("Row {0}: {1} {2} does not match with {3}").format(
                                d.idx, d.party_type, d.party, d.reference_type
                            )
                        )

        self.validate_orders()
        self.validate_invoices()
