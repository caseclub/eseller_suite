# eSeller Suite (Forked and Enhanced for Amazon Integration with ERPNext)

This is a **forked and heavily modified version** of the original [efeone/eseller_suite](https://github.com/efeone/eseller_suite) repository. The original project is an e-commerce integration suite for ERPNext, but this fork focuses on **drastic improvements to Amazon Seller Central integrations**, including settlement reconciliation, FBA inventory syncing, and Amazon Pay processing. These changes make it more robust, idempotent, and suitable for high-volume Amazon sellers using ERPNext.

If you're looking for enhanced Amazon-SP-API-based tools for ERPNext (e.g., automated journal entries, refunds, inventory reconciliation, and payment transfers), this fork is optimized for that. It preserves core functionality from the original while overhauling key scripts for better error handling, performance, and compliance with Amazon's APIs.

## Key Changes from the Original Repo
This fork introduces **drastic modifications** to the core logic in several files, based on real-world usage and debugging. Highlights include:
- **amazon_process_settlement_report.py**: 
  - Enhanced idempotency for refunds and cancellations (e.g., handles concurrent cancellations, legacy adjustments, and stricter duplicate checks).
  - Improved handling of multi-currency settlements, exchange rates, and late document allocations.
  - Added queuing for journal entry finalization with rounding adjustments.
  - Better filtering and sorting of reports using `after_date` and internal settlement dates.
  - Decryption and decompression for encrypted reports.
  - Shortening of remarks in GL/Payment Ledger Entries to reduce database bloat.
- **amazon_sync_fba_inventory.py**:
  - Daily (scheduled at 8 AM) FBA inventory sync with aggregation across marketplaces.
  - Handles inbound inventory transfers and reconciliations, skipping non-stock items.
  - Improved error handling for negative stock, zero valuations, and batch/serial items.
  - Throttling to avoid API rate limits.
- **amazon_pay_process_settlement_report.py**:
  - Processes Amazon Pay settlements for fees and transfers.
  - Creates consolidated journal entries for pending fees and payment entries for net transfers.
  - Checks account balances before creating entries to prevent overdrafts.
  - Scheduled to run daily at 1 AM.
- **General Improvements**:
  - Better logging, debugging toggles, and timezone-aware scheduling.
  - Compatibility with ERPNext v14/v15 (tested on Python 3.12).
  - Removal of noisy errors and addition of retries for locked documents.
  - No external package installations required (uses ERPNext's built-in libs like pycryptodome).

These changes make the app more reliable for production use, especially for sellers dealing with refunds, multi-marketplace inventory, and automated accounting in ERPNext.

## Features
- **Amazon Settlement Reconciliation**: Automates processing of V2 settlement reports via Amazon's Reports API. Handles orders, refunds, fees, promotions, and creates/cancels Sales Invoices, Credit Notes, and Journal Entries in ERPNext.
- **FBA Inventory Sync**: Daily synchronization of Fulfillable By Amazon (FBA) inventory levels to ERPNext warehouses, including inbound transfers and stock reconciliations.
- **Amazon Pay Settlements**: Processes pay settlement reports to create journal entries for fees and payment entries for bank transfers.
- **Idempotency and Error Resilience**: Prevents duplicates, handles concurrent operations, and logs errors without crashing.
- **Scheduling**: Built-in hooks for daily/hourly runs via ERPNext's scheduler.
- **Multi-Currency Support**: Uses ERPNext's exchange rates for accurate accounting.
- **Debug Mode**: Toggleable for testing without affecting production schedules.

## Prerequisites
- ERPNext v14 or v15 installed and running.
- Amazon Seller Central account with SP-API access (including Reports, Inventory, and Finances roles).
- Custom fields/doctypes from the original eSeller Suite app (this fork assumes they're installed).
- Python libraries like `pycryptodome`, `requests`, `pytz` (already in ERPNext environments).

## Installation
1. **Clone the Repository**:
   ```
   git clone https://github.com/caseclub/eseller_suite.git
   cd eseller_suite
   ```

2. **Install the App in ERPNext**:
   From your ERPNext bench directory:
   ```
   bench get-app https://github.com/caseclub/eseller_suite
   bench --site your-site-name install-app eseller_suite
   bench migrate
   ```

3. **Configure Amazon SP-API Settings**:
   - Go to ERPNext > Amazon SP API Settings > Create a new document.
   - Fill in your Amazon credentials: Access Key ID, Secret Key, Refresh Token, etc.
   - Set custom fields like warehouses (e.g., `afn_warehouse`, `custom_amazon_inbound_warehouse`), accounts (e.g., `custom_amazon_usd_clearing_account`), and marketplaces.
   - Enable scheduling in hooks.py (already included).

4. **Enable Scheduler**:
   Ensure ERPNext's background jobs are running: `bench start-scheduler`.

## Usage
### Running Manually (for Testing)
- Settlement Processing: `frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_process_settlement_report.process_settlements")`
- FBA Inventory Sync: `frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_sync_fba_inventory.process_fba_inventory")`
- Amazon Pay Settlements: `frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_pay_process_settlement_report.process_settlement_reports")`

### Scheduled Runs
- Settlements: Runs daily via `run_daily_settlement_sync()` (processes newest reports).
- FBA Inventory: Runs hourly but executes only at 8 AM system time.
- Amazon Pay: Runs hourly but executes only at 1 AM system time.

### Debugging
- Set `DEBUG = True` in the scripts to enable verbose prints.
- Check ERPNext logs for errors (e.g., `frappe.log_error` calls).

### Customization
- Adjust `after_date` in settings to filter reports.
- Modify warehouse/account mappings in `get_currency_accounts_map()`.

## Configuration Tips
- **Timezones**: Set your system's timezone in ERPNext (System Settings > Time Zone) to match your business (e.g., 'America/New_York').
- **API Rate Limits**: Scripts include sleeps to avoid throttling; adjust if needed.
- **Currencies**: Ensure exchange rates are set in ERPNext for USD/CAD/MXN.
- **Warehouses**: Define FBA, inbound, and staging warehouses in settings.

## Known Issues & Limitations
- Requires "Inventory" role in Amazon SP-API for FBA sync.
- No support for installing additional packages (uses ERPNext's env).
- Tested on North American marketplaces; may need tweaks for others.
- If reports are encrypted, ensure `pycryptodome` is available.
- Setup does not install all required custom fields within the UI

## Contributing
Contributions are welcome! Fork this repo, make changes, and submit a pull request. Focus on:
- Adding support for more marketplaces or report types.
- Improving error handling or performance.
- Documentation updates.

Please follow the original repo's coding style and include tests.

## Credits
- Original repository: [efeone/eseller_suite](https://github.com/efeone/eseller_suite) by efeone.
- Built on ERPNext framework.
- Thanks to the ERPNext community for core utilities like exchange rate handling.

## License
This project is licensed under the GNU General Public License v3 (GPLv3). See the [LICENSE](LICENSE) file for details.
