import uuid, hashlib, base64, gzip, time, urllib.parse as up, requests, socket
from datetime import datetime
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
import frappe
import csv
import io
import textwrap
from erpnext.accounts.utils import get_balance_on
import pytz

DEBUG = False  # Set to False to disable debug prints

"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_pay_process_settlement_report.process_settlement_reports")
"""
def process_settlement_reports():
    # Get ERPNext's system time zone (e.g., 'America/New_York'); update in System Settings if wrong
    system_tz_str = frappe.db.get_single_value("System Settings", "time_zone")
    system_tz = pytz.timezone(system_tz_str)
    now = datetime.now(system_tz)
    if now.hour != 1 and DEBUG == False:
        return  # Only run at 1 AM in system time zone
    
    if DEBUG: print("Starting process_settlement_reports")
    amz_pay_settings = frappe.get_all(
        "Amazon SP API Settings",
        filters={"is_active": 1},
        pluck="name",
    )
    if not amz_pay_settings:
        if DEBUG: print("No active Amazon Pay Settings found")
        return

    for setting_name in amz_pay_settings:
        settings = frappe.get_doc("Amazon SP API Settings", setting_name)
        ACCESS_KEY_ID = settings.custom_access_key_id
        REGION_CODE = settings.custom_region_code
        MAX_REPORTS = 2
        private_key_str = textwrap.dedent(settings.custom_private_key)
        
    private_key_bytes = private_key_str.encode('utf-8')
    PRIV = serialization.load_pem_private_key(private_key_bytes, None)
    if DEBUG: print("Private key loaded")

    LIVE_HOST, SBX_HOST = "pay-api.amazon.com", "sandbox.pay-api.amazon.com"
    BASE_LIVE, BASE_SBX = "/live/v2", "/sandbox/v2"
    ALGO = "AMZN-PAY-RSASSA-PSS-V2"
    SETTLE_TYPES = {
        "_GET_FLAT_FILE_OFFAMAZONPAYMENTS_SETTLEMENT_DATA_",
    }

    # ── helpers ───────────────────────────────────────────────────────────
    def _canon_qs(qs):
        if not qs: return ""
        pairs = up.parse_qsl(qs, keep_blank_values=True); pairs.sort()
        return "&".join(f"{up.quote(k,safe='-_.~')}={up.quote(v,safe='-_.~')}" for k,v in pairs)

    def _sign(method, host, path_qs):
        parsed = up.urlsplit(path_qs)
        hdrs = {
            "accept": "application/json",
            "content-type": "application/json",
            "x-amz-pay-date": datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"),
            "x-amz-pay-host": host,
            "x-amz-pay-region": REGION_CODE,
            "x-amz-pay-idempotency-key": str(uuid.uuid4()),
        }
        signed = ";".join(sorted(hdrs))
        canon_hdrs = "".join(f"{k}:{hdrs[k]}\n" for k in sorted(hdrs))
        payload_sha = hashlib.sha256(b"").hexdigest()
        canonical = (
            f"{method}\n{parsed.path}\n{_canon_qs(parsed.query)}\n"
            f"{canon_hdrs}\n{signed}\n{payload_sha}"
        )
        sts = f"{ALGO}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
        sig = PRIV.sign(
            sts.encode(),
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=32),
            hashes.SHA256()
        )
        hdrs["authorization"] = (
            f"{ALGO} PublicKeyId={ACCESS_KEY_ID}, SignedHeaders={signed}, "
            f"Signature={base64.b64encode(sig).decode()}"
        )
        return hdrs

    def _host_ok(host):
        try: socket.gethostbyname(host); return True
        except socket.gaierror: return False

    # ── 1. Collect newest COMPLETED settlement reports (max 2) ────────────
    reports, token = [], None
    while len(reports) < MAX_REPORTS:
        qs = f"{BASE_LIVE}/reports?{('nextToken='+token) if token else ''}"
        r  = requests.get(f"https://{LIVE_HOST}{qs}", headers=_sign("GET", LIVE_HOST, qs))
        r.raise_for_status()
        chunk = r.json()
        for rep in chunk.get("reports", []):
            if rep["processingStatus"] == "COMPLETED" \
               and rep["reportType"].split("_SANDBOX")[0] in SETTLE_TYPES \
               and rep.get("reportDocumentId"):
                reports.append(rep)
                if len(reports) == MAX_REPORTS:
                    break
        token = chunk.get("nextToken")
        if not token: break
    if DEBUG: print(f"Collected {len(reports)} reports")

    if len(reports) < 2:
        frappe.log_error("Fewer than 2 suitable settlement reports found.", "Amazon Pay Settlement")
        if DEBUG: print("Error: Fewer than 2 reports")
        return

    # ── 2. Download and parse the documents ───────────────────────────────
    company = frappe.db.get_default("company")
    if DEBUG: print(f"Company: {company}")
    report_data = {}
    for rep in reports:
        rid, did, rtype = rep["reportId"], rep["reportDocumentId"], rep["reportType"]
        if DEBUG: print(f"Processing report {rid}")
        sbx = "SANDBOX" in rtype
        host, base = (SBX_HOST, BASE_SBX) if sbx else (LIVE_HOST, BASE_LIVE)
        if sbx and not _host_ok(host):
            frappe.log_error(f"🔒 sandbox host not resolvable, skipping {did}", "Amazon Pay Settlement")
            if DEBUG: print(f"Skipping sandbox {did}")
            continue
        doc_path = f"{base}/report-documents/{did}"

        # poll until ready (max ~20 s)
        js = None
        processed = False
        for _ in range(7):
            meta = requests.get(f"https://{host}{doc_path}", headers=_sign("GET", host, doc_path))
            if meta.status_code == 202:
                time.sleep(3)
                continue
            if meta.status_code == 404:
                frappe.log_error(f"🛑 404 for {did} on {host} — skip", "Amazon Pay Settlement")
                if DEBUG: print(f"404 for {did}")
                break
            meta.raise_for_status()
            js = meta.json()
            if "url" not in js:
                frappe.log_error(f"‼️ not ready for {did}", "Amazon Pay Settlement")
                if DEBUG: print(f"Not ready for {did}")
                continue  # Changed from break to continue for retry
            blob = requests.get(js["url"]).content
            if js.get("compressionAlgorithm","").upper() == "GZIP":
                blob = gzip.decompress(blob)
            content = blob.decode('utf-8')
            lines = content.splitlines(keepends=False)

            # Parse settlement end date
            settlement_end = None
            for line in lines:
                if line.startswith('"SettlementEndDate"'):
                    parts = line.split(',', 1)
                    if len(parts) == 2:
                        settlement_end_str = parts[1].strip('"')
                        settlement_end = datetime.strptime(settlement_end_str, "%Y-%m-%dT%H:%M:%S %z").date()
                    break
            if DEBUG: print(f"Settlement end for {rid}: {settlement_end}")

            # Find data header
            data_start = None
            for i, line in enumerate(lines):
                if line.startswith('"TransactionPostedDate"'):
                    data_start = i
                    break
            if data_start is None:
                frappe.log_error(f"No data header found for {rid}", "Amazon Pay Settlement")
                if DEBUG: print(f"No data header for {rid}")
                continue

            # Parse CSV data
            dict_reader = csv.DictReader(lines[data_start:])
            rows = list(dict_reader)
            report_data[rid] = {
                'rows': rows,
                'end_date': settlement_end or frappe.utils.today()
            }

            if DEBUG: print(f"Parsed {rid} with {len(rows)} rows")
            processed = True
            break  # Add break here to exit loop after successful processing
        if not processed:
            frappe.log_error(f"Failed to process {rid} after retries", "Amazon Pay Settlement")

    # ── 3. Process older report for fees journal entry ────────────────────
    newest = reports[0]
    older = reports[1]
    older_id = older['reportId']
    older_data = report_data.get(older_id)
    if older_data:
        pending_fees = []
        total_pending = 0
        for idx, row in enumerate(older_data['rows']):
            if row.get('TotalTransactionFee'):
                fee = -float(row['TotalTransactionFee'])
                if fee > 0:
                    seller_order_id = row.get('SellerOrderId')
                    so = None
                    if seller_order_id:
                        so = frappe.db.get_value('Sales Order', {'custom_woocommerce_order_id': seller_order_id}, 'name')
                        if not so:
                            frappe.logger().warning(f"Sales Order not found for SellerOrderId {seller_order_id} in report {older_id}")
                    pending_fees.append({
                        'fee': fee,
                        'so': so,
                        'seller_order_id': seller_order_id
                    })
                    total_pending += fee
        if total_pending > 0 and pending_fees:
            cheque_no = f"{older_id}"
            if frappe.db.exists('Journal Entry', {'cheque_no': cheque_no, 'docstatus': 1, 'company': company}):
                if DEBUG: print(f"Consolidated JE for {older_id} already exists")
            else:
                clearing_balance = get_balance_on(settings.custom_amazon_pay_clearing_account, date=older_data['end_date'])
                print(f"older end_date: {older_data['end_date']}")
                if clearing_balance < total_pending:
                    if DEBUG: print(f"Skip JE for {older_id}: insufficient balance ({clearing_balance} < {total_pending})")
                else:
                    try:
                        accounts = []
                        for item in pending_fees:
                            accounts.append({
                                'account': settings.custom_amazon_pay_fees_account, 
                                'debit_in_account_currency': item['fee'],
                                'custom_merchant_order_id': item['seller_order_id'],
                                'custom_sales_order': item['so']
                            })
                            accounts.append({
                                'account': settings.custom_amazon_pay_clearing_account, 
                                'credit_in_account_currency': item['fee']
                            })
                        user_remark = f"Consolidated fees from Amazon Pay settlement {older_id}"
                        je = frappe.get_doc({
                            'doctype': 'Journal Entry',
                            'company': company,
                            'posting_date': older_data['end_date'],
                            'cheque_no': cheque_no,
                            'cheque_date': older_data['end_date'],
                            'accounts': accounts,
                            'user_remark': user_remark
                        })
                        je.insert()
                        je.submit()
                        frappe.db.commit()

                        if DEBUG: print(f"Created consolidated JE for {older_id}")
                    except Exception as e:
                        frappe.log_error(f"Failed to create consolidated JE for {older_id}: {str(e)}", "Amazon Pay Settlement")
                        if DEBUG: print(f"Error creating consolidated JE for {older_id}: {str(e)}")

    # ── 4. Process newest report for transfer payment entry ───────────────
    new_id = newest['reportId']
    new_data = report_data.get(new_id)
    if new_data:
        transfer_net = next((float(row['NetTransactionAmount']) for row in new_data['rows'] if row.get('TransactionType') == 'Transfer'), None)
        if transfer_net is not None:
            transfer_amount = -transfer_net  # positive
            if DEBUG: print(f"New {new_id} transfer: {transfer_amount}")
            if transfer_amount > 0 and not frappe.db.exists('Payment Entry', {'reference_no': new_id, 'docstatus': 1, 'company': company}):
                # Check balance
                clearing_balance = get_balance_on(settings.custom_amazon_pay_clearing_account, date=new_data['end_date'])
                print(f"newer end_date: {new_data['end_date']}")
                if clearing_balance < transfer_amount:
                    #frappe.log_error(f"Insufficient balance in clearing account ({clearing_balance} < {transfer_amount}) for PE {new_id}", "Amazon Pay Settlement")
                    if DEBUG: print(f"Skip PE for {new_id}: insufficient balance")
                else:
                    try:
                        pe = frappe.get_doc({
                            'doctype': 'Payment Entry',
                            'company': company,
                            'payment_type': 'Internal Transfer',
                            'posting_date': new_data['end_date'],
                            'paid_from': settings.custom_amazon_pay_clearing_account,
                            'paid_to': settings.custom_default_bank_account,
                            "mode_of_payment": settings.custom_bank_transfer_mode_of_payment,
                            'paid_amount': transfer_amount,
                            'received_amount': transfer_amount,
                            'reference_no': new_id,
                            'reference_date': new_data['end_date']
                        })
                        pe.insert()
                        pe.submit()
                        frappe.db.commit()

                        if DEBUG: print(f"Created PE for {new_id}")
                    except Exception as e:
                        frappe.log_error(f"Failed to create PE for {new_id}: {str(e)}", "Amazon Pay Settlement")
                        if DEBUG: print(f"Error creating PE for {new_id}: {str(e)}")

    if DEBUG: print("Finished process_settlement_reports")