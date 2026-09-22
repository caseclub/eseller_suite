# amazon_sync_fba_inventory.py
# =========================================
#  Syncs Amazon FBA inventory quantities to ERPNext using the FBA Inventory API,
#  with a 6 AM MYI ALL report request and 7 AM report comparison for inbound inventory.
# =========================================
from __future__ import annotations
import csv
import gzip
import inspect
import io
import json, requests
import re

from datetime import datetime, timedelta, timezone
import time
from zoneinfo import ZoneInfo
import frappe
from . import amazon_repository as _amazon_repository
from .amazon_repository import _sp_get, AmazonRepository

from urllib.parse import urlencode

from collections import defaultdict

from erpnext.stock.doctype.batch.batch import get_batch_qty
from erpnext.stock.stock_ledger import NegativeStockError

import pytz

# When True all docs are left as drafts and not submitted
DEBUG = False  # Toggle to False to disable all debug prints. Also set to True to run the progam on demand as opposed to during the set time

# Belt-and-suspenders guard: if a single sync run would zero out more than this
# fraction of the Amazon-linked items currently holding stock in a warehouse,
# treat the API pull as partial/unhealthy and SKIP the zero-out for that warehouse
# (reported adjustments still apply). Prevents a truncated response from wiping stock.
MAX_ZERO_OUT_FRACTION = 0.10  # tune to your catalog's normal daily sell-through-to-zero rate

# ──────────────────────────────────────────
# Helper Functions
# ──────────────────────────────────────────
def parse_marketplaces(mkt_str: str) -> list[str]:
    if not mkt_str:
        return []
    return [m.strip() for m in mkt_str.split(',') if m.strip()]


MYI_ALL_REPORT_TYPE = "GET_FBA_MYI_ALL_INVENTORY_DATA"
MYI_REPORT_LOOKBACK_HOURS = 48
MYI_US_MARKETPLACE_ID = "ATVPDKIKX0DER"
FINISHED_GOODS_WAREHOUSE = "Finished Goods Post Production - CC"
MAX_INBOUND_FLOW_EXAMPLES_PER_GROUP = 10
MAX_INBOUND_FLOW_EXAMPLES_TOTAL = 50
MAX_DOCUMENT_NAMES_PER_GROUP = 10


def _sp_response_data(response):
    """Return a JSON dictionary from the project's SP-API helper response."""
    if isinstance(response, dict):
        return response
    json_method = getattr(response, "json", None)
    if callable(json_method):
        data = json_method()
        if isinstance(data, dict):
            return data
    raise TypeError("SP-API helper returned an unsupported response type")


def _sp_response_payload(response):
    data = _sp_response_data(response)
    payload = data.get("payload")
    return payload if isinstance(payload, dict) else data


def _sp_post_existing(path, body, settings, max_retry: int = 10):
    """
    POST to SP-API using the repository's existing auth primitives.

    Newer/older amazon_repository.py revisions may not expose a generic
    `_sp_post` helper, so fall back to the same LWA token/domain/timeout
    primitives already used by `_sp_get`.
    """
    sp_post = getattr(_amazon_repository, "_sp_post", None)
    if callable(sp_post):
        kwargs = {}
        try:
            if "return_full" in inspect.signature(sp_post).parameters:
                kwargs["return_full"] = True
        except (TypeError, ValueError):
            pass
        return sp_post(path, body, settings, **kwargs)

    get_lwa_token = getattr(_amazon_repository, "_get_lwa_token", None)
    sp_domain = getattr(_amazon_repository, "SP_DOMAIN", None)
    timeout = getattr(_amazon_repository, "SPAPI_TIMEOUT", (12.0, 45.0))
    if not callable(get_lwa_token) or not sp_domain:
        raise RuntimeError(
            "amazon_repository exposes neither _sp_post nor the "
            "_get_lwa_token/SP_DOMAIN primitives required for SP-API POST"
        )

    url = f"https://{sp_domain}{path}"
    token = get_lwa_token(settings)
    headers = {
        "host": sp_domain,
        "user-agent": "ERPNext-eSellerSuite/1.0",
        "x-amz-access-token": token,
        "accept": "application/json",
        "content-type": "application/json",
    }

    retryable_statuses = {429, 500, 502, 503, 504}
    last_error = None

    for attempt in range(max_retry):
        try:
            response = requests.post(
                url,
                headers=headers,
                json=body,
                timeout=timeout,
            )
        except requests.exceptions.RequestException as exc:
            last_error = exc
            frappe.logger().warning(
                f"SP-API POST {type(exc).__name__} for {path} "
                f"(attempt {attempt + 1}/{max_retry}): {str(exc)[:250]}"
            )
            if attempt == max_retry - 1:
                raise
            time.sleep(min(2 + attempt, 20))
            continue

        if 200 <= response.status_code < 300:
            return response.json()

        response_excerpt = (response.text or "")[:500]
        if response.status_code not in retryable_statuses:
            raise RuntimeError(
                f"SP-API POST {path} failed with HTTP {response.status_code}: "
                f"{response_excerpt}"
            )

        last_error = RuntimeError(
            f"SP-API POST {path} returned retryable HTTP {response.status_code}: "
            f"{response_excerpt}"
        )
        retry_after_raw = response.headers.get("Retry-After")
        try:
            retry_after = int(retry_after_raw) if retry_after_raw else (2 + attempt)
        except (TypeError, ValueError):
            retry_after = 2 + attempt
        frappe.logger().info(
            f"SP-API POST {response.status_code}, sleeping {retry_after}s for {path}"
        )
        time.sleep(min(retry_after, 60))

    raise RuntimeError(
        f"SP-API POST {path} failed after {max_retry} attempts: {last_error}"
    )


def request_manage_inventory_report(settings, marketplace_ids):
    """Request one fresh US MYI ALL report; do not poll, download, or persist its ID."""
    response = _sp_post_existing(
        "/reports/2021-06-30/reports",
        {
            "reportType": MYI_ALL_REPORT_TYPE,
            "marketplaceIds": [MYI_US_MARKETPLACE_ID],
        },
        settings,
    )
    report_id = _sp_response_payload(response).get("reportId")
    if not report_id:
        raise RuntimeError("Amazon createReport response did not include reportId")
    if DEBUG:
        print(
            f"[DEBUG] Requested MYI ALL report: {report_id} "
            f"for US marketplace {MYI_US_MARKETPLACE_ID}"
        )
    return report_id


def _utc_iso(value):
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_amazon_datetime(value):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def _list_recent_manage_inventory_reports(settings):
    """Return recent MYI ALL report candidates without assuming which daily report is valid."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=MYI_REPORT_LOOKBACK_HOURS)
    response = _sp_get(
        "/reports/2021-06-30/reports",
        {
            "reportTypes": MYI_ALL_REPORT_TYPE,
            "marketplaceIds": MYI_US_MARKETPLACE_ID,
            "createdSince": _utc_iso(cutoff),
            "pageSize": 100,
        },
        settings,
        return_full=True,
    )
    reports = _sp_response_payload(response).get("reports") or []
    return [report for report in reports if isinstance(report, dict)]


def _report_candidate_summary(reports):
    candidates = []
    for report in reports:
        candidates.append(
            f"{report.get('reportId', '<no-id>')}@{report.get('createdTime', '<no-time>')}"
            f"[{report.get('processingStatus', '<no-status>')},"
            f"doc={'yes' if report.get('reportDocumentId') else 'no'}]"
        )
    return ", ".join(candidates[:25]) or "<none>"


def _select_scheduled_manage_inventory_report(reports, target_date, local_tz):
    """Select the strict local-date 6 AM DONE report for one calendar date."""
    scheduled = []
    for report in reports:
        if report.get("reportType") != MYI_ALL_REPORT_TYPE:
            continue
        created_utc = _parse_amazon_datetime(report.get("createdTime"))
        if created_utc is None:
            continue
        created_local = created_utc.astimezone(local_tz)
        if created_local.date() == target_date and created_local.hour == 6:
            scheduled.append((created_local, report))

    valid = [
        (created_local, report)
        for created_local, report in scheduled
        if report.get("processingStatus") == "DONE" and report.get("reportDocumentId")
    ]
    if not valid:
        if not scheduled:
            same_date = []
            for report in reports:
                created_utc = _parse_amazon_datetime(report.get("createdTime"))
                if created_utc is None:
                    continue
                created_local = created_utc.astimezone(local_tz)
                if created_local.date() == target_date:
                    same_date.append(
                        f"{report.get('reportId', '<no-id>')}@{created_local.isoformat()}"
                        f"[{report.get('processingStatus', '<no-status>')}]"
                    )
            detail = ", ".join(same_date[:10]) or "<none>"
            return None, f"missing strict 6 AM report; same-date candidates: {detail}"

        detail = []
        for created_local, report in scheduled:
            reason = []
            if report.get("processingStatus") != "DONE":
                reason.append(f"status={report.get('processingStatus')}")
            if not report.get("reportDocumentId"):
                reason.append("missing reportDocumentId")
            detail.append(
                f"{report.get('reportId', '<no-id>')}@{created_local.isoformat()}"
                f" ({', '.join(reason) or 'unusable'})"
            )
        return None, "6 AM candidate(s) unusable: " + "; ".join(detail)

    six_am = datetime.combine(target_date, datetime.min.time(), tzinfo=local_tz).replace(hour=6)
    valid.sort(
        key=lambda item: (
            abs((item[0] - six_am).total_seconds()),
            -item[0].timestamp(),
            str(item[1].get("reportId") or ""),
        )
    )
    return valid[0][1], None


def _parse_required_nonnegative_quantity(value, field_name, asin):
    """Parse a required Amazon quantity without turning missing/malformed data into zero."""
    if value is None:
        raise ValueError(f"{asin}: missing {field_name}")
    text = str(value).strip()
    if text == "":
        raise ValueError(f"{asin}: blank {field_name}")
    try:
        parsed = float(text)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{asin}: malformed {field_name}={value!r}") from exc
    if parsed < 0 or not parsed.is_integer():
        raise ValueError(f"{asin}: invalid {field_name}={value!r}")
    return int(parsed)


def _normalize_report_header(value):
    return str(value or "").lstrip("\ufeff").strip().lower()


def _parse_manage_inventory_report(report_bytes):
    """Return valid raw snapshots, per-ASIN failures, and schema metadata."""
    text = report_bytes.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    if not reader.fieldnames:
        raise ValueError("MYI ALL report has no header row")

    reader.fieldnames = [_normalize_report_header(name) for name in reader.fieldnames]
    report_headers = set(reader.fieldnames)
    fundamental_headers = {
        "asin",
        "condition",
        "afn-inbound-shipped-quantity",
        "afn-inbound-receiving-quantity",
    }
    fc_transfer_header = "afn-fc-transfer-quantity"
    partial_report = fc_transfer_header not in report_headers
    core_headers = (
        {
            "afn-fulfillable-quantity",
            "afn-unsellable-quantity",
            "afn-reserved-quantity",
        }
        if partial_report
        else {
            "afn-warehouse-quantity",
            "afn-researching-quantity",
            fc_transfer_header,
        }
    )
    missing_headers = sorted(
        (fundamental_headers | core_headers) - report_headers
    )
    if missing_headers:
        raise ValueError(
            "MYI ALL report is missing required columns: " + ", ".join(missing_headers)
        )

    schema_metadata = {
        "partial": partial_report,
        "missing_headers": [fc_transfer_header] if partial_report else [],
        "unavailable_components": ["F"] if partial_report else [],
        "parsed_row_count": 0,
    }

    raw_by_asin = {}
    invalid_asins = {}
    parsed_rows = 0
    for raw_row in reader:
        row = {
            _normalize_report_header(key): value
            for key, value in raw_row.items()
            if key is not None
        }
        asin = str(row.get("asin") or "").strip()
        if not asin:
            continue

        condition = "".join(
            ch for ch in str(row.get("condition") or "").lower() if ch.isalnum()
        )
        # Only explicit New-condition inventory participates in this sync.
        if condition not in {"new", "newitem"}:
            continue
        if asin in invalid_asins:
            continue

        try:
            quantities = {
                "S": _parse_required_nonnegative_quantity(
                    row.get("afn-inbound-shipped-quantity"),
                    "afn-inbound-shipped-quantity",
                    asin,
                ),
                "R": _parse_required_nonnegative_quantity(
                    row.get("afn-inbound-receiving-quantity"),
                    "afn-inbound-receiving-quantity",
                    asin,
                ),
            }
            if partial_report:
                quantities["C"] = sum(
                    _parse_required_nonnegative_quantity(
                        row.get(field_name), field_name, asin
                    )
                    for field_name in (
                        "afn-fulfillable-quantity",
                        "afn-unsellable-quantity",
                        "afn-reserved-quantity",
                    )
                )
            else:
                quantities.update(
                    {
                        "warehouse": _parse_required_nonnegative_quantity(
                            row.get("afn-warehouse-quantity"),
                            "afn-warehouse-quantity",
                            asin,
                        ),
                        "researching": _parse_required_nonnegative_quantity(
                            row.get("afn-researching-quantity"),
                            "afn-researching-quantity",
                            asin,
                        ),
                        "F": _parse_required_nonnegative_quantity(
                            row.get(fc_transfer_header),
                            fc_transfer_header,
                            asin,
                        ),
                    }
                )
        except ValueError as exc:
            invalid_asins[asin] = str(exc)
            raw_by_asin.pop(asin, None)
            continue

        aggregate = raw_by_asin.setdefault(
            asin,
            {"C": 0, "S": 0, "R": 0, "F": None}
            if partial_report
            else {"warehouse": 0, "S": 0, "R": 0, "researching": 0, "F": 0},
        )
        for key, value in quantities.items():
            aggregate[key] += value
        parsed_rows += 1

    snapshots = {}
    for asin, aggregate in raw_by_asin.items():
        if partial_report:
            snapshots[asin] = {
                "C": aggregate["C"],
                "S": aggregate["S"],
                "R": aggregate["R"],
                "F": None,
            }
            continue
        # A negative derived Core is intentionally floored to zero. Because F is
        # added back later, this can only bias the protected result high, which
        # is the desired conservative failure direction.
        core = max(
            0,
            aggregate["warehouse"] - aggregate["F"] - aggregate["researching"],
        )
        snapshots[asin] = {
            "C": core,
            "S": aggregate["S"],
            "R": aggregate["R"],
            "F": aggregate["F"],
        }

    schema_metadata["parsed_row_count"] = parsed_rows
    if DEBUG:
        print(
            f"[DEBUG] Parsed {parsed_rows} MYI ALL New-condition rows into "
            f"{len(snapshots)} valid ASIN snapshots; invalid ASINs={len(invalid_asins)}; "
            f"schema={'partial C/S/R' if partial_report else 'complete C/S/R/F'}"
        )
    return snapshots, invalid_asins, schema_metadata


def _download_manage_inventory_report(report, settings):
    report_document_id = report.get("reportDocumentId")
    if not report_document_id:
        raise RuntimeError("selected MYI ALL report is missing reportDocumentId")

    document_response = _sp_get(
        f"/reports/2021-06-30/documents/{report_document_id}",
        {},
        settings,
        return_full=True,
    )
    document = _sp_response_payload(document_response)
    if document.get("encryptionDetails"):
        raise RuntimeError(
            "Legacy encrypted report document is not supported by existing project context"
        )

    download_url = document.get("url")
    if not download_url:
        raise RuntimeError("Amazon report document response did not include a download URL")

    download = requests.get(download_url, timeout=90)
    download.raise_for_status()
    report_bytes = download.content
    if str(document.get("compressionAlgorithm") or "").upper() == "GZIP":
        report_bytes = gzip.decompress(report_bytes)

    return _parse_manage_inventory_report(report_bytes)


def _empty_report_source(label, expected_date):
    return {
        "label": label,
        "expected_date": expected_date,
        "available": False,
        "report": None,
        "snapshots": {},
        "invalid_asins": {},
        "partial": False,
        "missing_headers": [],
        "unavailable_components": [],
        "parsed_row_count": 0,
        "error": None,
    }


def _load_daily_manage_inventory_sources(settings):
    """Discover, strictly select, download, and parse today's/yesterday's raw reports."""
    local_tz = ZoneInfo("America/Los_Angeles")
    local_today = datetime.now(local_tz).date()
    yesterday = local_today - timedelta(days=1)
    today_source = _empty_report_source("TODAY", local_today)
    yesterday_source = _empty_report_source("YESTERDAY", yesterday)

    try:
        reports = _list_recent_manage_inventory_reports(settings)
    except Exception as exc:
        reason = f"Reports API discovery failed ({type(exc).__name__}: {exc})"
        today_source["error"] = reason
        yesterday_source["error"] = reason
        return today_source, yesterday_source, []

    for source in (today_source, yesterday_source):
        selected, selection_error = _select_scheduled_manage_inventory_report(
            reports, source["expected_date"], local_tz
        )
        source["report"] = selected
        if selection_error:
            source["error"] = selection_error
            continue
        try:
            (
                snapshots,
                invalid_asins,
                schema_metadata,
            ) = _download_manage_inventory_report(selected, settings)
            source["snapshots"] = snapshots
            source["invalid_asins"] = invalid_asins
            source.update(schema_metadata)
            source["available"] = True
        except Exception as exc:
            source["error"] = (
                f"report {selected.get('reportId', '<no-id>')} unusable "
                f"({type(exc).__name__}: {exc})"
            )

    if today_source["available"] and yesterday_source["available"]:
        today_time = _parse_amazon_datetime(today_source["report"].get("createdTime"))
        yesterday_time = _parse_amazon_datetime(
            yesterday_source["report"].get("createdTime")
        )
        if today_time is not None and yesterday_time is not None:
            hours_apart = (today_time - yesterday_time).total_seconds() / 3600.0
            if not 20 <= hours_apart <= 28:
                frappe.log_error(
                    f"Strict calendar-date MYI reports are {hours_apart:.2f} hours apart. "
                    "The calendar-date/6-AM selection remains authoritative.\n"
                    f"TODAY={today_source['report'].get('reportId')} "
                    f"{today_source['report'].get('createdTime')}\n"
                    f"YESTERDAY={yesterday_source['report'].get('reportId')} "
                    f"{yesterday_source['report'].get('createdTime')}",
                    "FBA MYI Daily Report Timestamp Sanity Error",
                )

    return today_source, yesterday_source, reports


def _api_required_quantity(container, key, field_name, asin):
    if not isinstance(container, dict) or key not in container:
        raise ValueError(f"{asin}: missing LIVE {field_name}")
    return _parse_required_nonnegative_quantity(container.get(key), f"LIVE {field_name}", asin)


def _build_live_api_snapshots(summaries):
    """Map current Inventory Summaries to explicit per-ASIN C/S/R/F snapshots."""
    snapshots = {}
    invalid_asins = {}

    for summary in summaries:
        if summary.get("condition", "") != "NewItem":
            continue
        asin = str(summary.get("asin") or "").strip()
        if not asin:
            continue
        if asin in invalid_asins:
            continue

        try:
            if "totalQuantity" not in summary:
                raise ValueError(f"{asin}: missing LIVE totalQuantity")
            total_quantity = _parse_required_nonnegative_quantity(
                summary.get("totalQuantity"), "LIVE totalQuantity", asin
            )
            details = summary.get("inventoryDetails")
            if not isinstance(details, dict):
                raise ValueError(f"{asin}: missing LIVE inventoryDetails")

            inbound_working = _api_required_quantity(
                details, "inboundWorkingQuantity", "inboundWorkingQuantity", asin
            )
            shipped = _api_required_quantity(
                details, "inboundShippedQuantity", "inboundShippedQuantity", asin
            )
            receiving = _api_required_quantity(
                details, "inboundReceivingQuantity", "inboundReceivingQuantity", asin
            )

            researching_obj = details.get("researchingQuantity")
            researching = _api_required_quantity(
                researching_obj,
                "totalResearchingQuantity",
                "researchingQuantity.totalResearchingQuantity",
                asin,
            )
            reserved_obj = details.get("reservedQuantity")
            fc_transfer = _api_required_quantity(
                reserved_obj,
                "pendingTransshipmentQuantity",
                "reservedQuantity.pendingTransshipmentQuantity",
                asin,
            )

            core_from_total = max(
                0,
                total_quantity
                - inbound_working
                - shipped
                - receiving
                - researching
                - fc_transfer,
            )

            # Preserve the current program's max(total-derived, fulfillable-derived)
            # main-FBA semantics when fulfillableQuantity is explicitly available.
            core = core_from_total
            if "fulfillableQuantity" in details and details.get("fulfillableQuantity") is not None:
                fulfillable = _parse_required_nonnegative_quantity(
                    details.get("fulfillableQuantity"), "LIVE fulfillableQuantity", asin
                )
                core = max(core, fulfillable)
        except ValueError as exc:
            invalid_asins[asin] = str(exc)
            snapshots.pop(asin, None)
            continue

        candidate = {"C": core, "S": shipped, "R": receiving, "F": fc_transfer}
        if asin not in snapshots:
            snapshots[asin] = candidate
        else:
            # Preserve the existing cross-marketplace de-duplication behavior:
            # take the maximum observed quantity rather than summing duplicate
            # marketplace views of the same physical ASIN.
            for key in ("C", "S", "R", "F"):
                snapshots[asin][key] = max(snapshots[asin][key], candidate[key])

    return snapshots, invalid_asins


def _current_erp_qty_by_asin(warehouse):
    rows = frappe.db.sql(
        """
        SELECT i.custom_asin AS asin, SUM(b.actual_qty) AS actual_qty
        FROM `tabItem` i
        INNER JOIN `tabBin` b ON i.name = b.item_code
        WHERE b.warehouse = %s
          AND i.custom_asin IS NOT NULL
          AND i.custom_asin != ''
          AND i.disabled = 0
          AND i.is_stock_item = 1
        GROUP BY i.custom_asin
        """,
        warehouse,
        as_dict=True,
    )
    return {
        str(row.asin): int(row.actual_qty or 0)
        for row in rows
        if row.asin
    }


def _erp_items_with_multiple_asins():
    """Return enabled stock Items whose custom_asin contains 2+ distinct ASINs."""
    rows = frappe.db.sql(
        """
        SELECT i.name AS item_code, i.custom_asin AS custom_asin
        FROM `tabItem` i
        WHERE i.disabled = 0
          AND i.is_stock_item = 1
          AND i.custom_asin IS NOT NULL
          AND i.custom_asin != ''
        """,
        as_dict=True,
    )

    issues = []
    for row in rows:
        raw_value = str(row.custom_asin or "").strip()
        detected = []
        for token in re.findall(r"(?<![A-Z0-9])[A-Z0-9]{10}(?![A-Z0-9])", raw_value.upper()):
            if token not in detected:
                detected.append(token)
        if len(detected) > 1:
            issues.append(
                {
                    "item_code": str(row.item_code),
                    "custom_asin": raw_value,
                    "detected_asins": detected,
                }
            )
    return issues


def _allocate_shared_warehouse_gain(eligible, warehouse_gain):
    """Deterministically allocate one integer downstream-gain budget proportionally."""
    keys = ("S", "R", "F")
    eligible = {key: max(int(eligible.get(key, 0)), 0) for key in keys}
    total_eligible = sum(eligible.values())
    total_matched = min(max(int(warehouse_gain), 0), total_eligible)
    if not total_eligible or not total_matched:
        return {key: 0 for key in keys}

    matched = {
        key: (total_matched * eligible[key]) // total_eligible
        for key in keys
    }
    remainder = total_matched - sum(matched.values())
    order_index = {key: index for index, key in enumerate(keys)}
    remainder_order = sorted(
        keys,
        key=lambda key: (
            -((total_matched * eligible[key]) % total_eligible),
            order_index[key],
        ),
    )
    for key in remainder_order:
        if remainder <= 0:
            break
        if matched[key] < eligible[key]:
            matched[key] += 1
            remainder -= 1
    return matched


def _protect_snapshot_transition(old_snapshot, new_snapshot):
    """Mode 1/2 one-cycle transition matching and false-low carry."""
    shipped_drop = max(old_snapshot["S"] - new_snapshot["S"], 0)
    receiving_gain = max(new_snapshot["R"] - old_snapshot["R"], 0)
    shipped_to_receiving = min(shipped_drop, receiving_gain)
    remaining_shipped_drop = shipped_drop - shipped_to_receiving

    fc_eligible = {
        "S": remaining_shipped_drop,
        "R": max(old_snapshot["R"] - new_snapshot["R"], 0),
        "F": 0,
    }
    fc_gain = max(new_snapshot["F"] - old_snapshot["F"], 0)
    fc_matched = _allocate_shared_warehouse_gain(fc_eligible, fc_gain)

    eligible = {
        "S": fc_eligible["S"] - fc_matched["S"],
        "R": fc_eligible["R"] - fc_matched["R"],
        "F": max(old_snapshot["F"] - new_snapshot["F"], 0),
    }
    warehouse_gain = max(new_snapshot["C"] - old_snapshot["C"], 0)
    matched = _allocate_shared_warehouse_gain(eligible, warehouse_gain)
    carry = {key: eligible[key] - matched[key] for key in ("S", "R", "F")}

    protected_inbound = (
        new_snapshot["S"] + new_snapshot["R"] + carry["S"] + carry["R"]
    )
    protected_fc = new_snapshot["F"] + carry["F"]
    main_target = new_snapshot["C"] + protected_fc

    return main_target, protected_inbound, {
        "shipped_to_receiving": shipped_to_receiving,
        "fc_gain": fc_gain,
        "fc_eligible": fc_eligible,
        "fc_matched": fc_matched,
        "warehouse_gain": warehouse_gain,
        "eligible": eligible,
        "matched": matched,
        "carry": carry,
        "protected_fc": protected_fc,
    }


def _snapshot_is_complete(snapshot):
    return snapshot is not None and snapshot.get("F") is not None


def _protect_partial_snapshot_transition(partial_snapshot, current_snapshot):
    """Conservatively overlay known partial C/S/R onto a complete current snapshot."""
    if not _snapshot_is_complete(current_snapshot):
        raise ValueError("partial MYI protection requires a complete current snapshot")
    if partial_snapshot is None or partial_snapshot.get("F") is not None:
        raise ValueError("partial MYI protection requires an unknown F component")

    shipped_drop = max(partial_snapshot["S"] - current_snapshot["S"], 0)
    receiving_gain = max(current_snapshot["R"] - partial_snapshot["R"], 0)
    shipped_to_receiving = min(shipped_drop, receiving_gain)
    eligible = {
        "S": shipped_drop - shipped_to_receiving,
        "R": max(partial_snapshot["R"] - current_snapshot["R"], 0),
        "F": 0,
    }
    core_gain = max(current_snapshot["C"] - partial_snapshot["C"], 0)
    matched = _allocate_shared_warehouse_gain(eligible, core_gain)
    carry = {
        key: eligible[key] - matched[key]
        for key in ("S", "R")
    }

    return (
        current_snapshot["C"] + current_snapshot["F"],
        current_snapshot["S"]
        + current_snapshot["R"]
        + carry["S"]
        + carry["R"],
        {
            "shipped_to_receiving": shipped_to_receiving,
            "core_gain": core_gain,
            "eligible": eligible,
            "matched": matched,
            "carry": carry,
        },
    )


def _is_expected_live_only_asin(
    asin,
    today_source,
    yesterday_source,
    live_snapshots,
    live_invalid_asins,
    live_global_error,
):
    """Return whether report absence is routine coverage for a valid LIVE ASIN."""
    if (
        live_global_error
        or asin in live_invalid_asins
        or not _snapshot_is_complete(live_snapshots.get(asin))
    ):
        return False
    for source in (today_source, yesterday_source):
        if (
            not source["available"]
            or source.get("error")
            or asin in source["invalid_asins"]
            or asin in source["snapshots"]
        ):
            return False
    return True


def _source_asin_reason(source, asin):
    report = source.get("report") or {}
    identity = (
        f"{source['label']} report {report.get('reportId', '<no-id>')}"
        f"@{report.get('createdTime', '<no-time>')}"
    )
    if not source["available"]:
        return source["error"] or f"{identity} unavailable"
    if asin in source["invalid_asins"]:
        return f"{identity}: {source['invalid_asins'][asin]}"
    if asin not in source["snapshots"]:
        return f"ASIN ABSENT from {identity}"
    return None


def _mode4_targets(
    asin,
    today_snapshot,
    yesterday_snapshot,
    live_snapshot,
    current_main,
    current_inbound,
):
    """Best-effort Mode 4 protection without inventing absent source quantities."""
    if live_snapshot is not None:
        current_snapshot = live_snapshot
        current_source = "LIVE API"
    elif today_snapshot is not None:
        current_snapshot = today_snapshot
        current_source = "TODAY report"
    else:
        current_snapshot = None
        current_source = None

    report_baseline = today_snapshot or yesterday_snapshot
    if current_snapshot is not None:
        reliable_core = current_snapshot["C"]
        candidate_inbound = current_snapshot["S"] + current_snapshot["R"]
        candidate_fc = current_snapshot["F"]

        if report_baseline is not None:
            inbound_baseline = report_baseline["S"] + report_baseline["R"]
            fc_baseline = report_baseline["F"]
            baseline_source = "TODAY report" if today_snapshot is not None else "YESTERDAY report"
        else:
            # ERP Inbound is itself the protected S+R aggregate, so it is safe to
            # use as a component-level fallback floor. Main FBA cannot safely be
            # split into historical Core vs FC, so do not fabricate an FC baseline.
            inbound_baseline = max(current_inbound, 0)
            fc_baseline = None
            baseline_source = "current ERP Inbound (S+R aggregate)"

        protected_inbound = max(candidate_inbound, inbound_baseline)
        protected_fc = (
            max(candidate_fc, fc_baseline)
            if fc_baseline is not None
            else candidate_fc
        )
        main_target = reliable_core + protected_fc
        return main_target, protected_inbound, {
            "current_source": current_source,
            "baseline_source": baseline_source,
            "candidate_inbound": candidate_inbound,
            "candidate_fc": candidate_fc,
            "protected_inbound": protected_inbound,
            "protected_fc": protected_fc,
            "reliable_core": reliable_core,
            "fc_baseline_available": fc_baseline is not None,
        }

    # No trustworthy current Core snapshot exists. Do not manufacture a downward
    # current target from source absence. A yesterday-only report may still raise
    # a protected pool, but it cannot justify lowering current ERP stock.
    main_target = max(current_main, 0)
    inbound_target = max(current_inbound, 0)
    if yesterday_snapshot is not None:
        main_target = max(
            main_target, yesterday_snapshot["C"] + yesterday_snapshot["F"]
        )
        inbound_target = max(
            inbound_target, yesterday_snapshot["S"] + yesterday_snapshot["R"]
        )

    return main_target, inbound_target, {
        "current_source": "<none>",
        "baseline_source": "YESTERDAY report" if yesterday_snapshot is not None else "<none>",
        "candidate_inbound": None,
        "candidate_fc": None,
        "protected_inbound": inbound_target,
        "protected_fc": yesterday_snapshot["F"] if yesterday_snapshot is not None else None,
        "reliable_core": None,
        "fc_baseline_available": yesterday_snapshot is not None,
    }


def _build_protected_inventory_targets(
    today_source,
    yesterday_source,
    live_snapshots,
    live_invalid_asins,
    live_global_error,
    current_main_by_asin,
    current_inbound_by_asin,
):
    relevant_asins = (
        set(today_source["snapshots"])
        | set(yesterday_source["snapshots"])
        | set(today_source["invalid_asins"])
        | set(yesterday_source["invalid_asins"])
        | set(live_snapshots)
        | set(live_invalid_asins)
        | set(current_main_by_asin)
        | set(current_inbound_by_asin)
    )

    main_targets = {}
    inbound_targets = {}
    mode_by_asin = {}
    degradation_lines = []

    for asin in sorted(relevant_asins):
        today_snapshot = (
            today_source["snapshots"].get(asin) if today_source["available"] else None
        )
        yesterday_snapshot = (
            yesterday_source["snapshots"].get(asin)
            if yesterday_source["available"]
            else None
        )
        live_snapshot = live_snapshots.get(asin) if not live_global_error else None
        today_complete_snapshot = (
            today_snapshot if _snapshot_is_complete(today_snapshot) else None
        )
        yesterday_complete_snapshot = (
            yesterday_snapshot if _snapshot_is_complete(yesterday_snapshot) else None
        )
        current_main = int(current_main_by_asin.get(asin, 0) or 0)
        current_inbound = int(current_inbound_by_asin.get(asin, 0) or 0)

        if (
            yesterday_complete_snapshot is not None
            and today_complete_snapshot is not None
            and live_snapshot is not None
        ):
            mode = "YESTERDAY -> PROTECTED TODAY -> LIVE API"
            _, _, report_diagnostics = _protect_snapshot_transition(
                yesterday_complete_snapshot, today_complete_snapshot
            )
            protected_today = dict(today_complete_snapshot)
            for key in ("S", "R", "F"):
                protected_today[key] += report_diagnostics["carry"][key]
            main_target, inbound_target, diagnostics = _protect_snapshot_transition(
                protected_today, live_snapshot
            )
            if DEBUG:
                print(
                    f"[DEBUG] {asin} mode={mode} yesterday={yesterday_snapshot} "
                    f"today={today_snapshot} protected_today={protected_today} "
                    f"live={live_snapshot} first_carry={report_diagnostics['carry']} "
                    f"first_S->F/R->F={report_diagnostics['fc_matched']} "
                    f"second_S->R={diagnostics['shipped_to_receiving']} "
                    f"second_S->F/R->F={diagnostics['fc_matched']} "
                    f"warehouse_gain={diagnostics['warehouse_gain']} "
                    f"eligible={diagnostics['eligible']} matched={diagnostics['matched']} "
                    f"carry={diagnostics['carry']} main={main_target} inbound={inbound_target}"
                )

        elif today_complete_snapshot is not None and live_snapshot is not None:
            mode = "TODAY REPORT -> LIVE API"
            main_target, inbound_target, diagnostics = _protect_snapshot_transition(
                today_complete_snapshot, live_snapshot
            )
            if DEBUG:
                print(
                    f"[DEBUG] {asin} mode={mode} old={today_snapshot} "
                    f"new={live_snapshot} old_source=TODAY report "
                    f"{today_source['report'].get('reportId')} "
                    f"new_source=LIVE S->R={diagnostics['shipped_to_receiving']} "
                    f"S->F/R->F={diagnostics['fc_matched']} "
                    f"warehouse_gain={diagnostics['warehouse_gain']} "
                    f"eligible={diagnostics['eligible']} matched={diagnostics['matched']} "
                    f"carry={diagnostics['carry']} main={main_target} inbound={inbound_target}"
                )

        elif yesterday_complete_snapshot is not None and live_snapshot is not None:
            mode = "YESTERDAY REPORT -> LIVE API"
            main_target, inbound_target, diagnostics = _protect_snapshot_transition(
                yesterday_complete_snapshot, live_snapshot
            )
            if DEBUG:
                print(
                    f"[DEBUG] {asin} mode={mode} old={yesterday_snapshot} "
                    f"new={live_snapshot} old_source=YESTERDAY report "
                    f"{yesterday_source['report'].get('reportId')} "
                    f"new_source=LIVE S->R={diagnostics['shipped_to_receiving']} "
                    f"S->F/R->F={diagnostics['fc_matched']} "
                    f"warehouse_gain={diagnostics['warehouse_gain']} "
                    f"eligible={diagnostics['eligible']} matched={diagnostics['matched']} "
                    f"carry={diagnostics['carry']} main={main_target} inbound={inbound_target}"
                )

        elif (
            today_complete_snapshot is not None
            and yesterday_complete_snapshot is not None
        ):
            mode = "NORMAL TWO-REPORT"
            main_target, inbound_target, diagnostics = _protect_snapshot_transition(
                yesterday_complete_snapshot, today_complete_snapshot
            )
            old_source = (
                f"YESTERDAY report {yesterday_source['report'].get('reportId')}"
            )
            new_source = f"TODAY report {today_source['report'].get('reportId')}"
            if DEBUG:
                print(
                    f"[DEBUG] {asin} mode={mode} old={yesterday_snapshot} "
                    f"new={today_snapshot} old_source={old_source} new_source={new_source} "
                    f"S->R={diagnostics['shipped_to_receiving']} "
                    f"S->F/R->F={diagnostics['fc_matched']} "
                    f"warehouse_gain={diagnostics['warehouse_gain']} "
                    f"eligible={diagnostics['eligible']} matched={diagnostics['matched']} "
                    f"carry={diagnostics['carry']} main={main_target} inbound={inbound_target}"
                )

        else:
            mode = "PROBLEM-CATEGORY INCREASE-ONLY SAFETY MODE"
            main_target, inbound_target, diagnostics = _mode4_targets(
                asin,
                today_complete_snapshot,
                yesterday_complete_snapshot,
                live_snapshot,
                current_main,
                current_inbound,
            )
            if DEBUG:
                print(
                    f"[DEBUG] {asin} mode={mode} current_source={diagnostics['current_source']} "
                    f"baseline={diagnostics['baseline_source']} core={diagnostics['reliable_core']} "
                    f"candidate_inbound={diagnostics['candidate_inbound']} "
                    f"candidate_fc={diagnostics['candidate_fc']} "
                    f"protected_inbound={diagnostics['protected_inbound']} "
                    f"protected_fc={diagnostics['protected_fc']} "
                    f"current_main={current_main} current_inbound={current_inbound} "
                    f"main={main_target} inbound={inbound_target}"
                )

        base_mode = mode
        partial_overlays = []
        if _snapshot_is_complete(live_snapshot):
            for label, partial_snapshot in (
                ("YESTERDAY", yesterday_snapshot),
                ("TODAY", today_snapshot),
            ):
                if partial_snapshot is None or partial_snapshot.get("F") is not None:
                    continue
                (
                    partial_main,
                    partial_inbound,
                    partial_diagnostics,
                ) = _protect_partial_snapshot_transition(
                    partial_snapshot, live_snapshot
                )
                main_target = max(main_target, partial_main)
                inbound_target = max(inbound_target, partial_inbound)
                partial_overlays.append(label)
                if DEBUG:
                    print(
                        f"[DEBUG] {asin} partial_overlay={label} "
                        f"partial={partial_snapshot} live={live_snapshot} "
                        f"S->R={partial_diagnostics['shipped_to_receiving']} "
                        f"core_gain={partial_diagnostics['core_gain']} "
                        f"eligible={partial_diagnostics['eligible']} "
                        f"matched={partial_diagnostics['matched']} "
                        f"carry={partial_diagnostics['carry']} "
                        f"base_mode={base_mode} main={main_target} "
                        f"inbound={inbound_target}"
                    )

        if partial_overlays == ["YESTERDAY", "TODAY"]:
            mode = "PARTIAL YESTERDAY + PARTIAL TODAY (C/S/R) -> LIVE API"
        elif partial_overlays == ["YESTERDAY"]:
            mode = (
                "PARTIAL YESTERDAY (C/S/R) -> TODAY -> LIVE API"
                if today_complete_snapshot is not None
                else "PARTIAL YESTERDAY (C/S/R) -> LIVE API"
            )
        elif partial_overlays == ["TODAY"]:
            mode = (
                "YESTERDAY -> PARTIAL TODAY (C/S/R) -> LIVE API"
                if yesterday_complete_snapshot is not None
                else "PARTIAL TODAY (C/S/R) -> LIVE API"
            )

        main_targets[asin] = max(int(main_target), 0)
        inbound_targets[asin] = max(int(inbound_target), 0)
        mode_by_asin[asin] = mode

        expected_live_only = _is_expected_live_only_asin(
            asin,
            today_source,
            yesterday_source,
            live_snapshots,
            live_invalid_asins,
            live_global_error,
        )
        reasons = []
        for source in (today_source, yesterday_source):
            if asin in source["invalid_asins"]:
                reasons.append(_source_asin_reason(source, asin))
        if asin in live_invalid_asins:
            reasons.append(live_invalid_asins[asin])

        if (
            base_mode == "PROBLEM-CATEGORY INCREASE-ONLY SAFETY MODE"
            and not expected_live_only
            and live_snapshot is None
            and not live_global_error
        ):
            for source in (today_source, yesterday_source):
                if (
                    source["available"]
                    and asin not in source["snapshots"]
                    and asin not in source["invalid_asins"]
                ):
                    reasons.append(_source_asin_reason(source, asin))
            if asin not in live_snapshots and asin not in live_invalid_asins:
                reasons.append("ASIN ABSENT from LIVE API")

        reasons = [reason for reason in reasons if reason]
        if reasons:
            degradation_lines.append(
                f"ASIN {asin}: mode={mode}; " + "; ".join(reasons)
            )

    return main_targets, inbound_targets, mode_by_asin, degradation_lines


def _log_degraded_asins(lines):
    if not lines:
        return

    grouped_counts = defaultdict(int)
    for line in lines:
        mode_match = re.search(r"\bmode=([^;]+)", line)
        mode = mode_match.group(1) if mode_match else "<unknown mode>"
        categories = []
        for label in ("TODAY", "YESTERDAY", "LIVE API"):
            if f"ABSENT from {label}" in line:
                categories.append(f"absent from {label}")
        if any(
            marker in line
            for marker in (": missing ", ": blank ", ": malformed ", ": invalid ")
        ):
            categories.append("invalid or malformed source quantity")
        grouped_counts[(mode, tuple(categories or ["other ASIN-specific reason"]))] += 1

    representative_lines = lines[:40]
    log_lines = [
        f"Total affected ASINs: {len(lines)}",
        "Grouped reason/mode counts:",
    ]
    for (mode, categories), count in sorted(grouped_counts.items()):
        log_lines.append(
            f"- {mode} | {', '.join(categories)}: {count}"
        )
    log_lines.extend(["", "Representative ASIN lines (maximum 40):"])
    log_lines.extend(representative_lines)
    omitted_count = len(lines) - len(representative_lines)
    if omitted_count:
        log_lines.append(f"{omitted_count} additional ASINs omitted")

    frappe.log_error(
        "\n".join(log_lines),
        "FBA MYI Per-ASIN Degraded Protection",
    )


def _log_daily_report_errors(today_source, yesterday_source, reports, mode_by_asin):
    affected_sources = [
        source
        for source in (today_source, yesterday_source)
        if not source["available"] or source.get("partial")
    ]
    if not affected_sources:
        return

    mode_counts = defaultdict(int)
    for mode in mode_by_asin.values():
        mode_counts[mode] += 1
    issue_lines = []
    for source in affected_sources:
        if source.get("partial"):
            report = source.get("report") or {}
            issue_lines.append(
                f"{source['label']} expected {source['expected_date']}: "
                f"report {report.get('reportId', '<no-id>')}"
                f"@{report.get('createdTime', '<no-time>')} parsed partially; "
                f"missing headers={source.get('missing_headers') or []}; "
                f"unavailable components={source.get('unavailable_components') or []}; "
                f"partial valid ASIN snapshots={len(source['snapshots'])}; "
                f"invalid ASINs={len(source['invalid_asins'])}"
            )
        else:
            issue_lines.append(
                f"{source['label']} expected {source['expected_date']}: "
                f"{source['error'] or 'unavailable'}"
            )
    issue_text = "\n".join(issue_lines)
    frappe.log_error(
        f"{issue_text}\n"
        f"Discovered candidates: {_report_candidate_summary(reports)}\n"
        f"Per-ASIN operating modes: {dict(mode_counts)}",
        "FBA MYI Daily Report Protection Error",
    )


def _new_inbound_flow_diagnostics(asin_inbound, settings):
    """Create the run-local collector before any inventory documents are changed."""
    return {
        "active": False,
        "prep_warehouse": settings.custom_amazon_fba_staging_area,
        "finished_goods_warehouse": FINISHED_GOODS_WAREHOUSE,
        "inbound_warehouse": settings.custom_amazon_inbound_warehouse,
        "target_count": len(asin_inbound),
        "valid_target_count": 0,
        "positive_shortage_count": 0,
        "shortages_exceeding_prep_count": 0,
        "initial_short_after_prep_qty": 0,
        "drafts_inspected": 0,
        "drafts_eligible": 0,
        "drafts_revalidation_ineligible": 0,
        "drafts_debug_would_submit": 0,
        "drafts_attempted": 0,
        "drafts_submitted_verified": 0,
        "drafts_failed_submission": 0,
        "drafts_committed_unverified": 0,
        "submitted_finished_to_prep_qty": 0,
        "prep_valid_row_count": 0,
        "prep_valid_row_qty": 0,
        "finished_valid_row_count": 0,
        "finished_valid_row_qty": 0,
        "aggregated_entry_name": "<none>",
        "aggregated_entry_status": "not created",
        "aggregated_item_count": 0,
        "aggregated_child_row_count": 0,
        "aggregated_sources": [],
        "prep_submitted_qty": 0,
        "finished_submitted_qty": 0,
        "reconciliation_shortage_count": 0,
        "reconciliation_shortage_qty": 0,
        "skipped_item_count": 0,
        "valuation_attempted": 0,
        "valuation_successful": 0,
        "valuation_failed": 0,
        "valuation_debug_drafts": 0,
        "trigger_reasons": {},
        "inventory_documents": {},
        "global_errors": [],
        "events": [],
        "items": {},
    }


def _concise_inventory_exception(exc):
    return f"{type(exc).__name__}: {str(exc).strip() or '<no message>'}"


def _record_inventory_trigger(diagnostics, code, summary, identifier=None):
    """Record one deterministic, deduplicated consolidated-log trigger."""
    if diagnostics is None:
        return
    try:
        triggers = diagnostics.setdefault("trigger_reasons", {})
        trigger = triggers.setdefault(
            code,
            {"summary": summary, "count": 0, "identifiers": []},
        )
        if identifier is None:
            trigger["count"] += 1
            return
        identifier = str(identifier)
        if identifier not in trigger["identifiers"]:
            trigger["identifiers"].append(identifier)
            trigger["count"] += 1
    except Exception:
        # Diagnostics must never affect inventory processing.
        return


def _upsert_inventory_document(
    diagnostics,
    doctype,
    document_id,
    purpose,
    origin,
    status,
    source_warehouses=None,
    target_warehouse=None,
    movement_quantity=None,
    affected_item_count=None,
    reconciliation_metrics=None,
):
    """Register an inventory document once and update its final known state."""
    if diagnostics is None:
        return
    try:
        document_id = str(document_id or "<not assigned>")
        key = (doctype, document_id)
        if document_id == "<not assigned>":
            key = (
                doctype,
                document_id,
                purpose,
                tuple(sorted(source_warehouses or [])),
                target_warehouse,
            )
        documents = diagnostics.setdefault("inventory_documents", {})
        record = documents.setdefault(
            key,
            {
                "doctype": doctype,
                "document_id": document_id,
                "purpose": purpose,
                "origin": origin,
                "status": status,
                "source_warehouses": [],
                "target_warehouse": None,
                "movement_quantity": 0,
                "affected_item_count": 0,
                "changed_row_count": 0,
                "total_increase_qty": 0,
                "total_decrease_qty": 0,
                "net_quantity_delta": 0,
                "quantity_changed": False,
            },
        )
        record.update({
            "purpose": purpose,
            "origin": origin,
            "status": status,
        })
        for warehouse in source_warehouses or []:
            if warehouse and warehouse not in record["source_warehouses"]:
                record["source_warehouses"].append(warehouse)
        if target_warehouse is not None:
            record["target_warehouse"] = target_warehouse
        if movement_quantity is not None:
            record["movement_quantity"] = float(movement_quantity or 0)
        if affected_item_count is not None:
            record["affected_item_count"] = int(affected_item_count or 0)
        if reconciliation_metrics is not None:
            record.update(reconciliation_metrics)
    except Exception:
        return


def _reconciliation_quantity_metrics(items, current_quantities):
    """Summarize per-row proposed quantity changes without netting rows first."""
    deltas = []
    for row in items:
        key = (row["item_code"], row["warehouse"])
        current_qty = float(current_quantities[key])
        deltas.append(float(row.get("qty") or 0) - current_qty)
    changed_deltas = [delta for delta in deltas if abs(delta) > 1e-9]
    total_increase = sum(delta for delta in changed_deltas if delta > 0)
    total_decrease = sum(-delta for delta in changed_deltas if delta < 0)
    return {
        "changed_row_count": len(changed_deltas),
        "total_increase_qty": total_increase,
        "total_decrease_qty": total_decrease,
        "net_quantity_delta": sum(changed_deltas),
        "quantity_changed": bool(changed_deltas),
    }


def _record_inbound_flow_event(
    diagnostics,
    outcome,
    item_state=None,
    reason=None,
    document_name=None,
    quantity=0,
):
    """Record complete totals in memory; the logger truncates examples only."""
    if diagnostics is None:
        return
    try:
        diagnostics["active"] = True
        event = {
            "outcome": outcome,
            "reason": reason or "<none>",
            "document_name": document_name,
            "event_quantity": quantity or 0,
        }
        if item_state:
            for key in (
                "asin",
                "item_code",
                "target_inbound_qty",
                "initial_inbound_qty",
                "initial_prep_qty",
                "initial_finished_goods_qty",
                "initial_positive_shortage",
                "eligible_draft_names",
                "draft_outcome",
                "draft_expected_qty",
                "fresh_inbound_qty",
                "fresh_prep_qty",
                "fresh_finished_goods_qty",
                "requested_prep_qty",
                "valid_prep_row_qty",
                "submitted_prep_qty",
                "requested_finished_qty",
                "valid_finished_row_qty",
                "submitted_finished_qty",
                "aggregated_entry_name",
                "aggregated_outcome",
                "final_inbound_qty",
                "reconciliation_qty",
                "skipped",
            ):
                event[key] = item_state.get(key)
        diagnostics["events"].append(event)
    except Exception:
        # This collector is temporary diagnostics and must never affect stock.
        return


def _record_inbound_flow_global_error(diagnostics, message):
    if diagnostics is None:
        return
    try:
        diagnostics["active"] = True
        diagnostics["global_errors"].append(str(message))
    except Exception:
        return


def _log_temporary_consolidated_fallbacks_and_failures(
    today_source,
    yesterday_source,
    report_candidates,
    live_snapshots,
    live_invalid_asins,
    live_global_error,
    current_main_by_asin,
    current_inbound_by_asin,
    main_targets,
    inbound_targets,
    mode_by_asin,
    inbound_flow_diagnostics=None,
):
    """Temporarily consolidate source failures and non-normal per-ASIN modes."""
    try:
        MAX_ASIN_EXAMPLES_PER_GROUP = 10
        MAX_ASIN_EXAMPLES_TOTAL = 50
        normal_mode = "YESTERDAY -> PROTECTED TODAY -> LIVE API"
        known_modes = {
            normal_mode,
            "TODAY REPORT -> LIVE API",
            "YESTERDAY REPORT -> LIVE API",
            "NORMAL TWO-REPORT",
            "PROBLEM-CATEGORY INCREASE-ONLY SAFETY MODE",
            "YESTERDAY -> PARTIAL TODAY (C/S/R) -> LIVE API",
            "PARTIAL YESTERDAY (C/S/R) -> TODAY -> LIVE API",
            "PARTIAL YESTERDAY (C/S/R) -> LIVE API",
            "PARTIAL TODAY (C/S/R) -> LIVE API",
            "PARTIAL YESTERDAY + PARTIAL TODAY (C/S/R) -> LIVE API",
        }

        relevant_asins = sorted(
            set(mode_by_asin)
            | set(today_source["snapshots"])
            | set(yesterday_source["snapshots"])
            | set(today_source["invalid_asins"])
            | set(yesterday_source["invalid_asins"])
            | set(live_snapshots)
            | set(live_invalid_asins)
            | set(current_main_by_asin)
            | set(current_inbound_by_asin)
        )
        expected_live_only_asins = [
            asin
            for asin in relevant_asins
            if _is_expected_live_only_asin(
                asin,
                today_source,
                yesterday_source,
                live_snapshots,
                live_invalid_asins,
                live_global_error,
            )
        ]
        expected_live_only_set = set(expected_live_only_asins)

        def report_reason_and_category(source, asin):
            reason = _source_asin_reason(source, asin)
            if source["available"]:
                if asin in source["invalid_asins"]:
                    return reason, "ASIN invalid or malformed"
                if asin in source["snapshots"]:
                    return None, "valid source"
                if reason:
                    return reason, "ASIN absent"
                return None, "other recorded failure"

            error_text = str(source.get("error") or "")
            if error_text.startswith("Reports API discovery failed"):
                return reason, "source globally unavailable"
            return reason, "source report unavailable"

        def live_reason_and_category(asin):
            if live_global_error:
                return live_global_error, "source globally unavailable"
            if asin in live_invalid_asins:
                return live_invalid_asins[asin], "ASIN invalid or malformed"
            if asin not in live_snapshots:
                return "ASIN ABSENT from LIVE API", "ASIN absent"
            if live_snapshots.get(asin) is not None:
                return None, "valid source"
            return None, "other recorded failure"

        trigger_reasons = {}

        def add_trigger(code, text):
            trigger_reasons.setdefault(code, text)

        source_trigger_codes = {
            "TODAY": ("01", "03", "05", "07"),
            "YESTERDAY": ("02", "04", "06", "08"),
        }
        for source in (today_source, yesterday_source):
            unavailable_code, processing_code, partial_code, invalid_code = (
                source_trigger_codes[source["label"]]
            )
            report = source.get("report") or {}
            report_id = report.get("reportId", "<none>")
            if not source["available"]:
                add_trigger(
                    unavailable_code,
                    f"{source['label']} report unavailable: "
                    f"{source.get('error') or 'no usable report'}",
                )
                if source.get("report") and source.get("error"):
                    add_trigger(
                        processing_code,
                        f"{source['label']} report {report_id} failed during "
                        f"download, decryption, decoding, or parsing: "
                        f"{source['error']}",
                    )
            if source.get("partial"):
                add_trigger(
                    partial_code,
                    f"{source['label']} report {report_id} has a partial schema "
                    f"or unavailable inventory components: "
                    f"missing_headers={source.get('missing_headers') or []}; "
                    f"unavailable_components="
                    f"{source.get('unavailable_components') or []}",
                )
            if source["invalid_asins"]:
                add_trigger(
                    invalid_code,
                    f"{source['label']} report {report_id} contains "
                    f"{len(source['invalid_asins'])} invalid ASIN row(s)",
                )

        if live_global_error:
            add_trigger(
                "09",
                "LIVE inventory source is globally unavailable or incomplete: "
                + str(live_global_error),
            )
        if live_invalid_asins:
            add_trigger(
                "10",
                f"LIVE inventory source contains {len(live_invalid_asins)} "
                "invalid ASIN row(s)",
            )

        non_expected_asins = [
            asin for asin in relevant_asins if asin not in expected_live_only_set
        ]
        for source, code in ((today_source, "11"), (yesterday_source, "12")):
            absent = [
                asin
                for asin in non_expected_asins
                if source["available"]
                and asin not in source["snapshots"]
                and asin not in source["invalid_asins"]
            ]
            if absent:
                add_trigger(
                    code,
                    f"{len(absent)} non-expected ASIN(s) are absent from "
                    f"{source['label']} even though that source is available",
                )
        live_missing_or_invalid = [
            asin
            for asin in non_expected_asins
            if not live_global_error
            and (
                asin in live_invalid_asins
                or asin not in live_snapshots
                or not _snapshot_is_complete(live_snapshots.get(asin))
            )
        ]
        if live_missing_or_invalid:
            add_trigger(
                "13",
                f"{len(live_missing_or_invalid)} ASIN(s) required by the run "
                "are absent or invalid in LIVE",
            )

        mode_trigger_codes = {
            "TODAY REPORT -> LIVE API": "14",
            "YESTERDAY REPORT -> LIVE API": "15",
            "NORMAL TWO-REPORT": "16",
            "PARTIAL TODAY (C/S/R) -> LIVE API": "17",
            "PARTIAL YESTERDAY (C/S/R) -> LIVE API": "18",
            "YESTERDAY -> PARTIAL TODAY (C/S/R) -> LIVE API": "19",
            "PARTIAL YESTERDAY (C/S/R) -> TODAY -> LIVE API": "20",
            "PARTIAL YESTERDAY + PARTIAL TODAY (C/S/R) -> LIVE API": "21",
            "PROBLEM-CATEGORY INCREASE-ONLY SAFETY MODE": "22",
        }
        non_expected_mode_counts = defaultdict(int)
        for asin, mode in mode_by_asin.items():
            if asin not in expected_live_only_set:
                non_expected_mode_counts[mode] += 1
        for mode, code in mode_trigger_codes.items():
            count = non_expected_mode_counts.get(mode, 0)
            if count:
                explanation = (
                    " because LIVE was unavailable"
                    if mode == "NORMAL TWO-REPORT"
                    else ""
                )
                add_trigger(
                    code,
                    f"Mode {mode} was used for {count} non-expected ASIN(s)"
                    f"{explanation}",
                )

        flow = inbound_flow_diagnostics or {}
        for code, trigger in sorted((flow.get("trigger_reasons") or {}).items()):
            identifiers = sorted(trigger.get("identifiers") or [])
            detail = f"; occurrences={trigger.get('count', 0)}"
            if identifiers:
                detail += "; identifiers=" + ", ".join(identifiers)
            add_trigger(code, trigger.get("summary", code) + detail)

        if not trigger_reasons:
            return

        mode_counts = defaultdict(int)
        for mode in mode_by_asin.values():
            mode_counts[mode] += 1
        all_modes = sorted(known_modes | set(mode_counts))
        genuine_non_normal_count = sum(
            count
            for mode, count in non_expected_mode_counts.items()
            if mode != normal_mode
        )

        def append_report_source_summary(lines, source):
            report = source.get("report") or {}
            label = source["label"]
            schema_status = (
                "partial"
                if source.get("partial")
                else "complete"
                if source["available"]
                else "unavailable"
            )
            full_snapshot_count = (
                len(source["snapshots"])
                if source["available"] and not source.get("partial")
                else 0
            )
            partial_snapshot_count = (
                len(source["snapshots"])
                if source.get("partial")
                else 0
            )
            lines.extend(
                [
                    f"{label} expected date: {source['expected_date']}",
                    f"{label} availability: {bool(source['available'])}",
                    f"{label} schema status: {schema_status}",
                    f"{label} selected report ID: {report.get('reportId', '<none>')}",
                    f"{label} createdTime: {report.get('createdTime', '<none>')}",
                    f"{label} processingStatus: {report.get('processingStatus', '<none>')}",
                    f"{label} has reportDocumentId: {bool(report.get('reportDocumentId'))}",
                    f"{label} parsed snapshot count: {len(source['snapshots'])}",
                    f"{label} full snapshot count: {full_snapshot_count}",
                    f"{label} partial snapshot count: {partial_snapshot_count}",
                    f"{label} missing headers: "
                    f"{source.get('missing_headers') or '<none>'}",
                    f"{label} unavailable components: "
                    f"{source.get('unavailable_components') or '<none>'}",
                    f"{label} invalid-ASIN count: {len(source['invalid_asins'])}",
                    f"{label} source error: {source.get('error') or '<none>'}",
                ]
            )

        lines = [
            "TEMPORARY CONSOLIDATED INVENTORY FALLBACK/FAILURE DIAGNOSTIC",
            f"Run timestamp (America/Los_Angeles): "
            f"{datetime.now(ZoneInfo('America/Los_Angeles')).isoformat()}",
            "",
            "REPORT TRIGGER REASONS",
        ]
        for number, code in enumerate(sorted(trigger_reasons), start=1):
            lines.append(f"{number}. {trigger_reasons[code]}")

        lines.extend(
            [
                "",
                "STOCK ENTRIES AND STOCK RECONCILIATIONS",
            ]
        )
        document_records = sorted(
            (flow.get("inventory_documents") or {}).values(),
            key=lambda record: (
                record.get("doctype", ""),
                record.get("document_id", ""),
                record.get("purpose", ""),
            ),
        )
        if not document_records:
            lines.append("- <none>")
        for record in document_records:
            source_warehouses = sorted(record.get("source_warehouses") or [])
            lines.extend(
                [
                    f"- DocType: {record['doctype']}",
                    f"  Document ID: {record['document_id']}",
                    f"  Purpose: {record['purpose']}",
                    f"  Origin: {record['origin']}",
                    f"  Final known status/outcome: {record['status']}",
                    "  Source warehouse(s): "
                    + (", ".join(source_warehouses) or "<none>"),
                    f"  Target warehouse: "
                    f"{record.get('target_warehouse') or '<none>'}",
                    f"  Total relevant movement quantity: "
                    f"{record.get('movement_quantity', 0)}",
                    f"  Affected-item count: "
                    f"{record.get('affected_item_count', 0)}",
                ]
            )
            if record["doctype"] == "Stock Reconciliation":
                lines.extend(
                    [
                        f"  Changed-row count: "
                        f"{record.get('changed_row_count', 0)}",
                        f"  Total increase quantity: "
                        f"{record.get('total_increase_qty', 0)}",
                        f"  Total decrease quantity: "
                        f"{record.get('total_decrease_qty', 0)}",
                        f"  Net quantity delta: "
                        f"{record.get('net_quantity_delta', 0)}",
                        f"  Any quantity changed: "
                        f"{bool(record.get('quantity_changed'))}",
                    ]
                )

        lines.extend(
            [
            "",
            "COMPLETE RUN SUMMARY (totals and global errors are not truncated)",
            ]
        )
        append_report_source_summary(lines, today_source)
        lines.append("")
        append_report_source_summary(lines, yesterday_source)
        lines.extend(
            [
                "",
                f"LIVE valid snapshot count: {len(live_snapshots)}",
                f"LIVE invalid-ASIN count: {len(live_invalid_asins)}",
                f"LIVE global error: {live_global_error or '<none>'}",
                "",
                "All discovered report candidates "
                "(complete; formatted with _report_candidate_summary()):",
            ]
        )
        sorted_candidates = sorted(
            report_candidates,
            key=lambda report: (
                str(report.get("createdTime") or ""),
                str(report.get("reportId") or ""),
                str(report.get("processingStatus") or ""),
            ),
        )
        if sorted_candidates:
            lines.extend(
                f"- {_report_candidate_summary([report])}"
                for report in sorted_candidates
            )
        else:
            lines.append("- <none>")

        lines.extend(["", "Complete per-ASIN operating-mode counts:"])
        lines.extend(f"- {mode}: {mode_counts.get(mode, 0)}" for mode in all_modes)
        expected_examples = expected_live_only_asins[:2]
        expected_examples_text = ", ".join(expected_examples) or "<none>"
        lines.extend(
            [
                f"Total ASINs using any genuine non-normal fallback/failure mode: "
                f"{genuine_non_normal_count}",
                f"Expected/non-failing LIVE-only ASIN coverage: "
                f"{len(expected_live_only_asins)} ASINs absent from both usable "
                f"reports but valid in LIVE; representative examples: "
                f"{expected_examples_text}",
                "",
                "GROUPED NON-NORMAL FALLBACKS/FAILURES",
                "Expected LIVE-only coverage is excluded from these groups and totals.",
                "Group totals are complete; representative examples alone are truncated.",
            ]
        )

        grouped_asins = defaultdict(list)
        reason_details = {}
        for asin in relevant_asins:
            if asin in expected_live_only_set:
                continue
            mode = mode_by_asin.get(asin, "<missing operating mode>")
            if mode == normal_mode:
                continue
            today_reason, today_category = report_reason_and_category(
                today_source, asin
            )
            yesterday_reason, yesterday_category = report_reason_and_category(
                yesterday_source, asin
            )
            live_reason, live_category = live_reason_and_category(asin)
            group_key = (
                mode,
                today_category,
                yesterday_category,
                live_category,
            )
            grouped_asins[group_key].append(asin)
            reason_details[asin] = (
                today_reason,
                yesterday_reason,
                live_reason,
            )

        examples_shown = 0
        if not grouped_asins:
            lines.append("- <none>")
        for group_number, group_key in enumerate(sorted(grouped_asins), start=1):
            mode, today_category, yesterday_category, live_category = group_key
            group_asins = sorted(grouped_asins[group_key])
            remaining_example_capacity = max(
                MAX_ASIN_EXAMPLES_TOTAL - examples_shown, 0
            )
            example_count = min(
                len(group_asins),
                MAX_ASIN_EXAMPLES_PER_GROUP,
                remaining_example_capacity,
            )
            example_asins = group_asins[:example_count]

            lines.extend(
                [
                    "",
                    f"Fallback/failure group {group_number}",
                    f"Mode: {mode}",
                    f"TODAY reason category: {today_category}",
                    f"YESTERDAY reason category: {yesterday_category}",
                    f"LIVE reason category: {live_category}",
                    f"Total ASINs in group: {len(group_asins)}",
                ]
            )

            for asin in example_asins:
                today_reason, yesterday_reason, live_reason = reason_details[asin]
                lines.extend(
                    [
                        f"  Representative ASIN: {asin}",
                        f"    Exact selected mode: {mode}",
                    ]
                )
                if today_reason:
                    lines.append(f"    Exact TODAY reason: {today_reason}")
                if yesterday_reason:
                    lines.append(
                        f"    Exact YESTERDAY reason: {yesterday_reason}"
                    )
                if live_reason:
                    lines.append(f"    Exact LIVE reason: {live_reason}")
                today_snapshot = today_source["snapshots"].get(asin)
                yesterday_snapshot = yesterday_source["snapshots"].get(asin)
                live_snapshot = live_snapshots.get(asin)
                if today_snapshot is not None:
                    lines.append(
                        "    TODAY snapshot: "
                        + json.dumps(today_snapshot, sort_keys=True)
                    )
                if yesterday_snapshot is not None:
                    lines.append(
                        "    YESTERDAY snapshot: "
                        + json.dumps(yesterday_snapshot, sort_keys=True)
                    )
                if live_snapshot is not None:
                    lines.append(
                        "    LIVE snapshot: "
                        + json.dumps(live_snapshot, sort_keys=True)
                    )
                lines.extend(
                    [
                        f"    Current ERP Main FBA quantity: "
                        f"{int(current_main_by_asin.get(asin, 0) or 0)}",
                        f"    Current ERP Inbound quantity: "
                        f"{int(current_inbound_by_asin.get(asin, 0) or 0)}",
                        f"    Final calculated Main FBA target: "
                        f"{main_targets.get(asin, '<missing>')}",
                        f"    Final calculated Inbound target: "
                        f"{inbound_targets.get(asin, '<missing>')}",
                    ]
                )

            examples_shown += example_count
            omitted_count = len(group_asins) - example_count
            if omitted_count:
                if example_count == 0:
                    lines.append(
                        "TRUNCATED: no ASIN examples shown for this group because "
                        "the overall 50-ASIN diagnostic limit was reached."
                    )
                elif example_count < min(
                    len(group_asins), MAX_ASIN_EXAMPLES_PER_GROUP
                ):
                    lines.append(
                        f"TRUNCATED: showing {example_count} of {len(group_asins)} "
                        f"ASINs in this fallback group; {omitted_count} additional "
                        "ASINs omitted because the overall 50-ASIN diagnostic limit "
                        "was reached."
                    )
                else:
                    lines.append(
                        f"TRUNCATED: showing {example_count} of {len(group_asins)} "
                        f"ASINs in this fallback group; {omitted_count} additional "
                        "ASINs omitted."
                    )

        lines.extend(
            [
                "",
                f"Detailed representative ASIN examples shown: {examples_shown}",
                f"Per-group example limit: {MAX_ASIN_EXAMPLES_PER_GROUP}",
                f"Overall example limit: {MAX_ASIN_EXAMPLES_TOTAL}",
            ]
        )

        flow = inbound_flow_diagnostics or {}
        lines.extend(
            [
                "",
                "FINISHED GOODS / PREP / INBOUND FLOW SUMMARY",
                "Summary totals are complete and are not truncated.",
                f"Configured Prep warehouse: {flow.get('prep_warehouse', '<unavailable>')}",
                f"Exact Finished Goods warehouse: "
                f"{flow.get('finished_goods_warehouse', FINISHED_GOODS_WAREHOUSE)}",
                f"Configured Inbound warehouse: "
                f"{flow.get('inbound_warehouse', '<unavailable>')}",
                f"Number of valid Inbound targets: {flow.get('valid_target_count', 0)}",
                f"Number of positive Inbound shortages: "
                f"{flow.get('positive_shortage_count', 0)}",
                f"Number of shortages initially exceeding available Prep: "
                f"{flow.get('shortages_exceeding_prep_count', 0)}",
                f"Total quantity initially short after available Prep: "
                f"{flow.get('initial_short_after_prep_qty', 0)}",
                f"Draft Stock Entries inspected: {flow.get('drafts_inspected', 0)}",
                f"Draft Stock Entries found eligible: {flow.get('drafts_eligible', 0)}",
                f"Drafts becoming ineligible during pre-submit revalidation: "
                f"{flow.get('drafts_revalidation_ineligible', 0)}",
                f"Drafts that would be submitted in DEBUG mode: "
                f"{flow.get('drafts_debug_would_submit', 0)}",
                f"Draft submissions actually attempted: {flow.get('drafts_attempted', 0)}",
                f"Drafts successfully submitted and verified: "
                f"{flow.get('drafts_submitted_verified', 0)}",
                f"Drafts failing submission: {flow.get('drafts_failed_submission', 0)}",
                f"Committed drafts whose submitted state could not be verified: "
                f"{flow.get('drafts_committed_unverified', 0)}",
                f"Total submitted Finished Goods to Prep quantity: "
                f"{flow.get('submitted_finished_to_prep_qty', 0)}",
                f"Valid Prep to Inbound rows: {flow.get('prep_valid_row_count', 0)}; "
                f"quantity: {flow.get('prep_valid_row_qty', 0)}",
                f"Valid Finished Goods to Inbound rows: "
                f"{flow.get('finished_valid_row_count', 0)}; "
                f"quantity: {flow.get('finished_valid_row_qty', 0)}",
                f"Aggregated Inbound Stock Entry name: "
                f"{flow.get('aggregated_entry_name', '<none>')}",
                f"Aggregated Inbound Stock Entry final status: "
                f"{flow.get('aggregated_entry_status', 'not created')}",
                f"Aggregated Inbound Stock Entry item count: "
                f"{flow.get('aggregated_item_count', 0)}",
                f"Aggregated Inbound Stock Entry child-row count: "
                f"{flow.get('aggregated_child_row_count', 0)}",
                "Aggregated source warehouses: "
                + (", ".join(flow.get("aggregated_sources") or []) or "<none>"),
                "Aggregated source mode: "
                + (
                    "both Prep and Finished Goods"
                    if len(flow.get("aggregated_sources") or []) == 2
                    else "one source warehouse"
                    if len(flow.get("aggregated_sources") or []) == 1
                    else "no source rows"
                ),
                f"Prep quantity successfully submitted: "
                f"{flow.get('prep_submitted_qty', 0)}",
                f"Finished Goods quantity successfully submitted: "
                f"{flow.get('finished_submitted_qty', 0)}",
                f"Shortages handed to final Inbound Stock Reconciliation: "
                f"{flow.get('reconciliation_shortage_count', 0)}; "
                f"quantity: {flow.get('reconciliation_shortage_qty', 0)}",
                f"Items deliberately skipped because quantities could not be verified: "
                f"{flow.get('skipped_item_count', 0)}",
                f"Valuation corrections attempted: {flow.get('valuation_attempted', 0)}",
                f"Valuation corrections successful: {flow.get('valuation_successful', 0)}",
                f"Valuation corrections failed: {flow.get('valuation_failed', 0)}",
                f"Valuation corrections left as DEBUG drafts: "
                f"{flow.get('valuation_debug_drafts', 0)}",
                "Concise global errors:",
            ]
        )
        global_errors = flow.get("global_errors") or []
        if global_errors:
            lines.extend(f"- {error}" for error in global_errors)
        else:
            lines.append("- <none>")

        lines.extend(
            [
                "",
                "GROUPED FINISHED GOODS / PREP / INBOUND FLOW DETAILS",
                "Group totals are complete; representative examples and document names are truncated.",
            ]
        )
        grouped_flow_events = defaultdict(list)
        for event in flow.get("events") or []:
            grouped_flow_events[(event["outcome"], event["reason"])].append(event)

        flow_examples_shown = 0
        if not grouped_flow_events:
            lines.append("- <none>")
        for group_number, group_key in enumerate(sorted(grouped_flow_events), start=1):
            outcome, reason = group_key
            group_events = grouped_flow_events[group_key]
            document_names = sorted({
                event["document_name"]
                for event in group_events
                if event.get("document_name")
            })
            shown_document_names = document_names[:MAX_DOCUMENT_NAMES_PER_GROUP]
            remaining_capacity = max(
                MAX_INBOUND_FLOW_EXAMPLES_TOTAL - flow_examples_shown, 0
            )
            item_events = [event for event in group_events if event.get("item_code")]
            example_count = min(
                len(item_events),
                MAX_INBOUND_FLOW_EXAMPLES_PER_GROUP,
                remaining_capacity,
            )
            example_events = item_events[:example_count]
            group_quantity = sum(
                event.get("event_quantity", 0) or 0 for event in group_events
            )
            lines.extend(
                [
                    "",
                    f"Inbound-flow group {group_number}",
                    f"Outcome: {outcome}",
                    f"Reason: {reason}",
                    f"Total recorded outcomes in group: {len(group_events)}",
                    f"Total affected item count in group: "
                    f"{len({event.get('item_code') for event in item_events})}",
                    f"Total outcome quantity in group: {group_quantity}",
                    "Representative document names: "
                    + (", ".join(shown_document_names) or "<none>"),
                ]
            )
            omitted_documents = len(document_names) - len(shown_document_names)
            if omitted_documents:
                lines.append(
                    f"TRUNCATED: showing {len(shown_document_names)} of "
                    f"{len(document_names)} document names in this group; "
                    f"{omitted_documents} additional document names omitted."
                )

            for event in example_events:
                final_item_state = (flow.get("items") or {}).get(
                    event.get("item_code"), {}
                )
                event = dict(event)
                event.update(final_item_state)
                eligible_names = event.get("eligible_draft_names") or []
                eligible_name_text = (
                    ", ".join(eligible_names[:MAX_DOCUMENT_NAMES_PER_GROUP])
                    or "<none>"
                )
                if len(eligible_names) > MAX_DOCUMENT_NAMES_PER_GROUP:
                    eligible_name_text += (
                        f"; TRUNCATED: {len(eligible_names) - MAX_DOCUMENT_NAMES_PER_GROUP} "
                        "additional eligible draft names omitted"
                    )
                lines.extend(
                    [
                        f"  Representative ASIN: {event.get('asin') or '<none>'}",
                        f"    ERP item code: {event.get('item_code')}",
                        f"    Target Inbound quantity: {event.get('target_inbound_qty')}",
                        f"    Initial Inbound quantity: {event.get('initial_inbound_qty')}",
                        f"    Initial Prep quantity: {event.get('initial_prep_qty')}",
                        f"    Initial Finished Goods quantity: "
                        f"{event.get('initial_finished_goods_qty')}",
                        f"    Initial positive shortage: "
                        f"{event.get('initial_positive_shortage')}",
                        f"    Eligible draft Stock Entries: {eligible_name_text}",
                        f"    Draft status/outcome: {event.get('draft_outcome') or '<none>'}",
                        f"    Draft expected movement quantity: "
                        f"{event.get('draft_expected_qty') or 0}",
                        f"    Fresh post-draft Inbound quantity: {event.get('fresh_inbound_qty')}",
                        f"    Fresh post-draft Prep quantity: {event.get('fresh_prep_qty')}",
                        f"    Fresh post-draft Finished Goods quantity: "
                        f"{event.get('fresh_finished_goods_qty')}",
                        f"    Requested Prep transfer quantity: "
                        f"{event.get('requested_prep_qty') or 0}",
                        f"    Quantity represented by valid Prep rows: "
                        f"{event.get('valid_prep_row_qty') or 0}",
                        f"    Successfully submitted Prep quantity: "
                        f"{event.get('submitted_prep_qty') or 0}",
                        f"    Requested Finished Goods transfer quantity: "
                        f"{event.get('requested_finished_qty') or 0}",
                        f"    Quantity represented by valid Finished Goods rows: "
                        f"{event.get('valid_finished_row_qty') or 0}",
                        f"    Successfully submitted Finished Goods quantity: "
                        f"{event.get('submitted_finished_qty') or 0}",
                        f"    Aggregated Stock Entry: "
                        f"{event.get('aggregated_entry_name') or '<none>'}",
                        f"    Aggregated outcome: "
                        f"{event.get('aggregated_outcome') or '<none>'}",
                        f"    Final freshly read Inbound quantity: "
                        f"{event.get('final_inbound_qty')}",
                        f"    Quantity handed to Inbound Stock Reconciliation: "
                        f"{event.get('reconciliation_qty') or 0}",
                        f"    Deliberately skipped: {bool(event.get('skipped'))}",
                        f"    Exact concise reason: {event.get('reason')}",
                    ]
                )

            flow_examples_shown += example_count
            omitted_items = len(item_events) - example_count
            if omitted_items:
                lines.append(
                    f"TRUNCATED: showing {example_count} of {len(item_events)} "
                    f"items in this group; {omitted_items} additional items omitted."
                )

        lines.extend(
            [
                "",
                f"Detailed inbound-flow examples shown: {flow_examples_shown}",
                f"Inbound-flow per-group example limit: "
                f"{MAX_INBOUND_FLOW_EXAMPLES_PER_GROUP}",
                f"Inbound-flow overall example limit: "
                f"{MAX_INBOUND_FLOW_EXAMPLES_TOTAL}",
                f"Document-name per-group limit: {MAX_DOCUMENT_NAMES_PER_GROUP}",
            ]
        )
        frappe.log_error(
            "\n".join(lines),
            "amazon sync temporary consolidated fallback and failure log",
        )
    except Exception:
        # Temporary diagnostics must never change or abort the inventory sync.
        return


# ──────────────────────────────────────────
# Inbound Processing
# ──────────────────────────────────────────
def _read_bin_actual_qty(item_code, warehouse):
    return float(
        frappe.db.get_value(
            "Bin",
            {"item_code": item_code, "warehouse": warehouse},
            "actual_qty",
        )
        or 0
    )


def _stock_entry_item_quantities(stock_entry):
    quantities = defaultdict(float)
    for row in stock_entry.items or []:
        quantities[row.item_code] += float(row.qty or 0)
    return dict(quantities)


def _eligible_finished_to_prep_draft(
    stock_entry,
    needed_item_codes,
    company,
    prep_wh,
):
    if (
        stock_entry.docstatus != 0
        or stock_entry.stock_entry_type != "Material Transfer"
        or stock_entry.company != company
        or frappe.utils.getdate(stock_entry.posting_date)
        > frappe.utils.getdate(frappe.utils.today())
        or not stock_entry.items
    ):
        return False
    if any(
        row.s_warehouse != FINISHED_GOODS_WAREHOUSE
        or row.t_warehouse != prep_wh
        for row in stock_entry.items
    ):
        return False
    return any(row.item_code in needed_item_codes for row in stock_entry.items)


def _source_valuation_reconciliation_rows(
    item_code,
    source_wh,
    current_qty,
    valuation_rate,
    has_batch,
    has_serial,
):
    rows = []
    if has_serial:
        serial_nos = frappe.db.sql_list(
            """
            SELECT name FROM `tabSerial No`
            WHERE item_code = %s AND warehouse = %s
            """,
            (item_code, source_wh),
        )
        if len(serial_nos) == current_qty:
            rows.append({
                "item_code": item_code,
                "warehouse": source_wh,
                "qty": current_qty,
                "valuation_rate": valuation_rate,
                "serial_no": "\n".join(serial_nos),
            })
    elif has_batch:
        batches = frappe.get_all(
            "Batch", filters={"item": item_code}, fields=["name"]
        )
        for batch in batches:
            batch_qty = float(
                get_batch_qty(batch.name, source_wh, item_code) or 0
            )
            if batch_qty > 0:
                rows.append({
                    "item_code": item_code,
                    "warehouse": source_wh,
                    "qty": batch_qty,
                    "valuation_rate": valuation_rate,
                    "batch_no": batch.name,
                })
    else:
        rows.append({
            "item_code": item_code,
            "warehouse": source_wh,
            "qty": current_qty,
            "valuation_rate": valuation_rate,
        })
    return rows


def _valid_source_transfer_rows(
    item_code,
    source_wh,
    inbound_wh,
    requested_qty,
    valuation_rate,
    has_batch,
    has_serial,
):
    rows = []
    if requested_qty <= 0:
        return rows, 0
    if has_serial:
        whole_requested_qty = int(requested_qty)
        serial_nos = frappe.db.sql_list(
            """
            SELECT name FROM `tabSerial No`
            WHERE item_code = %s AND warehouse = %s
            LIMIT %s
            """,
            (item_code, source_wh, whole_requested_qty),
        )
        represented_qty = min(len(serial_nos), whole_requested_qty)
        if represented_qty > 0:
            rows.append({
                "item_code": item_code,
                "s_warehouse": source_wh,
                "t_warehouse": inbound_wh,
                "qty": represented_qty,
                "basic_rate": valuation_rate,
                "serial_no": "\n".join(serial_nos[:represented_qty]),
            })
        return rows, represented_qty
    if has_batch:
        remaining = requested_qty
        batches = frappe.get_all(
            "Batch",
            filters={"item": item_code},
            fields=["name"],
            order_by="creation asc",
        )
        for batch in batches:
            if remaining <= 0:
                break
            batch_qty = float(
                get_batch_qty(batch.name, source_wh, item_code) or 0
            )
            if batch_qty > 0:
                move_qty = min(batch_qty, remaining)
                rows.append({
                    "item_code": item_code,
                    "s_warehouse": source_wh,
                    "t_warehouse": inbound_wh,
                    "qty": move_qty,
                    "basic_rate": valuation_rate,
                    "batch_no": batch.name,
                })
                remaining -= move_qty
        return rows, requested_qty - remaining
    rows.append({
        "item_code": item_code,
        "s_warehouse": source_wh,
        "t_warehouse": inbound_wh,
        "qty": requested_qty,
        "basic_rate": valuation_rate,
    })
    return rows, requested_qty


def process_inbound_inventory(asin_inbound, settings, diagnostics=None):
    prep_wh = settings.custom_amazon_fba_staging_area
    inbound_wh = settings.custom_amazon_inbound_warehouse
    company = settings.company
    adjustment_account = settings.custom_amazon_inventory_adjustment_account
    diagnostics = diagnostics or _new_inbound_flow_diagnostics(
        asin_inbound, settings
    )
    existing_draft_purpose = (
        "Complete an existing Finished Goods-to-Prep transfer needed by the "
        "inbound workflow"
    )
    aggregated_entry_purpose = (
        "Move available Prep and/or Finished Goods stock into Amazon FBA "
        "Inbound to satisfy protected inbound targets"
    )
    valuation_reconciliation_purpose = (
        "Align source-warehouse Bin valuation with the Item valuation before "
        "transfer; no quantity change"
    )
    inbound_reconciliation_purpose = (
        "Set ERP Amazon FBA Inbound quantities to the remaining protected "
        "inbound targets after transfers"
    )

    if DEBUG: print(f"[DEBUG] Starting inbound inventory processing for warehouse: {inbound_wh}")

    def mark_item_skipped(state, reason, document_name=None):
        if state.get("skipped"):
            return
        state["skipped"] = True
        state["failure_reason"] = reason
        state["planned_rows"] = []
        diagnostics["skipped_item_count"] += 1
        _record_inventory_trigger(
            diagnostics,
            "25",
            "A required ERP warehouse quantity could not be read or verified",
            state.get("item_code"),
        )
        _record_inventory_trigger(
            diagnostics,
            "26",
            "An item was deliberately skipped to prevent double counting because quantities could not be verified",
            state.get("item_code"),
        )
        _record_inbound_flow_event(
            diagnostics,
            "Item deliberately skipped to prevent double counting",
            state,
            reason,
            document_name,
        )

    def read_three_quantities(state):
        return (
            _read_bin_actual_qty(state["item_code"], inbound_wh),
            _read_bin_actual_qty(state["item_code"], prep_wh),
            _read_bin_actual_qty(
                state["item_code"], FINISHED_GOODS_WAREHOUSE
            ),
        )

    item_states = {}
    initially_needed_beyond_prep = set()

    # Establish initial shortages only to decide whether existing drafts are relevant.
    for asin, target_qty in asin_inbound.items():
        target_qty = float(target_qty or 0)
        if DEBUG: print(f"[DEBUG] Processing inbound ASIN: {asin} with target_qty: {target_qty}")
        item_code = frappe.db.get_value("Item", {"custom_asin": asin, "disabled": 0}, "name")
        if not item_code:
            if DEBUG: print(f"[DEBUG] No matching item_code found for ASIN: {asin}")
            continue

        # ADDED: Skip if not a stock item
        if not frappe.get_value("Item", item_code, "is_stock_item"):
            if DEBUG: print(f"[DEBUG] Skipping non-stock item: {item_code}")
            continue
        diagnostics["valid_target_count"] += 1
        state = {
            "asin": asin,
            "item_code": item_code,
            "target_inbound_qty": target_qty,
            "eligible_draft_names": [],
            "draft_outcome": None,
            "draft_expected_qty": 0,
            "requested_prep_qty": 0,
            "valid_prep_row_qty": 0,
            "submitted_prep_qty": 0,
            "requested_finished_qty": 0,
            "valid_finished_row_qty": 0,
            "submitted_finished_qty": 0,
            "aggregated_entry_name": None,
            "aggregated_outcome": None,
            "final_inbound_qty": None,
            "reconciliation_qty": 0,
            "skipped": False,
            "planned_rows": [],
            "has_batch": frappe.get_value("Item", item_code, "has_batch_no"),
            "has_serial": frappe.get_value("Item", item_code, "has_serial_no"),
        }
        diagnostics["items"][item_code] = state
        item_states[item_code] = state
        try:
            current_inbound, current_prep, current_finished = read_three_quantities(
                state
            )
        except Exception as exc:
            reason = "Initial warehouse quantity read failed: " + _concise_inventory_exception(exc)
            mark_item_skipped(state, reason)
            continue
        state.update({
            "initial_inbound_qty": current_inbound,
            "initial_prep_qty": current_prep,
            "initial_finished_goods_qty": current_finished,
        })
        diff = max(target_qty - current_inbound, 0)
        state["initial_positive_shortage"] = diff
        if diff <= 0:
            continue
        diagnostics["positive_shortage_count"] += 1
        if diff > max(current_prep, 0):
            diagnostics["active"] = True
            diagnostics["shortages_exceeding_prep_count"] += 1
            diagnostics["initial_short_after_prep_qty"] += (
                diff - max(current_prep, 0)
            )
            initially_needed_beyond_prep.add(item_code)
            _record_inbound_flow_event(
                diagnostics,
                "Positive Inbound shortage initially exceeded available Prep",
                state,
                "Available Prep could not completely cover the current shortage",
                quantity=diff - max(current_prep, 0),
            )

    # Existing Finished Goods-to-Prep drafts are considered only when useful.
    eligible_drafts = []
    if initially_needed_beyond_prep:
        try:
            draft_rows = frappe.get_all(
                "Stock Entry",
                filters={
                    "docstatus": 0,
                    "stock_entry_type": "Material Transfer",
                    "company": company,
                    "posting_date": ["<=", frappe.utils.today()],
                },
                fields=["name", "posting_date", "posting_time", "creation"],
                order_by="posting_date asc, posting_time asc, creation asc, name asc",
            )
            diagnostics["drafts_inspected"] = len(draft_rows)
        except Exception as exc:
            draft_rows = []
            _record_inventory_trigger(
                diagnostics,
                "27",
                "Searching for eligible draft Stock Entries failed",
            )
            _record_inbound_flow_global_error(
                diagnostics,
                "Draft Stock Entry search failed: " + _concise_inventory_exception(exc),
            )

        for draft_row in draft_rows:
            try:
                draft = frappe.get_doc("Stock Entry", draft_row.name)
                if not _eligible_finished_to_prep_draft(
                    draft,
                    initially_needed_beyond_prep,
                    company,
                    prep_wh,
                ):
                    continue
                quantities = _stock_entry_item_quantities(draft)
                relevant_codes = sorted(
                    set(quantities) & initially_needed_beyond_prep
                )
                eligible_drafts.append((draft.name, relevant_codes))
                diagnostics["drafts_eligible"] += 1
                _upsert_inventory_document(
                    diagnostics,
                    "Stock Entry",
                    draft.name,
                    existing_draft_purpose,
                    "pre-existing draft acted upon by this run",
                    "selected; pending immediate revalidation",
                    source_warehouses=sorted({
                        row.s_warehouse for row in draft.items if row.s_warehouse
                    }),
                    target_warehouse=prep_wh,
                    movement_quantity=sum(quantities.values()),
                    affected_item_count=len(quantities),
                )
                for item_code in relevant_codes:
                    state = item_states[item_code]
                    state["eligible_draft_names"].append(draft.name)
                    _record_inbound_flow_event(
                        diagnostics,
                        "Eligible Finished Goods to Prep draft discovered",
                        state,
                        "Draft met all initial eligibility conditions",
                        draft.name,
                        quantities.get(item_code, 0),
                    )
            except Exception as exc:
                _record_inventory_trigger(
                    diagnostics,
                    "28",
                    "Inspecting or parsing a candidate draft Stock Entry failed",
                    draft_row.name,
                )
                _record_inbound_flow_global_error(
                    diagnostics,
                    f"Draft {draft_row.name} could not be inspected: "
                    + _concise_inventory_exception(exc),
                )

    submitted_draft_baselines = {}
    submitted_draft_qty_by_item = defaultdict(float)
    submitted_draft_names_by_item = defaultdict(list)
    for draft_name, initially_relevant_codes in eligible_drafts:
        try:
            draft = frappe.get_doc("Stock Entry", draft_name)
            draft.reload()
        except Exception as exc:
            diagnostics["drafts_revalidation_ineligible"] += 1
            reason = "Immediate draft reload failed: " + _concise_inventory_exception(exc)
            _record_inventory_trigger(
                diagnostics,
                "31",
                "An eligible draft became ineligible during pre-submit revalidation",
                draft_name,
            )
            _upsert_inventory_document(
                diagnostics,
                "Stock Entry",
                draft_name,
                existing_draft_purpose,
                "pre-existing draft acted upon by this run",
                "revalidation failed; not submitted",
            )
            for item_code in initially_relevant_codes:
                _record_inbound_flow_event(
                    diagnostics,
                    "Eligible draft became ineligible during revalidation",
                    item_states.get(item_code),
                    reason,
                    draft_name,
                )
            continue

        currently_needed = set()
        pre_submit_quantities = {}
        for item_code, state in item_states.items():
            if state.get("skipped"):
                continue
            try:
                current_inbound, current_prep, current_finished = (
                    read_three_quantities(state)
                )
            except Exception as exc:
                mark_item_skipped(
                    state,
                    "Immediate pre-submit Bin reread failed: "
                    + _concise_inventory_exception(exc),
                    draft_name,
                )
                continue
            pre_submit_quantities[item_code] = (
                current_inbound,
                current_prep,
                current_finished,
            )
            if state["target_inbound_qty"] - current_inbound > max(current_prep, 0):
                currently_needed.add(item_code)

        if not _eligible_finished_to_prep_draft(
            draft, currently_needed, company, prep_wh
        ):
            diagnostics["drafts_revalidation_ineligible"] += 1
            _record_inventory_trigger(
                diagnostics,
                "31",
                "An eligible draft became ineligible during pre-submit revalidation",
                draft_name,
            )
            _upsert_inventory_document(
                diagnostics,
                "Stock Entry",
                draft_name,
                existing_draft_purpose,
                "pre-existing draft acted upon by this run",
                "became ineligible during revalidation; not submitted",
            )
            for item_code in initially_relevant_codes:
                state = item_states.get(item_code)
                _record_inbound_flow_event(
                    diagnostics,
                    "Eligible draft became ineligible during revalidation",
                    state,
                    "Draft was no longer a valid, currently useful Finished Goods to Prep draft",
                    draft_name,
                )
            continue

        draft_quantities = _stock_entry_item_quantities(draft)
        revalidated_codes = sorted(set(draft_quantities) & currently_needed)
        for item_code in revalidated_codes:
            state = item_states[item_code]
            state["draft_outcome"] = "revalidated and eligible"
            _record_inbound_flow_event(
                diagnostics,
                "Eligible draft revalidated",
                state,
                "Draft remained eligible immediately before submission",
                draft_name,
                draft_quantities.get(item_code, 0),
            )

        if DEBUG:
            diagnostics["drafts_debug_would_submit"] += 1
            _upsert_inventory_document(
                diagnostics,
                "Stock Entry",
                draft_name,
                existing_draft_purpose,
                "pre-existing draft acted upon by this run",
                "DEBUG: remained draft; submission not attempted",
            )
            for item_code in revalidated_codes:
                state = item_states[item_code]
                state["draft_outcome"] = "would submit in non-DEBUG mode"
                _record_inbound_flow_event(
                    diagnostics,
                    "DEBUG-only planned draft submission",
                    state,
                    "Would submit in non-DEBUG mode; no quantity was moved",
                    draft_name,
                    draft_quantities.get(item_code, 0),
                )
            continue

        diagnostics["drafts_attempted"] += 1
        _upsert_inventory_document(
            diagnostics,
            "Stock Entry",
            draft_name,
            existing_draft_purpose,
            "pre-existing draft acted upon by this run",
            "submission attempted",
        )
        for item_code in revalidated_codes:
            _record_inbound_flow_event(
                diagnostics,
                "Draft submission attempted",
                item_states[item_code],
                "Submission attempted after immediate revalidation",
                draft_name,
                draft_quantities.get(item_code, 0),
            )
        savepoint_name = "amazon_finished_to_prep_" + re.sub(
            r"[^A-Za-z0-9_]", "_", draft_name
        )
        committed = False
        try:
            frappe.db.savepoint(savepoint_name)
            draft.submit()
            frappe.db.commit()
            committed = True
            for item_code, quantities in pre_submit_quantities.items():
                if item_code not in draft_quantities:
                    continue
                submitted_draft_baselines.setdefault(item_code, quantities)
            draft.reload()
            if draft.docstatus != 1:
                raise RuntimeError(
                    f"committed Stock Entry reloaded with docstatus={draft.docstatus}"
                )
            diagnostics["drafts_submitted_verified"] += 1
            diagnostics["submitted_finished_to_prep_qty"] += sum(
                draft_quantities.values()
            )
            _upsert_inventory_document(
                diagnostics,
                "Stock Entry",
                draft_name,
                existing_draft_purpose,
                "pre-existing draft acted upon by this run",
                "submitted, committed, and docstatus verified",
            )
            for item_code, moved_qty in draft_quantities.items():
                if item_code not in item_states:
                    continue
                submitted_draft_qty_by_item[item_code] += moved_qty
                submitted_draft_names_by_item[item_code].append(draft_name)
                state = item_states[item_code]
                state["draft_expected_qty"] += moved_qty
                state["draft_outcome"] = "submitted, committed, and docstatus verified"
                _record_inbound_flow_event(
                    diagnostics,
                    "Eligible draft submitted and verified",
                    state,
                    "Submission committed and reloaded with docstatus 1",
                    draft_name,
                    moved_qty,
                )
        except Exception as exc:
            reason = _concise_inventory_exception(exc)
            if committed:
                diagnostics["drafts_committed_unverified"] += 1
                outcome = "Submitted draft could not be verified"
                reason = "Committed state could not be safely verified: " + reason
                _record_inventory_trigger(
                    diagnostics,
                    "33",
                    "A committed draft's submitted state could not be verified",
                    draft_name,
                )
                _upsert_inventory_document(
                    diagnostics,
                    "Stock Entry",
                    draft_name,
                    existing_draft_purpose,
                    "pre-existing draft acted upon by this run",
                    "submission state unverified after commit",
                )
                for item_code in set(draft_quantities) & set(item_states):
                    state = item_states[item_code]
                    state["draft_outcome"] = "committed state could not be verified"
                    mark_item_skipped(state, reason, draft_name)
                    _record_inbound_flow_event(
                        diagnostics,
                        outcome,
                        state,
                        reason,
                        draft_name,
                        draft_quantities.get(item_code, 0),
                    )
            else:
                diagnostics["drafts_failed_submission"] += 1
                _record_inventory_trigger(
                    diagnostics,
                    "32",
                    "A draft Stock Entry submission failed",
                    draft_name,
                )
                _upsert_inventory_document(
                    diagnostics,
                    "Stock Entry",
                    draft_name,
                    existing_draft_purpose,
                    "pre-existing draft acted upon by this run",
                    "submission failed",
                )
                try:
                    frappe.db.rollback(save_point=savepoint_name)
                except Exception as rollback_exc:
                    _record_inbound_flow_global_error(
                        diagnostics,
                        f"Draft {draft_name} savepoint rollback failed: "
                        + _concise_inventory_exception(rollback_exc),
                    )
                for item_code in revalidated_codes:
                    state = item_states[item_code]
                    state["draft_outcome"] = "submission failed"
                    _record_inbound_flow_event(
                        diagnostics,
                        "Eligible draft failed submission",
                        state,
                        reason,
                        draft_name,
                        draft_quantities.get(item_code, 0),
                    )

    # Always allocate from fresh Bins after draft processing. Submitted-draft
    # deltas are verified before either source can be used.
    for item_code, state in item_states.items():
        if state.get("skipped"):
            continue
        try:
            current_inbound, current_prep, current_finished = read_three_quantities(
                state
            )
        except Exception as exc:
            mark_item_skipped(
                state,
                "Fresh post-draft Bin reread failed: "
                + _concise_inventory_exception(exc),
                (submitted_draft_names_by_item.get(item_code) or [None])[-1],
            )
            continue
        state.update({
            "fresh_inbound_qty": current_inbound,
            "fresh_prep_qty": current_prep,
            "fresh_finished_goods_qty": current_finished,
        })
        moved_qty = submitted_draft_qty_by_item.get(item_code, 0)
        if moved_qty:
            baseline = submitted_draft_baselines.get(item_code)
            delta_verified = bool(
                baseline
                and current_prep >= baseline[1] + moved_qty
                and current_finished <= baseline[2] - moved_qty
            )
            if not delta_verified:
                reason = (
                    "Fresh post-draft quantities did not safely reflect the "
                    f"submitted movement of {moved_qty}"
                )
                _record_inventory_trigger(
                    diagnostics,
                    "34",
                    "A draft's expected warehouse quantity movement could not be verified",
                    (submitted_draft_names_by_item.get(item_code) or [item_code])[-1],
                )
                mark_item_skipped(
                    state,
                    reason,
                    (submitted_draft_names_by_item.get(item_code) or [None])[-1],
                )
                _record_inbound_flow_event(
                    diagnostics,
                    "Fresh Bin reread or submitted-draft delta verification failed",
                    state,
                    reason,
                    (submitted_draft_names_by_item.get(item_code) or [None])[-1],
                    moved_qty,
                )

    def add_source_transfer_rows(source_wh, request_key, valid_key, states):
        correction_rows = []
        correction_states = []
        ready_states = []
        failed_corrections = set()

        for state in states:
            requested_qty = state.get(request_key, 0)
            if state.get("skipped") or requested_qty <= 0:
                continue
            item_code = state["item_code"]
            try:
                current_qty = _read_bin_actual_qty(item_code, source_wh)
                bin_data = frappe.db.get_value(
                    "Bin",
                    {"item_code": item_code, "warehouse": source_wh},
                    ["valuation_rate"],
                    as_dict=True,
                ) or {}
                bin_rate = bin_data.get("valuation_rate", 0)
                item_valuation_rate = (
                    frappe.get_value("Item", item_code, "valuation_rate") or 0
                )
                valuation_rate = (
                    item_valuation_rate if item_valuation_rate > 0 else 0.01
                )
            except Exception as exc:
                mark_item_skipped(
                    state,
                    f"Fresh {source_wh} quantity/rate read failed: "
                    + _concise_inventory_exception(exc),
                )
                continue

            state["transfer_valuation_rate"] = valuation_rate
            if bin_rate != valuation_rate:
                diagnostics["valuation_attempted"] += 1
                try:
                    item_rows = _source_valuation_reconciliation_rows(
                        item_code,
                        source_wh,
                        current_qty,
                        valuation_rate,
                        state["has_batch"],
                        state["has_serial"],
                    )
                except Exception as exc:
                    item_rows = []
                    reason = "Valuation correction rows failed: " + _concise_inventory_exception(exc)
                else:
                    reason = "No valid batch or serial valuation rows could be constructed"
                if not item_rows:
                    diagnostics["valuation_failed"] += 1
                    failed_corrections.add(item_code)
                    _record_inventory_trigger(
                        diagnostics,
                        "36",
                        "A required source valuation correction failed",
                        item_code,
                    )
                    _record_inbound_flow_event(
                        diagnostics,
                        "Source valuation correction failed",
                        state,
                        reason,
                        quantity=requested_qty,
                    )
                    continue
                correction_rows.extend(item_rows)
                correction_states.append(state)
            ready_states.append(state)

        if correction_rows:
            source_label = (
                "Prep" if source_wh == prep_wh else "Finished Goods"
            )
            source_sr = None
            source_sr_inserted = False
            zero_quantity_metrics = {
                "changed_row_count": 0,
                "total_increase_qty": 0,
                "total_decrease_qty": 0,
                "net_quantity_delta": 0,
                "quantity_changed": False,
            }
            try:
                source_sr = frappe.get_doc({
                    "doctype": "Stock Reconciliation",
                    "company": company,
                    "posting_date": frappe.utils.today(),
                    "purpose": "Stock Reconciliation",
                    "expense_account": adjustment_account,
                    "items": correction_rows,
                })
                source_sr.insert(ignore_permissions=True)
                source_sr_inserted = True
                _upsert_inventory_document(
                    diagnostics,
                    "Stock Reconciliation",
                    source_sr.name,
                    valuation_reconciliation_purpose,
                    "newly created by this run",
                    "inserted as draft",
                    source_warehouses=[source_wh],
                    target_warehouse=source_wh,
                    movement_quantity=0,
                    affected_item_count=len({
                        state["item_code"] for state in correction_states
                    }),
                    reconciliation_metrics=zero_quantity_metrics,
                )
                if DEBUG:
                    frappe.db.commit()
                    diagnostics["valuation_debug_drafts"] += len(correction_states)
                    _upsert_inventory_document(
                        diagnostics,
                        "Stock Reconciliation",
                        source_sr.name,
                        valuation_reconciliation_purpose,
                        "newly created by this run",
                        "DEBUG draft; not submitted",
                    )
                    for state in correction_states:
                        _record_inbound_flow_event(
                            diagnostics,
                            "DEBUG-only valuation correction draft",
                            state,
                            "Valuation correction was inserted as a draft and not submitted",
                            source_sr.name,
                        )
                else:
                    source_sr.submit()
                    frappe.db.commit()
                    diagnostics["valuation_successful"] += len(correction_states)
                    _upsert_inventory_document(
                        diagnostics,
                        "Stock Reconciliation",
                        source_sr.name,
                        valuation_reconciliation_purpose,
                        "newly created by this run",
                        "submitted",
                    )
                    for state in correction_states:
                        _record_inbound_flow_event(
                            diagnostics,
                            "Source valuation correction submitted",
                            state,
                            "Valuation correction was submitted before transfer allocation",
                            source_sr.name,
                        )
            except Exception as exc:
                diagnostics["valuation_failed"] += len(correction_states)
                reason = "Source valuation correction failed: " + _concise_inventory_exception(exc)
                source_sr_name = getattr(source_sr, "name", None)
                _record_inventory_trigger(
                    diagnostics,
                    "36",
                    "A required source valuation correction failed",
                    source_sr_name or source_label,
                )
                _upsert_inventory_document(
                    diagnostics,
                    "Stock Reconciliation",
                    source_sr_name,
                    valuation_reconciliation_purpose,
                    "newly created by this run",
                    "submission failed" if source_sr_inserted else "insertion failed",
                    source_warehouses=[source_wh],
                    target_warehouse=source_wh,
                    movement_quantity=0,
                    affected_item_count=len({
                        state["item_code"] for state in correction_states
                    }),
                    reconciliation_metrics=zero_quantity_metrics,
                )
                for state in correction_states:
                    failed_corrections.add(state["item_code"])
                    _record_inbound_flow_event(
                        diagnostics,
                        "Source valuation correction failed",
                        state,
                        reason,
                        getattr(locals().get("source_sr"), "name", None),
                        state.get(request_key, 0),
                    )
                frappe.log_error(
                    frappe.get_traceback(),
                    f"{source_label} Stock Reconciliation Error",
                )
                raise

        for state in ready_states:
            item_code = state["item_code"]
            if state.get("skipped") or item_code in failed_corrections:
                continue
            try:
                current_qty = _read_bin_actual_qty(item_code, source_wh)
            except Exception as exc:
                mark_item_skipped(
                    state,
                    f"Fresh {source_wh} quantity reread after valuation handling failed: "
                    + _concise_inventory_exception(exc),
                )
                continue
            requested_qty = min(max(current_qty, 0), state.get(request_key, 0))
            try:
                rows, represented_qty = _valid_source_transfer_rows(
                    item_code,
                    source_wh,
                    inbound_wh,
                    requested_qty,
                    state["transfer_valuation_rate"],
                    state["has_batch"],
                    state["has_serial"],
                )
            except Exception as exc:
                rows, represented_qty = [], 0
                _record_inventory_trigger(
                    diagnostics,
                    "37",
                    "Building a required transfer row failed",
                    item_code,
                )
                _record_inbound_flow_event(
                    diagnostics,
                    "Transfer row construction failed",
                    state,
                    _concise_inventory_exception(exc),
                    quantity=requested_qty,
                )
            state[valid_key] = represented_qty
            state["planned_rows"].extend(rows)
            if represented_qty < requested_qty:
                _record_inventory_trigger(
                    diagnostics,
                    "38",
                    "Batch or serial restrictions reduced transferable quantity below the requested quantity",
                    item_code,
                )
                _record_inbound_flow_event(
                    diagnostics,
                    "Batch or serial restrictions reduced transferable quantity",
                    state,
                    f"Requested {requested_qty}; valid rows represented {represented_qty}",
                    quantity=requested_qty - represented_qty,
                )

    allocatable_states = []
    for state in item_states.values():
        if state.get("skipped"):
            continue
        diff = state["target_inbound_qty"] - state.get("fresh_inbound_qty", 0)
        if diff <= 0:
            continue
        state["post_draft_shortage"] = diff
        state["requested_prep_qty"] = min(
            max(state.get("fresh_prep_qty", 0), 0), diff
        )
        allocatable_states.append(state)

    add_source_transfer_rows(
        prep_wh,
        "requested_prep_qty",
        "valid_prep_row_qty",
        allocatable_states,
    )

    for state in allocatable_states:
        if state.get("skipped"):
            continue
        remaining_diff = (
            state["post_draft_shortage"] - state["valid_prep_row_qty"]
        )
        if state["valid_prep_row_qty"] and remaining_diff > 0:
            _record_inbound_flow_event(
                diagnostics,
                "Prep supplied only part of the shortage",
                state,
                "Valid Prep rows did not cover the full post-draft shortage",
                quantity=remaining_diff,
            )
        state["requested_finished_qty"] = min(
            max(state.get("fresh_finished_goods_qty", 0), 0),
            max(remaining_diff, 0),
        )
        if state["requested_finished_qty"] > 0:
            _record_inventory_trigger(
                diagnostics,
                "35",
                "A direct Finished Goods-to-Inbound transfer was requested or attempted",
                state["item_code"],
            )

    add_source_transfer_rows(
        FINISHED_GOODS_WAREHOUSE,
        "requested_finished_qty",
        "valid_finished_row_qty",
        allocatable_states,
    )

    transfer_items = []
    for state in allocatable_states:
        if state.get("skipped"):
            continue
        transfer_items.extend(state["planned_rows"])
        if state["valid_finished_row_qty"] > 0:
            _record_inbound_flow_event(
                diagnostics,
                "Finished Goods supplied remaining Inbound shortage",
                state,
                "Finished Goods rows were added after valid Prep-row quantity was applied",
                quantity=state["valid_finished_row_qty"],
            )

    prep_rows = [
        row for row in transfer_items if row["s_warehouse"] == prep_wh
    ]
    finished_rows = [
        row
        for row in transfer_items
        if row["s_warehouse"] == FINISHED_GOODS_WAREHOUSE
    ]
    diagnostics["prep_valid_row_count"] = len(prep_rows)
    diagnostics["prep_valid_row_qty"] = sum(row["qty"] for row in prep_rows)
    diagnostics["finished_valid_row_count"] = len(finished_rows)
    diagnostics["finished_valid_row_qty"] = sum(
        row["qty"] for row in finished_rows
    )

    # Submit the same existing-style run-level aggregate, now with explicit
    # child-row warehouses for one or both sources.
    if transfer_items:
        diagnostics["active"] = True
        sources = sorted({row["s_warehouse"] for row in transfer_items})
        diagnostics["aggregated_sources"] = sources
        diagnostics["aggregated_item_count"] = len({
            row["item_code"] for row in transfer_items
        })
        diagnostics["aggregated_child_row_count"] = len(transfer_items)
        if DEBUG: print(f"[DEBUG] Creating Stock Entry with {len(transfer_items)} items...")
        se = None
        inserted = False
        try:
            stock_entry_values = {
                "doctype": "Stock Entry",
                "company": company,
                "stock_entry_type": "Material Transfer",
                "to_warehouse": inbound_wh,
                "posting_date": frappe.utils.today(),
                "items": transfer_items,
            }
            if len(sources) == 1:
                stock_entry_values["from_warehouse"] = sources[0]
            se = frappe.get_doc(stock_entry_values)
            se.insert(ignore_permissions=True)
            inserted = True
            diagnostics["aggregated_entry_name"] = se.name
            diagnostics["aggregated_entry_status"] = "inserted as draft"
            _upsert_inventory_document(
                diagnostics,
                "Stock Entry",
                se.name,
                aggregated_entry_purpose,
                "newly created by this run",
                "inserted as draft",
                source_warehouses=sources,
                target_warehouse=inbound_wh,
                movement_quantity=sum(row["qty"] for row in transfer_items),
                affected_item_count=diagnostics["aggregated_item_count"],
            )
            for state in allocatable_states:
                if not state.get("planned_rows") or state.get("skipped"):
                    continue
                state["aggregated_entry_name"] = se.name
                state["aggregated_outcome"] = "inserted as draft"
                _record_inbound_flow_event(
                    diagnostics,
                    "Aggregated Stock Entry inserted as draft",
                    state,
                    "Valid source rows were included in the run-level document",
                    se.name,
                    state["valid_prep_row_qty"]
                    + state["valid_finished_row_qty"],
                )
            if DEBUG:
                frappe.db.commit()
                diagnostics["aggregated_entry_status"] = "DEBUG draft; not submitted"
                _upsert_inventory_document(
                    diagnostics,
                    "Stock Entry",
                    se.name,
                    aggregated_entry_purpose,
                    "newly created by this run",
                    "DEBUG draft; not submitted",
                )
                for state in allocatable_states:
                    if not state.get("planned_rows") or state.get("skipped"):
                        continue
                    state["aggregated_outcome"] = "DEBUG draft; not submitted"
                    _record_inbound_flow_event(
                        diagnostics,
                        "DEBUG-only aggregated transfer draft",
                        state,
                        "Transfer rows were planned but no quantity was submitted",
                        se.name,
                        state["valid_prep_row_qty"]
                        + state["valid_finished_row_qty"],
                    )
            else:
                se.submit()
                frappe.db.commit()
                diagnostics["aggregated_entry_status"] = "submitted"
                diagnostics["prep_submitted_qty"] = diagnostics["prep_valid_row_qty"]
                diagnostics["finished_submitted_qty"] = diagnostics["finished_valid_row_qty"]
                _upsert_inventory_document(
                    diagnostics,
                    "Stock Entry",
                    se.name,
                    aggregated_entry_purpose,
                    "newly created by this run",
                    "submitted",
                )
                for state in allocatable_states:
                    if not state.get("planned_rows") or state.get("skipped"):
                        continue
                    state["submitted_prep_qty"] = state["valid_prep_row_qty"]
                    state["submitted_finished_qty"] = state["valid_finished_row_qty"]
                    state["aggregated_outcome"] = "submitted"
                    _record_inbound_flow_event(
                        diagnostics,
                        "Aggregated Stock Entry submitted",
                        state,
                        "Run-level transfer submitted successfully",
                        se.name,
                        state["submitted_prep_qty"]
                        + state["submitted_finished_qty"],
                    )
        except Exception as exc:
            submit_traceback = frappe.get_traceback()
            failure_reason = _concise_inventory_exception(exc)
            diagnostics["aggregated_entry_status"] = (
                "submission failed" if inserted else "insertion failed"
            )
            if se and getattr(se, "name", None):
                diagnostics["aggregated_entry_name"] = se.name
            _record_inventory_trigger(
                diagnostics,
                "39",
                "Inserting or submitting the aggregated Stock Entry failed",
                getattr(se, "name", None) or "<not assigned>",
            )
            _upsert_inventory_document(
                diagnostics,
                "Stock Entry",
                getattr(se, "name", None),
                aggregated_entry_purpose,
                "newly created by this run",
                diagnostics["aggregated_entry_status"],
                source_warehouses=sources,
                target_warehouse=inbound_wh,
                movement_quantity=sum(row["qty"] for row in transfer_items),
                affected_item_count=diagnostics["aggregated_item_count"],
            )
            for state in allocatable_states:
                if not state.get("planned_rows") or state.get("skipped"):
                    continue
                state["aggregated_entry_name"] = diagnostics["aggregated_entry_name"]
                state["aggregated_outcome"] = diagnostics["aggregated_entry_status"]
                state["submitted_prep_qty"] = 0
                state["submitted_finished_qty"] = 0
                _record_inbound_flow_event(
                    diagnostics,
                    "Aggregated Stock Entry submission failed",
                    state,
                    failure_reason,
                    getattr(se, "name", None),
                    state["valid_prep_row_qty"]
                    + state["valid_finished_row_qty"],
                )
            cleanup_succeeded = False
            if inserted:
                try:
                    se.reload()
                    if se.docstatus == 0:
                        se.delete()
                    elif se.docstatus == 1:
                        se.cancel()
                        se.delete()
                    frappe.db.commit()
                    cleanup_succeeded = True
                    diagnostics["aggregated_entry_status"] += "; cleaned up"
                except Exception as cleanup_exc:
                    _record_inventory_trigger(
                        diagnostics,
                        "40",
                        "Cleanup or cancellation of a failed aggregated Stock Entry also failed",
                        getattr(se, "name", None) or "<not assigned>",
                    )
                    _upsert_inventory_document(
                        diagnostics,
                        "Stock Entry",
                        getattr(se, "name", None),
                        aggregated_entry_purpose,
                        "newly created by this run",
                        "submission failed; cleanup failed",
                    )
                    _record_inbound_flow_global_error(
                        diagnostics,
                        "Aggregated Stock Entry cleanup failed: "
                        + _concise_inventory_exception(cleanup_exc),
                    )
                    frappe.log_error(
                        frappe.get_traceback(), "Stock Entry Cleanup Error"
                    )
            if cleanup_succeeded:
                _upsert_inventory_document(
                    diagnostics,
                    "Stock Entry",
                    getattr(se, "name", None),
                    aggregated_entry_purpose,
                    "newly created by this run",
                    "submission failed; cancelled/deleted during cleanup",
                )
                for state in allocatable_states:
                    if not state.get("planned_rows") or state.get("skipped"):
                        continue
                    state["aggregated_outcome"] = diagnostics["aggregated_entry_status"]
                    _record_inbound_flow_event(
                        diagnostics,
                        "Aggregated Stock Entry cleaned up after failure",
                        state,
                        "Failed run-level transfer document was cleaned up",
                        getattr(se, "name", None),
                    )
            if isinstance(exc, NegativeStockError):
                frappe.log_error(
                    submit_traceback, "Stock Entry NegativeStockError"
                )
            else:
                frappe.log_error(submit_traceback, "Stock Entry Submit Error")

    # Second pass: collect reconciliations where qty doesn't match
    reconcile_items = []
    inbound_reconciliation_current_quantities = {}
    for state in item_states.values():
        if state.get("skipped"):
            continue
        item_code = state["item_code"]
        target_qty = state["target_inbound_qty"]
        try:
            current_inbound = _read_bin_actual_qty(item_code, inbound_wh)
        except Exception as exc:
            mark_item_skipped(
                state,
                "Final fresh Inbound quantity read failed: "
                + _concise_inventory_exception(exc),
                diagnostics.get("aggregated_entry_name"),
            )
            continue
        state["final_inbound_qty"] = current_inbound
        if current_inbound == target_qty:
            if DEBUG: print(f"[DEBUG] Inbound qty matches for {item_code}: {current_inbound} == {target_qty}")
            continue

        if DEBUG: print(f"[DEBUG] Inbound qty mismatch for {item_code}: {current_inbound} != {target_qty}")
        item_valuation_rate = frappe.get_value("Item", item_code, "valuation_rate") or 0
        item_dict = {
            "item_code": item_code,
            "warehouse": inbound_wh,
            "qty": target_qty,
        }
        if item_valuation_rate > 0:
            item_dict["valuation_rate"] = item_valuation_rate
        else:
            item_dict["valuation_rate"] = 0.01
        reconcile_items.append(item_dict)
        inbound_reconciliation_current_quantities[(item_code, inbound_wh)] = (
            current_inbound
        )
        remaining_shortage = max(target_qty - current_inbound, 0)
        if remaining_shortage > 0:
            state["reconciliation_qty"] = remaining_shortage
            diagnostics["reconciliation_shortage_count"] += 1
            diagnostics["reconciliation_shortage_qty"] += remaining_shortage
            _record_inbound_flow_event(
                diagnostics,
                "Remaining shortage handed to Inbound Stock Reconciliation",
                state,
                "Fresh Inbound quantity remained below the protected target",
                diagnostics.get("aggregated_entry_name"),
                remaining_shortage,
            )

    # Fetch Amazon items in inbound warehouse with positive qty not reported by Amazon, assume 0
    inbound_amazon_items = frappe.db.sql("""
        SELECT i.name as item_code, i.custom_asin as asin, b.actual_qty, b.valuation_rate
        FROM `tabItem` i
        INNER JOIN `tabBin` b ON i.name = b.item_code
        WHERE b.warehouse = %s AND i.custom_asin IS NOT NULL AND b.actual_qty > 0 AND i.disabled = 0 AND i.is_stock_item = 1
    """, inbound_wh, as_dict=True)

    # Candidates to zero: stocked Amazon items in this warehouse NOT reported by the API
    inbound_zero_candidates = [r for r in inbound_amazon_items if r.asin not in asin_inbound]
    inbound_total_stocked = len(inbound_amazon_items)  # Amazon items currently holding stock here
    # Guard: refuse to zero if the unreported share exceeds the threshold (likely a partial pull)
    if inbound_total_stocked and (len(inbound_zero_candidates) / inbound_total_stocked) > MAX_ZERO_OUT_FRACTION:
        _record_inventory_trigger(
            diagnostics,
            "46",
            "The Inbound zero-out guard blocked reconciliation rows",
        )
        frappe.log_error(
            f"Skipping inbound zero-out for {inbound_wh}: "
            f"{len(inbound_zero_candidates)}/{inbound_total_stocked} stocked Amazon items "
            f"unreported (> {MAX_ZERO_OUT_FRACTION:.0%}); treating as partial API pull.",
            "FBA Inventory Zero-Out Guard",
        )
    else:
        for row in inbound_zero_candidates:  # already excludes reported ASINs
            item_valuation_rate = frappe.get_value("Item", row.item_code, "valuation_rate") or 0
            item_dict = {
                "item_code": row.item_code,
                "warehouse": inbound_wh,
                "qty": 0,
            }
            if item_valuation_rate > 0:
                item_dict["valuation_rate"] = item_valuation_rate
            else:
                item_dict["valuation_rate"] = 0.01
            reconcile_items.append(item_dict)
            inbound_reconciliation_current_quantities[
                (row.item_code, inbound_wh)
            ] = float(row.actual_qty or 0)

    # Create and submit Stock Reconciliation if needed
    if reconcile_items:
        if DEBUG: print(f"[DEBUG] Creating Stock Reconciliation with {len(reconcile_items)} items...")
        inbound_sr = None
        inbound_sr_inserted = False
        inbound_reconciliation_metrics = _reconciliation_quantity_metrics(
            reconcile_items,
            inbound_reconciliation_current_quantities,
        )
        try:  # ADDED: Wrap for error logging
            inbound_sr = frappe.get_doc({
                "doctype": "Stock Reconciliation",
                "company": company,
                "posting_date": frappe.utils.today(),
                "purpose": "Stock Reconciliation",
                "expense_account": adjustment_account,
                "items": reconcile_items,
            })
            inbound_sr.insert(ignore_permissions=True)
            inbound_sr_inserted = True
            _upsert_inventory_document(
                diagnostics,
                "Stock Reconciliation",
                inbound_sr.name,
                inbound_reconciliation_purpose,
                "newly created by this run",
                "inserted as draft",
                source_warehouses=[inbound_wh],
                target_warehouse=inbound_wh,
                movement_quantity=abs(
                    inbound_reconciliation_metrics["total_increase_qty"]
                ) + abs(inbound_reconciliation_metrics["total_decrease_qty"]),
                affected_item_count=len({
                    row["item_code"] for row in reconcile_items
                }),
                reconciliation_metrics=inbound_reconciliation_metrics,
            )
            if DEBUG: print(f"[DEBUG] Inserted SR: {inbound_sr.name}")
            if DEBUG:
                if DEBUG: print(f"[DEBUG] DEBUG mode: leaving inbound SR {inbound_sr.name} as DRAFT (not submitted)")
                frappe.db.commit()  # persist draft
                _upsert_inventory_document(
                    diagnostics,
                    "Stock Reconciliation",
                    inbound_sr.name,
                    inbound_reconciliation_purpose,
                    "newly created by this run",
                    "DEBUG draft; not submitted",
                )
            else:
                inbound_sr.submit()
                frappe.db.commit()
                _upsert_inventory_document(
                    diagnostics,
                    "Stock Reconciliation",
                    inbound_sr.name,
                    inbound_reconciliation_purpose,
                    "newly created by this run",
                    "submitted",
                )
                if DEBUG: print(f"[DEBUG] Submitted inbound SR: {inbound_sr.name}")
        except Exception:
            inbound_sr_name = getattr(inbound_sr, "name", None)
            _record_inventory_trigger(
                diagnostics,
                "44",
                "Inbound Stock Reconciliation insertion or submission failed",
                inbound_sr_name or "<not assigned>",
            )
            _upsert_inventory_document(
                diagnostics,
                "Stock Reconciliation",
                inbound_sr_name,
                inbound_reconciliation_purpose,
                "newly created by this run",
                "submission failed" if inbound_sr_inserted else "insertion failed",
                source_warehouses=[inbound_wh],
                target_warehouse=inbound_wh,
                movement_quantity=abs(
                    inbound_reconciliation_metrics["total_increase_qty"]
                ) + abs(inbound_reconciliation_metrics["total_decrease_qty"]),
                affected_item_count=len({
                    row["item_code"] for row in reconcile_items
                }),
                reconciliation_metrics=inbound_reconciliation_metrics,
            )
            frappe.log_error(frappe.get_traceback(), "Inbound Stock Reconciliation Error")
            raise

# ──────────────────────────────────────────
# Orchestrator
# ──────────────────────────────────────────
def process_fba_inventory():
    temporary_log_inputs = None
    inbound_flow_diagnostics = None
    try:  # ADDED: High-level wrap for entire function
        repo = AmazonRepository("q3opu7c5ac")
        settings = repo.amz_setting
        if DEBUG: print("[DEBUG] Starting FBA inventory sync...")

        marketplace_ids = parse_marketplaces(settings.custom_marketplace)
        if DEBUG: print(f"[DEBUG] Fetching for marketplaces: {marketplace_ids}")

        # Discover, strictly select, download, and parse both daily reports first.
        today_source, yesterday_source, report_candidates = (
            _load_daily_manage_inventory_sources(settings)
        )

        # Gather LIVE data after report discovery. Do not abort if the live pull
        # is unavailable; report-only Mode 1 and per-ASIN Mode 4 must still run.
        summaries = []
        live_global_error = None
        try:
            for mkt_id in marketplace_ids:
                if DEBUG: print(f"[DEBUG] Querying marketplace: {mkt_id}")
                base_qs = {
                    "granularityType": "Marketplace",
                    "granularityId": mkt_id,
                    "marketplaceIds": mkt_id,
                    "details": "true",
                }
                next_token = None
                page = 1
                while True:
                    qs = dict(base_qs)
                    if next_token:
                        qs["nextToken"] = next_token
                    if DEBUG: print(f"[DEBUG] Fetching page {page} for {mkt_id}...")
                    resp = _sp_get(
                        "/fba/inventory/v1/summaries",
                        qs,
                        settings,
                        return_full=True,
                    )
                    page_summaries = resp.get("payload", {}).get(
                        "inventorySummaries", []
                    )
                    summaries.extend(page_summaries)
                    if DEBUG:
                        print(
                            f"[DEBUG] Fetched {len(page_summaries)} summaries "
                            f"from page {page} for {mkt_id}"
                        )
                    next_token = resp.get("pagination", {}).get("nextToken")
                    if not next_token:
                        break
                    time.sleep(1)
                    page += 1
                time.sleep(2)
        except Exception as exc:
            live_global_error = (
                f"LIVE Inventory Summaries API unavailable/incomplete "
                f"({type(exc).__name__}: {exc})"
            )
            summaries = []
            frappe.log_error(
                f"{live_global_error}\n\n{frappe.get_traceback()}",
                "FBA Inventory Summaries Protection Error",
            )

        live_snapshots, live_invalid_asins = _build_live_api_snapshots(summaries)
        if DEBUG:
            print(
                f"[DEBUG] LIVE snapshots={len(live_snapshots)}, "
                f"invalid={len(live_invalid_asins)}, global_error={live_global_error}"
            )

        wh = settings.afn_warehouse
        inbound_wh = settings.custom_amazon_inbound_warehouse
        current_main_by_asin = _current_erp_qty_by_asin(wh)
        current_inbound_by_asin = _current_erp_qty_by_asin(inbound_wh)

        (
            asin_fulfillable,
            final_inbound_by_asin,
            mode_by_asin,
            degradation_lines,
        ) = _build_protected_inventory_targets(
            today_source,
            yesterday_source,
            live_snapshots,
            live_invalid_asins,
            live_global_error,
            current_main_by_asin,
            current_inbound_by_asin,
        )

        _log_daily_report_errors(
            today_source, yesterday_source, report_candidates, mode_by_asin
        )
        _log_degraded_asins(degradation_lines)
        try:
            inbound_flow_diagnostics = _new_inbound_flow_diagnostics(
                final_inbound_by_asin, settings
            )
        except Exception:
            # Temporary diagnostic initialization must not affect inventory.
            inbound_flow_diagnostics = None
        temporary_log_inputs = (
            today_source,
            yesterday_source,
            report_candidates,
            live_snapshots,
            live_invalid_asins,
            live_global_error,
            current_main_by_asin,
            current_inbound_by_asin,
            asin_fulfillable,
            final_inbound_by_asin,
            mode_by_asin,
        )

        if DEBUG:
            print(f"[DEBUG] Final main-FBA targets: {asin_fulfillable}")
            print(f"[DEBUG] Final inbound targets: {final_inbound_by_asin}")

        # Report enabled stock Items whose ERP ASIN field contains multiple ASINs.
        # The sync intentionally does not split or reinterpret this bad mapping; it
        # is surfaced for correction so one ERP Item maps to one ASIN.
        multiple_asin_items = _erp_items_with_multiple_asins()
        if multiple_asin_items:
            for issue in multiple_asin_items:
                _record_inventory_trigger(
                    inbound_flow_diagnostics,
                    "24",
                    "An ERP Item contains multiple ASINs where one usable ASIN is required",
                    issue["item_code"],
                )
            multiple_asin_lines = [
                f"Item {issue['item_code']}: custom_asin={issue['custom_asin']!r}; "
                f"detected_asins={', '.join(issue['detected_asins'])}"
                for issue in multiple_asin_items
            ]
            frappe.log_error(
                "ERP Item(s) contain more than one ASIN in custom_asin. "
                "Correct each Item so it contains exactly one ASIN:\n"
                + "\n".join(multiple_asin_lines),
                "ERP Item Has Multiple ASINs",
            )

        # Report Amazon ASINs that cannot be mapped to an enabled ERP Item.
        # Keep this as one consolidated error per sync run so catalog/configuration
        # gaps are visible without flooding the Error Log. Zero/zero targets are
        # inert and are intentionally ignored.
        target_asins = sorted(set(asin_fulfillable) | set(final_inbound_by_asin))
        missing_erp_asins = []
        for asin in target_asins:
            item_code = frappe.db.get_value(
                "Item", {"custom_asin": asin, "disabled": 0}, "name"
            )
            if not item_code:
                main_target = int(asin_fulfillable.get(asin, 0) or 0)
                inbound_target = int(final_inbound_by_asin.get(asin, 0) or 0)
                if main_target == 0 and inbound_target == 0:
                    continue
                missing_erp_asins.append(asin)

        if missing_erp_asins:
            for asin in missing_erp_asins:
                _record_inventory_trigger(
                    inbound_flow_diagnostics,
                    "23",
                    "A positive protected target cannot be applied because no enabled ERP Item exists",
                    asin,
                )
            missing_lines = [
                f"ASIN {asin}: main_target={asin_fulfillable.get(asin, '<none>')}, "
                f"inbound_target={final_inbound_by_asin.get(asin, '<none>')}"
                for asin in missing_erp_asins
            ]
            frappe.log_error(
                "Amazon inventory data contains ASIN(s) with no matching enabled "
                "ERP Item. These ASINs will be skipped and ERP stock will not be "
                "updated for them:\n" + "\n".join(missing_lines),
                "Amazon ASIN Missing From ERP",
            )

        # All source discovery, mode selection, and target protection is complete.
        # Only now may ERP inventory be mutated.
        company = settings.company
        adjustment_account = settings.custom_amazon_inventory_adjustment_account
        items_list = []
        main_reconciliation_current_quantities = {}
        main_reconciliation_purpose = (
            "Set ERP Main FBA quantities to the run's protected Main FBA targets"
        )

        for asin, new_qty in asin_fulfillable.items():
            item_code = frappe.db.get_value(
                "Item", {"custom_asin": asin, "disabled": 0}, "name"
            )
            if not item_code:
                if DEBUG: print(f"[DEBUG] No matching item_code found for ASIN: {asin}")
                continue
            if not frappe.get_value("Item", item_code, "is_stock_item"):
                if DEBUG: print(f"[DEBUG] Skipping non-stock item: {item_code}")
                continue

            try:
                bin_data = frappe.db.get_value(
                    "Bin",
                    {"item_code": item_code, "warehouse": wh},
                    ["actual_qty", "valuation_rate"],
                    as_dict=True,
                ) or {}
            except Exception:
                _record_inventory_trigger(
                    inbound_flow_diagnostics,
                    "25",
                    "A required ERP warehouse quantity could not be read or verified",
                    item_code,
                )
                raise
            current_qty = bin_data.get("actual_qty", 0)
            if DEBUG:
                print(
                    f"[DEBUG] Current qty in Bin: {current_qty} vs "
                    f"New qty: {new_qty} - {item_code}"
                )
            if int(current_qty) == new_qty:
                continue

            item_valuation_rate = (
                frappe.get_value("Item", item_code, "valuation_rate") or 0
            )
            items_list.append({
                "item_code": item_code,
                "warehouse": wh,
                "qty": new_qty,
                "valuation_rate": (
                    item_valuation_rate if item_valuation_rate > 0 else 0.01
                ),
            })
            main_reconciliation_current_quantities[(item_code, wh)] = float(
                current_qty or 0
            )

        # Preserve the existing belt-and-suspenders zero-out guard. Because every
        # currently stocked Amazon ASIN is included in per-ASIN mode selection,
        # source absence alone cannot enter this zero-out path.
        amazon_items_in_wh = frappe.db.sql(
            """
            SELECT i.name as item_code, i.custom_asin as asin,
                   b.actual_qty, b.valuation_rate
            FROM `tabItem` i
            INNER JOIN `tabBin` b ON i.name = b.item_code
            WHERE b.warehouse = %s
              AND i.custom_asin IS NOT NULL
              AND b.actual_qty > 0
              AND i.disabled = 0
              AND i.is_stock_item = 1
            """,
            wh,
            as_dict=True,
        )
        zero_candidates = [
            row for row in amazon_items_in_wh if row.asin not in asin_fulfillable
        ]
        total_stocked = len(amazon_items_in_wh)
        if (
            total_stocked
            and (len(zero_candidates) / total_stocked) > MAX_ZERO_OUT_FRACTION
        ):
            _record_inventory_trigger(
                inbound_flow_diagnostics,
                "45",
                "The Main FBA zero-out guard blocked reconciliation rows",
            )
            frappe.log_error(
                f"Skipping fulfillable zero-out for {wh}: "
                f"{len(zero_candidates)}/{total_stocked} stocked Amazon items "
                f"unreported (> {MAX_ZERO_OUT_FRACTION:.0%}); "
                "treating as partial API pull.",
                "FBA Inventory Zero-Out Guard",
            )
        else:
            for row in zero_candidates:
                item_valuation_rate = (
                    frappe.get_value("Item", row.item_code, "valuation_rate") or 0
                )
                items_list.append({
                    "item_code": row.item_code,
                    "warehouse": wh,
                    "qty": 0,
                    "valuation_rate": (
                        item_valuation_rate if item_valuation_rate > 0 else 0.01
                    ),
                })
                main_reconciliation_current_quantities[(row.item_code, wh)] = (
                    float(row.actual_qty or 0)
                )

        if DEBUG: print(f"[DEBUG] Total items to reconcile: {len(items_list)}")
        if items_list:
            main_sr = None
            main_sr_inserted = False
            main_reconciliation_metrics = _reconciliation_quantity_metrics(
                items_list,
                main_reconciliation_current_quantities,
            )
            try:
                main_sr = frappe.get_doc({
                    "doctype": "Stock Reconciliation",
                    "company": company,
                    "posting_date": frappe.utils.today(),
                    "purpose": "Stock Reconciliation",
                    "expense_account": adjustment_account,
                    "items": items_list,
                })
                main_sr.insert(ignore_permissions=True)
                main_sr_inserted = True
                _upsert_inventory_document(
                    inbound_flow_diagnostics,
                    "Stock Reconciliation",
                    main_sr.name,
                    main_reconciliation_purpose,
                    "newly created by this run",
                    "inserted as draft",
                    source_warehouses=[wh],
                    target_warehouse=wh,
                    movement_quantity=abs(
                        main_reconciliation_metrics["total_increase_qty"]
                    ) + abs(main_reconciliation_metrics["total_decrease_qty"]),
                    affected_item_count=len({
                        row["item_code"] for row in items_list
                    }),
                    reconciliation_metrics=main_reconciliation_metrics,
                )
                if DEBUG:
                    print(f"[DEBUG] Inserted SR: {main_sr.name}")
                    print(
                        f"[DEBUG] DEBUG mode: leaving FBA fulfillable SR "
                        f"{main_sr.name} as DRAFT (not submitted)"
                    )
                    frappe.db.commit()
                    _upsert_inventory_document(
                        inbound_flow_diagnostics,
                        "Stock Reconciliation",
                        main_sr.name,
                        main_reconciliation_purpose,
                        "newly created by this run",
                        "DEBUG draft; not submitted",
                    )
                else:
                    main_sr.submit()
                    frappe.db.commit()
                    _upsert_inventory_document(
                        inbound_flow_diagnostics,
                        "Stock Reconciliation",
                        main_sr.name,
                        main_reconciliation_purpose,
                        "newly created by this run",
                        "submitted",
                    )
            except Exception:
                main_sr_name = getattr(main_sr, "name", None)
                _record_inventory_trigger(
                    inbound_flow_diagnostics,
                    "43",
                    "Main FBA Stock Reconciliation insertion or submission failed",
                    main_sr_name or "<not assigned>",
                )
                _upsert_inventory_document(
                    inbound_flow_diagnostics,
                    "Stock Reconciliation",
                    main_sr_name,
                    main_reconciliation_purpose,
                    "newly created by this run",
                    "submission failed" if main_sr_inserted else "insertion failed",
                    source_warehouses=[wh],
                    target_warehouse=wh,
                    movement_quantity=abs(
                        main_reconciliation_metrics["total_increase_qty"]
                    ) + abs(main_reconciliation_metrics["total_decrease_qty"]),
                    affected_item_count=len({
                        row["item_code"] for row in items_list
                    }),
                    reconciliation_metrics=main_reconciliation_metrics,
                )
                frappe.log_error(
                    frappe.get_traceback(),
                    "Fulfillable Stock Reconciliation Error",
                )
                raise

        process_inbound_inventory(
            final_inbound_by_asin,
            settings,
            inbound_flow_diagnostics,
        )

    except Exception as exc:
        _record_inventory_trigger(
            inbound_flow_diagnostics,
            "47",
            "An unhandled inventory-processing exception occurred",
            type(exc).__name__,
        )
        frappe.log_error(frappe.get_traceback(), "FBA Inventory Process Error")
        raise
    finally:
        if temporary_log_inputs is not None:
            _log_temporary_consolidated_fallbacks_and_failures(
                *temporary_log_inputs,
                inbound_flow_diagnostics=inbound_flow_diagnostics,
            )

# ──────────────────────────────────────────
# Scheduler wrapper
# ──────────────────────────────────────────
"""
frappe.call("eseller_suite.eseller_suite.doctype.amazon_sp_api_settings.amazon_sync_fba_inventory.run_daily_fba_inventory_sync")

NOTE:
You need to Manually Create Opening Stock Entries Before Running the Initial Sync
Go to Stock > Stock Transactions > Stock Entry > New
Set Stock Entry Type to "Material Receipt"
Set Target Warehouse to your relevant warehouses (Amazon FBA, Amazon FBA Inbound, Amazon FBA Prep Area
"""
@frappe.whitelist()
def run_daily_fba_inventory_sync():
    """Hourly scheduler entry: request MYI ALL at 6 AM and run the inventory sync at 7 AM."""
    
    pst_tz = ZoneInfo("America/Los_Angeles")
    now = datetime.now(pst_tz)

    if not DEBUG and now.hour == 6:
        try:
            repo = AmazonRepository("q3opu7c5ac")
            settings = repo.amz_setting
            marketplace_ids = parse_marketplaces(settings.custom_marketplace)
            request_manage_inventory_report(settings, marketplace_ids)
        except Exception as exc:
            frappe.log_error(
                f"6 AM MYI ALL report request failed ({type(exc).__name__}: {exc}); "
                "the 7 AM inventory sync will continue normally.\n\n"
                f"{frappe.get_traceback()}",
                "FBA MYI Report Request Error",
            )
        return

    if not DEBUG and now.hour != 7:
        return
    
    try:  # ADDED: Wrap scheduler call
        frappe.get_doc("Amazon SP API Settings", "q3opu7c5ac")  # Load to ensure active
        process_fba_inventory()
    except Exception:
        frappe.log_error(frappe.get_traceback(), "Daily FBA Inventory Sync Error")
        raise
