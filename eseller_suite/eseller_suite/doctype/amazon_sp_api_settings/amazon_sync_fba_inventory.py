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
    """Return valid raw C/S/R/F snapshots plus per-ASIN parse failures."""
    text = report_bytes.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    if not reader.fieldnames:
        raise ValueError("MYI ALL report has no header row")

    reader.fieldnames = [_normalize_report_header(name) for name in reader.fieldnames]
    required_headers = {
        "asin",
        "condition",
        "afn-warehouse-quantity",
        "afn-inbound-shipped-quantity",
        "afn-inbound-receiving-quantity",
        "afn-researching-quantity",
        "afn-fc-transfer-quantity",
    }
    missing_headers = sorted(required_headers - set(reader.fieldnames))
    if missing_headers:
        raise ValueError(
            "MYI ALL report is missing required columns: " + ", ".join(missing_headers)
        )

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
                "warehouse": _parse_required_nonnegative_quantity(
                    row.get("afn-warehouse-quantity"), "afn-warehouse-quantity", asin
                ),
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
                "researching": _parse_required_nonnegative_quantity(
                    row.get("afn-researching-quantity"),
                    "afn-researching-quantity",
                    asin,
                ),
                "F": _parse_required_nonnegative_quantity(
                    row.get("afn-fc-transfer-quantity"),
                    "afn-fc-transfer-quantity",
                    asin,
                ),
            }
        except ValueError as exc:
            invalid_asins[asin] = str(exc)
            raw_by_asin.pop(asin, None)
            continue

        aggregate = raw_by_asin.setdefault(
            asin, {"warehouse": 0, "S": 0, "R": 0, "researching": 0, "F": 0}
        )
        for key, value in quantities.items():
            aggregate[key] += value
        parsed_rows += 1

    snapshots = {}
    for asin, aggregate in raw_by_asin.items():
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

    if DEBUG:
        print(
            f"[DEBUG] Parsed {parsed_rows} MYI ALL New-condition rows into "
            f"{len(snapshots)} valid ASIN snapshots; invalid ASINs={len(invalid_asins)}"
        )
    return snapshots, invalid_asins


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
            snapshots, invalid_asins = _download_manage_inventory_report(selected, settings)
            source["snapshots"] = snapshots
            source["invalid_asins"] = invalid_asins
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
        current_main = int(current_main_by_asin.get(asin, 0) or 0)
        current_inbound = int(current_inbound_by_asin.get(asin, 0) or 0)

        if (
            yesterday_snapshot is not None
            and today_snapshot is not None
            and live_snapshot is not None
        ):
            mode = "YESTERDAY -> PROTECTED TODAY -> LIVE API"
            _, _, report_diagnostics = _protect_snapshot_transition(
                yesterday_snapshot, today_snapshot
            )
            protected_today = dict(today_snapshot)
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

        elif today_snapshot is not None and live_snapshot is not None:
            mode = "TODAY REPORT -> LIVE API"
            main_target, inbound_target, diagnostics = _protect_snapshot_transition(
                today_snapshot, live_snapshot
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

        elif yesterday_snapshot is not None and live_snapshot is not None:
            mode = "YESTERDAY REPORT -> LIVE API"
            main_target, inbound_target, diagnostics = _protect_snapshot_transition(
                yesterday_snapshot, live_snapshot
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

        elif today_snapshot is not None and yesterday_snapshot is not None:
            mode = "NORMAL TWO-REPORT"
            main_target, inbound_target, diagnostics = _protect_snapshot_transition(
                yesterday_snapshot, today_snapshot
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
                today_snapshot,
                yesterday_snapshot,
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

        main_targets[asin] = max(int(main_target), 0)
        inbound_targets[asin] = max(int(inbound_target), 0)
        mode_by_asin[asin] = mode

        if mode == "PROBLEM-CATEGORY INCREASE-ONLY SAFETY MODE":
            # If both otherwise-valid daily reports simply omit this ASIN, that
            # absence is routine and should not create an Error Log entry. Mode 4
            # protection still applies; only the per-ASIN reporting is suppressed.
            routine_both_report_absence = (
                today_source["available"]
                and yesterday_source["available"]
                and asin not in today_source["snapshots"]
                and asin not in yesterday_source["snapshots"]
                and asin not in today_source["invalid_asins"]
                and asin not in yesterday_source["invalid_asins"]
            )
            if not routine_both_report_absence:
                today_reason = _source_asin_reason(today_source, asin)
                yesterday_reason = _source_asin_reason(yesterday_source, asin)
                if live_global_error:
                    live_reason = live_global_error
                elif asin in live_invalid_asins:
                    live_reason = live_invalid_asins[asin]
                elif asin not in live_snapshots:
                    live_reason = "ASIN ABSENT from LIVE API"
                else:
                    live_reason = None
                reasons = [
                    reason
                    for reason in (today_reason, yesterday_reason, live_reason)
                    if reason
                ]
                degradation_lines.append(
                    f"ASIN {asin}: mode={mode}; "
                    + "; ".join(reasons or ["fallback selected"])
                )

    return main_targets, inbound_targets, mode_by_asin, degradation_lines


def _log_degraded_asins(lines):
    if not lines:
        return
    chunk_size = 40
    for index in range(0, len(lines), chunk_size):
        chunk = lines[index:index + chunk_size]
        frappe.log_error(
            "\n".join(chunk),
            "FBA MYI Per-ASIN Degraded Protection",
        )


def _log_daily_report_errors(today_source, yesterday_source, reports, mode_by_asin):
    failed = [
        source
        for source in (today_source, yesterday_source)
        if not source["available"]
    ]
    if not failed:
        return

    mode_counts = defaultdict(int)
    for mode in mode_by_asin.values():
        mode_counts[mode] += 1
    failed_text = "\n".join(
        f"{source['label']} expected {source['expected_date']}: "
        f"{source['error'] or 'unavailable'}"
        for source in failed
    )
    frappe.log_error(
        f"{failed_text}\n"
        f"Discovered candidates: {_report_candidate_summary(reports)}\n"
        f"Per-ASIN operating modes: {dict(mode_counts)}",
        "FBA MYI Daily Report Protection Error",
    )


# ──────────────────────────────────────────
# Inbound Processing
# ──────────────────────────────────────────
def process_inbound_inventory(asin_inbound, settings):
    prep_wh = settings.custom_amazon_fba_staging_area
    inbound_wh = settings.custom_amazon_inbound_warehouse
    company = settings.company
    adjustment_account = settings.custom_amazon_inventory_adjustment_account

    if DEBUG: print(f"[DEBUG] Starting inbound inventory processing for warehouse: {inbound_wh}")

    # First pass: collect transfers for increases
    transfer_items = []
    prep_reconcile_items = []
    transfer_pending = []
    for asin, target_qty in asin_inbound.items():
        if DEBUG: print(f"[DEBUG] Processing inbound ASIN: {asin} with target_qty: {target_qty}")
        item_code = frappe.db.get_value("Item", {"custom_asin": asin, "disabled": 0}, "name")
        if not item_code:
            if DEBUG: print(f"[DEBUG] No matching item_code found for ASIN: {asin}")
            continue

        # ADDED: Skip if not a stock item
        if not frappe.get_value("Item", item_code, "is_stock_item"):
            if DEBUG: print(f"[DEBUG] Skipping non-stock item: {item_code}")
            continue

        current_inbound = frappe.db.get_value("Bin", {"item_code": item_code, "warehouse": inbound_wh}, "actual_qty") or 0
        diff = target_qty - current_inbound
        if DEBUG: print(f"[DEBUG] Current inbound qty: {current_inbound}, diff: {diff}")
        if diff <= 0:
            continue

        current_prep = frappe.db.get_value("Bin", {"item_code": item_code, "warehouse": prep_wh}, "actual_qty") or 0
        transfer_qty = min(current_prep, diff)
        if DEBUG: print(f"[DEBUG] Current prep qty: {current_prep}, transfer_qty: {transfer_qty}")
        if transfer_qty <= 0:
            continue

        bin_data_prep = frappe.db.get_value(
            "Bin",
            {"item_code": item_code, "warehouse": prep_wh},
            ["valuation_rate"],
            as_dict=True
        ) or {}
        bin_rate = bin_data_prep.get("valuation_rate", 0)

        item_valuation_rate = frappe.get_value("Item", item_code, "valuation_rate") or 0
        val_rate = item_valuation_rate if item_valuation_rate > 0 else 0.01

        has_batch = frappe.get_value("Item", item_code, "has_batch_no")
        has_serial = frappe.get_value("Item", item_code, "has_serial_no")

        reconcile_needed = (bin_rate != val_rate)

        if reconcile_needed:
            item_reconcile_items = []
            if has_serial:
                serial_nos = frappe.db.sql_list("""SELECT name FROM `tabSerial No` WHERE item_code = %s AND warehouse = %s""", (item_code, prep_wh))
                if len(serial_nos) == current_prep:
                    item_reconcile_items.append({
                        "item_code": item_code,
                        "warehouse": prep_wh,
                        "qty": current_prep,
                        "valuation_rate": val_rate,
                        "serial_no": '\n'.join(serial_nos),
                    })
            elif has_batch:
                batches = frappe.get_all("Batch", filters={"item": item_code}, fields=["name"])
                for batch in batches:
                    batch_qty = get_batch_qty(batch.name, prep_wh, item_code) or 0
                    if batch_qty > 0:
                        item_reconcile_items.append({
                            "item_code": item_code,
                            "warehouse": prep_wh,
                            "qty": batch_qty,
                            "valuation_rate": val_rate,
                            "batch_no": batch.name,
                        })
            else:
                item_reconcile_items.append({
                    "item_code": item_code,
                    "warehouse": prep_wh,
                    "qty": current_prep,
                    "valuation_rate": val_rate,
                })
            if item_reconcile_items:
                prep_reconcile_items += item_reconcile_items
                transfer_pending.append((item_code, transfer_qty, has_batch, has_serial, val_rate))
            else:
                if DEBUG: print(f"[DEBUG] Could not create reconcile items for {item_code}, skipping transfer")
        else:
            transfer_pending.append((item_code, transfer_qty, has_batch, has_serial, val_rate))

    # Create and submit Prep Stock Reconciliation if needed
    if prep_reconcile_items:
        if DEBUG: print(f"[DEBUG] Creating Prep Stock Reconciliation with {len(prep_reconcile_items)} items...")
        try:  # ADDED: Wrap for error logging
            prep_sr = frappe.get_doc({
                "doctype": "Stock Reconciliation",
                "company": company,
                "posting_date": frappe.utils.today(),
                "purpose": "Stock Reconciliation",
                "expense_account": adjustment_account,
                "items": prep_reconcile_items,
            })
            prep_sr.insert(ignore_permissions=True)
            if DEBUG: print(f"[DEBUG] Inserted Prep SR: {prep_sr.name}")
            if DEBUG:
                if DEBUG: print(f"[DEBUG] DEBUG mode: leaving Prep SR {prep_sr.name} as DRAFT (not submitted)")
                frappe.db.commit()  # persist draft
            else:
                prep_sr.submit()
                frappe.db.commit()
                if DEBUG: print(f"[DEBUG] Submitted Prep SR: {prep_sr.name}")
        except Exception:
            frappe.log_error(frappe.get_traceback(), "Prep Stock Reconciliation Error")
            raise  # Re-raise to propagate if needed

    # Now process pending transfers
    for item_code, transfer_qty, has_batch, has_serial, val_rate in transfer_pending:
        current_prep = frappe.db.get_value("Bin", {"item_code": item_code, "warehouse": prep_wh}, "actual_qty") or 0
        transfer_qty = min(current_prep, transfer_qty)
        if transfer_qty <= 0:
            continue
        if has_serial:
            serial_nos = frappe.db.sql_list("""SELECT name FROM `tabSerial No` WHERE item_code = %s AND warehouse = %s LIMIT %s""", (item_code, prep_wh, transfer_qty))
            if len(serial_nos) == transfer_qty:
                transfer_items.append({
                    "item_code": item_code,
                    "s_warehouse": prep_wh,
                    "t_warehouse": inbound_wh,
                    "qty": transfer_qty,
                    "basic_rate": val_rate,
                    "serial_no": '\n'.join(serial_nos),
                })
            else:
                if DEBUG: print(f"[DEBUG] Insufficient serial nos for {item_code}, skipping transfer")
        elif has_batch:
            batches = frappe.get_all("Batch", filters={"item": item_code}, fields=["name"], order_by="creation asc")
            remaining = transfer_qty
            for batch in batches:
                if remaining <= 0:
                    break
                batch_qty = get_batch_qty(batch.name, prep_wh, item_code) or 0
                if batch_qty > 0:
                    move_qty = min(batch_qty, remaining)
                    transfer_items.append({
                        "item_code": item_code,
                        "s_warehouse": prep_wh,
                        "t_warehouse": inbound_wh,
                        "qty": move_qty,
                        "basic_rate": val_rate,
                        "batch_no": batch.name,
                    })
                    remaining -= move_qty
            if remaining > 0:
                if DEBUG: print(f"[DEBUG] Insufficient batch qty for {item_code}, transferred {transfer_qty - remaining}, remaining {remaining} will be handled by reconciliation")
        else:
            transfer_items.append({
                "item_code": item_code,
                "s_warehouse": prep_wh,
                "t_warehouse": inbound_wh,
                "qty": transfer_qty,
                "basic_rate": val_rate,
            })

    # Create and submit Stock Entry if needed
    if transfer_items:
        if DEBUG: print(f"[DEBUG] Creating Stock Entry with {len(transfer_items)} items...")
        se = frappe.get_doc({
            "doctype": "Stock Entry",
            "company": company,
            "stock_entry_type": "Material Transfer",
            "from_warehouse": prep_wh,
            "to_warehouse": inbound_wh,
            "posting_date": frappe.utils.today(),
            "items": transfer_items,
        })
        se.insert(ignore_permissions=True)
        if DEBUG: print(f"[DEBUG] Inserted SE: {se.name}")
        try:
            if DEBUG:
                print(f"[DEBUG] DEBUG mode: leaving SE {se.name} as DRAFT (not submitted)")
                frappe.db.commit()
            else:
                se.submit()
                frappe.db.commit()
                if DEBUG: print(f"[DEBUG] Submitted SE: {se.name}")
        except NegativeStockError as e:
            if DEBUG: print(f"[DEBUG] NegativeStockError during submit: {str(e)}")
            # Safely delete draft
            try:
                se.reload()  # Reload to get current status
                if se.docstatus == 0:
                    se.delete()
                elif se.docstatus == 1:
                    se.cancel()
                    se.delete()
                frappe.db.commit()
            except Exception as del_e:
                if DEBUG: print(f"[DEBUG] Error during cleanup delete: {str(del_e)}")
                frappe.log_error(frappe.get_traceback(), "Stock Entry Cleanup Error")
            if DEBUG: print("[DEBUG] Deleted draft SE, falling back to reconciliation")
            frappe.log_error(frappe.get_traceback(), "Stock Entry NegativeStockError")  # ADDED: Log specific error
        except Exception as e:
            if DEBUG: print(f"[DEBUG] Unexpected error during SE submit: {str(e)}")
            # Safely delete
            try:
                se.reload()  # Reload to get current status
                if se.docstatus == 0:
                    se.delete()
                elif se.docstatus == 1:
                    se.cancel()
                    se.delete()
                frappe.db.commit()
            except Exception as del_e:
                if DEBUG: print(f"[DEBUG] Error during cleanup delete: {str(del_e)}")
                frappe.log_error(frappe.get_traceback(), "Stock Entry Cleanup Error")
            frappe.log_error(frappe.get_traceback(), "Stock Entry Submit Error")  # ADDED: Log with traceback
            raise

    # Second pass: collect reconciliations where qty doesn't match
    reconcile_items = []
    for asin, target_qty in asin_inbound.items():
        item_code = frappe.db.get_value("Item", {"custom_asin": asin, "disabled": 0}, "name")
        if not item_code:
            continue

        # ADDED: Skip if not a stock item
        if not frappe.get_value("Item", item_code, "is_stock_item"):
            if DEBUG: print(f"[DEBUG] Skipping non-stock item: {item_code}")
            continue

        current_inbound = frappe.db.get_value("Bin", {"item_code": item_code, "warehouse": inbound_wh}, "actual_qty") or 0
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

    # Create and submit Stock Reconciliation if needed
    if reconcile_items:
        if DEBUG: print(f"[DEBUG] Creating Stock Reconciliation with {len(reconcile_items)} items...")
        try:  # ADDED: Wrap for error logging
            sr = frappe.get_doc({
                "doctype": "Stock Reconciliation",
                "company": company,
                "posting_date": frappe.utils.today(),
                "purpose": "Stock Reconciliation",
                "expense_account": adjustment_account,
                "items": reconcile_items,
            })
            sr.insert(ignore_permissions=True)
            if DEBUG: print(f"[DEBUG] Inserted SR: {sr.name}")
            if DEBUG:
                if DEBUG: print(f"[DEBUG] DEBUG mode: leaving inbound SR {sr.name} as DRAFT (not submitted)")
                frappe.db.commit()  # persist draft
            else:
                sr.submit()
                frappe.db.commit()
                if DEBUG: print(f"[DEBUG] Submitted inbound SR: {sr.name}")
        except Exception:
            frappe.log_error(frappe.get_traceback(), "Inbound Stock Reconciliation Error")
            raise

# ──────────────────────────────────────────
# Orchestrator
# ──────────────────────────────────────────
def process_fba_inventory():
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

        if DEBUG:
            print(f"[DEBUG] Final main-FBA targets: {asin_fulfillable}")
            print(f"[DEBUG] Final inbound targets: {final_inbound_by_asin}")

        # Report enabled stock Items whose ERP ASIN field contains multiple ASINs.
        # The sync intentionally does not split or reinterpret this bad mapping; it
        # is surfaced for correction so one ERP Item maps to one ASIN.
        multiple_asin_items = _erp_items_with_multiple_asins()
        if multiple_asin_items:
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

            bin_data = frappe.db.get_value(
                "Bin",
                {"item_code": item_code, "warehouse": wh},
                ["actual_qty", "valuation_rate"],
                as_dict=True,
            ) or {}
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

        if DEBUG: print(f"[DEBUG] Total items to reconcile: {len(items_list)}")
        if items_list:
            try:
                sr = frappe.get_doc({
                    "doctype": "Stock Reconciliation",
                    "company": company,
                    "posting_date": frappe.utils.today(),
                    "purpose": "Stock Reconciliation",
                    "expense_account": adjustment_account,
                    "items": items_list,
                })
                sr.insert(ignore_permissions=True)
                if DEBUG:
                    print(f"[DEBUG] Inserted SR: {sr.name}")
                    print(
                        f"[DEBUG] DEBUG mode: leaving FBA fulfillable SR "
                        f"{sr.name} as DRAFT (not submitted)"
                    )
                    frappe.db.commit()
                else:
                    sr.submit()
                    frappe.db.commit()
            except Exception:
                frappe.log_error(
                    frappe.get_traceback(),
                    "Fulfillable Stock Reconciliation Error",
                )
                raise

        process_inbound_inventory(final_inbound_by_asin, settings)

    except Exception:
        frappe.log_error(frappe.get_traceback(), "FBA Inventory Process Error")
        raise

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