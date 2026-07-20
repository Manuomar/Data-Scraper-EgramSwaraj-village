"""
View_Activity_Details_Report_fast.py (VOUCHER MODULE VERSION)
======================================
Multi-threaded scraper for Activity Details, specifically using the 
voucher_wise_summary_report as the source of activity_codes.

Optimized:
  - Global ThreadPoolExecutor
  - Persistent connection pooling (HTTPAdapter)
  - Asynchronous DB queue for non-blocking inserts
"""

import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import queue

import requests
from bs4 import BeautifulSoup

from mysql_connector import Voucher_Wise_Summary_Report_DB
from mysql_connector import View_Activity_Details_Report_DB
from rate_limiter import RateLimiter
import config

# ---------------------------------------------------------
# TUNABLE SETTINGS
# ---------------------------------------------------------
# MAX_WORKERS = config.MAX_WORKERS
MAX_WORKERS = 20
REQUEST_TIMEOUT = 45
MAX_RETRIES = 4
RETRY_BACKOFF = 2

REQUESTS_PER_SECOND = 10.0
BURST = 10
INSERT_BATCH_SIZE = 200   # activity detail records before flush

# ---------------------------------------------------------
# Shared rate limiter + per-thread session
# ---------------------------------------------------------
_rate_limiter = RateLimiter(rate=REQUESTS_PER_SECOND, burst=BURST)
_thread_local = threading.local()
_success_counter = {"n": 0}
_counter_lock = threading.Lock()

# Bounded queue — agar DB slow ho toh threads wait karein (backpressure)
# MAX_WORKERS * 3 se zyada pending items queue mein nahi rahenge
_db_queue = queue.Queue(maxsize=MAX_WORKERS * 4)

_stats = {"db_saved": 0, "empty": 0, "failed": 0}
_stats_lock = threading.Lock()


def _db_writer_worker():
    """Background thread to handle MySQL inserts asynchronously in bulk."""
    print("[DB_WORKER] Started View Activity DB writer thread.")
    
    # Ek hi persistent connection — ek baar banta hai, saari activity inserts + updates isi pe
    conn = Voucher_Wise_Summary_Report_DB._connect()
    batch_size = 300
    
    while True:
        items = []
        item = _db_queue.get()
        if item is None:
            _db_queue.task_done()
            break
        items.append(item)
        
        # Drain queue up to batch_size
        while len(items) < batch_size:
            try:
                itm = _db_queue.get_nowait()
                if itm is None:
                    _db_queue.put(None)
                    break
                items.append(itm)
            except queue.Empty:
                break
                
        valid_data_list = []
        activity_codes_to_mark = []
        db_saved_count = 0
        empty_count = 0
        
        for itm in items:
            activity_code, activity_data, is_empty = itm
            if activity_data is not None:
                activity_codes_to_mark.append(activity_code)
                if not is_empty:
                    valid_data_list.append(activity_data)
                    db_saved_count += 1
                else:
                    empty_count += 1
                    
        try:
            if valid_data_list:
                View_Activity_Details_Report_DB.insertDataBatch(valid_data_list, conn=conn)
                
            if activity_codes_to_mark:
                Voucher_Wise_Summary_Report_DB.updateActivityDetailsFetchedBatch(activity_codes_to_mark, conn=conn)
                
            with _stats_lock:
                _stats["db_saved"] += db_saved_count
                _stats["empty"] += empty_count
        except Exception as e:
            print(f"[DB_WORKER] BATCH ERROR: {e}")
            
        for _ in items:
            _db_queue.task_done()
            
    conn.close()
    print("[DB_WORKER] DB writer thread stopped.")


def _get_session():
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0 (compatible; DataCollector/1.0)"})
        adapter = requests.adapters.HTTPAdapter(pool_connections=1, pool_maxsize=1, max_retries=3)
        s.mount('http://', adapter)
        s.mount('https://', adapter)
        _thread_local.session = s
    return _thread_local.session


def _note_success():
    with _counter_lock:
        _success_counter["n"] += 1
        if _success_counter["n"] % 200 == 0:
            _rate_limiter.recover(amount=1.1, ceiling=REQUESTS_PER_SECOND)


def _request_with_retry(method, url, session, **kwargs):
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        _rate_limiter.acquire()
        try:
            resp = getattr(session, method)(url, timeout=REQUEST_TIMEOUT, **kwargs)
            if resp.status_code in (429, 503):
                _rate_limiter.penalize(penalty_seconds=15 * attempt)
                last_exc = Exception(f"HTTP {resp.status_code} (rate limited)")
                continue
            resp.raise_for_status()
            _note_success()
            return resp
        except Exception as e:
            last_exc = e
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF * attempt)

    print(f"[FAILED] {method.upper()} {url} -> {last_exc}")
    return None


# ---------------------------------------------------------
# Core extraction helpers (identical logic to original)
# ---------------------------------------------------------
def _expand_multivalue_row(row_dict):
    list_fields = {k: v for k, v in row_dict.items() if isinstance(v, list)}
    if not list_fields:
        return [row_dict]

    max_len = max(len(v) for v in list_fields.values())
    for k in list_fields:
        if len(row_dict[k]) < max_len:
            row_dict[k] += [""] * (max_len - len(row_dict[k]))

    expanded = []
    for i in range(max_len):
        new_row = {}
        for k, v in row_dict.items():
            new_row[k] = v[i] if isinstance(v, list) else v
        expanded.append(new_row)
    return expanded


def _table_to_json(table, section_name=None):
    headers = []
    thead = table.find("thead")
    if thead:
        headers = [th.get_text(strip=True) for th in thead.find_all("th")]
    else:
        first_row = table.find("tr")
        if first_row:
            headers = [td.get_text(strip=True) for td in first_row.find_all(["td", "th"])]

    tbody = table.find("tbody")
    rows = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]

    table_data = []
    for row in rows:
        cells = row.find_all(["td", "th"])
        if not cells:
            continue
        row_data = {}
        for i, cell in enumerate(cells):
            key = headers[i] if i < len(headers) else f"Column{i+1}"
            for br in cell.find_all("br"):
                br.replace_with("\n")
            text = cell.get_text(separator="\n", strip=True)
            lines = [line.strip() for line in text.split("\n") if line.strip()]
            value = lines if len(lines) > 1 else lines[0] if lines else ""

            if section_name == "Physical Progress Details" and "onclick" in cell.attrs:
                onclick_val = cell.attrs.get("onclick", "")
                match = re.search(r"showAssetDetailsPopup\((\d+)\)", onclick_val)
                if match:
                    row_data["Asset ID"] = match.group(1)

            row_data[key] = value
        table_data.append(row_data)

    normalized_rows = []
    for row in table_data:
        normalized_rows.extend(_expand_multivalue_row(row))
    return normalized_rows


def _extractSingleData(activity_code, session):
    url = "https://egramswaraj.gov.in/getViewActivityDetailsReport.do"
    payload = {
        "workType": "1",
        "activityCd": activity_code,
        "assetCd": "",
        "captchaAnswer": "",
    }

    response = _request_with_retry("post", url, session, data=payload)
    if response is None:
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    section = soup.find("section", class_="main-reports")
    cards = section.find_all("div", class_="card") if section else []

    all_tables_json = {}
    for card in cards:
        button = card.find("button", class_="btn-link")
        section_name = button.get_text(strip=True) if button else "Unnamed Section"
        tables = card.find_all("table")
        tables_data = [_table_to_json(t, section_name=section_name) for t in tables]
        if tables_data:
            all_tables_json[section_name] = (
                tables_data if len(tables_data) > 1 else tables_data[0]
            )

    return all_tables_json


def _processActivity(row):
    """
    Fetch detail for one activity. Returns activity_code on completion.
    """
    activity_code = row["activity_code"]
    plan_code = row["plan_code"]
    village_code = row["village_code"]
    session = _get_session()

    try:
        activityData = _extractSingleData(activity_code, session)
        if activityData is None:
            with _stats_lock:
                _stats["failed"] += 1
            _db_queue.put((activity_code, None, False))  # mark done, nothing to insert
            return activity_code

        # ---- post-processing ----
        has_data = False
        if "Activity Details" in activityData:
            merged_activity_details = {}
            for section in activityData["Activity Details"]:
                for detail in section:
                    merged_activity_details.update(detail)
            activityData["Activity Details"] = [merged_activity_details]

            for item in activityData["Activity Details"]:
                item["village_code"] = village_code
                item["plan_code"] = plan_code
                item["activity_code"] = activity_code
            has_data = True

        if "Fund Allocation" in activityData:
            for item in activityData["Fund Allocation"]:
                item["village_code"] = village_code
                item["plan_code"] = plan_code
                item["activity_code"] = activity_code
            has_data = True

        for optional_section in (
            "Technical Approval Details",
            "Administrative Approval Details",
            "Physical Progress Details",
        ):
            if optional_section in activityData:
                for item in activityData[optional_section]:
                    item["village_code"] = village_code
                    item["plan_code"] = plan_code
                    item["activity_code"] = activity_code
                has_data = True

        is_empty = not has_data
        _db_queue.put((activity_code, activityData, is_empty))
        return activity_code

    except Exception as e:
        print(f"[ERROR] activity={activity_code} -> {e}")
        with _stats_lock:
            _stats["failed"] += 1
        _db_queue.put((activity_code, None, False))
        return activity_code


# ---------------------------------------------------------
# Main entry point (same signature as original)
# ---------------------------------------------------------
def extractData():
    # Pehle tables create kar lo takki har activity insert me CREATE TABLE na chale
    View_Activity_Details_Report_DB.ensureTables()

    # Yaha par hum voucher wise summary report se activity codes le rahe hain
    data = Voucher_Wise_Summary_Report_DB.getActivityCodes()
    total = len(data)
    
    if total == 0:
        return

    print(f"View Activity Details: {total} activities to process (parallel, {MAX_WORKERS} workers)")

    db_thread = threading.Thread(target=_db_writer_worker, daemon=True)
    db_thread.start()

    done = 0

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    futures = {executor.submit(_processActivity, row): row for row in data}

    for future in as_completed(futures):
        done += 1
        try:
            future.result()
        except Exception as e:
            print(f"[ERROR] Worker exception -> {e}")

        if done % 100 == 0 or done == total:
            with _stats_lock:
                saved  = _stats["db_saved"]
                empty  = _stats["empty"]
                failed = _stats["failed"]
            q_pending = _db_queue.qsize()
            print(
                f"[PROGRESS] Scraped {done}/{total} | "
                f"DB Saved: {saved} | Empty: {empty} | "
                f"Failed: {failed} | Queue: {q_pending} pending"
            )

    executor.shutdown(wait=True)

    print("All activities scraped. Waiting for DB queue to flush...")
    _db_queue.put(None)
    db_thread.join()

    with _stats_lock:
        print(
            f"\n[DONE] View Activity Details finished."
            f" Total={total} | DB Saved={_stats['db_saved']}"
            f" | Empty={_stats['empty']} | Failed={_stats['failed']}"
        )


if __name__ == "__main__":
    try:
        extractData()
    except KeyboardInterrupt:
        print("\n[STOP] Script forcefully interrupted by user! Exiting immediately...")
        import os
        os._exit(1)

