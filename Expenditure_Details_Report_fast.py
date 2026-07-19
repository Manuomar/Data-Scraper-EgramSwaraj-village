"""
Expenditure_Details_Report_fast.py  (EGRAMSWARAJ VERSION)
===============================================================
Fetches Expenditure Details section directly from egramswaraj 
View Activity Details API.

Optimized:
  - Global ThreadPoolExecutor
  - Persistent connection pooling (HTTPAdapter)
  - Asynchronous DB queue for non-blocking inserts
"""

import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue

import requests
from bs4 import BeautifulSoup

from mysql_connector import Voucher_Wise_Summary_Report_DB
from mysql_connector import Expenditure_Details_Report_DB
from rate_limiter import RateLimiter
import config

# ---------------------------------------------------------
# TUNABLE SETTINGS
# ---------------------------------------------------------
MAX_WORKERS = config.MAX_WORKERS
REQUEST_TIMEOUT = 25
MAX_RETRIES = 4
RETRY_BACKOFF = 2

REQUESTS_PER_SECOND = 8.0
BURST = 8

# ---------------------------------------------------------
# Shared rate limiter + per-thread session
# ---------------------------------------------------------
_rate_limiter = RateLimiter(rate=REQUESTS_PER_SECOND, burst=BURST)
_thread_local = threading.local()
_success_counter = {"n": 0}
_counter_lock = threading.Lock()

_db_queue = queue.Queue()


def _db_writer_worker():
    """Background thread to handle MySQL inserts asynchronously."""
    print("[DB_WORKER] Started Expenditure DB writer thread.")
    while True:
        item = _db_queue.get()
        if item is None:
            _db_queue.task_done()
            break
            
        activity_code, activity_data = item
        
        try:
            if activity_data is not None and len(activity_data.get("Expenditure Details", [])) > 0:
                Expenditure_Details_Report_DB.insertData(activity_data)
            
            # Always mark as fetched to avoid re-fetching broken pages
            Voucher_Wise_Summary_Report_DB.updateExpenditureDetailsFetched(activity_code)
        except Exception as e:
            print(f"[DB_WORKER] ERROR for activity {activity_code}: {e}")
            
        _db_queue.task_done()
        
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
            text = cell.get_text(separator=" ", strip=True)
            row_data[key] = text
        
        # Only add rows that have actual data
        if any(row_data.values()):
            table_data.append(row_data)

    return table_data


def _extractExpenditureData(activity_code, session):
    """Egramswaraj ke same URL se expenditure details fetch karta hai."""
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

    expenditure_data = {"Expenditure Details": []}
    
    for card in cards:
        button = card.find("button", class_="btn-link")
        section_name = button.get_text(strip=True) if button else ""
        
        # We only care about the Expenditure Details section
        if "Expenditure Details" in section_name:
            tables = card.find_all("table")
            for table in tables:
                parsed_table = _table_to_json(table, section_name=section_name)
                expenditure_data["Expenditure Details"].extend(parsed_table)
            break
            
    return expenditure_data


def _processActivity(row):
    """
    Ek activity ka expenditure fetch karta hai.
    """
    activity_code = row["activity_code"]
    plan_code = row["plan_code"]
    village_code = row.get("village_code")
    session = _get_session()

    try:
        activityData = _extractExpenditureData(activity_code, session)
        if activityData is None:
            _db_queue.put((activity_code, None))
            return activity_code

        if "Expenditure Details" in activityData:
            for item in activityData["Expenditure Details"]:
                item["village_code"] = village_code if village_code else 0
                item["plan_code"] = plan_code
                item["activity_code"] = activity_code

        _db_queue.put((activity_code, activityData))
        return activity_code

    except Exception as e:
        print(f"[ERROR] expenditure_activity={activity_code} -> {e}")
        _db_queue.put((activity_code, None))
        return activity_code


def extractData():
    """
    Voucher_Wise_Summary_Report table se pending activity codes uthata hai
    aur unka expenditure detail fetch karke DB mein daalta hai.
    """
    # Create empty table just in case there are no pending activities
    Expenditure_Details_Report_DB.insertData({})
    
    data = Voucher_Wise_Summary_Report_DB.getExpenditurePendingActivities()
    total = len(data)
    
    if total == 0:
        return

    print(f"Expenditure Details (EgramSwaraj): {total} activities to process (parallel, {MAX_WORKERS} workers)")

    db_thread = threading.Thread(target=_db_writer_worker, daemon=True)
    db_thread.start()

    done = 0

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    futures = {executor.submit(_processActivity, row): row for row in data}

    for future in as_completed(futures):
        done += 1
        try:
            activity_code = future.result()
        except Exception as e:
            activity_code = "Unknown"
            print(f"[ERROR] Worker exception -> {e}")

        remaining = total - done
        if done % 10 == 0 or remaining < 10:
            print(f"[{remaining} left] Completed scraping expenditure_activity={activity_code}")

    executor.shutdown(wait=True)

    print("All activities scraped. Waiting for DB queue to finish inserts...")
    _db_queue.put(None)
    db_thread.join()

    print(f"Expenditure Details (EgramSwaraj) done. Processed {total} activities.")


if __name__ == "__main__":
    try:
        extractData()
    except KeyboardInterrupt:
        print("\n[STOP] Script forcefully interrupted by user! Exiting immediately...")
        import os
        os._exit(1)

