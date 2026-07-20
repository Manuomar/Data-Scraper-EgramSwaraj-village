"""
M_ActionSoft_Photo_Uploaded_Report_fast.py  (VOUCHER MODULE VERSION)
======================================================================
Multi-threaded scraper for M-ActionSoft Photo Uploaded Report.

Optimized:
  - Global ThreadPoolExecutor
  - Persistent connection pooling (HTTPAdapter)
  - Asynchronous DB queue for non-blocking inserts
"""

import time
import threading
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue

import requests
from bs4 import BeautifulSoup

from mysql_connector import Approved_Action_Plan_Report_DB
from mysql_connector import M_ActionSoft_Photo_Uploaded_Report_DB
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
INSERT_BATCH_SIZE = 300

# ---------------------------------------------------------
# Shared rate limiter + per-thread session
# ---------------------------------------------------------
_rate_limiter = RateLimiter(rate=REQUESTS_PER_SECOND, burst=BURST)
_thread_local = threading.local()
_success_counter = {"n": 0}
_counter_lock = threading.Lock()

_stats = {"db_saved": 0, "empty": 0, "failed": 0}
_stats_lock = threading.Lock()

_db_queue = queue.Queue(maxsize=MAX_WORKERS * 4)


def _db_writer_worker():
    """Background thread to handle MySQL inserts asynchronously."""
    print("[DB_WORKER] Started Photo Upload DB writer thread.")
    conn = Approved_Action_Plan_Report_DB._connect()
    
    while True:
        item = _db_queue.get()
        if item is None:
            _db_queue.task_done()
            break
            
        village_code, records = item
        
        try:
            if records is not None:
                if records:
                    M_ActionSoft_Photo_Uploaded_Report_DB.insertData(records, conn=conn)
                    with _stats_lock:
                        _stats["db_saved"] += len(records)
                else:
                    with _stats_lock:
                        _stats["empty"] += 1
                
                # Always mark as fetched to avoid re-fetching broken pages (only if no network error)
                Approved_Action_Plan_Report_DB.updatePhotoUploadedData(village_code, conn=conn)
        except Exception as e:
            print(f"[DB_WORKER] ERROR for village {village_code}: {e}")
            
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
# Core extraction (identical logic to parent module)
# ---------------------------------------------------------
def _extractVillageLevelData(fyear, gpCode, session):
    url = "https://egramswaraj.gov.in/photoUploadedPlanYearWise.do"
    payload = {
        "fyear": fyear,
        "lbType": "99",
        "gpCode": gpCode,
    }

    response = _request_with_retry("post", url, session, data=payload)
    if response is None:
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table", {"class": "table table-striped table-bordered table-hover"})
    if not table:
        return []

    headers = [th.get_text(strip=True) for th in table.find_all("th")]
    data = []

    for tr in table.find_all("tr")[2:]:  # Skip info row + header row
        tds = tr.find_all("td")
        if len(tds) < len(headers):
            continue

        activity_text = tds[1].get_text(strip=True)
        match = re.match(r"\((\d+)\)(.+)", activity_text)
        activity_id = match.group(1) if match else ""
        activity_name = match.group(2).strip() if match else activity_text

        img_tag = tds[7].find("img")
        img_url = "https://egramswaraj.gov.in/" + img_tag["src"] if img_tag else None

        row = {
            headers[0]: tds[0].get_text(strip=True),
            "Activity ID": activity_id,
            headers[1]: activity_name,
            headers[2]: tds[2].get_text(strip=True),
            headers[3]: tds[3].get_text(strip=True),
            headers[4]: tds[4].get_text(strip=True),
            headers[5]: tds[5].get_text(strip=True),
            headers[6]: tds[6].get_text(strip=True),
            headers[7]: {
                "status": tds[7].get_text(strip=True),
                "image_url": img_url,
            },
            headers[8]: tds[8].get_text(strip=True),
        }
        data.append(row)

    return data


def _processVillage(row):
    """Fetch photo-upload data for one village. Returns (village_code, records)."""
    fyear = config.FINANCIAL_YEAR
    village_code = row["village_code"]
    session = _get_session()

    try:
        records = _extractVillageLevelData(fyear, village_code, session)
        if records is None:
            with _stats_lock:
                _stats["failed"] += 1
            _db_queue.put((village_code, None))
            return village_code
            
        for item in records:
            item["fyear"] = fyear
            item["village_code"] = village_code
        
        _db_queue.put((village_code, records))
        return village_code
    except Exception as e:
        print(f"[ERROR] village={village_code} -> {e}")
        with _stats_lock:
            _stats["failed"] += 1
        _db_queue.put((village_code, None))
        return village_code


# ---------------------------------------------------------
# Main entry point
# ---------------------------------------------------------
def extractData(district_filter=None):
    """
    district_filter: District name string (e.g. 'Agra'), case-insensitive.
                      None = sab districts (poora UP).
    """
    M_ActionSoft_Photo_Uploaded_Report_DB.ensureTable()
    
    villageDataSet = Approved_Action_Plan_Report_DB.getVillageData(district_filter=district_filter)
    total = len(villageDataSet)
    
    if total == 0:
        return

    if district_filter:
        print(f"[DISTRICT FILTER] '{district_filter}' -> {total} villages to process")
    else:
        print(f"Photo Upload report: {total} villages to process (parallel, {MAX_WORKERS} workers)")

    db_thread = threading.Thread(target=_db_writer_worker, daemon=True)
    db_thread.start()

    completed_count = 0

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    futures = {executor.submit(_processVillage, row): row for row in villageDataSet}

    for future in as_completed(futures):
        row = futures[future]
        completed_count += 1
        try:
            future.result()
        except Exception as e:
            print(f"[ERROR] Exception processing village {row['village_code']}: {e}")
            with _stats_lock:
                _stats["failed"] += 1
                
        if completed_count % 10 == 0 or completed_count == total:
            with _stats_lock:
                saved = _stats["db_saved"]
                empty = _stats["empty"]
                failed = _stats["failed"]
            print(f"[PROGRESS] Completed processing {completed_count}/{total} villages | Saved: {saved} | Empty: {empty} | Failed: {failed}")

    executor.shutdown(wait=True)

    print("All villages scraped. Waiting for DB queue to finish inserts...")
    _db_queue.put(None)
    db_thread.join()

    with _stats_lock:
        print(f"\n[DONE] Photo Upload report done. Processed {total} villages | Saved: {_stats['db_saved']} | Empty: {_stats['empty']} | Failed: {_stats['failed']}")


if __name__ == "__main__":
    try:
        extractData(district_filter=config.DISTRICT_FILTER)
    except KeyboardInterrupt:
        print("\n[STOP] Script forcefully interrupted by user! Exiting immediately...")
        import os
        os._exit(1)

