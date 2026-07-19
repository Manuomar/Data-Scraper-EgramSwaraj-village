"""
Voucher_Wise_Summary_Report_fast.py  (STANDALONE VOUCHER-ONLY MODULE)
=======================================================================
Sirf voucher-wise payment data nikalta hai, per village, per month.

Flow (Optimized):
  1. Approved_Action_Plan_Report_DB.getData2()  -> un villages ki list jinka voucher data fetch nahi hua
  2. Villages parallel me process hote hain (global ThreadPoolExecutor).
  3. Har village sequentially apne months aur vouchers fetch karta hai using pooled requests.Session.
  4. Sab data DB queue me jata hai.
  5. Asynchronous DB background thread data ko insert karta hai aur village ko done mark karta hai.
"""

import requests
from bs4 import BeautifulSoup
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue

from mysql_connector import Approved_Action_Plan_Report_DB
from mysql_connector import Voucher_Wise_Summary_Report_DB
from rate_limiter import RateLimiter

import config

# ---------------------------------------------------------
# TUNABLE SETTINGS
# ---------------------------------------------------------
# MAX_WORKERS = config.MAX_WORKERS          # thread count for parallel village processing
MAX_WORKERS = 75         # thread count for parallel village processing
REQUEST_TIMEOUT = 20      # seconds
MAX_RETRIES = 4           # retry count per failed request
RETRY_BACKOFF = 2         # seconds, multiplied by attempt number
INSERT_BATCH_SIZE = 500   # voucher records accumulate before writing to MySQL

# ---- SHARED, GLOBAL RATE LIMIT (safe hits/sec to the govt server) ----
REQUESTS_PER_SECOND = 35.0 # 8.0
BURST = 35 #8
_rate_limiter = RateLimiter(rate=REQUESTS_PER_SECOND, burst=BURST)

_thread_local = threading.local()
_success_counter = {"n": 0}
_counter_lock = threading.Lock()

# DB Queue for Background Insertion
# Items in queue: (village_code, data_list, is_complete)
# Sentinel to stop: None
_db_queue = queue.Queue()

# Months to pull vouchers for (financial year months, April=4 ... March=3)
MONTHS = config.VOUCHER_MONTHS


def _db_writer_worker():
    """Background thread to handle MySQL inserts asynchronously."""
    print("[DB_WORKER] Started DB writer thread.")
    while True:
        item = _db_queue.get()
        if item is None:
            _db_queue.task_done()
            break
            
        village_code, data_batch, is_complete = item
        
        try:
            if data_batch:
                Voucher_Wise_Summary_Report_DB.insertData(data_batch)
            
            if is_complete:
                Approved_Action_Plan_Report_DB.updateVoucherDetails(village_code)
                print(f"[DB_WORKER] Marked village {village_code} as COMPLETED in DB.")
        except Exception as e:
            print(f"[DB_WORKER] ERROR for village {village_code}: {e}")
            
        _db_queue.task_done()
        
    print("[DB_WORKER] DB writer thread stopped.")


def _get_session():
    """Returns a thread-local persistent requests.Session with connection pooling."""
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; DataCollector/1.0)"
        })
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

    print(f"[FAILED] {url} -> {last_exc}")
    return None


def _extractDataSingleDetails(singleUrl, session):
    url = f"https://egramswaraj.gov.in/{singleUrl}"
    response = _request_with_retry("get", url, session)
    if response is None:
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    data = {}

    for li in soup.select("ul.row_listing li"):
        left = li.select_one(".leftDiv")
        right = li.select_one(".rightDiv")
        if left and right:
            key = left.get_text(strip=True).replace(":", "")
            value = right.get_text(" | ", strip=True)
            data[key] = value

    table = soup.find("table", class_="formtable")
    if table:
        rows = table.find_all("tr")
        for row in rows:
            cols = row.find_all("td")
            if len(cols) == 4:
                key1 = cols[0].get_text(strip=True)
                val1 = cols[1].get_text(" | ", strip=True)
                key2 = cols[2].get_text(strip=True)
                val2 = cols[3].get_text(" | ", strip=True)
                if key1:
                    data[key1] = val1
                if key2:
                    data[key2] = val2
            elif len(cols) == 2:
                key = cols[0].get_text(strip=True)
                val = cols[1].get_text(" | ", strip=True)
                if key:
                    data[key] = val

    payments = []
    for row in soup.select("tr[data-ng-repeat]"):
        cols = row.find_all("td")
        if len(cols) == 4:
            payments.append({
                "mode": cols[0].get_text(strip=True),
                "details": cols[1].get_text(" | ", strip=True),
                "name": cols[2].get_text(strip=True),
                "amount": cols[3].get_text(strip=True),
            })
    data["payments"] = payments

    file_link = soup.find("a", onclick=True)
    if file_link:
        data["attached_file_name"] = file_link.get_text(strip=True)
        data["attached_file_href"] = file_link.get("href")
        onclick = file_link.get("onclick")
        match = re.search(r"'(FileRedirect\.jsp[^']+)'", onclick)
        if match:
            data["real_file_url"] = f"https://egramswaraj.gov.in/{match.group(1)}"

    data["voucher_details_url"] = url
    return data


def _extractVoucherList(fyear, planYear, month, row, session):
    url = "https://egramswaraj.gov.in/voucherWiseReport.do"
    payload = {
        "voucherWise": "Y",
        "finYear": planYear,
        "month": month,
        "schemewise": "P",
        "state": config.STATE_CODE,
        "district": row["district_code"],
        "block": row["block_code"],
        "village": row["village_code"],
        "schemeCode": -1,
    }

    response = _request_with_retry("get", url, session, params=payload)
    if response is None:
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    rows = soup.select("table tbody tr")

    hrefs = []
    for row1 in rows:
        cols = row1.find_all("td")
        row_data = []
        for col in cols:
            link = col.find("a")
            if link:
                row_data.append({"text": link.get_text(strip=True), "href": link.get("href")})
            else:
                row_data.append(col.get_text(strip=True))

        if not row_data or row_data[0] == "No Data Found!!":
            continue

        if len(row_data) > 5 and isinstance(row_data[5], dict) and "href" in row_data[5]:
            hrefs.append(row_data[5]["href"])

    return hrefs


def process_village(row):
    """Worker function to process all vouchers for a single village."""
    village_code = row["village_code"]
    session = _get_session()
    fyear = int(config.FINANCIAL_YEAR)
    planYear = f"{fyear}-{fyear + 1}"

    village_success = True
    voucher_data_batch = []

    for month in MONTHS:
        hrefs = _extractVoucherList(fyear, planYear, month, row, session)
        if hrefs is None:
            village_success = False
            print(f"[WARNING] Network error for village {village_code} on month {month}. Won't mark as done.")
            continue
            
        if hrefs:
            for href in hrefs:
                detail = _extractDataSingleDetails(href, session)
                if detail:
                    detail["village_code"] = village_code
                    voucher_data_batch.append(detail)
                    
                # Queue large batches to avoid massive memory usage per village
                if len(voucher_data_batch) >= INSERT_BATCH_SIZE:
                    _db_queue.put((village_code, voucher_data_batch, False))
                    voucher_data_batch = []

    # Done with this village: queue the remaining data and set is_complete flag
    # If village_success is False, we queue remaining data but is_complete is False
    _db_queue.put((village_code, voucher_data_batch, village_success))


def extractData(district_filter=None):
    """
    district_filter: District name string (e.g. 'Agra'), case-insensitive.
                      None = sab districts (poora UP).
    """
    data = Approved_Action_Plan_Report_DB.getData2(district_filter=district_filter)
    dataSetLen = len(data)

    if district_filter:
        print(f"[DISTRICT FILTER] '{district_filter}' -> {dataSetLen} villages to process")
    else:
        print(f"Voucher-Wise Summary: {dataSetLen} villages to process")

    if dataSetLen == 0:
        return

    # Start DB writer thread
    db_thread = threading.Thread(target=_db_writer_worker, daemon=True)
    db_thread.start()

    print(f"Starting village-level ThreadPoolExecutor with {MAX_WORKERS} workers...")
    
    completed_count = 0
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    futures = {executor.submit(process_village, row): row for row in data}
    
    for future in as_completed(futures):
        row = futures[future]
        completed_count += 1
        try:
            future.result()
            if completed_count % 10 == 0 or completed_count == dataSetLen:
                print(f"[PROGRESS] Completed processing {completed_count}/{dataSetLen} villages")
        except Exception as e:
            print(f"[ERROR] Exception processing village {row['village_code']}: {e}")

    executor.shutdown(wait=True)

    # Stop DB writer thread
    print("All villages scraped. Waiting for DB queue to finish inserts...")
    _db_queue.put(None)
    db_thread.join()
    
    print("Voucher-Wise Summary done.")


if __name__ == "__main__":
    try:
        extractData()
    except KeyboardInterrupt:
        print("\n[STOP] Script forcefully interrupted by user! Exiting immediately...")
        import os
        os._exit(1)

