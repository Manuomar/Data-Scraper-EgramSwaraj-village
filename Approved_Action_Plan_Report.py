"""
Approved_Action_Plan_Report.py
======================================================================
Multi-threaded scraper for Approved Action Plan Report.

Optimized:
  - Global ThreadPoolExecutor
  - Persistent connection pooling (HTTPAdapter)
  - Asynchronous DB queue for non-blocking inserts
"""

import requests
from bs4 import BeautifulSoup
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue

from mysql_connector import Approved_Action_Plan_Report_DB
from rate_limiter import RateLimiter
import config

MAX_WORKERS = config.MAX_WORKERS  # thread count for parallel block-level requests

_rate_limiter = RateLimiter(rate=8.0, burst=8)
_thread_local = threading.local()

_db_queue = queue.Queue()


def _db_writer_worker():
    """Background thread to handle MySQL inserts asynchronously."""
    print("[DB_WORKER] Started Approved Action Plan DB writer thread.")
    while True:
        item = _db_queue.get()
        if item is None:
            _db_queue.task_done()
            break
            
        rows = item
        
        try:
            if rows:
                Approved_Action_Plan_Report_DB.insertData(rows)
        except Exception as e:
            print(f"[DB_WORKER] ERROR inserting rows: {e}")
            
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


def _post_with_retry(session, url, data, max_retries=4, timeout=30):
    last_exc = None
    for attempt in range(1, max_retries + 1):
        _rate_limiter.acquire()
        try:
            resp = session.post(url, data=data, timeout=timeout)
            if resp.status_code in (429, 503):
                _rate_limiter.penalize(penalty_seconds=15 * attempt)
                last_exc = Exception(f"HTTP {resp.status_code} (rate limited)")
                continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_exc = e
            if attempt < max_retries:
                time.sleep(2 * attempt)
    print(f"[FAILED] POST {url} -> {last_exc}")
    return None


def _extractBlockLevelData():
    url = "https://egramswaraj.gov.in/approveActionPlanData.do"

    payload = {
        "state_code": config.STATE_CODE,
        "plan_year": getattr(config, "VILLAGE_FETCH_YEAR", config.FINANCIAL_YEAR),
        "status_Level": "3",
        "accordionflg": "3",
    }

    session = _get_session()
    response = _post_with_retry(session, url, payload)
    if response is None:
        return []

    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table", id="statewise-report")
    if not table:
        return []
    rows = table.find_all("tr")[2:]  # Skip header rows

    data = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 6:
            continue

        onclick_attr = cols[5].find("a")["onclick"]

        match = re.search(
            r"getgpreport\('(\d+)',(\d+),'(\d+)','(\d+)','(\d+)',(\d+)\)", onclick_attr
        )
        if match:
            (
                state_code,
                status_code,
                year,
                district_code,
                block_code,
                local_body_typ_cd,
            ) = match.groups()

            row_data = {
                "s_no": cols[0].get_text(strip=True),
                "district_panchayat": cols[1].get_text(strip=True),
                "block_panchayat": cols[2].get_text(strip=True),
                "main_plan_approved": cols[3].get_text(strip=True),
                "supplementary_plan_approved": cols[4].get_text(strip=True),
                "total_approved": cols[5].get_text(strip=True),
                "state_code": state_code,
                "status_Level": status_code,
                "plan_year": year,
                "zp_code": district_code,
                "bp_code": block_code,
                "local_body_typ_cd": local_body_typ_cd,
            }

            data.append(row_data)

    return data


def _extractVillageLevelData(zp_code, bp_code, session):
    url = "https://egramswaraj.gov.in/approveActionPlanData.do"

    payload = {
        "state_code": config.STATE_CODE,
        "plan_year": getattr(config, "VILLAGE_FETCH_YEAR", config.FINANCIAL_YEAR),
        "status_Level": "3",
        "accordionflg": "3",
        "zp_code": zp_code,
        "bp_code": bp_code,
    }

    response = _post_with_retry(session, url, payload)
    if response is None:
        return []

    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table", {"id": "statewise-report"})
    if not table:
        return []

    rows = table.find_all("tr")[2:]

    data = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 8:
            continue

        s_no = cols[0].get_text(strip=True)
        district = cols[1].get_text(strip=True)
        block = cols[2].get_text(strip=True)
        village = cols[3].get_text(strip=True)
        village_code = cols[4].get_text(strip=True)

        plan_raw = cols[5].get_text(strip=True)
        match = re.match(r"(\d+)\((.*?) Plan\)", plan_raw)
        if match:
            plan_code = int(match.group(1))
            plan_type = match.group(2)
        else:
            plan_code = None
            plan_type = None

        data.append(
            {
                "s_no": int(s_no),
                "district_panchayat": district,
                "block_panchayat": block,
                "village_panchayat": village,
                "village_code": int(village_code),
                "plan_code": plan_code,
                "plan": plan_type,
            }
        )

    return data


def _processBlock(blockRow):
    """Fetch villages for one block and return formatted rows (does NOT insert)."""
    zp_code = blockRow["zp_code"]
    bp_code = blockRow["bp_code"]
    session = _get_session()

    try:
        villageDataSet = _extractVillageLevelData(zp_code, bp_code, session)
    except Exception as e:
        print(f"[ERROR] block {blockRow['block_panchayat']} ({bp_code}) -> {e}")
        return []

    rows = []
    for villageRow in villageDataSet:
        rows.append({
            "plan_year": blockRow["plan_year"],
            "district": blockRow["district_panchayat"],
            "district_code": blockRow["zp_code"],
            "block": blockRow["block_panchayat"],
            "block_code": blockRow["bp_code"],
            "village_panchayat": villageRow["village_panchayat"],
            "village_code": villageRow["village_code"],
            "plan_code": villageRow["plan_code"],
            "plan": villageRow["plan"],
        })
        
    _db_queue.put(rows)
    return rows


def extractAndInsertData(limit_blocks=None, district_filter=None):
    """
    Fetches block list, then for every block fetches its villages IN PARALLEL,
    inserting into DB incrementally (block-by-block) as results come in.

    limit_blocks:    First N blocks (for quick test). None = all.
    district_filter: District name string to process only one district,
                     e.g. 'Agra'. None = all districts (full UP run).
                     Case-insensitive match on district_panchayat column.
    """
    blockDataSet = _extractBlockLevelData()
    
    if not blockDataSet:
        print("No blocks found. Exiting.")
        return

    if district_filter:
        before = len(blockDataSet)
        blockDataSet = [
            b for b in blockDataSet
            if b["district_panchayat"].strip().lower() == district_filter.strip().lower()
        ]
        print(f"[DISTRICT FILTER] '{district_filter}' -> {len(blockDataSet)} blocks "
              f"(out of {before} total)")

    try:
        inserted_blocks = Approved_Action_Plan_Report_DB.getInsertedBlockCodes()
    except Exception as e:
        print(f"[WARNING] Could not fetch inserted blocks (maybe table doesn't exist yet): {e}")
        inserted_blocks = set()

    if inserted_blocks:
        before_resume = len(blockDataSet)
        blockDataSet = [b for b in blockDataSet if int(b["bp_code"]) not in inserted_blocks]
        print(f"[RESUME] Skipped {before_resume - len(blockDataSet)} already processed blocks. {len(blockDataSet)} blocks remaining.")

    if limit_blocks:
        print(f"[TEST MODE] limiting to first {limit_blocks} block(s) out of {len(blockDataSet)} found.")
        blockDataSet = blockDataSet[:limit_blocks]

    total = len(blockDataSet)
    if total == 0:
        print("No remaining blocks to process.")
        return
        
    print(f"Found {total} blocks. Fetching villages (parallel, incremental insert)...")

    db_thread = threading.Thread(target=_db_writer_worker, daemon=True)
    db_thread.start()

    done = 0
    total_villages = 0

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    future_to_block = {executor.submit(_processBlock, b): b for b in blockDataSet}

    for future in as_completed(future_to_block):
        block = future_to_block[future]
        done += 1
        try:
            rows = future.result()
        except Exception as e:
            print(f"[ERROR] {block['block_panchayat']} -> {e}")
            rows = []

        if rows:
            total_villages += len(rows)

        print(f"[{done}/{total}] {block['district_panchayat']} / {block['block_panchayat']} "
              f"-> {len(rows)} villages (total so far: {total_villages})")

    executor.shutdown(wait=True)

    print("All blocks scraped. Waiting for DB queue to finish inserts...")
    _db_queue.put(None)
    db_thread.join()
    print(f"Done. Inserted {total_villages} village rows across {total} blocks.")


# Used by run_step1_get_villages.py
def extractData(limit_blocks=None, district_filter=None):
    # This function is synchronous but using _processBlock will queue items to a non-existent thread 
    # if we don't handle it. However, run_step1_get_villages.py calls extractAndInsertData, not this directly typically.
    # To be safe, we will just use the old synchronous pattern for this specific return-based function 
    # since it's only meant to return data, not insert it.
    
    blockDataSet = _extractBlockLevelData()

    if district_filter:
        before = len(blockDataSet)
        blockDataSet = [
            b for b in blockDataSet
            if b["district_panchayat"].strip().lower() == district_filter.strip().lower()
        ]
        print(f"[DISTRICT FILTER] '{district_filter}' -> {len(blockDataSet)} blocks "
              f"(out of {before} total)")

    if limit_blocks:
        print(f"[TEST MODE] limiting to first {limit_blocks} block(s) out of {len(blockDataSet)} found.")
        blockDataSet = blockDataSet[:limit_blocks]

    ApprovedPlanData = []
    
    # Do not use _processBlock directly here to avoid queuing to dead DB thread
    for blockRow in blockDataSet:
        zp_code = blockRow["zp_code"]
        bp_code = blockRow["bp_code"]
        session = _get_session()
        try:
            villageDataSet = _extractVillageLevelData(zp_code, bp_code, session)
        except Exception as e:
            print(f"[ERROR] block {blockRow['block_panchayat']} ({bp_code}) -> {e}")
            continue

        for villageRow in villageDataSet:
            ApprovedPlanData.append({
                "plan_year": blockRow["plan_year"],
                "district": blockRow["district_panchayat"],
                "district_code": blockRow["zp_code"],
                "block": blockRow["block_panchayat"],
                "block_code": blockRow["bp_code"],
                "village_panchayat": villageRow["village_panchayat"],
                "village_code": villageRow["village_code"],
                "plan_code": villageRow["plan_code"],
                "plan": villageRow["plan"],
            })
            
    return ApprovedPlanData
