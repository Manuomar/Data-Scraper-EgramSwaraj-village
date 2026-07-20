# eGramSwaraj Data Scraper — Voucher Module

**Step 1 (Villages & Action Plan)**, **Step 2 (Photo Uploaded Report)**, **Step 3 (Voucher-Wise Summary)**, **Step 4 (Activity Details)**, aur **Step 5 (Expenditure Details)** ko fetch karta hai.

---

## Folder Structure

```text
DataScrap EgramSwaraj/
│
├── config.py                                  ← MASTER CONFIG: DB, district filter, months, workers
├── run_full_pipeline.py                       ← MAIN SCRIPT: Saare steps sequentially chalata hai
│
├── run_step1_get_villages.py                  ← STEP 1: Village list & Approved Action Plan fetcher
├── run_voucher_pipeline.py                    ← STEP 3: Voucher pipeline runner (wrapper)
├── M_ActionSoft_Photo_Uploaded_Report_fast.py ← STEP 2: Photo Uploaded Report fetcher
├── Voucher_Wise_Summary_Report_fast.py        ← STEP 3: Voucher scraping logic (main)
├── View_Activity_Details_Report_fast.py       ← STEP 4: Activity Details scraping logic
├── Expenditure_Details_Report_fast.py         ← STEP 5: Expenditure Details scraping logic
├── rate_limiter.py                            ← Shared token-bucket rate limiter
│
└── mysql_connector/
    ├── __init__.py
    ├── Approved_Action_Plan_Report_DB.py      ← Step 1 DB: CREATE + INSERT + status update
    ├── Voucher_Wise_Summary_Report_DB.py      ← Step 3 DB: CREATE + INSERT + resume tracking
    ├── View_Activity_Details_Report_DB.py     ← Step 4 DB: 5 tables + INSERT (activity, fund, approvals, progress)
    └── Expenditure_Details_Report_DB.py       ← Step 5 DB: expenditure table + INSERT
```

---

## Setup & Configuration (`config.py`)

Sabse pehle `config.py` file open karo aur apni settings set karo:

| Setting | Description |
|---------|-------------|
| `DB_CONFIG` | MySQL host, user, password, database name |
| `STATE_CODE` | State code (9 = UP) |
| `FINANCIAL_YEAR` | e.g. `"2026"` for FY 2026-27 |
| `DISTRICT_FILTER` | `"Agra"` for one district, `None` for full state |
| `VOUCHER_MONTHS` | e.g. `[4, 5, 6, 7]` (April to July) |
| `MAX_WORKERS` | Thread count for parallel execution (default: 25) |
| `TEST_MODE` | `True` for 1-block test run |

---

## Kaise Chalao (How to Run)

`config.py` set karne ke baad, poori pipeline ek command se chala do:

```bash
cd "DataScrap EgramSwaraj"
python run_full_pipeline.py
```

### Pipeline Steps

| Step | Script | Kya karta hai |
|------|--------|---------------|
| 1 | `run_step1_get_villages.py` | Villages fetch karta hai → `approved_action_plan_report` table |
| 2 | `M_ActionSoft_Photo_Uploaded_Report_fast.py` | Photo upload data fetch karta hai *(Step 2 currently skipped in pipeline)* |
| 3 | `run_voucher_pipeline.py` | Voucher-wise payment data → `voucher_wise_summary_report` table |
| 4 | `View_Activity_Details_Report_fast.py` | Activity details (5 tables) → DB |
| 5 | `Expenditure_Details_Report_fast.py` | Expenditure records → `view_activity_details_expenditure_details` |

---

## Progress Output (kya dikhega)

Har script run hote waqt console (terminal) par yeh format mein real-time progress dikhayega:

```
[PROGRESS] Scraped 500/525608 | DB Saved: 487 | Empty: 8 | Failed: 5 | Queue: 12 pending
```

**Iska matlab kya hai?**
- **DB Saved:** Actual data jo successful raha aur successfully MySQL mein save ho gaya.
- **Empty:** Wo activities jinka request website pe gaya, lekin server ne koi data nahi diya (blank page / no records found). Yeh normal hai.
- **Failed:** Network error, Server Timeout (503/504), ya crash jiski wajah se website connect hi nahi hui. *(Important: Yeh failed entries DB mein 'done' mark nahi hoti, dobara run karne pe script inhi se shuru hogi).*
- **Queue pending:** Scraping threads kitni tezi se memory mein data bhar rahe hain jo abhi DB mein insert hona baaki hai. (Strict limit `80` rakhi gayi hai taaki memory full na ho, pehle ye lakhon me chali jati thi jisse crash hone pe data loss hota tha).

Aur script khatam hone pe final summary:
```
[DONE] View Activity Details finished. Total=525608 | DB Saved=510000 | Empty=10000 | Failed=5608
```

---

## Crash-Safe Design & Zero Data Loss

Script beech mein rokne par (`Ctrl+C` se terminate karne par) ya server crash hone par **koi data loss nahi hoga:**

- `os._exit(1)` instantly terminate karta hai taaki waiting queue freeze na ho.
- Jo activities/villages DB mein successfully gayi aur jinka response mila (chahe blank ho), wahi `fetched = 1` mark hongi.
- Jo network error ki wajah se fail hui (Failed count), unko `fetched` mark nahi kiya jayega. Dobara chalane pe directly wahin se attempt karegi.
- `INSERT IGNORE` + `UNIQUE KEY` lagayi gayi hai, isliye duplicate records kabhi nahi banenge.

---

## Architecture & Optimizations (Technical Upgrade)

Ye scripts highly optimized multi-threaded architecture par design ki gayi hain:

1. **Persistent Connection Pooling:** Har DB worker ek single MySQL connection open rakhta hai (poori script ke liye). Isse connection open/close hone ka delay completely khatam ho gaya.
2. **Consumer-Side Batching (Bulk Insert):** DB worker ab memory queue se ek ek activity insert nahi karta. Wo queue se ek baar mein **300 activities** ka batch uthata hai, aur MySQL ko **bulk insert (`executemany`)** bhejta hai. Jo process pehle 1800 queries leti thi, ab wo sirf 6 bulk queries me puri ho jati hai.
3. **Bounded Backpressure Queue:** `queue.Queue(maxsize = MAX_WORKERS * 4)` lagaya gaya hai. Agar DB insert time le, toh fast scraping threads temporarily ruk jayenge. Memory bloat / OOM issue permanently fixed.
4. **Token Bucket Rate Limiter (`rate_limiter.py`):** Server 429/503 de toh threads slow down hote hain (penalty), aur successful hone par dobara speed badha lete hain.

---

## DB Tables Created

| Script | Tables |
|--------|--------|
| Step 1 | `approved_action_plan_report` |
| Step 3 | `voucher_wise_summary_report`, `voucher_wise_summary_report_payment` |
| Step 4 | `view_activity_details_activity_details`, `view_activity_details_fund_allocation`, `view_activity_details_technical_approval_details`, `view_activity_details_administrative_approval_details`, `view_activity_details_physical_progress_details` |
| Step 5 | `view_activity_details_expenditure_details` |
| Step 2 | `m_actionsoft_photo_uploaded_report` |

---

## Individual Scripts Chalana (Debug ke liye)

Agar aapko manually koi ek step chalana ho:

```bash
# Sirf Step 1: Villages List
python run_step1_get_villages.py

# Sirf Step 3: Voucher Summary
python run_voucher_pipeline.py

# Sirf Step 4: Activity Details
python View_Activity_Details_Report_fast.py

# Sirf Step 5: Expenditure Details
python Expenditure_Details_Report_fast.py
```

---

## Bug Fix History (Recent Core Updates)

| Feature / Fix | Impact |
|---------------|--------|
| **Consumer-Side DB Batching** | MySQL Bulk inserts (`executemany`). Queue block (80 pending) issue resolved, inserts happen instantly in batches of 300. |
| **Strict Network Failure Logic** | If server returns `None` (timeout), script no longer marks it as fetched. Prevents silent data-loss on crashes. |
| **Voucher Sub-failure Logic** | Agar kisi ek village ke andar 1 particular voucher detail fail ho, toh poora village fail mark hoga taaki dobara load ho sake. |
| **Photo Uploaded Upgrade** | Step 2 (M_ActionSoft) ab baaki files jaisa same Fast Architecture (persistent connection, bounded queue, real-time stats) use karta hai. |
| **Bounded Queue (`maxsize`)** | Prevents RAM overload, if killed by user `Ctrl+C`, max 80 unprocessed items are dropped instead of 50,000+. |
| **`ensureTables()` Optimization** | Removes Lakhs of redundant `CREATE TABLE IF NOT EXISTS` commands during extraction. |
