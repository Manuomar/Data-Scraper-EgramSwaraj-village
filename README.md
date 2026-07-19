# Voucher Module

**Step 1 (Villages & Action Plan)**, **Step 2 (Photo Uploaded Report)**, **Step 3 (Voucher-Wise Summary)**, **Step 4 (Activity Details)**, aur **Step 5 (Expenditure Details)** ko fetch karta hai.
 
## Folder Structure

```text
voucher_module/
│
├── config.py                               ← MASTER CONFIG: DB settings, district filter, test mode, months, etc.
├── run_full_pipeline.py                    ← MAIN SCRIPT: Yeh saare steps ek saath sequentially chalata hai
│
├── run_step1_get_villages.py               ← STEP 1: Village list & Approved Action Plan fetcher
├── M_ActionSoft_Photo_Uploaded_Report_fast.py ← STEP 2: Photo Uploaded Report fetcher
├── run_voucher_pipeline.py                 ← STEP 3: Voucher Wise Summary fetcher
├── run_activity_details.py                 ← STEP 4 runner (wrapper)
│
├── Approved_Action_Plan_Report.py          ← Step 1 ka scraping logic
├── Voucher_Wise_Summary_Report_fast.py     ← Step 3 ka scraping logic
├── View_Activity_Details_Report_fast.py    ← Step 4 ka scraping logic
├── Expenditure_Details_Report_fast.py      ← Step 5 ka scraping logic
├── rate_limiter.py                         ← Shared token-bucket rate limiter
│
└── mysql_connector/
    ├── __init__.py
    ├── Approved_Action_Plan_Report_DB.py   ← Table CREATE + INSERT (Step 1 DB Logic)
    ├── Voucher_Wise_Summary_Report_DB.py   ← Voucher table CREATE + INSERT (Step 3 DB Logic)
    ├── View_Activity_Details_Report_DB.py  ← Activity Details table (Step 4 DB Logic)
    └── Expenditure_Details_Report_DB.py    ← Expenditure table CREATE + INSERT (Step 5 DB Logic)
```

## Setup & Configuration (`config.py`)

Sabse pehle `config.py` file ko open karo aur apni settings update karo. Ab alag-alag files mein modifications karne ki zaroorat nahi hai.

1. **Database Config:** Apna MySQL details (password, database name) `DB_CONFIG` mein set karo.
2. **Filters & State:** `STATE_CODE`, `FINANCIAL_YEAR`, aur `DISTRICT_FILTER` apne requirement ke hisaab se set karo.
    - Example: `DISTRICT_FILTER = "Agra"` (Sirf Agra district ke liye)
    - Example: `DISTRICT_FILTER = None` (Poore state ke liye)
3. **Voucher Months:** `VOUCHER_MONTHS` set karo. Default mein poora saal `[4, 5, 6, 7, 8, 9, 10, 11, 12, 1, 2, 3]` set hai.
4. **Performance & Test Mode:** `MAX_WORKERS` parallel execution ke liye. Pehli baar run karne se pehle `TEST_MODE = True` aur `TEST_BLOCK_LIMIT = 1` set karke check kar sakte ho (1 block test).

## Kaise Chalao (How to Run)

Ek baar `config.py` set hone ke baad, aap directly poori pipeline ek saath chala sakte ho:

```bash
cd "voucher_module"
python run_full_pipeline.py
```

### Yeh kya karega?
1. **STEP 1:** Pehle `run_step1_get_villages.py` execute hoga. Yeh `approved_action_plan_report` table banayega (agar nahi hai) aur villages ka data populate karega.
2. **STEP 2:** Fir `M_ActionSoft_Photo_Uploaded_Report_fast.py` chalega, jo un villages ke uploaded photos ki details fetch karega.
3. **STEP 3:** Phir `run_voucher_pipeline.py` chalega, jo sirf un villages ke vouchers layega jinke `get_voucher_details = 0` hai.
4. **STEP 4:** Fir `View_Activity_Details_Report_fast.py` (Activity Details) run hoga, vouchers ki specific details extract karne ke liye.
5. **STEP 5:** Last mein `Expenditure_Details_Report_fast.py` chalega, jo har activity ke expenditure details `view_activity_details_expenditure_details` table mein daalta hai.

**Crash-Safe Design:** Script beech mein ruk jaye to wapas `python run_full_pipeline.py` run karo, already processed villages aur vouchers skip ho jayenge aur jaha se ruka tha wahi se aage badhega.

## 🚀 Recent Architectural Optimizations
Haal hi mein saari 5 scraping files (`Voucher_Wise_Summary`, `View_Activity`, `Expenditure`, `Photo_Uploaded`, aur `Approved_Action_Plan`) mein major performance upgrades kiye gaye hain:
1. **Asynchronous DB Queue:** DB inserts ko ab ek background worker thread (`_db_writer_worker`) handle karta hai. Isse main scraping threads block nahi hote, aur network I/O speed 5-10x badh gayi hai.
2. **Persistent Connection Pooling:** Har worker thread ke paas apna `requests.Session` aur `HTTPAdapter` hai, jo TCP connections ko zinda rakhta hai (`pool_connections=1`). Isse website par baar-baar naye connection ban banane ka time bachta hai.
3. **Global ThreadPoolExecutor:** Village ya activity level parsing parallel tarike se chalayi gayi hai.
4. **Crash-Proof & Duplicate Prevention:** MySQL database files mein properly `UNIQUE KEY`s lagayi gayi hain aur queries mein `INSERT IGNORE` (ya atomic batch commits) ka use kiya gaya hai. Agar script crash ho jaye, to start karne par duplicate records nahi bante aur ruki hui jagah se flawlessly resume hoti hai.

## Individual Scripts Run Karna

Agar aapko sirf specific step run karna hai (mostly debugging ya partial data fetch ke liye), to unko manually bhi chala sakte ho:

```bash
# Sirf Step 1: Villages List
python run_step1_get_villages.py

# Sirf Step 2: Photo Uploads
python M_ActionSoft_Photo_Uploaded_Report_fast.py

# Sirf Step 3: Voucher Summary
python run_voucher_pipeline.py

# Sirf Step 4: Activity Details
python run_activity_details.py

# Sirf Step 5: Expenditure Details
python Expenditure_Details_Report_fast.py
```
