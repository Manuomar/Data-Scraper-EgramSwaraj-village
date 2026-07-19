"""
config.py
======================================
Configuration file for the Voucher Module pipeline.
Modify these parameters to change how the pipeline runs without editing the scripts.
"""

# ---- Database Configuration ----
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",           # <-- apna MySQL/phpMyAdmin password daalo agar blank nahi hai
    "database": "egram_all_data_2026-27", # <-- apna database naam yahan set karo
}

# ---- Global Pipeline Settings ----
STATE_CODE = "9"              # 9 is typically UP
FINANCIAL_YEAR = "2026"       # e.g., "2026" for 2026-2027
VILLAGE_FETCH_YEAR = "2025"   # 2026 action plan is empty, so use 2025 to fetch villages

# Sirf ek district ka data nikalna ho toh district name likho (case-insensitive).
# Poora UP chahiye toh None rakhna.
# Example: DISTRICT_FILTER = "Agra"
DISTRICT_FILTER = None

# ---- Voucher Fetch Settings ----
# Months to pull vouchers for (financial year months, April=4 ... March=3)
# VOUCHER_MONTHS = [4, 5, 6, 7, 8, 9, 10, 11, 12, 1, 2, 3] 
VOUCHER_MONTHS = [4, 5, 6, 7] # (financial yr - 2026-27)
# [4, 5, 6, 7, 8, 9, 10, 11, 12, 1, 2, 3] for complete year

# Thread count for parallel execution
MAX_WORKERS = 25

# ---- Test Mode Settings (For Step 1 mostly) ----
# Pehli baar chalate waqt True rakho — 1 block test
TEST_MODE = False
TEST_BLOCK_LIMIT = 1
