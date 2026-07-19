"""
run_step1_get_villages.py
============================
Sirf Step 1 chalane ke liye — district/block/village list nikaal ke
`approved_action_plan_report` table (isi voucher_module ke DB_CONFIG
wale database mein) create + populate karta hai.

Yeh voucher fetch (run_voucher_pipeline.py) se PEHLE ek baar chalao,
warna voucher module ko koi village list nahi milegi
("Table doesn't exist" ya "0 villages" wali error aayegi).

Usage:
    python run_step1_get_villages.py
"""

import Approved_Action_Plan_Report
from mysql_connector import Approved_Action_Plan_Report_DB

import config

def run():
    limit_blocks = config.TEST_BLOCK_LIMIT if config.TEST_MODE else None

    print("=" * 65)
    print("  STEP 1: Approved Action Plan Report (district/block/village list)")
    if config.DISTRICT_FILTER:
        print(f"  [DISTRICT: {config.DISTRICT_FILTER}]")
    if config.TEST_MODE:
        print(f"  [TEST MODE — first {config.TEST_BLOCK_LIMIT} block(s) only]")
    print("=" * 65)

    Approved_Action_Plan_Report.extractAndInsertData(
        limit_blocks=limit_blocks,
        district_filter=config.DISTRICT_FILTER,
    )
    print("Step 1 done. ")


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("\n[STOP] Script forcefully interrupted by user! Exiting immediately...")
        import os
        os._exit(1)
