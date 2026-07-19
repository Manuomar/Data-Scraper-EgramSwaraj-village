"""
run_voucher_pipeline.py
=========================
Sirf Voucher-Wise Summary Report chalane ke liye standalone entry point.

PRE-REQUISITE:
    `approved_action_plan_report` table already exist honi chahiye,
    aur usme district/block/village data bhara hona chahiye
    (yeh tumhare main pipeline ke Step 1 se aata hai). Yeh module
    us table ko touch nahi karta except `get_voucher_details` flag update.

Usage:
    python run_voucher_pipeline.py
"""

import Voucher_Wise_Summary_Report_fast
import config

def run():
    print("=" * 65)
    print("  VOUCHER-WISE SUMMARY REPORT — STANDALONE MODULE")
    if config.DISTRICT_FILTER:
        print(f"  [DISTRICT: {config.DISTRICT_FILTER}]")
    print("=" * 65)
    Voucher_Wise_Summary_Report_fast.extractData(district_filter=config.DISTRICT_FILTER)
    print("=" * 65)
    print("  DONE")
    print("=" * 65)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("\n[STOP] Script forcefully interrupted by user! Exiting immediately...")
        import os
        os._exit(1)
