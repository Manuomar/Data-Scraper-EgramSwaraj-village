"""
run_activity_details.py
======================================
This script runs the Activity Details extraction process for the voucher module.
It will pull all unique activity codes from the voucher_wise_summary_report
table and fetch their details from the eGramSwaraj portal.
"""

from View_Activity_Details_Report_fast import extractData

if __name__ == "__main__":
    print("Starting Activity Details Extraction for Voucher Module...")
    try:
        extractData()
        print("Successfully finished Activity Details Extraction.")
    except KeyboardInterrupt:
        print("\n[STOP] Script forcefully interrupted by user! Exiting immediately...")
        import os
        os._exit(1)
    except Exception as e:
        print(f"\nAn error occurred: {e}")
