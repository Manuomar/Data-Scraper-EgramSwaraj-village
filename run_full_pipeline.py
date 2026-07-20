"""
python run_full_pipeline.py
======================================
This script runs the complete Voucher Module pipeline step-by-step.
All configurations (like district, financial year, db credentials) 
are read from config.py.
"""

import time
import config
import run_step1_get_villages
import run_voucher_pipeline
from M_ActionSoft_Photo_Uploaded_Report_fast import extractData as run_photo_uploaded
from View_Activity_Details_Report_fast import extractData as run_activity_details
from Expenditure_Details_Report_fast import extractData as run_expenditure_details

def main():
    print("=" * 70)
    print("  STARTING FULL VOUCHER MODULE PIPELINE")
    print(f"  Financial Year: {config.FINANCIAL_YEAR}")
    print(f"  District Filter: {config.DISTRICT_FILTER}")
    print(f"  Target DB: {config.DB_CONFIG['database']}")
    print("=" * 70)
    
    start_time = time.time()
    
    # -----------------------------------------------------
    # STEP 1: Fetch Villages & Approved Action Plan
    # -----------------------------------------------------
    print("\n>>> PIPELINE STEP 1: Fetching Villages (Approved Action Plan Report)")
    step1_start = time.time()
    try:
        run_step1_get_villages.run()
    except Exception as e:
        print(f"\n[ERROR in Step 1]: {e}")
        print("Pipeline aborted.")
        return
    finally:
        print(f"  [Step 1 Elapsed Time: {time.time() - step1_start:.2f} seconds]")
        
    # -----------------------------------------------------
    # STEP 2: Fetch Photo Uploaded Report
    # -----------------------------------------------------
    print("\n>>> PIPELINE STEP 2: Fetching M-ActionSoft Photo Uploaded Report")
    # step2_start = time.time()
    # try:
    #     run_photo_uploaded(district_filter=config.DISTRICT_FILTER)
    # except Exception as e:
    #     print(f"\n[ERROR in Step 2]: {e}")
    #     print("Pipeline aborted.")
    #     return
    # finally:
    #     print(f"  [Step 2 Elapsed Time: {time.time() - step2_start:.2f} seconds]")

    # -----------------------------------------------------
    # STEP 3: Fetch Voucher Wise Summary
    # -----------------------------------------------------
    print("\n>>> PIPELINE STEP 3: Fetching Voucher-Wise Summary Report")
    step3_start = time.time()
    try:
        run_voucher_pipeline.run()
    except Exception as e:
        print(f"\n[ERROR in Step 3]: {e}")
        print("Pipeline aborted.")
        return
    finally:
        print(f"  [Step 3 Elapsed Time: {time.time() - step3_start:.2f} seconds]")
        
    # -----------------------------------------------------
    # STEP 4: Fetch Activity Details
    # -----------------------------------------------------
    print("\n>>> PIPELINE STEP 4: Fetching View Activity Details Report")
    step4_start = time.time()
    try:
        print("Starting Activity Details Extraction for Voucher Module...")
        run_activity_details()
        print("Successfully finished Activity Details Extraction.")
    except Exception as e:
        print(f"\n[ERROR in Step 4]: {e}")
        print("Pipeline aborted.")
        return
    finally:
        print(f"  [Step 4 Elapsed Time: {time.time() - step4_start:.2f} seconds]")
        
    # -----------------------------------------------------
    # STEP 5: Fetch Expenditure Details
    # -----------------------------------------------------
    print("\n>>> PIPELINE STEP 5: Fetching Expenditure Details Report")
    step5_start = time.time()
    try:
        print("Starting Expenditure Details Extraction...")
        run_expenditure_details()
        print("Successfully finished Expenditure Details Extraction.")
    except Exception as e:
        print(f"\n[ERROR in Step 5]: {e}")
        print("Pipeline aborted.")
        return
    finally:
        print(f"  [Step 5 Elapsed Time: {time.time() - step5_start:.2f} seconds]")

    total_time = time.time() - start_time
    print("=" * 70)
    print(f"  PIPELINE COMPLETED SUCCESSFULLY IN {total_time:.2f} SECONDS")
    print("=" * 70)


if __name__ == "__main__":
    global_start = time.time()
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n[STOP] Pipeline forcefully interrupted by user after {time.time() - global_start:.2f} seconds! Exiting immediately...")
        import os
        os._exit(1)
