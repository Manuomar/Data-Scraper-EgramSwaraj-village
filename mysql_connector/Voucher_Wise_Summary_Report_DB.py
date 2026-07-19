import mysql.connector
from datetime import datetime

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

DB_CONFIG = config.DB_CONFIG


def _connect():
    return mysql.connector.connect(**DB_CONFIG)


def insertData(data):
    conn = _connect()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS voucher_wise_summary_report (
        sno INT AUTO_INCREMENT PRIMARY KEY,
        financial_year VARCHAR(25),
        month VARCHAR(25),
        district VARCHAR(100),
        block VARCHAR(100),
        village VARCHAR(100),
        village_code INT,
        type_of_transaction VARCHAR(100),
        activity_code VARCHAR(100),
        activity_name VARCHAR(255),
        scheme_name VARCHAR(255),
        voucher_date DATE,
        voucher_no VARCHAR(100),
        account_head TEXT,
        amount_in_rs DECIMAL(10,2),
        particulars TEXT,
        file_url TEXT,
        voucher_details_url VARCHAR(500),
        UNIQUE KEY uq_voucher_url (voucher_details_url)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS voucher_wise_summary_report_payment (
        sno INT AUTO_INCREMENT PRIMARY KEY,
        vwsr_sno INT,
        mode VARCHAR(100),
        details TEXT,
        name TEXT,
        amount DECIMAL(10,2)
    )
    """)

    insert_query = """
    INSERT IGNORE INTO voucher_wise_summary_report (
        financial_year, month, district, block, village, village_code, type_of_transaction,
        activity_code, activity_name, scheme_name, voucher_date, voucher_no, account_head,
        amount_in_rs, particulars, file_url, voucher_details_url
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    insert_query2 = """
    INSERT INTO voucher_wise_summary_report_payment (
        vwsr_sno, mode, details, name, amount
    ) VALUES (%s, %s, %s, %s, %s)
    """

    skipped = 0

    for row in data:
        try:
            text = row["Activity Code"]
            parts = [x.strip() for x in text.split("|", 1)]
            if len(parts) == 2:
                code, name = parts
            else:
                code = parts[0]
                name = None

            voucher_date = datetime.strptime(row["Voucher Date"], "%d/%m/%Y").strftime("%Y-%m-%d")

            values = (
                row["Financial Year"],
                row["Month"],
                row["Zilla Panchayat and Equivalent"],
                row["Block Panchayat and Equivalent"],
                row["Village Panchayat and Equivalent"],
                row["village_code"],
                row["Type Of Transaction"],
                code,
                name,
                row["Scheme Name"],
                voucher_date,
                row["Voucher No"],
                row["Account Head"],
                row["Amount (in Rs.)"].replace(",", "").replace("₹", "").strip(),
                row["Particulars"],
                row.get("real_file_url"),
                row["voucher_details_url"],
            )
            cursor.execute(insert_query, values)

            vwsr_sno = cursor.lastrowid
            if vwsr_sno:
                for pay in row.get("payments", []):
                    values2 = (
                        vwsr_sno,
                        pay["mode"],
                        pay["details"],
                        pay["name"],
                        pay["amount"].replace(",", "").replace("₹", "").strip(),
                    )
                    cursor.execute(insert_query2, values2)

        except Exception as e:
            skipped += 1
            print(f"[SKIP ROW] {row.get('voucher_details_url', '?')} -> {e}")
            continue

    conn.commit()
    cursor.close()
    conn.close()

    if skipped:
        print(f"[insertData] {skipped} row(s) skipped due to errors out of {len(data)}")

def getActivityCodes():
    conn = _connect()
    cursor = conn.cursor(dictionary=True)
    
    # Ensure column exists for tracking
    try:
        cursor.execute("ALTER TABLE voucher_wise_summary_report ADD COLUMN activity_details_fetched INT DEFAULT 0")
        conn.commit()
    except:
        pass
    
    cursor.execute("""
        SELECT DISTINCT activity_code, financial_year, village_code 
        FROM voucher_wise_summary_report 
        WHERE activity_details_fetched = 0 
          AND activity_code IS NOT NULL 
          AND activity_code != ''
    """)
    data = cursor.fetchall()
    
    for row in data:
        # Convert financial_year (e.g. 2024-2025) to plan_code integer (2024)
        if row['financial_year'] and '-' in row['financial_year']:
            row['plan_code'] = int(row['financial_year'].split('-')[0])
        else:
            try:
                row['plan_code'] = int(row['financial_year'])
            except (ValueError, TypeError):
                row['plan_code'] = 0
                
    cursor.close()
    conn.close()
    return data

def updateActivityDetailsFetched(activity_code):
    conn = _connect()
    cursor = conn.cursor()
    cursor.execute("UPDATE voucher_wise_summary_report SET activity_details_fetched = 1 WHERE activity_code = %s", (activity_code,))
    conn.commit()
    cursor.close()
    conn.close()


def getExpenditurePendingActivities():
    """Expenditure_Details_Report_fast.py ke liye:
    Un activities ko laata hai jinke expenditure details abhi fetch nahi hue.
    Returns list of dicts with keys: activity_code, plan_code, village_code
    """
    conn = _connect()
    cursor = conn.cursor(dictionary=True)

    # Ensure tracking column exists
    try:
        cursor.execute(
            "ALTER TABLE voucher_wise_summary_report "
            "ADD COLUMN expenditure_details_fetched INT DEFAULT 0"
        )
        conn.commit()
    except Exception:
        pass  # Column already exists

    cursor.execute("""
        SELECT DISTINCT activity_code, financial_year, village_code
        FROM voucher_wise_summary_report
        WHERE expenditure_details_fetched = 0
          AND activity_code IS NOT NULL
          AND activity_code != ''
    """)
    data = cursor.fetchall()

    for row in data:
        # Convert financial_year (e.g. '2024-2025') to plan_code integer (2024)
        fy = row.get('financial_year', '')
        if fy and '-' in str(fy):
            row['plan_code'] = int(str(fy).split('-')[0])
        else:
            try:
                row['plan_code'] = int(fy)
            except (ValueError, TypeError):
                row['plan_code'] = 0

    cursor.close()
    conn.close()
    return data


def updateExpenditureDetailsFetched(activity_code):
    """Expenditure_Details_Report_fast.py ke liye:
    Ek activity ko expenditure-fetched mark karta hai (crash-safe resume).
    """
    conn = _connect()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE voucher_wise_summary_report "
        "SET expenditure_details_fetched = 1 "
        "WHERE activity_code = %s",
        (activity_code,)
    )
    conn.commit()
    cursor.close()
    conn.close()
