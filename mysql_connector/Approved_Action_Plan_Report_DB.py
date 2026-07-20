"""
Approved_Action_Plan_Report_DB.py  (VOUCHER-MODULE VERSION — FULL)
=======================================================================
Is module mein ab do kaam ke liye functions hain:

  1. insertData() / table CREATE  -> Step 1 (village list) yahan se bhi
     chalaya ja sakta hai, agar table abhi database mein nahi bani.
  2. getData2() / updateVoucherDetails() -> Step 7 (voucher fetch) ke
     liye, jo already-populated table se read karta hai.

Agar table already kisi aur script se ban chuki hai (tumhare main
pipeline se), toh insertData() dobara chalane ki zaroorat nahi —
sirf getData2() / updateVoucherDetails() use honge.
"""

import mysql.connector
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

DB_CONFIG = config.DB_CONFIG


def _connect():
    return mysql.connector.connect(**DB_CONFIG)


_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS approved_action_plan_report (
    sno INT AUTO_INCREMENT PRIMARY KEY,
    plan_year INT,
    district VARCHAR(100),
    district_code INT,
    block VARCHAR(100),
    block_code INT,
    village_panchayat VARCHAR(100),
    village_code INT,
    plan_code INT,
    plan VARCHAR(100),
    get_photo_uploaded TINYINT DEFAULT 0,
    get_voucher_details TINYINT DEFAULT 0,
    UNIQUE (village_code, plan_code)
)
"""
# get_unspent_balance_data TINYINT DEFAULT 0, column hataya hai uper se

def _ensure_table(cursor):
    cursor.execute(_CREATE_TABLE_SQL)


def insertData(data):
    """
    Step 1 se aaya hua village list DB mein insert karta hai.
    Agar table nahi hai, pehle create karega.
    """
    conn = _connect()
    cursor = conn.cursor()
    _ensure_table(cursor)

    insert_query = """
    INSERT IGNORE INTO approved_action_plan_report (
        plan_year, district, district_code,
        block, block_code, village_panchayat, village_code,
        plan_code, plan
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    values_list = []
    for row in data:
        values = (
            row["plan_year"],
            row["district"],
            int(row["district_code"]),
            row["block"],
            int(row["block_code"]),
            row["village_panchayat"],
            int(row["village_code"]),
            int(row["plan_code"]),
            row["plan"],
        )
        values_list.append(values)
        
    if values_list:
        cursor.executemany(insert_query, values_list)

    conn.commit()
    cursor.close()
    conn.close()


def getInsertedBlockCodes():
    """
    Ek set of block_codes return karta hai jinke villages already DB me hain,
    taaki crash-safe resume me un blocks ka network call skip kiya ja sake.
    """
    conn = _connect()
    cursor = conn.cursor()
    _ensure_table(cursor)
    cursor.execute("SELECT DISTINCT block_code FROM approved_action_plan_report")
    rows = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()
    return set(r[0] for r in rows)


def getData2(district_filter=None):
    """
    Un villages ki list deta hai jinka voucher data abhi fetch nahi hua.
    (GROUP BY village_code -> ek village ek hi baar aayega, chahe usme
     multiple plan_code rows ho.)

    district_filter: District name string (case-insensitive) — sirf usi
                      district ke villages return honge. None = sab districts.
    """
    conn = _connect()
    cursor = conn.cursor(dictionary=True)
    _ensure_table(cursor)   # table missing ho toh crash na ho, empty list mile

    if district_filter:
        cursor.execute(
            "SELECT * FROM approved_action_plan_report "
            "WHERE get_voucher_details = 0 AND LOWER(district) = LOWER(%s) "
            "GROUP BY village_code",
            (district_filter.strip(),),
        )
    else:
        cursor.execute(
            "SELECT * FROM approved_action_plan_report "
            "WHERE get_voucher_details = 0 "
            "GROUP BY village_code"
        )

    result = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()
    return result


def updateVoucherDetails(village_code, conn=None):
    """Village ko 'voucher fetch ho gaya' mark karta hai (crash-safe resume)."""
    close_conn = False
    if conn is None:
        conn = _connect()
        close_conn = True
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE approved_action_plan_report SET get_voucher_details=1 WHERE village_code=%s",
        (village_code,),
    )
    conn.commit()
    cursor.close()
    if close_conn:
        conn.close()


def getVillageData(district_filter=None):
    """
    Un villages ki list deta hai jinka photo upload data abhi fetch nahi hua.
    (GROUP BY village_code -> ek village ek hi baar aayega.)

    district_filter: District name string (case-insensitive). None = sab districts.
    """
    conn = _connect()
    cursor = conn.cursor(dictionary=True)
    _ensure_table(cursor)

    if district_filter:
        cursor.execute(
            "SELECT * FROM approved_action_plan_report "
            "WHERE get_photo_uploaded = 0 AND LOWER(district) = LOWER(%s) "
            "GROUP BY village_code",
            (district_filter.strip(),),
        )
    else:
        cursor.execute(
            "SELECT * FROM approved_action_plan_report "
            "WHERE get_photo_uploaded = 0 "
            "GROUP BY village_code"
        )

    result = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()
    return result


def updatePhotoUploadedData(village_code, conn=None):
    """Village ko 'photo data fetch ho gaya' mark karta hai (crash-safe resume)."""
    close_conn = False
    if conn is None:
        conn = _connect()
        close_conn = True
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE approved_action_plan_report SET get_photo_uploaded=1 WHERE village_code=%s",
        (village_code,),
    )
    conn.commit()
    cursor.close()
    if close_conn:
        conn.close()
