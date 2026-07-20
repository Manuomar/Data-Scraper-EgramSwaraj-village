import mysql.connector

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

DB_CONFIG = config.DB_CONFIG

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS view_activity_details_expenditure_details (
    sno INT AUTO_INCREMENT PRIMARY KEY,
    village_code INT,
    plan_code INT,
    activity_code INT,
    scheme_name VARCHAR(255),
    component_name VARCHAR(255),
    voucher_no VARCHAR(100),
    expenditure_amount DECIMAL(15,2) NULL,
    voucher_type VARCHAR(100),
    payment_done_by VARCHAR(100),
    status VARCHAR(100),
    UNIQUE KEY uq_expenditure (activity_code, voucher_no, expenditure_amount)
)
"""

_INSERT_SQL = """
INSERT IGNORE INTO view_activity_details_expenditure_details (
    village_code,
    plan_code,
    activity_code,
    scheme_name,
    component_name,
    voucher_no,
    expenditure_amount,
    voucher_type,
    payment_done_by,
    status
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def _connect():
    return mysql.connector.connect(**DB_CONFIG)


def ensureTable():
    """
    Table create karta hai agar exist nahi karti.
    extractData() ke start mein ek baar call karo.
    """
    conn = _connect()
    cursor = conn.cursor()
    try:
        cursor.execute(_CREATE_TABLE_SQL)
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def _parse_row(row):
    """Ek dict row ko DB values tuple mein convert karta hai."""
    try:
        expenditure_str = str(row.get("Expenditure") or "").replace(",", "").strip()
        expenditure_amount = float(expenditure_str) if expenditure_str else None
    except Exception:
        expenditure_amount = None

    return (
        int(row.get("village_code", 0)),
        int(row.get("plan_code", 0)),
        int(row.get("activity_code", 0)),
        row.get("Scheme Name"),
        row.get("Component Name"),
        row.get("Voucher No."),
        expenditure_amount,
        row.get("Voucher Type"),
        row.get("Payment done by"),
        row.get("Status"),
    )


def insertDataBatch(rows, conn=None):
    """
    Ek batch of row dicts (list) ko ek hi DB connection mein insert karta hai.
    CRASH-PROOF: caller (DB worker) sirf is function ke succeed hone par
    activity ko 'done' mark karta hai.

    rows: list of dicts (already have village_code, plan_code, activity_code)
    """
    if not rows:
        return

    values_list = []
    for row in rows:
        try:
            values_list.append(_parse_row(row))
        except Exception as e:
            print(f"[DB] Row parse error for activity={row.get('activity_code')}: {e}")

    if not values_list:
        return

    close_conn = False
    if conn is None:
        conn = _connect()
        close_conn = True
    cursor = conn.cursor()
    try:
        cursor.executemany(_INSERT_SQL, values_list)
        conn.commit()
    finally:
        cursor.close()
        if close_conn:
            conn.close()


# ---------------------------------------------------------------------------
# Legacy wrapper — kept for backward compatibility (run_full_pipeline etc.)
# ---------------------------------------------------------------------------
def insertData(data, conn=None):
    """
    Legacy API: data = {"Expenditure Details": [row, ...]}
    Internally calls insertDataBatch.
    """
    if not data:
        return
    rows = data.get("Expenditure Details", [])
    if rows:
        insertDataBatch(rows, conn=conn)
