import mysql.connector

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

DB_CONFIG = config.DB_CONFIG

def _connect():
    return mysql.connector.connect(**DB_CONFIG)

def insertData(data):
    """
    Inserts data into the view_activity_details_expenditure_details table.
    data format:
    {
        "Expenditure Details": [
            {
                "village_code": 123,
                "plan_code": 2024,
                "activity_code": 126151241,
                "Scheme Name": "...",
                "Component Name": "...",
                "Voucher No.": "...",
                "Expenditure": "...",
                "Voucher Type": "...",
                "Payment done by": "...",
                "Status": "..."
            }, ...
        ]
    }
    """
    conn = _connect()
    cursor = conn.cursor()

    try:
        cursor.execute("""
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
        """)

        insert_query = """
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

        values_list = []
        if "Expenditure Details" in data:
            for row in data["Expenditure Details"]:
                try:
                    expenditure_str = str(row.get("Expenditure") or "").replace(",", "").strip()
                    expenditure_amount = float(expenditure_str) if expenditure_str else None
                except Exception:
                    expenditure_amount = None

                values = (
                    int(row.get("village_code", 0)),
                    int(row.get("plan_code", 0)),
                    int(row.get("activity_code", 0)),
                    row.get("Scheme Name"),
                    row.get("Component Name"),
                    row.get("Voucher No."),
                    expenditure_amount,
                    row.get("Voucher Type"),
                    row.get("Payment done by"),
                    row.get("Status")
                )
                values_list.append(values)

        if values_list:
            cursor.executemany(insert_query, values_list)

        conn.commit()
    finally:
        cursor.close()
        conn.close()
