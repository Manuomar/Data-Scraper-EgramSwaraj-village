"""
M_ActionSoft_Photo_Uploaded_Report_DB.py  (VOUCHER-MODULE VERSION)
===================================================================
Photo Uploaded Report ka data MySQL mein insert karta hai.
config.py se DB credentials liye jaate hain.
"""

import mysql.connector

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

    # Create table if not exists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS m_actionsoft_photo_uploaded_report (
        sno INT AUTO_INCREMENT PRIMARY KEY,
        fyear INT,
        village_code INT,
        activity_code INT,
        activity_name VARCHAR(2551),
        activity_status VARCHAR(20),
        asset_name VARCHAR(255),
        asset_stage VARCHAR(255),
        latitude VARCHAR(255),
        longitude VARCHAR(255),
        status VARCHAR(255),
        image_url VARCHAR(255),
        uploaded_date VARCHAR(50),
        UNIQUE KEY uq_photo (village_code, activity_code, image_url, uploaded_date)
    )
    """)

    insert_query = """
    INSERT IGNORE INTO m_actionsoft_photo_uploaded_report (
        fyear, village_code, activity_code, activity_name, activity_status, asset_name, asset_stage,
        latitude, longitude, status, image_url, uploaded_date
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    values_list = []
    for row in data:
        try:
            values = (
                row["fyear"],
                row["village_code"],
                row.get("Activity ID") or None,
                row.get("Activity Name"),
                row.get("Activity Status"),
                row.get("Asset Name"),
                row.get("Asset Stage"),
                ",".join(dict.fromkeys(row["Latitude"].split(","))) if row.get("Latitude") else None,
                ",".join(dict.fromkeys(row["Longitude"].split(","))) if row.get("Longitude") else None,
                row["Image"]["status"] if isinstance(row.get("Image"), dict) else row.get("Image"),
                row["Image"]["image_url"] if isinstance(row.get("Image"), dict) else None,
                row.get("Uploaded Date"),
            )
            values_list.append(values)
        except Exception as e:
            print(f"[SKIP ROW] village={row.get('village_code')} -> {e}")
            continue

    if values_list:
        cursor.executemany(insert_query, values_list)

    conn.commit()
    cursor.close()
    conn.close()
