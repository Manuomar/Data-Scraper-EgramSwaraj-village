import mysql.connector

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

DB_CONFIG = config.DB_CONFIG

def _connect():
    return mysql.connector.connect(**DB_CONFIG)


def ensureTables():
    conn = _connect()
    cursor = conn.cursor()

    # Step 2: Create table (only run this once)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS view_activity_details_activity_details (
        sno INT AUTO_INCREMENT PRIMARY KEY,
        village_code INT,
        plan_code INT,
        activity_code INT,
        activity_name VARCHAR(255),
        activity_description VARCHAR(255),
        theme_name VARCHAR(255),
        activity_type VARCHAR(100),
        state_name VARCHAR(45),
        local_body_name VARCHAR(100),
        financial_year VARCHAR(15),
        focus_area VARCHAR(100),
        activity_status VARCHAR(20),
        activity_cost DECIMAL(10,2) NULL,
        output_type VARCHAR(45),
        work_type VARCHAR(25),
        costless_activity VARCHAR(15),
        delegated_activity VARCHAR(15),
        sharable_activity VARCHAR(15),
        asset_type VARCHAR(15) NULL,
        asset_category VARCHAR(100) NULL,
        asset_sub_category VARCHAR(100) NULL,
        coverage_area VARCHAR(25) NULL,
        asset_unit_type VARCHAR(25) NULL,
        asset_no_of_unit DECIMAL(10,2) NULL,
        asset_unit_cost DECIMAL(10,2) NULL,
        asset_location VARCHAR(100) NULL,
        operational_type VARCHAR(100) NULL,
        remarks VARCHAR(255) NULL,
        UNIQUE KEY uq_activity (activity_code, plan_code)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS view_activity_details_fund_allocation (
        sno INT AUTO_INCREMENT PRIMARY KEY,
        village_code INT,
        plan_code INT,
        activity_code INT,
        plan_type VARCHAR(100),
        plan_status VARCHAR(100),
        scheme VARCHAR(100),
        scheme_component VARCHAR(100),
        amount_allocated DECIMAL(10,2) NULL,
        UNIQUE KEY uq_fund (activity_code, plan_code, scheme, scheme_component)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS view_activity_details_technical_approval_details (
        sno INT AUTO_INCREMENT PRIMARY KEY,
        village_code INT,
        plan_code INT,
        activity_code INT,
        technical_approved_cost DECIMAL(10,2) NULL,
        order_issuing_authority VARCHAR(100),
        technical_approval_order_number VARCHAR(15),
        technical_approval_date DATE NULL,
        UNIQUE KEY uq_tech_approval (activity_code, technical_approval_order_number)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS view_activity_details_administrative_approval_details (
        sno INT AUTO_INCREMENT PRIMARY KEY,
        village_code INT,
        plan_code INT,
        activity_code INT,
        admin_approval_order_no VARCHAR(15),
        admin_approval_order_issuing_authority VARCHAR(100),
        admin_approval_order_date DATE NULL,
        admin_approval_cost DECIMAL(10,2) NULL,
        scheme_name VARCHAR(100),
        component_name VARCHAR(100),
        allocated_amount_general DECIMAL(10,2) NULL,
        allocated_amount_sc DECIMAL(10,2) NULL,
        allocated_amount_st DECIMAL(10,2) NULL,
        total_allocation_amount DECIMAL(10,2) NULL,
        UNIQUE KEY uq_admin_approval (activity_code, admin_approval_order_no)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS view_activity_details_physical_progress_details (
        sno INT AUTO_INCREMENT PRIMARY KEY,
        village_code INT,
        plan_code INT,
        activity_code INT,
        asset_id INT NULL,
        asset_name VARCHAR(255) NULL,
        location_of_Asset VARCHAR(100) NULL,
        work_stage VARCHAR(255) NULL,
        work_stage_date Date NULL,
        geo_tagged VARCHAR(15) NULL,
        photo_moderate VARCHAR(15) NULL,
        remarks VARCHAR(255) NULL,
        enter_date DATE NULL,
        UNIQUE KEY uq_physical (activity_code, asset_id, work_stage, work_stage_date)
    )
    """)
    conn.commit()
    cursor.close()
    conn.close()

def insertDataBatch(data_list, conn=None):
    if not data_list:
        return

    close_conn = False
    if conn is None:
        conn = _connect()
        close_conn = True
    cursor = conn.cursor()
    
    activity_values = []
    fund_values = []
    tech_values = []
    admin_values = []
    physical_values = []
    
    for data in data_list:
        if "Activity Details" in data:
            for row in data["Activity Details"]:
                activity_values.append((
                    int(row["village_code"]),
                    int(row["plan_code"]),
                    int(row["activity_code"]),
                    row.get("Activity Name"),
                    row.get("Activity Description"),
                    row.get("Theme Name"),
                    row.get("Activity Type"),
                    row.get("State Name"),
                    row.get("Local Body Name"),
                    row.get("Financial Year"),
                    row.get("Focus Area"),
                    row.get("Activity Status"),
                    row.get("Activity Cost"),
                    row.get("Output Type"),
                    row.get("Work Type"),
                    row.get("Costless Activity"),
                    row.get("Delegated Activity"),
                    row.get("Sharable Activity"),
                    row.get("Asset Type") or None,
                    row.get("Asset Category") or None,
                    row.get("Asset Sub Category") or None,
                    row.get("Coverage Area") or None,
                    row.get("Asset Unit Type") or None,
                    row.get("Asset No. Of Unit") or None,
                    row.get("Asset Unit Cost") or None,
                    row.get("Asset Location") or None,
                    row.get("Operational Type") or None,
                    row.get("Remarks") or None,
                ))

        if "Fund Allocation" in data:
            for row in data["Fund Allocation"]:
                fund_values.append((
                    int(row["village_code"]),
                    int(row["plan_code"]),
                    int(row["activity_code"]),
                    row.get("Plan Type"),
                    row.get("Plan Status"),
                    row.get("Scheme"),
                    row.get("Scheme/Component"),
                    row.get("Amount Allocated") or None,
                ))

        if "Technical Approval Details" in data:
            for row in data["Technical Approval Details"]:
                tech_values.append((
                    int(row["village_code"]),
                    int(row["plan_code"]),
                    int(row["activity_code"]),
                    row.get("Technical Approved Cost (In Rs.)") or None,
                    row.get("Order Issuing Authority"),
                    row.get("Technical Approval Order Number"),
                    row.get("Technical Approval Date") or None,
                ))

        if "Administrative Approval Details" in data:
            for row in data["Administrative Approval Details"]:
                admin_values.append((
                    int(row["village_code"]),
                    int(row["plan_code"]),
                    int(row["activity_code"]),
                    row.get("Admin Approval order No."),
                    row.get("Admin Approval Order Issuing Authority"),
                    row.get("Admin Approval order Date") or None,
                    row.get("Admin Approval Cost") or None,
                    row.get("Scheme Name"),
                    row.get("Component Name"),
                    row.get("Allocated Amount(in Rs.) General") or None,
                    row.get("Allocated Amount(in Rs.) SC") or None,
                    row.get("Allocated Amount(in Rs.) ST") or None,
                    row.get("Total Allocation Amount(in Rs.)") or None,
                ))

        if "Physical Progress Details" in data:
            for row in data["Physical Progress Details"]:
                work_stage_date = row.get("Work Stage Date")
                if work_stage_date == "202511-07-01":
                    work_stage_date = "2025-07-01"

                physical_values.append((
                    int(row["village_code"]),
                    int(row["plan_code"]),
                    int(row["activity_code"]),
                    row.get("Asset ID") or None,
                    row.get("Asset Name") or None,
                    row.get("Location of Asset") or None,
                    row.get("Work Stage") or None,
                    work_stage_date or None,
                    row.get("Geo-tagged") or None,
                    row.get("Photo Moderate") or None,
                    row.get("remarks") or None,
                    row.get("enter_date") or None,
                ))

    # Perform bulk inserts
    if activity_values:
        cursor.executemany("""
        INSERT IGNORE INTO view_activity_details_activity_details (
            village_code, plan_code, activity_code, activity_name, activity_description,
            theme_name, activity_type, state_name, local_body_name, financial_year,
            focus_area, activity_status, activity_cost, output_type, work_type,
            costless_activity, delegated_activity, sharable_activity, asset_type,
            asset_category, asset_sub_category, coverage_area, asset_unit_type,
            asset_no_of_unit, asset_unit_cost, asset_location, operational_type, remarks
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, activity_values)
        
    if fund_values:
        cursor.executemany("""
        INSERT IGNORE INTO view_activity_details_fund_allocation (
            village_code, plan_code, activity_code, plan_type, plan_status,
            scheme, scheme_component, amount_allocated
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, fund_values)

    if tech_values:
        cursor.executemany("""
        INSERT IGNORE INTO view_activity_details_technical_approval_details (
            village_code, plan_code, activity_code, technical_approved_cost,
            order_issuing_authority, technical_approval_order_number, technical_approval_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, tech_values)

    if admin_values:
        cursor.executemany("""
        INSERT IGNORE INTO view_activity_details_administrative_approval_details (
            village_code, plan_code, activity_code, admin_approval_order_no,
            admin_approval_order_issuing_authority, admin_approval_order_date,
            admin_approval_cost, scheme_name, component_name, allocated_amount_general,
            allocated_amount_sc, allocated_amount_st, total_allocation_amount
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, admin_values)

    if physical_values:
        cursor.executemany("""
        INSERT IGNORE INTO view_activity_details_physical_progress_details (
            village_code, plan_code, activity_code, asset_id, asset_name,
            location_of_Asset, work_stage, work_stage_date, geo_tagged,
            photo_moderate, remarks, enter_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, physical_values)

    conn.commit()
    cursor.close()
    if close_conn:
        conn.close()


def insertData(data, conn=None):
    """Legacy wrapper for single dict insert"""
    if data:
        insertDataBatch([data], conn=conn)

