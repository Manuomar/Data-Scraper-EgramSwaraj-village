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

    # Insert Activity Details
    insert_query = """
    INSERT IGNORE INTO view_activity_details_activity_details (
        village_code,
        plan_code,
        activity_code,
        activity_name,
        activity_description,
        theme_name,
        activity_type,
        state_name,
        local_body_name,
        financial_year,
        focus_area,
        activity_status,
        activity_cost,
        output_type,
        work_type,
        costless_activity,
        delegated_activity,
        sharable_activity,
        asset_type,
        asset_category,
        asset_sub_category,
        coverage_area,
        asset_unit_type,
        asset_no_of_unit,
        asset_unit_cost,
        asset_location,
        operational_type,
        remarks
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    if "Activity Details" in data:
        values_list = []
        for row in data["Activity Details"]:
            values = (
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
                None if not row.get("Asset Type") else row["Asset Type"],
                None if not row.get("Asset Category") else row["Asset Category"],
                None if not row.get("Asset Sub Category") else row["Asset Sub Category"],
                None if not row.get("Coverage Area") else row["Coverage Area"],
                None if not row.get("Asset Unit Type") else row["Asset Unit Type"],
                None if not row.get("Asset No. Of Unit") else row["Asset No. Of Unit"],
                None if not row.get("Asset Unit Cost") else row["Asset Unit Cost"],
                None if not row.get("Asset Location") else row["Asset Location"],
                None if not row.get("Operational Type") else row["Operational Type"],
                None if not row.get("Remarks") else row["Remarks"],
            )
            values_list.append(values)
        if values_list:
            cursor.executemany(insert_query, values_list)

    # Insert Fund Allocation
    insert_query = """
    INSERT IGNORE INTO view_activity_details_fund_allocation (
        village_code,
        plan_code,
        activity_code,
        plan_type,
        plan_status,
        scheme,
        scheme_component,
        amount_allocated
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    if "Fund Allocation" in data:
        values_list = []
        for row in data["Fund Allocation"]:
            values = (
                int(row["village_code"]),
                int(row["plan_code"]),
                int(row["activity_code"]),
                row.get("Plan Type"),
                row.get("Plan Status"),
                row.get("Scheme"),
                row.get("Scheme/Component"),
                None if not row.get("Amount Allocated") else row.get("Amount Allocated"),
            )
            values_list.append(values)
        if values_list:
            cursor.executemany(insert_query, values_list)

    # Insert Technical Approval Details
    insert_query = """
    INSERT IGNORE INTO view_activity_details_technical_approval_details (
        village_code,
        plan_code,
        activity_code,
        technical_approved_cost,
        order_issuing_authority,
        technical_approval_order_number,
        technical_approval_date
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    if "Technical Approval Details" in data:
        values_list = []
        for row in data["Technical Approval Details"]:
            values = (
                int(row["village_code"]),
                int(row["plan_code"]),
                int(row["activity_code"]),
                (
                    None
                    if not row.get("Technical Approved Cost (In Rs.)")
                    else row.get("Technical Approved Cost (In Rs.)")
                ),
                row.get("Order Issuing Authority"),
                row.get("Technical Approval Order Number"),
                (
                    None
                    if not row.get("Technical Approval Date")
                    else row.get("Technical Approval Date")
                ),
            )
            values_list.append(values)
        if values_list:
            cursor.executemany(insert_query, values_list)

    # Insert Administrative Approval Details
    insert_query = """
    INSERT IGNORE INTO view_activity_details_administrative_approval_details (
        village_code,
        plan_code,
        activity_code,
        admin_approval_order_no,
        admin_approval_order_issuing_authority,
        admin_approval_order_date,
        admin_approval_cost,
        scheme_name,
        component_name,
        allocated_amount_general,
        allocated_amount_sc,
        allocated_amount_st,
        total_allocation_amount
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    if "Administrative Approval Details" in data:
        values_list = []
        for row in data["Administrative Approval Details"]:
            values = (
                int(row["village_code"]),
                int(row["plan_code"]),
                int(row["activity_code"]),
                row.get("Admin Approval order No."),
                row.get("Admin Approval Order Issuing Authority"),
                (
                    None
                    if not row.get("Admin Approval order Date")
                    else row.get("Admin Approval order Date")
                ),
                (
                    None
                    if not row.get("Admin Approval Cost")
                    else row.get("Admin Approval Cost")
                ),
                row.get("Scheme Name"),
                row.get("Component Name"),
                (
                    None
                    if not row.get("Allocated Amount(in Rs.) General")
                    else row.get("Allocated Amount(in Rs.) General")
                ),
                (
                    None
                    if not row.get("Allocated Amount(in Rs.) SC")
                    else row.get("Allocated Amount(in Rs.) SC")
                ),
                (
                    None
                    if not row.get("Allocated Amount(in Rs.) ST")
                    else row.get("Allocated Amount(in Rs.) ST")
                ),
                (
                    None
                    if not row.get("Total Allocation Amount(in Rs.)")
                    else row.get("Total Allocation Amount(in Rs.)")
                ),
            )
            values_list.append(values)
        if values_list:
            cursor.executemany(insert_query, values_list)

    # Insert Physical Progress Details
    insert_query = """
    INSERT IGNORE INTO view_activity_details_physical_progress_details (
        village_code,
        plan_code,
        activity_code,
        asset_id,
        asset_name,
        location_of_Asset,
        work_stage,
        work_stage_date,
        geo_tagged,
        photo_moderate,
        remarks,
        enter_date
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    if "Physical Progress Details" in data:
        values_list = []
        for row in data["Physical Progress Details"]:
            work_stage_date = row.get("Work Stage Date")
            if work_stage_date == "202511-07-01":
                work_stage_date = "2025-07-01"

            values = (
                int(row["village_code"]),
                int(row["plan_code"]),
                int(row["activity_code"]),
                None if not row.get("Asset ID") else row.get("Asset ID"),
                None if not row.get("Asset Name") else row.get("Asset Name"),
                None if not row.get("Location of Asset") else row.get("Location of Asset"),
                None if not row.get("Work Stage") else row.get("Work Stage"),
                None if not work_stage_date else work_stage_date,
                None if not row.get("Geo-tagged") else row.get("Geo-tagged"),
                None if not row.get("Photo Moderate") else row.get("Photo Moderate"),
                None if not row.get("remarks") else row.get("remarks"),
                None if not row.get("enter_date") else row.get("enter_date"),
            )
            values_list.append(values)
        if values_list:
            cursor.executemany(insert_query, values_list)

    # Step 4: Commit and close connection
    conn.commit()
    cursor.close()
    conn.close()
