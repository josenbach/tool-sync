#!/usr/bin/env python3
'''
Database Utilities Module
========================

Handles TipQA data access via Databricks Unity Catalog and the TipQA REST API.
Data source: manufacturing.bronze_tipqa.gt_master

Created: 2025-01-28
Author: Jae Osenbach
Purpose: Database connection and query utilities
'''

import math
import time
import pandas as pd
import requests
import os
from typing import Dict, Any, List
from utilities.logging_config import get_logger, log_database_operation


def read_sql_query(filename: str) -> str:
    """Read SQL query from queries folder."""
    query_path = os.path.join('queries', filename)
    try:
        with open(query_path, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        raise Exception(f"SQL query file not found: {query_path}")
    except Exception as e:
        raise Exception(f"Error reading SQL query file {query_path}: {e}")


def get_tipqa_connection(config: Dict[str, Any]):
    """Connect to Databricks SQL warehouse for TipQA data."""
    return get_databricks_connection(config['tipqa_databricks'])


def get_databricks_connection(db_config: Dict[str, Any]):
    """Connect to Databricks SQL warehouse using databricks-sql-connector."""
    from databricks import sql as databricks_sql

    db_logger = get_logger('database')
    start_time = time.time()

    host = db_config['host']
    http_path = db_config['http_path']
    token = db_config.get('token', '')

    log_and_print(f"Connecting to Databricks: {host}")

    try:
        conn = databricks_sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=token,
        )
        duration = time.time() - start_time
        log_database_operation(db_logger, 'connect', 'databricks', duration=duration)
        log_and_print(f"Successfully connected to Databricks in {duration:.2f}s")
        return conn
    except Exception as e:
        duration = time.time() - start_time
        error_msg = f"Failed to connect to Databricks after {duration:.2f}s: {e}"
        db_logger.error(error_msg, extra={"extra_fields": {
            "host": host,
            "duration": duration,
            "error": str(e),
        }})
        raise Exception(error_msg)


def get_all_tipqa_tools(conn, config: Dict[str, Any] = None) -> pd.DataFrame:
    """Fetch all JAI tools from the Databricks bronze_tipqa.gt_master table."""
    db_config = config['tipqa_databricks']
    query_template = read_sql_query('tipqa_tools_databricks.sql')
    query = query_template.format(
        catalog=db_config['catalog'],
        schema=db_config['schema'],
        table=db_config['table'],
    )

    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()

        df = pd.DataFrame(rows, columns=columns)
        log_and_print(f"Fetched {len(df)} tools from Databricks (TipQA)")

        original_count = len(df)
        df = df.drop_duplicates(subset=['serial_number'], keep='first')
        if len(df) < original_count:
            log_and_print(f"Removed {original_count - len(df)} duplicate TipQA records")

        return df
    except Exception as e:
        log_and_print(f"Error fetching TipQA tools from Databricks: {e}", 'error')
        raise


def get_tipqa_tools_by_serials(conn, serial_numbers: List[str], config: Dict[str, Any] = None) -> pd.DataFrame:
    """Fetch specific tools by serial number from Databricks."""
    db_config = config['tipqa_databricks']
    query_template = read_sql_query('tipqa_tools_databricks.sql')
    base_query = query_template.format(
        catalog=db_config['catalog'],
        schema=db_config['schema'],
        table=db_config['table'],
    )
    serial_list = "', '".join(serial_numbers)
    query = base_query.replace(
        "WHERE gm.BUSINESS_UNIT = 'JAI'",
        f"WHERE gm.BUSINESS_UNIT = 'JAI' AND gm.TOOL_NUM IN ('{serial_list}')",
    )

    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()

        df = pd.DataFrame(rows, columns=columns)
        log_and_print(f"Fetched {len(df)} tools from Databricks for serials: {', '.join(serial_numbers)}")
        return df
    except Exception as e:
        log_and_print(f"Error fetching TipQA tools from Databricks for specific serials: {e}", 'error')
        raise


def get_tipqa_tools_from_api(api_config: Dict[str, str], serial_numbers: List[str]) -> pd.DataFrame:
    """Fetch tools from the TipQA REST API and return a DataFrame matching the Databricks schema.

    Paginates through GET /GT_MASTER/?limit=10000&offset=N, filters to
    BUSINESS_UNIT='JAI' and the requested serial numbers, then transforms
    raw TipQA columns to match the schema produced by tipqa_tools_databricks.sql.
    """
    base_url = api_config['base_url'].rstrip('/')
    page_size = 10000
    all_rows: list[dict] = []
    offset = 0

    log_and_print(f"Fetching GT_MASTER from TipQA API ({base_url})...")
    start_time = time.time()

    while True:
        url = f"{base_url}/GT_MASTER/?limit={page_size}&offset={offset}"
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        page = resp.json()
        if not page:
            break
        all_rows.extend(page)
        log_and_print(f"  Fetched {len(page)} rows (offset={offset}, total so far={len(all_rows)})")
        if len(page) < page_size:
            break
        offset += page_size

    duration = time.time() - start_time
    log_and_print(f"Fetched {len(all_rows)} total rows from TipQA API in {duration:.1f}s")

    serial_set = {s.strip().upper() for s in serial_numbers}

    filtered = [
        row for row in all_rows
        if row.get('BUSINESS_UNIT') == 'JAI'
        and str(row.get('TOOL_NUM', '')).strip().upper() in serial_set
    ]
    log_and_print(f"Filtered to {len(filtered)} JAI rows matching requested serials")

    if not filtered:
        return pd.DataFrame(columns=[
            'serial_number', 'part_number', 'description', 'revision',
            'service_interval_seconds', 'asset_type', 'location',
            'last_maintenance_date', 'asset_serial_number', 'manufacturer',
            'maintenance_status', 'revision_status',
        ])

    records = [_transform_gt_master_row(row) for row in filtered]
    df = pd.DataFrame(records)
    df = df.drop_duplicates(subset=['serial_number'], keep='first')
    return df


def _transform_gt_master_row(row: dict) -> dict:
    """Transform a raw GT_MASTER API row to match the Databricks SQL schema."""
    part_number = (row.get('PART_NUMBER') or '').strip() or None
    if not part_number:
        model = (row.get('MODEL_NUM') or '').strip() or None
        part_number = model

    revision = (row.get('PART_REVISION') or '').strip() or 'A'

    frequency = row.get('FREQUENCY')
    freq_type = row.get('FREQUENCY_TYPE')
    service_interval = None
    if frequency is not None and freq_type:
        try:
            freq_val = float(frequency)
            if freq_type == 'Months':
                service_interval = int(freq_val * 30.4375 * 24 * 60 * 60)
            elif freq_type == 'Weeks':
                service_interval = int(freq_val * 7 * 24 * 60 * 60)
            elif freq_type == 'Days':
                service_interval = int(freq_val * 24 * 60 * 60)
        except (ValueError, TypeError):
            pass

    last_cal = row.get('LAST_CAL_DATE')
    if last_cal and isinstance(last_cal, str):
        last_cal = last_cal.replace('T', ' ')[:19]
    else:
        last_cal = None

    return {
        'serial_number': row.get('TOOL_NUM'),
        'part_number': part_number,
        'description': row.get('TOOL_NUM_DESC'),
        'revision': revision,
        'service_interval_seconds': service_interval,
        'asset_type': row.get('GTYPE'),
        'location': row.get('LOCATION_CODE'),
        'last_maintenance_date': last_cal,
        'asset_serial_number': row.get('MANUFACTURER_SN'),
        'manufacturer': row.get('MANUFACTURER'),
        'maintenance_status': row.get('GT_STATUS_CODE'),
        'revision_status': row.get('R_STATUS'),
    }


def log_and_print(message: str, level: str = 'info'):
    """Simple logging function for database utilities."""
    print(f"[{level.upper()}] {message}")
