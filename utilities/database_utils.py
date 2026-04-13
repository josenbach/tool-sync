#!/usr/bin/env python3
'''
Database Utilities Module
========================

Handles TipQA data access via Databricks Unity Catalog.
Data source: manufacturing.bronze_tipqa.gt_master

Created: 2025-01-28
Author: Jae Osenbach
Purpose: Database connection and query utilities
'''

import time
import pandas as pd
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


def log_and_print(message: str, level: str = 'info'):
    """Simple logging function for database utilities."""
    print(f"[{level.upper()}] {message}")
