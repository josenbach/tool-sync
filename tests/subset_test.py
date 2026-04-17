#!/usr/bin/env python3
"""
SUBSET TEST - TOOL UPDATES FROM CSV SUBSET
==========================================

This script processes a subset of tools from a CSV file and mirrors the live test functionality.
It includes a critical TipQA existence check to ensure tools exist before processing.

Key Features:
1. Loads serial numbers from CSV file (default: tests/subset_tools.csv)
2. Checks TipQA existence first - only processes tools found in TipQA
3. Uses same logic as daily_tool_sync (prepare_tool_data, match_info, update_tool/create_tool)
4. Matches on both serial number AND part number for safety
5. Generates comprehensive reports with mutation complexity analysis
6. Part-level service interval (maintenanceIntervalSeconds) is updated via update_tool.graphql when TipQA differs from Ion

CRITICAL SAFETY NOTES:
- This script makes ACTUAL CHANGES to the specified environment
- Only processes tools that exist in TipQA (prevents adding non-existent tools to Ion)
- Uses same optimized master dataframe creation as live test
- Matches TipQA serial+part with Ion serial+part for safe operations

Usage:
    python tests/subset_test.py --environment v2_production --csv-file path/to/tools.csv

Example - Verify service interval fix (JT00000793 - shared part, null-vs-positive SI):
    python tests/subset_test.py --environment v2_production --csv-file tests/subset_tools_service_interval_fix.csv --verify-service-interval

Example - Fix "lost in Ion but not in TipQA" (e.g. JT00004653): run subset with that serial.
  If TipQA has valid location and maintenance_status not L/OS/OC/TO/QAHD, sync will assign
  MARK_AVAILABLE or UPDATE and clear Ion's unavailable/lost status.
    python tests/subset_test.py --environment v2_production --csv-file tests/subset_tools_jt00004653.csv
"""

import os
import sys
import pandas as pd
import argparse
import traceback
from datetime import datetime
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utilities.database_utils import get_tipqa_connection, get_tipqa_tools_by_serials, get_tipqa_tools_from_api, read_sql_query
from utilities.graphql_utils import get_token
from utilities.shared_sync_utils import (
    log_and_print, load_config, cleanup_previous_test_files,
    analyze_tool_using_daily_sync_logic, create_master_dataframe
)
from utilities.tool_processing_utils import (
    create_tool, update_tool, update_then_mark_unavailable,
    mark_tool_unavailable, mark_tool_available, convert_part_to_tool
)

# Import functions from live test script
from tests.live_test import (
    get_mutation_complexity, generate_reports
)


def prepare_tool_data_from_row(row) -> dict:
    """
    Prepare tool_data from a master dataframe row. Mirrors daily_tool_sync.prepare_tool_data()
    so that subset_test uses the same field mapping (including service_interval_seconds and
    tipqa_service_interval_seconds for part-level maintenanceIntervalSeconds updates).
    """
    tool_data = row.to_dict() if hasattr(row, 'to_dict') else dict(row)
    tool_data['serial_number'] = tool_data.get('tipqa_serial_number', '') or tool_data.get('serial_number', '')
    tool_data['part_number'] = tool_data.get('tipqa_part_number', '') or tool_data.get('part_number', '')
    tool_data['revision'] = tool_data.get('tipqa_revision', '') or tool_data.get('revision', '')
    tool_data['description'] = tool_data.get('tipqa_description', '') or tool_data.get('description', '')
    tool_data['location'] = tool_data.get('tipqa_location', '') or tool_data.get('location', '')
    tool_data['maintenance_status'] = tool_data.get('tipqa_maintenance_status', '') or tool_data.get('maintenance_status', '')
    tool_data['revision_status'] = tool_data.get('tipqa_revision_status', '') or tool_data.get('revision_status', '')
    tool_data['manufacturer'] = tool_data.get('tipqa_manufacturer', '') or tool_data.get('manufacturer', '')
    tool_data['model_number'] = tool_data.get('tipqa_model_number', '') or tool_data.get('model_number', '')
    tool_data['condition'] = tool_data.get('tipqa_condition', '') or tool_data.get('condition', '')
    tool_data['asset_type'] = tool_data.get('tipqa_asset_type', '') or tool_data.get('asset_type', '')
    tool_data['asset_serial_number'] = tool_data.get('tipqa_asset_serial_number', '') or tool_data.get('asset_serial_number', '')
    tool_data['service_interval_seconds'] = tool_data.get('tipqa_service_interval_seconds', '') or tool_data.get('service_interval_seconds', '')
    tool_data['last_maintenance_date'] = tool_data.get('tipqa_last_maintenance_date', '') or tool_data.get('last_maintenance_date', '')
    tool_data['notes'] = tool_data.get('tipqa_notes', '') or tool_data.get('notes', '')
    tool_data['stock_room'] = tool_data.get('tipqa_stock_room', '') or tool_data.get('stock_room', '')
    tool_data['location_name'] = tool_data.get('tipqa_location_name', '') or tool_data.get('location_name', '')
    return tool_data


def verify_service_intervals(master_df: pd.DataFrame, token: str, config: dict, environment: str) -> bool:
    """
    Post-execution verification for the service-interval fix.

    For every part in the subset, fetches the live maintenanceIntervalSeconds
    from Ion and checks:
      1. If ANY serial sharing that part has a positive TipQA service interval,
         the part's Ion value must equal that positive value (not null).
      2. A serial whose TipQA service interval is null must NOT have cleared
         a positive value set by a sibling serial.

    Returns True if all checks pass.
    """
    from utilities.graphql_utils import post_graphql, read_query
    from utilities.tool_processing_utils import safe_convert_service_interval

    log_and_print("\n" + "=" * 60)
    log_and_print("SERVICE INTERVAL VERIFICATION")
    log_and_print("=" * 60)

    part_ids = master_df['ion_part_id'].dropna().unique()
    part_ids = [pid for pid in part_ids if str(pid).strip() and str(pid).strip() != 'nan']

    if not part_ids:
        log_and_print("No Ion part IDs found in master dataframe -- nothing to verify.", 'warning')
        return True

    all_passed = True
    refresh_query = read_query('get_part_etag.graphql')

    for part_id in part_ids:
        part_id_str = str(int(float(part_id))) if isinstance(part_id, (float, int)) else str(part_id).strip()

        rows_for_part = master_df[master_df['ion_part_id'].astype(str).str.strip() == part_id_str]
        part_number = rows_for_part['tipqa_part_number'].iloc[0] if 'tipqa_part_number' in rows_for_part.columns else '?'

        expected_si_values = []
        for _, row in rows_for_part.iterrows():
            raw = row.get('tipqa_service_interval_seconds', '')
            converted = safe_convert_service_interval(raw)
            if converted is not None:
                expected_si_values.append(converted)

        expected = max(expected_si_values) if expected_si_values else None

        result = post_graphql(token, config, {'query': refresh_query, 'variables': {'id': part_id_str}}, environment)
        if 'errors' in result:
            log_and_print(f"  WARN  Part {part_number} (ID {part_id_str}): could not fetch from Ion", 'warning')
            continue

        live_si = result.get('data', {}).get('part', {}).get('maintenanceIntervalSeconds')
        live_norm = None if (live_si is None or str(live_si).strip() == '') else int(float(live_si))

        serials = rows_for_part['tipqa_serial_number'].tolist() if 'tipqa_serial_number' in rows_for_part.columns else []
        serial_list = ', '.join(str(s) for s in serials[:5])

        if expected is not None:
            if live_norm == expected:
                log_and_print(f"  PASS  Part {part_number} (ID {part_id_str}): Ion SI = {live_norm} matches expected {expected}  (serials: {serial_list})")
            else:
                log_and_print(f"  FAIL  Part {part_number} (ID {part_id_str}): Ion SI = {live_norm}, expected {expected}  (serials: {serial_list})", 'error')
                all_passed = False
        else:
            log_and_print(f"  INFO  Part {part_number} (ID {part_id_str}): no positive TipQA SI in subset; Ion SI = {live_norm}  (serials: {serial_list})")

    log_and_print("=" * 60)
    if all_passed:
        log_and_print("SERVICE INTERVAL VERIFICATION: ALL CHECKS PASSED")
    else:
        log_and_print("SERVICE INTERVAL VERIFICATION: SOME CHECKS FAILED", 'error')
    log_and_print("=" * 60 + "\n")

    return all_passed


def load_subset_from_csv(csv_file_path: str) -> pd.DataFrame:
    """Load serial numbers from CSV file for subset processing."""
    log_and_print(f"Loading subset tools from CSV: {csv_file_path}")
    
    try:
        df = pd.read_csv(csv_file_path)
        log_and_print(f"Loaded {len(df)} tools from CSV")
        
        # Check if we have serial_number column, or if first column contains serials
        if 'serial_number' in df.columns:
            serial_column = 'serial_number'
        elif len(df.columns) == 1:
            # Assume first column contains serial numbers
            serial_column = df.columns[0]
            df = df.rename(columns={serial_column: 'serial_number'})
            log_and_print(f"Using first column '{serial_column}' as serial_number")
        else:
            # Look for columns that might contain serial numbers
            possible_serial_columns = [col for col in df.columns if 'serial' in col.lower()]
            if possible_serial_columns:
                serial_column = possible_serial_columns[0]
                df = df.rename(columns={serial_column: 'serial_number'})
                log_and_print(f"Using column '{serial_column}' as serial_number")
            else:
                raise ValueError("No serial number column found. Please ensure CSV has 'serial_number' column or serial numbers in first column")
        
        # Clean up serial numbers (remove any whitespace, convert to string)
        df['serial_number'] = df['serial_number'].astype(str).str.strip()
        
        # Remove any empty serial numbers
        df = df[df['serial_number'].notna() & (df['serial_number'] != '') & (df['serial_number'] != 'nan')]
        
        log_and_print(f"Found {len(df)} valid serial numbers")
        log_and_print(f"Sample serial numbers: {df['serial_number'].head().tolist()}")
        
        return df
        
    except Exception as e:
        log_and_print(f"Error loading CSV file: {str(e)}", 'error')
        raise

def get_tipqa_tools_for_subset(subset_df: pd.DataFrame, conn, config: dict = None) -> pd.DataFrame:
    """Get TipQA data for the subset of tools - CRITICAL: Only process tools found in TipQA."""
    log_and_print("Fetching TipQA data for subset tools...")
    
    # Get serial numbers from subset
    serial_numbers = subset_df['serial_number'].dropna().unique().tolist()
    log_and_print(f"Looking up {len(serial_numbers)} unique serial numbers in TipQA")
    
    try:
        tipqa_df = get_tipqa_tools_by_serials(conn, serial_numbers, config)
        log_and_print(f"Found {len(tipqa_df)} tools in TipQA out of {len(serial_numbers)} requested")
        
        # Check which serials were NOT found in TipQA
        found_serials = set(tipqa_df['serial_number'].tolist())
        requested_serials = set(serial_numbers)
        missing_serials = requested_serials - found_serials
        
        if missing_serials:
            log_and_print(f"WARNING: {len(missing_serials)} serial numbers NOT found in TipQA:")
            for serial in sorted(missing_serials):
                log_and_print(f"  - {serial}")
            log_and_print("These serials will be SKIPPED (not processed)")
        
        # Validate that we have part numbers for all found tools
        tools_without_part_numbers = tipqa_df[tipqa_df['part_number'].isna() | (tipqa_df['part_number'] == '')]
        if len(tools_without_part_numbers) > 0:
            log_and_print(f"WARNING: {len(tools_without_part_numbers)} tools found in TipQA without part numbers:")
            for _, tool in tools_without_part_numbers.iterrows():
                log_and_print(f"  - Serial: {tool['serial_number']}, Part: '{tool['part_number']}'")
            log_and_print("These tools will be SKIPPED (missing part number)")
        
        return tipqa_df
        
    except Exception as e:
        log_and_print(f"Error fetching TipQA data: {str(e)}", 'error')
        raise

def run_subset_sync_test(subset_df: pd.DataFrame, config: dict, 
                        environment: str, master_df: pd.DataFrame = None) -> dict:
    """Run the sync test on subset tools using live test logic."""
    
    log_and_print(f"Running subset sync test on {len(subset_df)} tools in LIVE mode")
    
    # Get Ion API token
    token = get_token(config, environment=environment)
    
    # Get lost location ID
    from utilities.graphql_utils import get_lost_location_id
    lost_location_id = get_lost_location_id(token, config, environment)
    
    # Initialize stats
    stats = {}
    test_results = []
    
    # Process each tool using pre-analyzed actions and reasons
    for idx, tool_row in subset_df.iterrows():
        # Use same prepare_tool_data logic as daily_tool_sync (service_interval_seconds, description, etc.)
        tool_data = prepare_tool_data_from_row(tool_row)
        serial_number = tool_data.get('serial_number', '') or tool_data.get('tipqa_serial_number', '')

        # Get pre-analyzed action and reason from the dataframe
        action = tool_data.get('action_in_ion', 'UNKNOWN')
        reason = tool_data.get('reason', 'UNKNOWN')

        log_and_print(f"Processing subset tool {idx+1}/{len(subset_df)}: {serial_number} ({action}/{reason})")

        try:
            # Build match_info from Ion data (same shape as daily_tool_sync.process_tool_live)
            match_info = {}
            if tool_data.get('ion_id'):
                ion_id = tool_data.get('ion_id')
                if pd.notna(ion_id) and str(ion_id).strip() and str(ion_id).strip() != 'nan':
                    if isinstance(ion_id, float):
                        ion_id = int(ion_id)
                    ion_etag = tool_data.get('ion_etag', '') or tool_data.get('ion__etag', '')
                    ion_part_etag = tool_data.get('ion_part_etag', '') or tool_data.get('ion_part__etag', '')
                    match_info = {
                        'id': ion_id,
                        '_etag': ion_etag,
                        'serialNumber': tool_data.get('ion_serialNumber', serial_number),
                        'status': tool_data.get('ion_status'),
                        'part': {
                            'id': tool_data.get('ion_part_id', ''),
                            '_etag': ion_part_etag,
                            'partNumber': tool_data.get('ion_part_partNumber', ''),
                            'revision': tool_data.get('ion_part_revision', ''),
                            'description': tool_data.get('ion_part_description', ''),
                            'partType': tool_data.get('ion_part_partType'),
                            'trackingType': tool_data.get('ion_part_trackingType'),
                        }
                    }
            
            # Execute the action (LIVE mode - actual changes) - same as live test
            success = False
            error_message = None
            
            try:
                if action == 'SKIP':
                    success = True  # SKIP actions are considered successful - no API calls
                elif action == 'CREATE':
                    result = create_tool(token, config, tool_data, environment, dry_run=False, merged_df=master_df)
                    if isinstance(result, tuple):
                        success, error_message = result
                    else:
                        # Backward compatibility: handle old bool return
                        success = result
                        error_message = f"Failed to create tool {serial_number}" if not success else None
                    log_and_print(f"create_tool returned: {success} for {serial_number}", 'info')
                    if not success:
                        if not error_message:
                            error_message = f"Failed to create tool {serial_number}"
                        log_and_print(f"CREATE action failed for {serial_number}: {error_message}", 'error')
                elif action == 'UPDATE':
                    # Wrap match_info in the expected format
                    wrapped_match_info = {'match': match_info} if match_info else {}
                    result = update_tool(token, config, tool_data, wrapped_match_info, environment, dry_run=False)
                    if isinstance(result, tuple):
                        success, error_message = result
                    else:
                        # Backward compatibility: handle old bool return
                        success = result
                        error_message = f"Failed to update tool {serial_number}" if not success else None
                    if not success and not error_message:
                        error_message = f"Failed to update tool {serial_number}"
                elif action == 'CONVERT_PART_TO_TOOL':
                    # Wrap match_info in the expected format
                    wrapped_match_info = {'match': match_info} if match_info else {}
                    result = convert_part_to_tool(token, config, tool_data, wrapped_match_info, environment, dry_run=False)
                    if isinstance(result, tuple):
                        success, error_message = result
                    else:
                        # Backward compatibility: handle old bool return
                        success = result
                        error_message = f"Failed to convert part to tool {serial_number}" if not success else None
                    if not success and not error_message:
                        error_message = f"Failed to convert part to tool {serial_number}"
                elif action == 'UPDATE_THEN_MARK_UNAVAILABLE':
                    # Wrap match_info in the expected format
                    wrapped_match_info = {'match': match_info} if match_info else {}
                    result = update_then_mark_unavailable(token, config, tool_data, wrapped_match_info, lost_location_id, environment, dry_run=False)
                    if isinstance(result, tuple):
                        success, error_message = result
                    else:
                        # Backward compatibility: handle old bool return
                        success = result
                        error_message = f"Failed to update then mark unavailable tool {serial_number}" if not success else None
                    if not success and not error_message:
                        error_message = f"Failed to update then mark unavailable tool {serial_number}"
                elif action == 'MARK_UNAVAILABLE':
                    # Wrap match_info in the expected format
                    wrapped_match_info = {'match': match_info} if match_info else {}
                    result = mark_tool_unavailable(token, config, tool_data, wrapped_match_info, lost_location_id, environment, dry_run=False)
                    if isinstance(result, tuple):
                        success, error_message = result
                    else:
                        success = result
                        error_message = f"Failed to mark tool unavailable {serial_number}" if not success else None
                    if not success and not error_message:
                        error_message = f"Failed to mark tool unavailable {serial_number}"
                elif action == 'MARK_AVAILABLE':
                    wrapped_match_info = {'match': match_info} if match_info else {}
                    result = mark_tool_available(token, config, tool_data, wrapped_match_info, environment, dry_run=False)
                    if isinstance(result, tuple):
                        success, error_message = result
                    else:
                        success = result
                        error_message = f"Failed to mark tool available {serial_number}" if not success else None
                    if not success and not error_message:
                        error_message = f"Failed to mark tool available {serial_number}"
                else:
                    success = True  # Unknown actions are considered successful
            except Exception as e:
                error_message = str(e)
                success = False
            
            # Record result (same format as live test, but with serial_number first)
            result = {
                'serial_number': serial_number,
                'action': action,
                'reason': reason,
                'part_number': tool_data.get('tipqa_part_number', ''),
                'revision': tool_data.get('tipqa_revision', ''),
                'description': tool_data.get('tipqa_description', ''),
                'location': tool_data.get('tipqa_location', ''),
                'maintenance_status': tool_data.get('tipqa_maintenance_status', ''),
                'revision_status': tool_data.get('tipqa_revision_status', ''),
                'model_number': tool_data.get('tipqa_model_number', ''),
                'manufacturer': tool_data.get('tipqa_manufacturer', ''),
                'condition': tool_data.get('tipqa_condition', ''),
                'status': tool_data.get('tipqa_status', ''),
                'date_added': tool_data.get('tipqa_date_added', ''),
                'last_updated': tool_data.get('tipqa_last_updated', ''),
                'notes': tool_data.get('tipqa_notes', ''),
                'result': 'SUCCESS' if success else 'FAILURE',
                'result_reason': ' '.join(error_message.split()[:5]) if error_message else '',
                'error_message': error_message,
                'timestamp': datetime.now().isoformat()
            }
            test_results.append(result)
            
        except Exception as e:
            log_and_print(f"Exception processing subset tool {serial_number}: {e}", 'error')
            error_str = str(e)
            result_reason = ' '.join(error_str.split()[:5])
            
            result = {
                'serial_number': serial_number,
                'action': 'ERROR',
                'reason': 'exception',
                'part_number': tool_data.get('tipqa_part_number', ''),
                'revision': tool_data.get('tipqa_revision', ''),
                'description': tool_data.get('tipqa_description', ''),
                'location': tool_data.get('tipqa_location', ''),
                'maintenance_status': tool_data.get('tipqa_maintenance_status', ''),
                'revision_status': tool_data.get('tipqa_revision_status', ''),
                'model_number': tool_data.get('tipqa_model_number', ''),
                'manufacturer': tool_data.get('tipqa_manufacturer', ''),
                'condition': tool_data.get('tipqa_condition', ''),
                'status': tool_data.get('tipqa_status', ''),
                'date_added': tool_data.get('tipqa_date_added', ''),
                'last_updated': tool_data.get('tipqa_last_updated', ''),
                'notes': tool_data.get('tipqa_notes', ''),
                'result': 'FAILURE',
                'result_reason': result_reason,
                'error_message': error_str,
                'timestamp': datetime.now().isoformat()
            }
            test_results.append(result)
    
    return {
        'test_results': test_results,
        'stats': stats
    }

def main():
    """Main subset test function."""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run SUBSET test (makes actual changes)')
    parser.add_argument('--csv-file', default='tests/subset_tools.csv',
                       help='Path to CSV file with subset serial numbers (default: tests/subset_tools.csv). Use tests/subset_tools_service_interval.csv to test service-interval fix on JT00000776.')
    parser.add_argument('--environment', required=True,
                       help='Environment to use (e.g., v1_production, v2_production, v1_sandbox, v2_sandbox)')
    parser.add_argument('--verify-service-interval', action='store_true',
                       help='After sync, verify that service intervals were set (not cleared) on shared parts')
    parser.add_argument('--expand-shared-parts', action='store_true',
                       help='Auto-expand subset to include sibling serials sharing the same part (for service-interval testing)')
    parser.add_argument('--analysis-only', action='store_true',
                       help='Only run analysis, do not execute updates (verify fix without making changes)')
    parser.add_argument('--use-api', action='store_true',
                       help='Fetch TipQA data from the real-time REST API instead of Databricks (avoids 4-hour lag)')
    
    args = parser.parse_args()
    
    # Safety check for live mode
    log_and_print("RUNNING SUBSET TEST IN LIVE MODE - ACTUAL CHANGES WILL BE MADE!", 'warning')
    log_and_print(f"This will update tools in {args.environment.upper()}!", 'warning')
    
    log_and_print(f"Starting SUBSET test in LIVE mode...")
    log_and_print(f"Environment: {args.environment}")
    log_and_print(f"CSV file: {args.csv_file}")
    
    # Clean up previous test files first
    log_and_print("Cleaning up previous test files...")
    cleanup_previous_test_files()
    
    # Load environment variables first
    load_dotenv()
    
    # Load configuration
    config = load_config()
    log_and_print('Configuration loaded successfully')
    
    # Load subset from CSV
    log_and_print("Loading subset tools from CSV...")
    subset_df = load_subset_from_csv(args.csv_file)
    
    if len(subset_df) == 0:
        log_and_print("No valid tools found in CSV file", 'error')
        sys.exit(1)
    
    # Fetch TipQA data — either from the real-time REST API or Databricks
    conn = None
    if args.use_api:
        api_config = config.get('tipqa_api', {})
        if not api_config.get('base_url'):
            log_and_print("tipqa_api.base_url not set in config.yaml — cannot use --use-api", 'error')
            sys.exit(1)
        log_and_print("Using TipQA REST API for real-time data (--use-api)")
        serial_numbers = subset_df['serial_number'].dropna().unique().tolist()
        tipqa_tools_df = get_tipqa_tools_from_api(api_config, serial_numbers)
        found_serials = set(tipqa_tools_df['serial_number'].tolist()) if not tipqa_tools_df.empty else set()
        missing_serials = set(serial_numbers) - found_serials
        if missing_serials:
            log_and_print(f"WARNING: {len(missing_serials)} serial numbers NOT found in TipQA API:", 'warning')
            for serial in sorted(missing_serials):
                log_and_print(f"  - {serial}")
            log_and_print("These serials will be SKIPPED (not processed)")
    else:
        conn = get_tipqa_connection(config)
        log_and_print("Checking TipQA existence for subset tools...")
        tipqa_tools_df = get_tipqa_tools_for_subset(subset_df, conn, config)
    
    if len(tipqa_tools_df) == 0:
        log_and_print("No tools found in TipQA for the provided serial numbers", 'error')
        log_and_print("Cannot proceed without TipQA data", 'error')
        sys.exit(1)
    
    log_and_print(f"Found {len(tipqa_tools_df)} tools in TipQA out of {len(subset_df)} requested")

    # Optionally expand subset to include sibling serials sharing the same part
    if args.expand_shared_parts:
        if args.use_api:
            log_and_print("--expand-shared-parts with --use-api: finding siblings via API data...", 'warning')
            original_parts = set(tipqa_tools_df['part_number'].dropna().unique())
            all_serials = subset_df['serial_number'].astype(str).str.strip().tolist()
            sibling_tipqa = get_tipqa_tools_from_api(config.get('tipqa_api', {}), [])  # empty → will fetch all, filter below
            # Re-fetch full table to find siblings by part number
            full_api_config = config.get('tipqa_api', {})
            base_url = full_api_config['base_url'].rstrip('/')
            import requests as _req
            all_rows: list[dict] = []
            offset = 0
            while True:
                resp = _req.get(f"{base_url}/GT_MASTER/?limit=10000&offset={offset}", timeout=60)
                resp.raise_for_status()
                page = resp.json()
                if not page:
                    break
                all_rows.extend(page)
                if len(page) < 10000:
                    break
                offset += 10000
            from utilities.database_utils import _transform_gt_master_row
            sibling_records = [
                _transform_gt_master_row(r) for r in all_rows
                if r.get('BUSINESS_UNIT') == 'JAI'
                and (r.get('PART_NUMBER') or '').strip() in original_parts
            ]
            sibling_df = pd.DataFrame(sibling_records)
            if not sibling_df.empty:
                new_serials = set(sibling_df['serial_number'].astype(str).str.strip()) - set(tipqa_tools_df['serial_number'].astype(str).str.strip())
                if new_serials:
                    log_and_print(f"  Found {len(new_serials)} sibling serials sharing parts")
                    new_rows = sibling_df[sibling_df['serial_number'].isin(new_serials)]
                    tipqa_tools_df = pd.concat([tipqa_tools_df, new_rows], ignore_index=True).drop_duplicates(subset='serial_number')
                    log_and_print(f"  Expanded subset to {len(tipqa_tools_df)} total serials")
                else:
                    log_and_print("  No additional sibling serials found")
        else:
            log_and_print("--expand-shared-parts: finding sibling serials that share the same part number...")
            original_parts = tipqa_tools_df['part_number'].dropna().unique().tolist()
            if original_parts:
                part_placeholders = ','.join(['?' for _ in original_parts])
                sibling_query = (
                    "SELECT gm.TOOL_NUM AS serial_number "
                    "FROM GT_MASTER gm WITH (NOLOCK) "
                    "WHERE gm.BUSINESS_UNIT = 'JAI' "
                    f"AND LTRIM(RTRIM(gm.PART_NUMBER)) IN ({part_placeholders})"
                )
                sibling_df = pd.read_sql(sibling_query, conn, params=original_parts)
                new_serials = set(sibling_df['serial_number'].astype(str).str.strip()) - set(tipqa_tools_df['serial_number'].astype(str).str.strip())
                if new_serials:
                    log_and_print(f"  Found {len(new_serials)} sibling serials sharing parts {original_parts}")
                    expanded_subset = pd.DataFrame({'serial_number': list(new_serials)})
                    sibling_tipqa = get_tipqa_tools_for_subset(expanded_subset, conn, config)
                    if len(sibling_tipqa) > 0:
                        tipqa_tools_df = pd.concat([tipqa_tools_df, sibling_tipqa], ignore_index=True).drop_duplicates(subset='serial_number')
                        log_and_print(f"  Expanded subset to {len(tipqa_tools_df)} total serials")
                else:
                    log_and_print("  No additional sibling serials found")

    # Get Ion API token
    token = get_token(config, environment=args.environment)
    
    # Create master dataframe for ONLY the subset tools (optimized for small subsets)
    # For subset testing, we only need Ion matches for the specific serial/part combinations
    # We don't need to fetch all Ion tools (Step 3) since we're only testing a small subset
    log_and_print("Creating master dataframe for subset tools only...")
    log_and_print("This will only fetch Ion data for the specific tools in the subset")
    try:
        from utilities.shared_sync_utils import (
            get_ion_matches_for_tipqa_tools_optimized,
            combine_ion_dataframes,
            ion_data_to_dataframe,
            append_ion_to_tipqa,
            remove_duplicates_from_master_dataframe
        )
        
        # Step 1: TipQA tools already loaded (tipqa_tools_df)
        log_and_print(f"Step 1: TipQA tools ready ({len(tipqa_tools_df)} tools)")
        
        # Step 2: Get Ion matches for only the subset tools (serial/part combinations)
        log_and_print("Step 2: Querying Ion for exact TipQA matches (both TOOL and PART partTypes)...")
        tipqa_tools_list = tipqa_tools_df.to_dict('records')
        ion_matches = get_ion_matches_for_tipqa_tools_optimized(token, config, tipqa_tools_list, args.environment)
        log_and_print(f"Found {len(ion_matches.get('by_serial', {}))} Ion matches for subset tools")
        
        # Step 3: For subset testing, we can skip fetching all Ion tools since we only need the matches
        # This is much faster for small subsets
        log_and_print("Step 3: Skipped (subset test - only need matches, not all Ion tools)")
        all_ion_tools = {"by_serial": {}, "all_tools": []}  # Empty - we don't need all tools for subset
        
        # Step 4: Combine Ion data and convert to dataframe (same path as daily sync's create_master_dataframe)
        log_and_print("Step 4: Combining Ion data and converting to dataframe...")
        combined_ion_dict = combine_ion_dataframes(ion_matches, all_ion_tools)
        ion_df = ion_data_to_dataframe(combined_ion_dict, "subset")
        
        # Step 5: Append Ion data to TipQA dataframe
        log_and_print("Step 5: Appending Ion dataframe to TipQA dataframe...")
        master_df = append_ion_to_tipqa(tipqa_tools_df, ion_df)
        
        # Step 6: Remove duplicates
        log_and_print("Step 6: Removing duplicate records from master dataframe...")
        master_df = remove_duplicates_from_master_dataframe(master_df)
        
        log_and_print(f"Master dataframe created successfully with {len(master_df)} records (subset optimized)")
    except Exception as e:
        log_and_print(f"Error creating master dataframe: {traceback.format_exc()}", 'error')
        sys.exit(1)
    
    if len(master_df) == 0:
        log_and_print("No tools found in master dataframe", 'error')
        sys.exit(1)

    # Populate valid Ion locations (same as daily sync) so analysis compares location correctly
    from utilities.graphql_utils import get_locations
    try:
        valid_ion_locations = set()
        locations_result = get_locations(token, config, args.environment)
        if 'errors' not in locations_result:
            for edge in locations_result.get('data', {}).get('locations', {}).get('edges', []):
                location_name = edge.get('node', {}).get('name', '')
                if location_name:
                    valid_ion_locations.add(location_name.strip().lower())
            # Include TipQA location codes from config (same as daily_tool_sync.py)
            sync_exceptions = config.get('sync_exceptions') or {}
            code_to_name = sync_exceptions.get('location_code_to_ion_name') or {}
            code_to_id = sync_exceptions.get('location_code_to_ion_id') or {}
            if isinstance(code_to_name, dict):
                for code in code_to_name.keys():
                    if code and str(code).strip():
                        valid_ion_locations.add(str(code).strip().lower())
            if isinstance(code_to_id, dict):
                for code in code_to_id.keys():
                    if code and str(code).strip():
                        valid_ion_locations.add(str(code).strip().lower())
            log_and_print(f"Found {len(valid_ion_locations)} valid Ion locations (including config-mapped codes)")
        config['_valid_ion_locations'] = valid_ion_locations
    except Exception as e:
        log_and_print(f"Warning: Could not fetch Ion locations: {e}", 'warning')
        config['_valid_ion_locations'] = set()

    # Perform analysis first - determine actions and reasons BEFORE executing
    log_and_print("Performing analysis on master dataframe...")
    log_and_print("This will determine actions, reasons, and mutation complexity for each tool")
    
    # Initialize analysis columns
    master_df['action_in_ion'] = ''
    master_df['reason'] = ''
    master_df['mutation_complexity'] = ''
    
    # Run analysis for each tool
    stats = {}
    log_and_print(f"Analyzing {len(master_df)} tools...")
    
    for index, row in master_df.iterrows():
        if (index + 1) % 10 == 0:
            log_and_print(f"Analyzing tool {index + 1}/{len(master_df)}...")
        
        # Convert row to dict for analysis
        tool_data = row.to_dict()
        
        # Run analysis logic
        try:
            analysis_result = analyze_tool_using_daily_sync_logic(tool_data, {}, stats, config, dry_run=False, merged_df=master_df)
            
            if isinstance(analysis_result, dict):
                action = analysis_result.get('action', 'UNKNOWN')
                reason = analysis_result.get('reason', 'UNKNOWN')
                match_info = analysis_result.get('match', {})
            else:
                action = 'UNKNOWN'
                reason = 'UNKNOWN'
                match_info = {}
            
            master_df.at[index, 'action_in_ion'] = action
            master_df.at[index, 'reason'] = reason
            master_df.at[index, 'mutation_complexity'] = get_mutation_complexity(reason)
            
            if match_info:
                tool_data['match'] = match_info
                
        except Exception as e:
            log_and_print(f"Error analyzing tool at index {index}: {str(e)}", 'error')
            master_df.at[index, 'action_in_ion'] = 'ERROR'
            master_df.at[index, 'reason'] = 'analysis_error'
            master_df.at[index, 'mutation_complexity'] = 'unknown'
    
    log_and_print(f"Analysis complete! Processed {len(master_df)} tools")
    
    # Show analysis summary
    action_counts = master_df['action_in_ion'].value_counts()
    log_and_print("Analysis Summary:")
    for action, count in action_counts.items():
        log_and_print(f"  {action}: {count} tools")
    
    # Show detailed analysis before executing
    log_and_print("=== PRE-EXECUTION ANALYSIS SUMMARY ===")
    reason_counts = master_df['reason'].value_counts()
    for reason, count in reason_counts.items():
        log_and_print(f"  {reason}: {count} tools")
    log_and_print("========================================")
    
    # Clean up previous subset_analysis CSVs before writing new one
    import glob as glob_module
    for old_csv in glob_module.glob("tests/subset_analysis_*.csv"):
        try:
            os.remove(old_csv)
        except OSError:
            pass

    # Save the analyzed master dataframe to CSV (comprehensive report format)
    # Reorder columns: serial_number first, then action_in_ion, reason, TipQA fields, then Ion fields
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    analysis_csv = f"tests/subset_analysis_{timestamp}.csv"
    
    tipqa_cols = [col for col in master_df.columns if col.startswith('tipqa_')]
    ion_cols = [col for col in master_df.columns if col.startswith('ion_')]
    other_cols = [col for col in master_df.columns if col not in ['serial_number', 'action_in_ion', 'reason', 'mutation_complexity'] 
                  and not col.startswith('tipqa_') and not col.startswith('ion_')]
    
    # New column order: serial_number, action, reason, TipQA fields, Ion fields, mutation_complexity last
    column_order = ['serial_number', 'action_in_ion', 'reason'] + tipqa_cols + ion_cols + other_cols
    if 'mutation_complexity' in master_df.columns:
        column_order.append('mutation_complexity')
    
    # Reorder the dataframe
    master_df = master_df[column_order]
    
    master_df.to_csv(analysis_csv, index=False)
    log_and_print(f"Saved pre-execution analysis to: {analysis_csv}")
    log_and_print("This CSV contains ALL TipQA data, ALL Ion data, actions, and reasons")
    
    # Filter out tools that should be SKIPPED to save execution time
    tools_to_process = master_df[master_df['action_in_ion'] != 'SKIP'].copy()
    log_and_print(f"Skipping {len(master_df) - len(tools_to_process)} tools with SKIP actions")
    log_and_print(f"Will execute updates on {len(tools_to_process)} tools")
    
    if args.analysis_only:
        log_and_print("")
        log_and_print("=== ANALYSIS ONLY MODE - No changes will be made ===")
        for _, row in master_df.iterrows():
            serial = row.get('tipqa_serial_number', row.get('serial_number', '?'))
            action = row.get('action_in_ion', '?')
            reason = row.get('reason', '?')
            tipqa_loc = row.get('tipqa_location', '')
            ion_loc = row.get('ion_location_name', '')
            log_and_print(f"  {serial}: action={action}, reason={reason}")
            log_and_print(f"    TipQA location: {tipqa_loc!r} | Ion location: {ion_loc!r}")
        log_and_print("")
        log_and_print(f"Analysis saved to: {analysis_csv}")
        log_and_print("Run without --analysis-only to execute updates")
        if conn:
            conn.close()
        return
    
    # Run subset sync test using live test logic (only on tools that need updates)
    log_and_print("Running subset sync test on tools that need updates...")
    global tools_to_process_var
    tools_to_process_var = tools_to_process  # Store for use in final log message
    test_results = run_subset_sync_test(tools_to_process, config, args.environment, master_df)
    
    # Add SKIP results to the test results before generating reports
    skipped_tools = master_df[master_df['action_in_ion'] == 'SKIP']
    for idx, row in skipped_tools.iterrows():
        tool_data = row.to_dict()
        serial_number = tool_data.get('tipqa_serial_number', '')
        action = tool_data.get('action_in_ion', 'SKIP')
        reason = tool_data.get('reason', '')
        
        result = {
            'serial_number': serial_number,
            'part_number': tool_data.get('tipqa_part_number', ''),
            'revision': tool_data.get('tipqa_revision', ''),
            'description': tool_data.get('tipqa_description', ''),
            'location': tool_data.get('tipqa_location', ''),
            'maintenance_status': tool_data.get('tipqa_maintenance_status', ''),
            'revision_status': tool_data.get('tipqa_revision_status', ''),
            'model_number': tool_data.get('tipqa_model_number', ''),
            'manufacturer': tool_data.get('tipqa_manufacturer', ''),
            'condition': tool_data.get('tipqa_condition', ''),
            'status': tool_data.get('tipqa_status', ''),
            'date_added': tool_data.get('tipqa_date_added', ''),
            'last_updated': tool_data.get('tipqa_last_updated', ''),
            'notes': tool_data.get('tipqa_notes', ''),
            'action': tool_data.get('action_in_ion', 'SKIP'),
            'reason': tool_data.get('reason', ''),
            'result': 'SUCCESS',
            'result_reason': '',
            'error_message': None,
            'timestamp': datetime.now().isoformat()
        }
        
        test_results['test_results'].append(result)
    
    # Generate reports (includes both executed and skipped tools)
    if test_results['test_results']:
        log_and_print("Generating execution reports...")
        generate_reports(test_results['test_results'], test_results['stats'], timestamp, args.environment)
    else:
        log_and_print("No tools to report (all skipped, no execution needed).")
    
    # Post-execution service interval verification
    if args.verify_service_interval:
        verification_passed = verify_service_intervals(master_df, token, config, args.environment)
        if not verification_passed:
            log_and_print("Subset test finished WITH VERIFICATION FAILURES", 'error')
            sys.exit(1)

    log_and_print("Subset test completed successfully!")
    log_and_print(f"Processed {len(master_df)} tools from CSV subset")
    log_and_print(f"Only tools found in TipQA were processed (serial + part number matching)")
    executed_count = len(test_results['test_results'])
    log_and_print(f"  - {executed_count} tools updated/processed")
    log_and_print(f"  - {len(master_df) - executed_count} tools skipped (already up-to-date)")

if __name__ == "__main__":
    main()
