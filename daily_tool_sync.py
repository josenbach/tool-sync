#!/usr/bin/env python3
'''
Daily Tool Synchronization Script - Live Mode Only
==================================================

This script performs live synchronization between TipQA and Ion.
It ALWAYS makes actual changes - there is no dry run mode.

For analysis and dry runs, use: tests/dry_run_test.py

Created: 2025-01-28
Author: Jae Osenbach
Purpose: Live tool synchronization orchestration
Version: 4.1 (Live mode only)

CRITICAL ARCHITECTURE NOTES:
============================

Part-Level vs Inventory-Level Fields:
-------------------------------------
Ion has a one-to-many relationship: one Part can have many Inventory items (serials).
TipQA is serial-based: each serial can have different values.

PART-LEVEL FIELDS (shared across all serials of the same part):
- description → part.description
- revision → part.revision
- service_interval_seconds → part.maintenanceIntervalSeconds

Update Behavior:
- Batch update aggregates using MOST COMMON value across all TipQA serials
- Comparison SKIPS if Ion already has a value (prevents ping-pong updates)
- Only updated if Ion is empty and TipQA has a value

INVENTORY-LEVEL FIELDS (per serial):
- location → inventory.locationId
- last_maintenance_date → inventory.lastMaintainedDate
- asset_serial_number → inventory.attributes.Asset Serial Number
- manufacturer → inventory.attributes.Manufacturer

Update Behavior:
- NEVER aggregated - always updated individually per serial
- Always compared and updated if they differ from TipQA

Query Organization:
------------------
All GraphQL queries are stored in queries/ folder and loaded via read_query().
All SQL queries are stored in queries/ folder and loaded via read_sql_query().
No queries are hardcoded in Python scripts.

See queries/master_data_flow.md for complete documentation.
'''

import os
import sys
import yaml
import time
import argparse
import threading
import glob
from typing import Dict, Any, List
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# Import our modular utilities
from utilities.database_utils import get_tipqa_connection, get_all_tipqa_tools
from utilities.graphql_utils import get_token, get_lost_location_id, post_graphql, read_query, organize_ion_data_by_serial, AuthenticationError
from utilities.shared_sync_utils import (
    log_and_print, load_config, create_master_dataframe,
    analyze_tool_using_daily_sync_logic, process_orphaned_ion_tools
)
from utilities.safety_utils import validate_tool_data, validate_environment_config
from utilities.tool_processing_utils import create_tool, update_tool, convert_part_to_tool, mark_tool_unavailable, mark_tool_available, update_then_mark_unavailable


def process_tool_live(tool_data: Dict, token: str, config: Dict[str, Any],
                      lost_location_id: str, environment: str, stats: Dict,
                      ion_data: Dict = None, retry_with_new_token: bool = False,
                      updated_parts_cache: set = None, parts_cache_lock: threading.Lock = None,
                      merged_df = None) -> tuple[bool, str, str]:
    """Process a single tool in live mode - always executes actions.

    Returns: (success: bool, new_token_if_needed: str, error_message: str)
    """
    import pandas as pd

    serial_number = tool_data.get('serial_number') or tool_data.get(
        'tipqa_serial_number', 'UNKNOWN')
    current_token = token

    try:
        # OPTIMIZED: Use pre-computed action and reason from master dataframe
        # Action and reason were already computed during pre-analysis phase
        action = tool_data.get('action_in_ion', 'UNKNOWN')
        reason = tool_data.get('reason', 'UNKNOWN')

        # Build match_info from tool_data (needed for UPDATE operations)
        # This avoids re-analyzing - we already know what needs to be done
        match_info = {}
        if action in [
    'UPDATE',
    'CONVERT_PART_TO_TOOL',
    'UPDATE_THEN_MARK_UNAVAILABLE',
    'MARK_UNAVAILABLE',
     'MARK_AVAILABLE']:
            ion_id = tool_data.get('ion_id', '')
            if pd.notna(ion_id) and str(ion_id).strip() and str(
                ion_id).strip() != 'nan':
                # Convert float to int if needed
                if isinstance(ion_id, float):
                    ion_id = int(ion_id)
                # Get etag - try both single and double underscore versions
                ion_etag = tool_data.get(
    'ion_etag', '') or tool_data.get(
        'ion__etag', '')
                ion_part_etag = tool_data.get(
    'ion_part_etag', '') or tool_data.get(
        'ion_part__etag', '')

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
                        'partType': tool_data.get('ion_part_partType', ''),
                        'trackingType': tool_data.get('ion_part_trackingType', ''),
                    }
                }

        if action == 'UNKNOWN' or not action:
            log_and_print(
    f"No action found for tool {serial_number} (action: {action})",
     'error')
            stats['errors'] = stats.get('errors', 0) + 1
            return False, current_token, f"No action found for tool {serial_number}"

        # Always execute the action (this is live mode)
        success = False
        error_message = None

        try:
            if action == 'SKIP':
                success = True  # SKIP actions are considered successful - no API calls
                error_message = None
            elif action == 'CREATE':
                result = create_tool(
    current_token,
    config,
    tool_data,
    environment,
    dry_run=False,
     merged_df=merged_df)
                if isinstance(result, tuple):
                    success, error_message = result
                else:
                    # Backward compatibility: handle old bool return
                    success = result
                    error_message = f"Failed to create tool {serial_number}" if not success else None
                if not success and not error_message:
                    error_message = f"Failed to create tool {serial_number}"
            elif action == 'UPDATE':
                result = update_tool(
    current_token,
    config,
    tool_data,
    match_info,
    environment,
    dry_run=False,
    updated_parts_cache=updated_parts_cache,
     parts_cache_lock=parts_cache_lock)
                if isinstance(result, tuple):
                    success, error_message = result
                else:
                    # Backward compatibility: handle old bool return
                    success = result
                    error_message = f"Failed to update tool {serial_number}" if not success else None
                if not success and not error_message:
                    error_message = f"Failed to update tool {serial_number}"
            elif action == 'CONVERT_PART_TO_TOOL':
                result = convert_part_to_tool(
    current_token,
    config,
    tool_data,
    match_info,
    environment,
     dry_run=False)
                if isinstance(result, tuple):
                    success, error_message = result
                else:
                    # Backward compatibility: handle old bool return
                    success = result
                    error_message = f"Failed to convert part to tool {serial_number}" if not success else None
                if not success and not error_message:
                    error_message = f"Failed to convert part to tool {serial_number}"
            elif action == 'UPDATE_THEN_MARK_UNAVAILABLE':
                result = update_then_mark_unavailable(
    current_token,
    config,
    tool_data,
    match_info,
    lost_location_id,
    environment,
    dry_run=False,
    updated_parts_cache=updated_parts_cache,
     parts_cache_lock=parts_cache_lock)
                if isinstance(result, tuple):
                    success, error_message = result
                else:
                    # Backward compatibility: handle old bool return
                    success = result
                    error_message = f"Failed to update then mark unavailable tool {serial_number}" if not success else None
                if not success and not error_message:
                    error_message = f"Failed to update then mark unavailable tool {serial_number}"
            elif action == 'MARK_UNAVAILABLE':
                result = mark_tool_unavailable(
    current_token,
    config,
    tool_data,
    match_info,
    lost_location_id,
    environment,
     dry_run=False)
                if isinstance(result, tuple):
                    success, error_message = result
                else:
                    # Backward compatibility: handle old bool return
                    success = result
                    error_message = f"Failed to mark tool unavailable {serial_number}" if not success else None
                if not success and not error_message:
                    error_message = f"Failed to mark tool unavailable {serial_number}"
            elif action == 'MARK_AVAILABLE':
                result = mark_tool_available(
    current_token,
    config,
    tool_data,
    match_info,
    environment,
     dry_run=False)
                if isinstance(result, tuple):
                    success, error_message = result
                else:
                    # Backward compatibility: handle old bool return
                    success = result
                    error_message = f"Failed to mark tool available {serial_number}" if not success else None
                if not success and not error_message:
                    error_message = f"Failed to mark tool available {serial_number}"
            else:
                success = True  # Unknown actions are considered successful
                error_message = None
        except AuthenticationError:
            # Authentication error - refresh token and retry
            log_and_print(
    f"Authentication error for tool {serial_number}, refreshing token...",
     'warning')
            try:
                current_token = get_token(config, environment)
                log_and_print("Token refreshed successfully", 'info')
                # Retry once with new token if retry flag is set
                if retry_with_new_token and action != 'SKIP':
                    log_and_print(
    f"Retrying tool {serial_number} with new token...", 'info')
                    success, new_token, error_msg = process_tool_live(
    tool_data, current_token, config, lost_location_id, environment, stats, ion_data, retry_with_new_token=False, merged_df=merged_df)
                    return success, new_token, error_msg
            except Exception as token_error:
                log_and_print(
    f"Failed to refresh token: {token_error}", 'error')
            success = False
            error_message = f"Authentication error for tool {serial_number}"
        except Exception as e:
            error_message = str(e)
            success = False
            # Check if it's an authentication error
            auth_error = 'Unable to validate authentication token' in str(
                e) or 'authentication' in str(e).lower() or 'UNAUTHORIZED' in str(e).upper()
            if auth_error:
                log_and_print(
    f"Authentication error for tool {serial_number}, refreshing token...",
     'warning')
                try:
                    current_token = get_token(config, environment)
                    log_and_print("Token refreshed successfully", 'info')
                    # Retry once with new token if retry flag is set
                    if retry_with_new_token and action != 'SKIP':
                        log_and_print(
    f"Retrying tool {serial_number} with new token...", 'info')
                        success, new_token, error_msg = process_tool_live(
    tool_data, current_token, config, lost_location_id, environment, stats, ion_data, retry_with_new_token=False, merged_df=merged_df)
                    return success, new_token, error_msg
                except Exception as token_error:
                    log_and_print(
    f"Failed to refresh token: {token_error}", 'error')
            else:
                log_and_print(
    f"Exception processing tool {serial_number}: {error_message}", 'error')

        # Update stats based on action and success
        if success:
            if action == 'CREATE':
                stats['created'] = stats.get('created', 0) + 1
            elif action == 'UPDATE':
                stats['updated'] = stats.get('updated', 0) + 1
            elif action == 'CONVERT_PART_TO_TOOL':
                stats['converted'] = stats.get('converted', 0) + 1
            elif action == 'UPDATE_THEN_MARK_UNAVAILABLE':
                stats['update_then_mark_unavailable'] = stats.get(
                    'update_then_mark_unavailable', 0) + 1
            elif action == 'MARK_UNAVAILABLE':
                stats['marked_unavailable'] = stats.get(
                    'marked_unavailable', 0) + 1
            elif action == 'MARK_AVAILABLE':
                stats['marked_available'] = stats.get(
                    'marked_available', 0) + 1
            elif action == 'SKIP':
                stats['skipped'] = stats.get('skipped', 0) + 1
        else:
            stats['errors'] = stats.get('errors', 0) + 1
            # Always log errors immediately to terminal (except auth errors
            # which are handled separately)
            if error_message and 'Unable to validate authentication token' not in error_message:
                log_and_print(
    f"ERROR: Tool {serial_number} ({action}/{reason}): {error_message}",
     'error')
            elif not error_message:
                # If error_message is empty, log a warning so we know something
                # failed
                log_and_print(
    f"ERROR: Tool {serial_number} ({action}/{reason}): Failed but no error message provided",
     'error')
            # Track error details for summary (thread-safe via stats_lock in wrapper)
            # Note: error_details will be appended in process_tool_wrapper for
            # thread safety

        return success, current_token, error_message or ''

    except Exception as e:
        error_msg = str(e)
        log_and_print(
    f"ERROR: Exception processing tool {serial_number}: {error_msg}",
     'error')
        stats['errors'] = stats.get('errors', 0) + 1
        return False, current_token, error_msg


def sync_tools_live(conn, token: str, config: Dict[str, Any],
                    lost_location_id: str, environment: str) -> Dict[str, Any]:
    """Main sync function - always runs in live mode."""

    stats = {
        "total_tools": 0,
        "created": 0,
        "updated": 0,
        "converted": 0,
        "skipped": 0,
        "errors": 0,
        "update_then_mark_unavailable": 0,
        "marked_unavailable": 0,
        "marked_available": 0,
        "_processed_count": 0,
        "error_details": []  # Thread-safe list managed via stats_lock
    }

    log_and_print('Starting LIVE tool synchronization...')
    log_and_print(f"ENVIRONMENT: {environment.upper()}")
    log_and_print('WARNING: This will make ACTUAL changes to Ion!', 'warning')

    # Get all tools from TipQA
    log_and_print('Fetching all tools from TipQA...')
    tipqa_tools = get_all_tipqa_tools(conn, config)
    stats['total_tools'] = len(tipqa_tools)

    log_and_print(f"Found {stats['total_tools']} tools in TipQA")

    # Create master dataframe using the proven approach
    log_and_print('Creating master dataframe with proper Ion matching...')
    master_df = create_master_dataframe(
    token, config, tipqa_tools, environment, dry_run_mode=False)
    log_and_print(f"Master dataframe created with {len(master_df)} records")

    # Fetch Ion locations once to validate TipQA locations
    # This prevents false positives when TipQA locations don't exist in Ion
    log_and_print('Fetching Ion locations to validate TipQA locations...')
    from utilities.graphql_utils import get_locations
    locations_result = get_locations(token, config, environment)
    valid_ion_locations = set()
    if 'errors' not in locations_result:
        for location in locations_result.get(
    'data',
    {}).get(
        'locations',
        {}).get(
            'edges',
             []):
            location_name = location['node'].get('name', '').strip()
            if location_name:
                valid_ion_locations.add(location_name.lower())
        # CRITICAL: Also include TipQA location codes from config mappings.
        # TipQA may use location codes (e.g. MAK02) while Ion uses names.
        # Without this, location changes from NULL->valid would be incorrectly
        # skipped when comparing (treating as "no valid location").
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
        # Store in config so it can be accessed by comparison functions
        config['_valid_ion_locations'] = valid_ion_locations
    else:
        log_and_print(
            f"Warning: Failed to fetch Ion locations: {locations_result.get('errors', [])}", 'warning')
        config['_valid_ion_locations'] = set()  # Empty set if fetch fails

    # OPTIMIZED: Pre-compute all actions and reasons upfront (before processing)
    # This way all field comparisons (descriptions, revisions, locations, etc.) happen ONCE upfront,
    # rather than repeatedly during the processing loop. This is much more efficient.
    # After pre-analysis, we filter out SKIP actions and only process tools
    # that need updates.
    log_and_print('Pre-computing actions and reasons for all tools...')
    log_and_print(
        'This compares TipQA and Ion fields (descriptions, revisions, locations, etc.) to determine what updates are needed...')

    import pandas as pd

    # Initialize analysis columns
    master_df['action_in_ion'] = ''
    master_df['reason'] = ''

    # Pre-compute analysis for each tool
    analysis_stats = {
        'skipped': 0,
        'created': 0,
        'updated': 0,
        'marked_unavailable': 0,
        'marked_available': 0,
        'update_then_mark_unavailable': 0,
        'converted': 0,
        'errors': 0
    }

    # Track mismatch reasons for UPDATE actions to help diagnose unnecessary
    # updates
    mismatch_reasons = {}

    # Pre-compute set of Ion part numbers for O(1) lookups during CREATE analysis
    # (avoids O(n) full-dataframe scan per tool in determine_create_mutation_complexity)
    _ion_pn_col = master_df['ion_part_partNumber']
    ion_part_numbers_set = set(
        _ion_pn_col.dropna().astype(str).str.strip().str.lower().unique()
    ) - {'', 'nan', 'none'}
    log_and_print(f"Pre-computed {len(ion_part_numbers_set)} unique Ion part numbers for fast lookups")

    log_and_print(f"Analyzing {len(master_df)} tools...")

    for index, row in master_df.iterrows():
        if index % 1000 == 0:
            log_and_print(f"Analyzed {index}/{len(master_df)} tools...")

        # Convert row to dict for analysis
        tool_data = row.to_dict()
        serial_number = tool_data.get('tipqa_serial_number', 'UNKNOWN')

        # Run analysis logic
        try:
            result = analyze_tool_using_daily_sync_logic(
                tool_data=tool_data,
                ion_data=None,  # Not needed - tool_data already has all Ion columns
                stats=analysis_stats,
                config=config,
                dry_run=False,
                merged_df=master_df,
                ion_part_numbers=ion_part_numbers_set
            )

            # Store results in dataframe
            action = result.get('action', 'UNKNOWN')
            reason = result.get('reason', 'unknown_reason')

            master_df.at[index, 'action_in_ion'] = action
            master_df.at[index, 'reason'] = reason

            # Update stats
            if action == 'SKIP':
                analysis_stats['skipped'] += 1
            elif action == 'CREATE':
                analysis_stats['created'] += 1
            elif action == 'UPDATE':
                analysis_stats['updated'] += 1
                # Track mismatch reasons for UPDATE actions
                if reason not in mismatch_reasons:
                    mismatch_reasons[reason] = 0
                mismatch_reasons[reason] += 1
            elif action == 'MARK_UNAVAILABLE':
                analysis_stats['marked_unavailable'] += 1
            elif action == 'MARK_AVAILABLE':
                analysis_stats['marked_available'] = analysis_stats.get(
                    'marked_available', 0) + 1
            elif action == 'UPDATE_THEN_MARK_UNAVAILABLE':
                analysis_stats['update_then_mark_unavailable'] += 1
                # Track mismatch reasons for UPDATE_THEN_MARK_UNAVAILABLE
                # actions
                if reason not in mismatch_reasons:
                    mismatch_reasons[reason] = 0
                mismatch_reasons[reason] += 1
            elif action == 'CONVERT_PART_TO_TOOL':
                analysis_stats['converted'] += 1

        except Exception as e:
            serial_number = tool_data.get('tipqa_serial_number', 'UNKNOWN')
            log_and_print(
                f"Error analyzing tool {serial_number}: {str(e)}", 'error')
            master_df.at[index, 'action_in_ion'] = 'ERROR'
            master_df.at[index, 'reason'] = f'analysis_error: {str(e)}'
            analysis_stats['errors'] += 1

    log_and_print("\n" + "=" * 70)
    log_and_print("PRE-ANALYSIS COMPLETED!")
    log_and_print("=" * 70)
    log_and_print(f"Analysis Summary:")
    log_and_print(f"  - SKIP: {analysis_stats['skipped']}")
    log_and_print(f"  - CREATE: {analysis_stats['created']}")
    log_and_print(f"  - UPDATE: {analysis_stats['updated']}")
    log_and_print(
        f"  - MARK_UNAVAILABLE: {analysis_stats['marked_unavailable']}")
    log_and_print(
        f"  - MARK_AVAILABLE: {analysis_stats.get('marked_available', 0)}")
    log_and_print(
        f"  - UPDATE_THEN_MARK_UNAVAILABLE: {analysis_stats['update_then_mark_unavailable']}")
    log_and_print(f"  - CONVERT_PART_TO_TOOL: {analysis_stats['converted']}")
    log_and_print(f"  - ERRORS: {analysis_stats['errors']}")

    # Write detailed diagnostic summary to markdown file instead of terminal
    # ONLY in debug mode (for Prefect compatibility - no file system access in
    # production)
    total_updates = analysis_stats['updated'] + \
        analysis_stats['update_then_mark_unavailable']

    # Check if debug mode is enabled (via environment variable)
    debug_mode = os.getenv(
    'DEBUG_MODE',
    'false').lower() in (
        'true',
        '1',
         'yes')

    if debug_mode:
        log_and_print(f"\n[DEBUG] Starting diagnostic file creation...")
        log_and_print(
            f"[DEBUG] total_updates = {total_updates}, mismatch_reasons count = {len(mismatch_reasons) if mismatch_reasons else 0}")

        import datetime

        # Ensure tests directory exists
        tests_dir = 'tests'
        os.makedirs(tests_dir, exist_ok=True)

        # Clean up old diagnostic files (keep only 1 of each type)
        # Pattern: update_diagnostic_*.md, mismatch_diagnostic_*.csv
        diagnostic_patterns = [
            'update_diagnostic_*.md',
            'mismatch_diagnostic_*.csv'
        ]

        for pattern in diagnostic_patterns:
            old_files = glob.glob(os.path.join(tests_dir, pattern))
            if len(old_files) > 0:
                # Sort by modification time and delete all but the newest
                old_files.sort(key=os.path.getmtime, reverse=True)
                # Keep first (newest), delete rest
                for old_file in old_files[1:]:
                    try:
                        os.remove(old_file)
                        log_and_print(
                            f"[DEBUG] Cleaned up old diagnostic file: {os.path.basename(old_file)}")
                    except Exception as e:
                        log_and_print(
    f"[DEBUG] Warning: Could not delete old diagnostic file {old_file}: {e}",
     'warning')

        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        diagnostic_file = os.path.join(
    tests_dir, f'update_diagnostic_{timestamp}.md')

        log_and_print(
            f"DEBUG: Writing diagnostic file to: {os.path.abspath(diagnostic_file)}")
        log_and_print(
            f"DEBUG: total_updates = {total_updates}, mismatch_reasons count = {len(mismatch_reasons) if mismatch_reasons else 0}")

        try:
            with open(diagnostic_file, 'w', encoding='utf-8') as f:
                f.write(f"# Update Diagnostic Report\n\n")
                f.write(
                    f"**Generated:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write(f"## Summary\n\n")
                total_tools = stats.get('total_tools', 'UNKNOWN')
                f.write(f"- **Total tools in TipQA:** {total_tools}\n")
                f.write(f"- **Tools marked for UPDATE:** {total_updates}\n")
                f.write(
                    f"- **Tools marked for UPDATE_THEN_MARK_UNAVAILABLE:** {analysis_stats['update_then_mark_unavailable']}\n")
                f.write(
                    f"- **Tools marked for UPDATE only:** {analysis_stats['updated']}\n\n")

                f.write(f"## Analysis Breakdown\n\n")
                f.write(f"- SKIP: {analysis_stats['skipped']}\n")
                f.write(f"- CREATE: {analysis_stats['created']}\n")
                f.write(f"- UPDATE: {analysis_stats['updated']}\n")
                f.write(
                    f"- MARK_UNAVAILABLE: {analysis_stats['marked_unavailable']}\n")
                f.write(
                    f"- MARK_AVAILABLE: {analysis_stats.get('marked_available', 0)}\n")
                f.write(
                    f"- UPDATE_THEN_MARK_UNAVAILABLE: {analysis_stats['update_then_mark_unavailable']}\n")
                f.write(
                    f"- CONVERT_PART_TO_TOOL: {analysis_stats['converted']}\n")
                f.write(f"- ERRORS: {analysis_stats['errors']}\n\n")

                # ALWAYS show update reasons summary section (even if no
                # updates)
                f.write(f"## Update Reasons Summary\n\n")
                f.write(f"**Total Updates:** {total_updates}\n")
                f.write(
                    f"**Mismatch Reasons Dict Size:** {len(mismatch_reasons) if mismatch_reasons else 0}\n\n")

                if total_updates > 0:
                    # If mismatch_reasons dict is empty, extract reasons from
                    # dataframe
                    if not mismatch_reasons or len(mismatch_reasons) == 0:
                        f.write(
    f"**WARNING:** mismatch_reasons dictionary is empty. Extracting reasons from dataframe...\n\n")
                        # Extract reasons directly from dataframe
                        update_rows = master_df[master_df['action_in_ion'].isin(
                            ['UPDATE', 'UPDATE_THEN_MARK_UNAVAILABLE'])]
                        f.write(
                            f"**DEBUG:** Found {len(update_rows)} rows with UPDATE actions in dataframe.\n\n")
                        if len(update_rows) > 0:
                            reason_counts = update_rows['reason'].value_counts(
                            )
                            mismatch_reasons = reason_counts.to_dict()
                            f.write(
                                f"Found **{len(mismatch_reasons)}** unique reason types from dataframe.\n\n")
                        else:
                            f.write(
    f"No UPDATE actions found in dataframe.\n\n")
                            mismatch_reasons = {}

                    if mismatch_reasons and len(mismatch_reasons) > 0:
                        # Sort by count descending
                        sorted_reasons = sorted(
    mismatch_reasons.items(), key=lambda x: x[1], reverse=True)

                        f.write(f"### Top 50 Reason Types\n\n")
                        f.write(f"| Count | Percentage | Reason |\n")
                        f.write(f"|-------|------------|--------|\n")
                        for reason, count in sorted_reasons[:50]:
                            percentage = (count / total_updates) * 100
                            # Escape pipe characters in reason for markdown
                            # table
                            reason_escaped = reason.replace('|', '\\|')
                            f.write(
                                f"| {count:,} | {percentage:.1f}% | {reason_escaped} |\n")

                        if len(sorted_reasons) > 50:
                            f.write(
                                f"\n*... and {len(sorted_reasons) - 50} more reason types*\n")

                        # Extract field-level mismatch statistics
                        import re
                        field_counts = {}
                        for reason, count in mismatch_reasons.items():
                            # Extract field names from mismatch details (e.g.,
                            # "asset_serial_number:", "manufacturer:")
                            mismatch_pattern = r'(\w+):\s*TipQA='
                            fields = re.findall(mismatch_pattern, reason)
                            for field in fields:
                                if field not in field_counts:
                                    field_counts[field] = 0
                                field_counts[field] += count

                        if field_counts:
                            f.write(
    f"\n### Field-Level Mismatch Breakdown\n\n")
                            f.write(
    f"This shows which specific fields are causing mismatches across all UPDATE actions.\n\n")
                            f.write(
    f"| Field | Tools Affected | Percentage |\n")
                            f.write(
    f"|-------|----------------|------------|\n")
                            sorted_fields = sorted(
    field_counts.items(), key=lambda x: x[1], reverse=True)
                            for field, count in sorted_fields:
                                percentage = (count / total_updates) * 100
                                f.write(
                                    f"| `{field}` | {count:,} | {percentage:.1f}% |\n")

                        # Sample MARK_AVAILABLE tools
                        mark_available_rows = master_df[master_df['action_in_ion']
                            == 'MARK_AVAILABLE']
                        if len(mark_available_rows) > 0:
                            f.write(
                                f"\n### Tools Marked Available (MARK_AVAILABLE)\n\n")
                            f.write(
                                f"Total: {len(mark_available_rows)} tools\n\n")
                            for idx, row in mark_available_rows.iterrows():
                                serial = row.get(
    'tipqa_serial_number', 'UNKNOWN')
                                reason = row.get('reason', 'N/A')
                                ion_status = row.get('ion_status', 'N/A')
                                ion_unavailable = row.get(
                                    'ion_unavailable', 'N/A')
                                tipqa_maintenance = row.get(
                                    'tipqa_maintenance_status', 'N/A')
                                tipqa_location = row.get(
                                    'tipqa_location', 'N/A')
                                f.write(f"#### Tool {serial}\n\n")
                                f.write(f"- **Reason:** {reason}\n")
                                f.write(f"- **Ion Status:** {ion_status}\n")
                                f.write(
    f"- **Ion Unavailable:** {ion_unavailable}\n")
                                f.write(
    f"- **TipQA Maintenance Status:** {tipqa_maintenance}\n")
                                f.write(
    f"- **TipQA Location:** {tipqa_location}\n\n")

                        # Sample mismatches
                        f.write(f"\n### Sample Mismatch Details\n\n")
                        f.write(f"First 10 tools marked for UPDATE:\n\n")
                        sample_count = 0
                        for index, row in master_df.iterrows():
                            if sample_count >= 10:
                                break
                            action = row.get('action_in_ion', '')
                            reason = row.get('reason', '')
                            if action in [
    'UPDATE', 'UPDATE_THEN_MARK_UNAVAILABLE']:
                                serial = row.get(
    'tipqa_serial_number', 'UNKNOWN')
                                part_number = row.get(
                                    'tipqa_part_number', 'UNKNOWN')

                                f.write(
    f"#### Tool {serial} (Part: {part_number})\n\n")
                                f.write(f"- **Action:** {action}\n")
                                f.write(f"- **Reason:** {reason}\n\n")

                                # Show TipQA vs Ion values for key fields
                                tipqa_asset_serial = row.get(
                                    'tipqa_asset_serial_number', '')
                                ion_asset_serial = row.get(
    'ion_attributes_Asset Serial Number', 'MISSING')
                                tipqa_manufacturer = row.get(
                                    'tipqa_manufacturer', '')
                                ion_manufacturer = row.get(
    'ion_attributes_Manufacturer', 'MISSING')
                                tipqa_description = row.get(
                                    'tipqa_description', '')
                                ion_description = row.get(
                                    'ion_part_description', 'MISSING')
                                tipqa_location = row.get('tipqa_location', '')
                                ion_location = row.get(
                                    'ion_location_name', 'MISSING')

                                # DIAGNOSTIC: Show raw TipQA
                                # service_interval_seconds value
                                import pandas as pd
                                tipqa_service_interval_raw = row.get(
                                    'tipqa_service_interval_seconds', '')
                                tipqa_service_interval_type = type(
                                    tipqa_service_interval_raw).__name__
                                tipqa_service_interval_isna = pd.isna(
                                    tipqa_service_interval_raw) if hasattr(pd, 'isna') else False
                                ion_service_interval_raw = row.get(
                                    'ion_part_maintenanceIntervalSeconds', '')
                                ion_service_interval_type = type(
                                    ion_service_interval_raw).__name__
                                ion_service_interval_isna = pd.isna(
                                    ion_service_interval_raw) if hasattr(pd, 'isna') else False

                                f.write(f"**Field Comparisons:**\n\n")
                                f.write(
    f"- `asset_serial_number`: TipQA=`{tipqa_asset_serial}` vs Ion=`{ion_asset_serial}`\n")
                                f.write(
    f"- `manufacturer`: TipQA=`{tipqa_manufacturer}` vs Ion=`{ion_manufacturer}`\n")
                                f.write(
    f"- `description`: TipQA=`{tipqa_description}` vs Ion=`{ion_description}`\n")
                                f.write(
    f"- `location`: TipQA=`{tipqa_location}` vs Ion=`{ion_location}`\n")
                                f.write(
    f"- `service_interval_seconds`: TipQA=`{tipqa_service_interval_raw}` (type: {tipqa_service_interval_type}, isna: {tipqa_service_interval_isna}) vs Ion=`{ion_service_interval_raw}` (type: {ion_service_interval_type}, isna: {ion_service_interval_isna})\n\n")

                                sample_count += 1

                        # DIAGNOSTIC: Show statistics about TipQA
                        # service_interval_seconds values
                        f.write(
    f"\n### Diagnostic: TipQA service_interval_seconds Value Analysis\n\n")
                        import pandas as pd
                        update_rows = master_df[master_df['action_in_ion'].isin(
                            ['UPDATE', 'UPDATE_THEN_MARK_UNAVAILABLE'])]
                        tipqa_service_intervals = update_rows['tipqa_service_interval_seconds']

                        f.write(
                            f"**For {len(update_rows)} tools marked for UPDATE:**\n\n")
                        f.write(
                            f"- Total tools: {len(tipqa_service_intervals)}\n")
                        f.write(
                            f"- NULL/NaN values: {tipqa_service_intervals.isna().sum()}\n")
                        f.write(
                            f"- Empty string values: {(tipqa_service_intervals == '').sum()}\n")
                        f.write(
                            f"- Zero values: {(tipqa_service_intervals == 0).sum()}\n")
                        f.write(
                            f"- Non-zero numeric values: {((tipqa_service_intervals.notna()) & (tipqa_service_intervals != '') & (tipqa_service_intervals != 0)).sum()}\n\n")

                        f.write(f"**Value type distribution:**\n\n")
                        f.write(f"```\n")
                        f.write(
                            f"{tipqa_service_intervals.apply(type).value_counts().to_string()}\n")
                        f.write(f"```\n\n")

                        f.write(f"**Sample raw values (first 20):**\n\n")
                        f.write(f"```\n")
                        for idx, val in enumerate(
                                tipqa_service_intervals.head(20)):
                            f.write(
                                f"{idx+1}. Value: {repr(val)}, Type: {type(val).__name__}, IsNA: {pd.isna(val) if hasattr(pd, 'isna') else 'N/A'}\n")
                        f.write(f"```\n\n")

                    # DIAGNOSTIC: Analyze Ion service_interval_seconds
                    # values, especially conversion failures
                    f.write(
                        f"### Diagnostic: Ion service_interval_seconds Value Analysis\n\n")
                    f.write(
                        f"This section analyzes Ion values to understand conversion failures.\n\n")

                    ion_service_intervals = update_rows['ion_part_maintenanceIntervalSeconds']
                    f.write(
                        f"**For {len(update_rows)} tools marked for UPDATE:**\n\n")
                    f.write(
                        f"- Total Ion values: {len(ion_service_intervals)}\n")
                    f.write(
                        f"- NULL/NaN values: {ion_service_intervals.isna().sum()}\n")
                    f.write(
                        f"- Empty string values: {(ion_service_intervals == '').sum()}\n")
                    f.write(
                        f"- String 'None' values: {(ion_service_intervals == 'None').sum()}\n")
                    f.write(
                        f"- String 'null' values: {(ion_service_intervals == 'null').sum()}\n")
                    f.write(
                        f"- Zero values: {(ion_service_intervals == 0).sum()}\n")
                    f.write(
                        f"- Non-zero numeric values: {((ion_service_intervals.notna()) & (ion_service_intervals != '') & (ion_service_intervals != 0) & (ion_service_intervals != 'None') & (ion_service_intervals != 'null')).sum()}\n\n")

                    f.write(f"**Ion value type distribution:**\n\n")
                    f.write(f"```\n")
                    type_counts = ion_service_intervals.apply(
                        lambda x: type(x).__name__).value_counts()
                    for type_name, count in type_counts.items():
                        f.write(f"{type_name}: {count}\n")
                    f.write(f"```\n\n")

                        # Find tools with conversion failures
                    conversion_failure_rows = update_rows[update_rows['reason'].str.contains(
                        'conversion failed', case=False, na=False)]
                    f.write(
                        f"**Tools with conversion failures: {len(conversion_failure_rows)}**\n\n")

                    if len(conversion_failure_rows) > 0:
                        f.write(
                            f"**Sample Ion values that failed conversion (first 30):**\n\n")
                        f.write(f"```\n")
                        for idx, (_, row) in enumerate(
    conversion_failure_rows.head(30).iterrows()):
                            ion_val = row.get(
    'ion_part_maintenanceIntervalSeconds', '')
                            tipqa_val = row.get(
    'tipqa_service_interval_seconds', '')
                            serial = row.get('tipqa_serial_number', 'UNKNOWN')
                            f.write(
                                f"{idx+1}. Serial: {serial}, TipQA: {repr(tipqa_val)} ({type(tipqa_val).__name__}), Ion: {repr(ion_val)} ({type(ion_val).__name__})\n")
                        f.write(f"```\n\n")

                        # Try to convert Ion values to see what fails
                        f.write(f"**Conversion test results:**\n\n")
                        conversion_test_results = {
    'success': 0, 'value_error': 0, 'type_error': 0, 'other_error': 0}
                        conversion_error_samples = []
                        for _, row in conversion_failure_rows.head(
                            100).iterrows():
                            ion_val = row.get(
    'ion_part_maintenanceIntervalSeconds', '')
                            try:
                                if pd.isna(ion_val) or ion_val == '':
                                    conversion_test_results['success'] += 1
                                else:
                                    test_int = int(float(ion_val))
                                    conversion_test_results['success'] += 1
                            except ValueError as e:
                                conversion_test_results['value_error'] += 1
                                if len(conversion_error_samples) < 10:
                                    conversion_error_samples.append((repr(ion_val), type(ion_val).__name__, str(e)))
                            except TypeError as e:
                                conversion_test_results['type_error'] += 1
                                if len(conversion_error_samples) < 10:
                                    conversion_error_samples.append((repr(ion_val), type(ion_val).__name__, str(e)))
                            except Exception as e:
                                conversion_test_results['other_error'] += 1
                                if len(conversion_error_samples) < 10:
                                    conversion_error_samples.append((repr(ion_val), type(ion_val).__name__, str(e)))

                        f.write(f"- Successful conversions: {conversion_test_results['success']}\n")
                        f.write(f"- ValueError: {conversion_test_results['value_error']}\n")
                        f.write(f"- TypeError: {conversion_test_results['type_error']}\n")
                        f.write(f"- Other errors: {conversion_test_results['other_error']}\n\n")

                        if conversion_error_samples:
                            f.write(f"**Sample conversion errors:**\n\n")
                            f.write(f"```\n")
                            for val_repr, val_type, error_msg in conversion_error_samples:
                                f.write(f"Value: {val_repr}, Type: {val_type}, Error: {error_msg}\n")
                            f.write(f"```\n\n")

                        # NEW SECTION: Service Interval Changes
                        f.write(f"\n### Service Interval Changes\n\n")
                        f.write(f"This section lists all tools where the service interval differs between TipQA and Ion.\n")
                        f.write(f"**Note:** These changes will be synchronized to Ion. Review for potential TipQA data issues.\n\n")

                        import pandas as pd
                        service_interval_changes = []

                        for index, row in master_df.iterrows():
                            action = row.get('action_in_ion', '')
                            if action in ['UPDATE', 'UPDATE_THEN_MARK_UNAVAILABLE']:
                                tipqa_si = row.get('tipqa_service_interval_seconds', '')
                                ion_si = row.get('ion_part_maintenanceIntervalSeconds', '')

                                # Normalize values for comparison
                                tipqa_si_str = str(tipqa_si).strip() if tipqa_si else ''
                                ion_si_str = str(ion_si).strip() if ion_si else ''

                                # Handle None/NaN values
                                if pd.isna(tipqa_si) or tipqa_si_str.lower() in ('nan', 'none', ''):
                                    tipqa_si_str = ''
                                if pd.isna(ion_si) or ion_si_str.lower() in ('nan', 'none', ''):
                                    ion_si_str = ''

                                # Convert to integers for comparison (empty/null = 0)
                                try:
                                    tipqa_si_int = int(float(tipqa_si_str)) if tipqa_si_str else 0
                                    ion_si_int = int(float(ion_si_str)) if ion_si_str else 0

                                    # Only include if they differ
                                    if tipqa_si_int != ion_si_int:
                                        serial = row.get('tipqa_serial_number', 'UNKNOWN')
                                        part_number = row.get('tipqa_part_number', 'UNKNOWN')
                                        revision = row.get('tipqa_revision', '')

                                        service_interval_changes.append({
                                            'serial': serial,
                                            'part_number': part_number,
                                            'revision': revision,
                                            'tipqa_value': tipqa_si_int if tipqa_si_str else 'NULL/Empty',
                                            'ion_value': ion_si_int if ion_si_str else 'NULL/Empty',
                                            'tipqa_raw': tipqa_si_str if tipqa_si_str else 'NULL/Empty',
                                            'ion_raw': ion_si_str if ion_si_str else 'NULL/Empty'
                                        })
                                except (ValueError, TypeError):
                                    # If conversion fails, still include if strings differ
                                    if tipqa_si_str != ion_si_str:
                                        serial = row.get('tipqa_serial_number', 'UNKNOWN')
                                        part_number = row.get('tipqa_part_number', 'UNKNOWN')
                                        revision = row.get('tipqa_revision', '')

                                        service_interval_changes.append({
                                            'serial': serial,
                                            'part_number': part_number,
                                            'revision': revision,
                                            'tipqa_value': tipqa_si_str if tipqa_si_str else 'NULL/Empty',
                                            'ion_value': ion_si_str if ion_si_str else 'NULL/Empty',
                                            'tipqa_raw': tipqa_si_str if tipqa_si_str else 'NULL/Empty',
                                            'ion_raw': ion_si_str if ion_si_str else 'NULL/Empty'
                                        })

                        if service_interval_changes:
                            f.write(f"**Total tools with service interval changes: {len(service_interval_changes)}**\n\n")
                            f.write(f"| Serial Number | Part Number | Revision | TipQA Value | Ion Value | TipQA Raw | Ion Raw |\n")
                            f.write(f"|---------------|-------------|----------|-------------|-----------|-----------|----------|\n")

                            # Sort by part number, then serial for easier review
                            service_interval_changes.sort(key=lambda x: (x['part_number'], x['serial']))

                            for change in service_interval_changes:
                                serial_escaped = str(change['serial']).replace('|', '\\|')
                                part_escaped = str(change['part_number']).replace('|', '\\|')
                                rev_escaped = str(change['revision']).replace('|', '\\|')
                                tipqa_val_escaped = str(change['tipqa_value']).replace('|', '\\|')
                                ion_val_escaped = str(change['ion_value']).replace('|', '\\|')
                                tipqa_raw_escaped = str(change['tipqa_raw']).replace('|', '\\|')
                                ion_raw_escaped = str(change['ion_raw']).replace('|', '\\|')

                                f.write(f"| {serial_escaped} | {part_escaped} | {rev_escaped} | {tipqa_val_escaped} | {ion_val_escaped} | {tipqa_raw_escaped} | {ion_raw_escaped} |\n")

                            f.write(f"\n**Summary by Part Number:**\n\n")
                            # Count changes by part number
                            part_counts = {}
                            for change in service_interval_changes:
                                part_key = f"{change['part_number']}-{change['revision']}"
                                if part_key not in part_counts:
                                    part_counts[part_key] = 0
                                part_counts[part_key] += 1

                            sorted_parts = sorted(part_counts.items(), key=lambda x: x[1], reverse=True)
                            f.write(f"| Part Number-Revision | Tools Affected |\n")
                            f.write(f"|----------------------|----------------|\n")
                            for part_key, count in sorted_parts:
                                part_escaped = part_key.replace('|', '\\|')
                                f.write(f"| {part_escaped} | {count} |\n")
                        else:
                            f.write(f"**No service interval changes detected.** All service intervals match between TipQA and Ion.\n\n")
                    else:
                        f.write(f"**NOTE:** No reason details available. This may indicate a bug in reason tracking.\n\n")
                else:
                    # No updates required
                    f.write(f"## No Updates Required\n\n")
                    f.write(f"No tools were marked for UPDATE in this run.\n\n")
                    f.write(f"- Total tools analyzed: {len(master_df)}\n")
                    f.write(f"- Tools skipped (already up-to-date): {analysis_stats['skipped']}\n")
                    f.write(f"- Tools to create: {analysis_stats['created']}\n")

                # File writing completed successfully
            log_and_print(f"\n{'='*70}")
            log_and_print(f"UPDATE DIAGNOSTIC REPORT WRITTEN TO:")
            log_and_print(f"  {os.path.abspath(diagnostic_file)}")
            log_and_print(f"  File size: {os.path.getsize(diagnostic_file) if os.path.exists(diagnostic_file) else 'FILE NOT FOUND'} bytes")
            log_and_print(f"{'='*70}\n")
        except Exception as e:
            log_and_print(f"ERROR writing diagnostic file: {e}", 'error')
            import traceback
            log_and_print(f"Full traceback:\n{traceback.format_exc()}", 'error')
            log_and_print(f"File path attempted: {os.path.abspath(diagnostic_file) if 'diagnostic_file' in locals() else 'N/A'}", 'error')
            log_and_print(f"DEBUG: total_updates = {total_updates}", 'error')
            log_and_print(f"DEBUG: mismatch_reasons type = {type(mismatch_reasons)}, length = {len(mismatch_reasons) if mismatch_reasons else 0}", 'error')
            # Fall back to brief terminal output
            if total_updates > 0:
                log_and_print(f"\n{'='*70}")
                log_and_print(f"UPDATE REASONS SUMMARY (for {total_updates} tools marked for UPDATE)")
                log_and_print(f"{'='*70}")
                if mismatch_reasons:
                    sorted_reasons = sorted(mismatch_reasons.items(), key=lambda x: x[1], reverse=True)
                    log_and_print(f"Found {len(sorted_reasons)} unique reason types (showing top 10):")
                    for reason, count in sorted_reasons[:10]:
                        percentage = (count / total_updates) * 100
                        log_and_print(f"  [{count:5d} tools ({percentage:5.1f}%)] {reason}")
                else:
                    log_and_print("WARNING: mismatch_reasons dictionary is empty!")
                log_and_print(f"{'='*70}\n")
    else:
        log_and_print("[DEBUG] Debug mode disabled - skipping diagnostic file generation (Prefect compatibility)")
    
    # DIAGNOSTIC CSV EXPORT: Export mismatched tools for analysis
    # ONLY in debug mode (for Prefect compatibility - no file system access in production)
    if debug_mode and total_updates > 0:
        try:
            import pandas as pd
            from datetime import datetime
            
            # Get all UPDATE and UPDATE_THEN_MARK_UNAVAILABLE actions
            mismatched_tools = master_df[
                master_df['action_in_ion'].isin(['UPDATE', 'UPDATE_THEN_MARK_UNAVAILABLE'])
            ].copy()
            
            if len(mismatched_tools) > 0:
                # Create diagnostic CSV with key comparison fields
                diagnostic_df = pd.DataFrame()
                diagnostic_df['action_in_ion'] = mismatched_tools['action_in_ion'].values
                diagnostic_df['reason'] = mismatched_tools['reason'].values
                diagnostic_df['tipqa_serial_number'] = mismatched_tools['tipqa_serial_number'].values
                diagnostic_df['tipqa_part_number'] = mismatched_tools['tipqa_part_number'].values
                
                # Add comparison fields side-by-side for easy analysis
                comparison_fields = [
                    ('description', 'tipqa_description', 'ion_part_description'),
                    ('revision', 'tipqa_revision', 'ion_part_revision'),
                    ('location', 'tipqa_location', 'ion_location_name'),
                    ('manufacturer', 'tipqa_manufacturer', 'ion_attributes_Manufacturer'),
                    ('asset_serial_number', 'tipqa_asset_serial_number', 'ion_attributes_Asset Serial Number'),
                    ('service_interval', 'tipqa_service_interval_seconds', 'ion_part_maintenanceIntervalSeconds'),
                    ('last_maintenance_date', 'tipqa_last_maintenance_date', 'ion_lastMaintainedDate'),
                ]
                
                for field_name, tipqa_col, ion_col in comparison_fields:
                    # Get values from dataframe columns (handle missing columns gracefully)
                    tipqa_vals = mismatched_tools[tipqa_col].astype(str).str.strip() if tipqa_col in mismatched_tools.columns else pd.Series([''] * len(mismatched_tools))
                    ion_vals = mismatched_tools[ion_col].astype(str).str.strip() if ion_col in mismatched_tools.columns else pd.Series([''] * len(mismatched_tools))
                    
                    diagnostic_df[f'{field_name}_tipqa'] = tipqa_vals.values
                    diagnostic_df[f'{field_name}_ion'] = ion_vals.values
                    
                    # Add a match indicator (case-insensitive comparison)
                    tipqa_normalized = tipqa_vals.str.lower().replace(['nan', 'none', 'null'], '', regex=False)
                    ion_normalized = ion_vals.str.lower().replace(['nan', 'none', 'null'], '', regex=False)
                    diagnostic_df[f'{field_name}_matches'] = (tipqa_normalized == ion_normalized).values | (
                        (tipqa_normalized == '') & (ion_normalized == '')
                    ).values
                
                # Add maintenance status fields
                diagnostic_df['tipqa_maintenance_status'] = mismatched_tools['tipqa_maintenance_status'].values if 'tipqa_maintenance_status' in mismatched_tools.columns else ''
                diagnostic_df['ion_status'] = mismatched_tools['ion_status'].values if 'ion_status' in mismatched_tools.columns else ''
                
                # Note: CSV cleanup is already handled above in the diagnostic file cleanup section
                
                # Save to CSV
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                csv_path = f'tests/mismatch_diagnostic_{timestamp}.csv'
                diagnostic_df.to_csv(csv_path, index=False)
                log_and_print(f"\nDIAGNOSTIC CSV exported: {csv_path}")
                log_and_print(f"  Exported {len(diagnostic_df)} mismatched tools for analysis")
                log_and_print(f"  Review this CSV to identify which fields are causing false positives")
        except Exception as e:
            log_and_print(f"Warning: Could not export diagnostic CSV: {e}", 'warning')
    elif not debug_mode:
        log_and_print("[DEBUG] Debug mode disabled - skipping diagnostic CSV export (Prefect compatibility)")
    # END DIAGNOSTIC CSV EXPORT BLOCK
    
    # Filter out SKIP actions - no need to process tools that don't need updates
    tools_to_process = master_df[master_df['action_in_ion'] != 'SKIP'].copy()
    log_and_print(f"\nFiltering tools: Skipping {len(master_df) - len(tools_to_process)} tools with SKIP actions")
    log_and_print(f"Will execute updates on {len(tools_to_process)} tools")
    
    # Extract orphaned Ion tools for separate processing
    # Orphaned tools are Ion tools that don't have matching TipQA serial numbers
    log_and_print('Extracting orphaned Ion tools from master dataframe...')
    orphaned_mask = (
        (master_df['tipqa_serial_number'].isna() | (master_df['tipqa_serial_number'] == '')) &
        (master_df['ion_serialNumber'].notna() & (master_df['ion_serialNumber'] != ''))
    )
    orphaned_df = master_df[orphaned_mask].copy()
    log_and_print(f"Found {len(orphaned_df)} orphaned Ion tools in master dataframe")
    
    # Convert orphaned dataframe to the format expected by process_orphaned_ion_tools()
    # Format: {'by_serial': {serial_number: [tool_dict, ...]}}
    ion_data = {'by_serial': {}}
    for _, row in orphaned_df.iterrows():
        serial_number = str(row.get('ion_serialNumber', '')).strip()
        if serial_number:
            # Convert dataframe row to nested dictionary format expected by process_orphaned_ion_tools
            location_id = str(row.get('ion_location_id', '')) if pd.notna(row.get('ion_location_id')) else ''
            location_name = str(row.get('ion_location_name', '')) if pd.notna(row.get('ion_location_name')) else ''
            location_dict = {}
            if location_id or location_name:
                location_dict = {
                    'id': location_id,
                    'name': location_name
                }
            
            tool_dict = {
                'id': str(row.get('ion_id', '')),
                'serialNumber': serial_number,
                '_etag': str(row.get('ion__etag', '')),
                'status': str(row.get('ion_status', '')),
                'lastMaintainedDate': str(row.get('ion_lastMaintainedDate', '')) if pd.notna(row.get('ion_lastMaintainedDate')) else None,
                'location': location_dict if location_dict else None,
                'part': {
                    'id': str(row.get('ion_part_id', '')),
                    'partNumber': str(row.get('ion_part_partNumber', '')),
                    'revision': str(row.get('ion_part_revision', '')),
                    'partType': str(row.get('ion_part_partType', '')),
                    'trackingType': str(row.get('ion_part_trackingType', '')),
                    'description': str(row.get('ion_part_description', '')),
                    'maintenanceIntervalSeconds': row.get('ion_part_maintenanceIntervalSeconds', None),
                    'attributes': [
                        {
                            'key': 'Asset Type',
                            'value': str(row.get('ion_part_attributes_Asset Type', ''))
                        }
                    ] if pd.notna(row.get('ion_part_attributes_Asset Type')) and str(row.get('ion_part_attributes_Asset Type', '')).strip() else []
                },
                'attributes': []
            }
            # Add inventory-level attributes
            if pd.notna(row.get('ion_attributes_Manufacturer')) and str(row.get('ion_attributes_Manufacturer', '')).strip():
                tool_dict['attributes'].append({
                    'key': 'Manufacturer',
                    'value': str(row.get('ion_attributes_Manufacturer', ''))
                })
            if pd.notna(row.get('ion_attributes_Asset Serial Number')) and str(row.get('ion_attributes_Asset Serial Number', '')).strip():
                tool_dict['attributes'].append({
                    'key': 'Asset Serial Number',
                    'value': str(row.get('ion_attributes_Asset Serial Number', ''))
                })
            
            if serial_number not in ion_data['by_serial']:
                ion_data['by_serial'][serial_number] = []
            ion_data['by_serial'][serial_number].append(tool_dict)
    
    log_and_print(f"Converted {len(orphaned_df)} orphaned tools to expected format")
    
    # Process only tools that need updates (excluding SKIP actions)
    total_tools = len(tools_to_process)
    log_and_print(f"\nProcessing {total_tools} tools that need updates (in parallel)...")
    
    # OPTIMIZATION: Batch update parts before processing tools to avoid redundant part updates
    # Group tools by part_number+revision and update each unique part once
    #
    # IMPORTANT: This batch update handles PART-LEVEL fields: description and service_interval.
    # - Description: Aggregated using most common value, only updated if Ion is empty
    #   (prevents ping-pong updates when TipQA serials have different descriptions for the same part)
    # - Service Interval: Aggregated using most common positive value, updated once per part
    #   when TipQA value differs from Ion (eliminates concurrency errors on shared parts)
    # - Revision: NOT handled in batch - handled individually in update_tool() when revision changes
    #   (revisions can legitimately change over time, so they're always compared and updated when they differ)
    #
    # Inventory-level fields (location, last_maintenance_date, asset_serial_number, manufacturer) are
    # NEVER aggregated and are always updated individually per serial in the tool processing phase.
    log_and_print('Batching part updates to avoid redundant updates for shared parts...', 'info')
    from utilities.tool_processing_utils import clean_part_number, clean_revision, safe_convert_service_interval, normalize_service_interval_for_comparison
    import re
    
    # Group tools by part_number+revision that need part-level updates
    # Collect all descriptions and service intervals for each part to determine
    # the most common values.
    # NOTE: Only part-level fields are aggregated here. Inventory-level fields are updated per-serial.
    #
    # OPTIMIZATION: The analysis phase already stored the update complexity in the
    # 'reason' column.  Reasons starting with 'update_inventory_and_part' indicate
    # a part-level update is needed.  Use vectorized filtering instead of calling
    # determine_update_mutation_complexity again per row.
    parts_to_update = {}
    
    _part_update_mask = (
        tools_to_process['action_in_ion'].isin(['UPDATE', 'UPDATE_THEN_MARK_UNAVAILABLE']) &
        tools_to_process['reason'].str.startswith('update_inventory_and_part', na=False)
    )
    _part_update_rows = tools_to_process[_part_update_mask]
    
    for idx, row in _part_update_rows.iterrows():
        part_number = clean_part_number(row.get('tipqa_part_number', ''))
        revision = clean_revision(row.get('tipqa_revision', ''))
        part_id = row.get('ion_part_id', '')
        
        if part_number and part_id:
            key = (part_number.lower(), revision.lower() if revision else 'a')
            if key not in parts_to_update:
                parts_to_update[key] = {
                    'part_id': part_id,
                    'part_number': part_number,
                    'revision': revision,
                    'descriptions': [],
                    'service_intervals': [],
                    'tools': []
                }
            tipqa_desc = str(row.get('tipqa_description', '')).strip()
            if tipqa_desc:
                parts_to_update[key]['descriptions'].append(tipqa_desc)
            si_val = safe_convert_service_interval(row.get('tipqa_service_interval_seconds', ''))
            if si_val is not None:
                parts_to_update[key]['service_intervals'].append(si_val)
            parts_to_update[key]['tools'].append(row.get('tipqa_serial_number', ''))
    
    # Determine most common description and service interval for each part
    for key, part_info in parts_to_update.items():
        if part_info['descriptions']:
            # Count descriptions (case-insensitive, normalized)
            desc_counter = Counter()
            for desc in part_info['descriptions']:
                # Normalize: lowercase, strip, normalize whitespace
                normalized = re.sub(r'\s+', ' ', str(desc).strip().lower())
                if normalized:
                    desc_counter[normalized] += 1
            # Use most common description (or first if tie)
            if desc_counter:
                most_common_desc_normalized = desc_counter.most_common(1)[0][0]
                # Find original case version (prefer non-empty, non-None)
                for desc in part_info['descriptions']:
                    normalized = re.sub(r'\s+', ' ', str(desc).strip().lower())
                    if normalized == most_common_desc_normalized:
                        part_info['description'] = str(desc).strip()
                        break
                else:
                    # Fallback: use the normalized version
                    part_info['description'] = most_common_desc_normalized
            else:
                part_info['description'] = ''
        else:
            part_info['description'] = ''
        
        # Determine most common service interval (only positive values)
        if part_info['service_intervals']:
            si_counter = Counter(part_info['service_intervals'])
            part_info['service_interval'] = si_counter.most_common(1)[0][0]
        else:
            part_info['service_interval'] = None
    
    # Initialize cache for tracking updated parts
    updated_parts_cache = set()  # Track which parts have been updated
    
    # Update each unique part once (description and/or service interval)
    if parts_to_update:
        log_and_print(f'Found {len(parts_to_update)} unique parts that need updates. Updating parts in batch...', 'info')
        
        from utilities.graphql_utils import get_part_etag, post_graphql, read_query
        
        for (part_num_key, rev_key), part_info in parts_to_update.items():
            part_id = part_info['part_id']
            part_number = part_info['part_number']
            revision = part_info['revision']
            description = part_info['description']
            batch_service_interval = part_info['service_interval']
            tool_count = len(part_info['tools'])
            
            if not part_id:
                continue
            
            get_part_query = read_query('get_part_etag.graphql')
            part_result = post_graphql(token, config, {'query': get_part_query, 'variables': {'id': part_id}}, environment)
            
            if 'errors' in part_result:
                log_and_print(f'Warning: Could not fetch part {part_number} for batch update: {part_result.get("errors")}', 'warning')
                continue
            
            current_part_data = part_result.get('data', {}).get('part', {})
            
            # Check if description needs update
            current_desc = current_part_data.get('description', '')
            current_desc_normalized = re.sub(r'\s+', ' ', str(current_desc).strip().lower()) if current_desc else ''
            tipqa_desc_normalized = re.sub(r'\s+', ' ', description.strip().lower()) if description else ''
            needs_desc_update = bool(tipqa_desc_normalized and current_desc_normalized != tipqa_desc_normalized)
            
            # Check if service interval needs update
            current_si = current_part_data.get('maintenanceIntervalSeconds')
            current_si_normalized = normalize_service_interval_for_comparison(current_si)
            tipqa_si_normalized = batch_service_interval if batch_service_interval else 0
            needs_si_update = (tipqa_si_normalized > 0 and tipqa_si_normalized != current_si_normalized)
            
            if not needs_desc_update and not needs_si_update:
                continue
            
            try:
                part_etag = get_part_etag(token, config, part_id, environment)
                if not part_etag:
                    log_and_print(f'Warning: Could not get etag for part {part_number} batch update', 'warning')
                    continue
                
                part_mutation = read_query('update_tool.graphql')
                part_variables_input = {
                    'id': part_id,
                    'etag': part_etag,
                }
                
                if needs_desc_update:
                    part_variables_input['description'] = description
                
                if needs_si_update:
                    part_variables_input['maintenanceIntervalSeconds'] = batch_service_interval
                
                update_result = post_graphql(token, config, {'query': part_mutation, 'variables': {'input': part_variables_input}}, environment)
                
                if 'errors' not in update_result:
                    updated_parts_cache.add((part_num_key, rev_key))
                    update_fields = []
                    if needs_desc_update:
                        update_fields.append('description')
                    if needs_si_update:
                        update_fields.append(f'maintenanceIntervalSeconds={batch_service_interval}')
                    log_and_print(f'Batch updated part {part_number} (revision {revision}) [{", ".join(update_fields)}] for {tool_count} tools', 'info')
                else:
                    log_and_print(f'Warning: Failed to batch update part {part_number}: {update_result.get("errors")}', 'warning')
            except Exception as e:
                log_and_print(f'Warning: Exception during batch part update for {part_number}: {e}', 'warning')
        
        log_and_print(f'Batch updated {len(updated_parts_cache)} parts. Processing tools...', 'info')
    else:
        log_and_print('No parts need batch updates. Processing tools...', 'info')
    
    # Thread-safe stats tracking and token management
    stats_lock = threading.Lock()
    token_lock = threading.Lock()
    parts_cache_lock = threading.Lock()  # Lock for updated_parts_cache
    current_batch_token = token  # Shared token for current batch
    
    def prepare_tool_data(row):
        """Prepare tool data from dataframe row."""
        tool_data = row.to_dict()
        
        # Map TipQA column names to expected processing function names
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
    
    def process_tool_wrapper(tool_data_tuple):
        """Wrapper function for parallel processing with thread-safe stats."""
        nonlocal current_batch_token, updated_parts_cache  # Allow modification of outer scope variables
        idx, tool_data = tool_data_tuple
        
        # Get current token (thread-safe)
        with token_lock:
            current_token = current_batch_token
        
        try:
            serial_number = tool_data.get('serial_number') or tool_data.get('tipqa_serial_number', 'UNKNOWN')
            # OPTIMIZED: Pass None for ion_data since analyze_tool_using_daily_sync_logic() uses tool_data
            # which already contains all Ion columns from the master dataframe
            # Also pass the updated_parts_cache to avoid redundant part updates
            success, updated_token, error_message = process_tool_live(tool_data, current_token, config, lost_location_id, environment, stats, None, retry_with_new_token=True, updated_parts_cache=updated_parts_cache, parts_cache_lock=parts_cache_lock, merged_df=master_df)
            
            # Update token if it was refreshed (thread-safe)
            if updated_token != current_token:
                with token_lock:
                    current_batch_token = updated_token
            
            # Update progress tracking and error details (thread-safe)
            with stats_lock:
                processed_count = stats.get('_processed_count', 0) + 1
                stats['_processed_count'] = processed_count
                
                # If failed, capture error details from the error message
                if not success:
                    # Try to extract action/reason from tool_data if available
                    action = tool_data.get('action_in_ion', 'UNKNOWN')
                    reason = tool_data.get('reason', 'UNKNOWN')
                    # Use error_message if available, otherwise create a descriptive fallback
                    if error_message:
                        error_msg = error_message
                    else:
                        # Create a more descriptive fallback based on action
                        if action == 'CREATE':
                            error_msg = f"Failed to create tool {serial_number} - no detailed error message available"
                        elif action == 'UPDATE':
                            error_msg = f"Failed to update tool {serial_number} - no detailed error message available"
                        else:
                            error_msg = f"Failed to process tool {serial_number} ({action}) - no detailed error message available"
                    
                    stats['error_details'].append({
                        'serial_number': serial_number,
                        'action': action,
                        'reason': reason,
                        'error': error_msg
                    })
                    
                    # Also log immediately to terminal for visibility
                    log_and_print(f"ERROR CAPTURED: {serial_number} ({action}): {error_msg}", 'error')
                
                # Show progress every 100 tools
                if processed_count % 100 == 0:
                    log_and_print(f"Progress: {processed_count}/{total_tools} tools processed. Created: {stats.get('created', 0)}, Updated: {stats.get('updated', 0)}, Skipped: {stats.get('skipped', 0)}, Errors: {stats.get('errors', 0)}")
            
            return success
        except Exception as e:
            serial_number = tool_data.get('serial_number') or tool_data.get('tipqa_serial_number', 'UNKNOWN')
            with stats_lock:
                stats['errors'] = stats.get('errors', 0) + 1
                stats['error_details'].append({
                    'serial_number': serial_number,
                    'action': 'UNKNOWN',
                    'reason': 'exception',
                    'error': str(e)
                })
                log_and_print(f"ERROR: Exception processing tool {serial_number}: {e}", 'error')
            return False
    
    # Prepare all tool data first (only for tools that need updates)
    tool_data_list = [(idx, prepare_tool_data(row)) for idx, row in tools_to_process.iterrows()]
    
    # Process tools in parallel batches to avoid overwhelming the API
    # Use 10-15 workers for good balance between speed and API rate limits
    max_workers = 15
    batch_size = 200  # Process 200 tools per batch, then refresh token
    
    log_and_print(f"Using {max_workers} parallel workers, processing in batches of {batch_size}")
    
    processed_count = 0
    for batch_start in range(0, len(tool_data_list), batch_size):
        batch_end = min(batch_start + batch_size, len(tool_data_list))
        batch = tool_data_list[batch_start:batch_end]
        
        # Refresh token before each batch (thread-safe)
        if batch_start > 0:
            log_and_print(f"Progress: {processed_count}/{total_tools} tools processed. Refreshing authentication token...", 'info')
            try:
                new_token = get_token(config, environment)
                with token_lock:
                    current_batch_token = new_token
                    token = new_token  # Update main token reference
                log_and_print("Token refreshed successfully", 'info')
            except Exception as e:
                log_and_print(f"Failed to refresh token: {e}", 'error')
        
        # Process batch in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_tool_wrapper, tool_data): tool_data for tool_data in batch}
            
            for future in as_completed(futures):
                try:
                    success = future.result()
                    processed_count += 1
                except Exception as e:
                    with stats_lock:
                        stats['errors'] = stats.get('errors', 0) + 1
                    log_and_print(f"Error in parallel processing: {e}", 'error')
        
        # Update main token from batch token
        with token_lock:
            token = current_batch_token
        
        # Show batch completion
        log_and_print(f"Completed batch: {processed_count}/{total_tools} tools processed", 'info')
    
    # Process orphaned Ion tools (tools in Ion but not in TipQA)
    log_and_print('Processing orphaned Ion tools...')
    process_orphaned_ion_tools(tipqa_tools, ion_data, stats, token, config, lost_location_id, environment, dry_run=False)
    
    return stats

def main():
    """Main function for daily tool synchronization."""
    flow_start_time = time.time()
    
    try:
        # Load environment variables first
        load_dotenv()
        # Load configuration
        config = load_config()
        log_and_print('Configuration loaded successfully')
        
        # Validate configuration
        environment = os.getenv('ENVIRONMENT', 'v2_production')
        config_errors = validate_environment_config(config, environment)
        if config_errors:
            raise Exception(f"Configuration validation failed: {', '.join(config_errors)}")
        
        # Connect to TipQA (via Databricks)
        conn = get_tipqa_connection(config)
        
        log_and_print(f"Starting LIVE synchronization with environment={environment}", 'warning')
        log_and_print('This script ALWAYS makes actual changes to Ion!', 'warning')
        log_and_print('For analysis/dry runs, use: python tests/dry_run_test.py --environment <env>', 'info')
        
        # Get token
        token = get_token(config, environment)
        
        # Get lost location ID
        lost_location_id = get_lost_location_id(token, config, environment)
        if not lost_location_id:
            raise Exception('Could not find lost location in Ion. Please ensure a location with "LOST" in the name exists.')
        
        # Run synchronization (always live mode)
        stats = sync_tools_live(conn, token, config, lost_location_id, environment)
        
        # Print summary
        log_and_print("\n=== SYNCHRONIZATION SUMMARY ===")
        log_and_print(f"Total tools in TipQA: {stats.get('total_tools', 0)}")
        log_and_print(f"Tools created: {stats.get('created', 0)}")
        log_and_print(f"Tools updated: {stats.get('updated', 0)}")
        log_and_print(f"Parts converted to tools: {stats.get('converted', 0)}")
        log_and_print(f"Tools marked unavailable: {stats.get('marked_unavailable', 0)}")
        log_and_print(f"Tools marked available: {stats.get('marked_available', 0)}")
        log_and_print(f"Tools updated then marked unavailable: {stats.get('update_then_mark_unavailable', 0)}")
        log_and_print(f"Tools skipped: {stats.get('skipped', 0)}")
        log_and_print(f"Errors: {stats.get('errors', 0)}")
        
        # Show update reasons summary again at the end for visibility
        total_updates = stats.get('updated', 0) + stats.get('update_then_mark_unavailable', 0)
        if total_updates > 1000:  # Only show if suspiciously high
            log_and_print(f"\n{'='*60}")
            log_and_print(f"WARNING: {total_updates} tools were updated - this seems high!")
            log_and_print(f"Please check the pre-analysis 'Update Reasons Summary' above")
            log_and_print(f"to see which fields are causing the mismatches.")
            log_and_print(f"{'='*60}\n")
        
        # Print detailed error summary
        if stats.get('errors', 0) > 0 and stats.get('error_details'):
            log_and_print("\n=== ERROR DETAILS ===")
            log_and_print(f"Total errors: {stats.get('errors', 0)}")
            error_details = stats['error_details']
            
            # First, print individual errors for easy scanning
            log_and_print("\n--- Individual Error Details ---")
            for idx, err in enumerate(error_details, 1):
                serial = err.get('serial_number', 'UNKNOWN')
                action = err.get('action', 'UNKNOWN')
                reason = err.get('reason', 'UNKNOWN')
                error_msg = err.get('error', 'No error message provided')
                log_and_print(f"\n{idx}. Tool: {serial}")
                log_and_print(f"   Action: {action}")
                log_and_print(f"   Reason: {reason}")
                log_and_print(f"   Error: {error_msg}")
            
            # Then group errors by type for summary
            log_and_print("\n--- Error Summary by Type ---")
            error_types = {}
            for err in error_details:
                error_msg = err.get('error', 'Unknown error')
                # Use first 200 chars for grouping, but show full message
                error_type_key = error_msg[:200] if len(error_msg) > 200 else error_msg
                if error_type_key not in error_types:
                    error_types[error_type_key] = {
                        'full_message': error_msg,
                        'serials': [],
                        'actions': set(),
                        'reasons': set()
                    }
                error_types[error_type_key]['serials'].append(err['serial_number'])
                if err.get('action'):
                    error_types[error_type_key]['actions'].add(err['action'])
                if err.get('reason'):
                    error_types[error_type_key]['reasons'].add(err['reason'])
            
            for error_type_key, error_info in error_types.items():
                full_message = error_info['full_message']
                serials = error_info['serials']
                actions = list(error_info['actions'])
                reasons = list(error_info['reasons'])
                
                log_and_print(f"\nError Type: {full_message}")
                log_and_print(f"  Affected tools ({len(serials)}): {', '.join(serials[:10])}")
                if len(serials) > 10:
                    log_and_print(f"  ... and {len(serials) - 10} more")
                if actions:
                    log_and_print(f"  Actions: {', '.join(actions)}")
                if reasons:
                    log_and_print(f"  Reasons: {', '.join(reasons[:3])}")  # Show first 3 reasons
        
        flow_duration = time.time() - flow_start_time
        log_and_print(f"\nTotal duration: {flow_duration:.2f} seconds")
        
        conn.close()

        return stats
        
    except Exception as e:
        log_and_print(f"Error in main: {e}", 'error')
        sys.exit(1)

if __name__ == '__main__':
    main()
