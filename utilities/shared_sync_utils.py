#!/usr/bin/env python3
'''
Shared Synchronization Utilities
===============================

Common functions used across all synchronization scripts (dry run, live test, daily sync).
This ensures consistent logic and reduces code duplication.

Created: 2025-01-14
Author: Jae Osenbach
Purpose: Shared utilities for tool synchronization
Version: 1.0
'''

import os
import yaml
import glob
import re
from typing import Dict, Any, List
from dotenv import load_dotenv

def log_and_print(message: str, level: str = 'info'):
    """Simple logging function for all scripts."""
    print(f"[{level.upper()}] {message}")

def select_standard_columns(df):
    """
    Select exactly the 36 standard columns for dry run CSV output in the specified order.
    
    This ensures the CSV always has the same structure and no extra columns.
    Missing columns will be added as empty strings.
    
    Column order:
    1-2: action_in_ion, reason
    3-14: TipQA fields (12 fields)
    15-36: Ion fields (22 fields)
    """
    import pandas as pd
    
    # Define the exact 36 columns in order
    standard_columns = [
        # Action/Reason (2)
        'action_in_ion',
        'reason',
        
        # TipQA fields (12)
        'tipqa_serial_number',
        'tipqa_part_number',
        'tipqa_description',
        'tipqa_revision',
        'tipqa_service_interval_seconds',
        'tipqa_asset_type',
        'tipqa_location',
        'tipqa_last_maintenance_date',
        'tipqa_asset_serial_number',
        'tipqa_manufacturer',
        'tipqa_maintenance_status',
        'tipqa_revision_status',
        
        # Ion fields (22)
        'ion_id',
        'ion_serialNumber',
        'ion__etag',
        'ion_status',
        'ion_lastMaintainedDate',
        'ion_location',
        'ion_location_id',
        'ion_location_name',
        'ion_attributes_Asset Serial Number',
        'ion_attributes_Manufacturer',
        'ion_attributes_Etag',
        'ion_part_id',
        'ion_part_partNumber',
        'ion_part_revision',
        'ion_part_partType',
        'ion_part_trackingType',
        'ion_part_description',
        'ion_part_maintenanceIntervalSeconds',
        'ion_part_attributes_Asset Type',
        'ion_part_attributes_Etag',
        'ion_abomInstallations_id',
        'ion_buildrequirement_id',
    ]
    
    # Create a new dataframe with only the standard columns
    result_df = pd.DataFrame(index=df.index)
    
    # Add each column, creating empty string columns if missing
    for col in standard_columns:
        if col in df.columns:
            result_df[col] = df[col]
        else:
            result_df[col] = ''
    
    return result_df[standard_columns]  # Ensure exact order

def load_config() -> Dict[str, Any]:
    """Load configuration from YAML file with environment variable substitution."""
    import os
    import re
    
    def substitute_env_vars(text: str) -> str:
        """Substitute environment variables in YAML content."""
        def replace_var(match):
            var_name = match.group(1)
            default_value = match.group(2) if match.group(2) else ''
            return os.getenv(var_name, default_value)
        
        # Pattern: ${VAR_NAME} or ${VAR_NAME:default_value}
        pattern = r'\$\{([^}:]+)(?::([^}]*))?\}'
        return re.sub(pattern, replace_var, text)
    
    try:
        with open('config.yaml', 'r') as f:
            content = f.read()
            substituted_content = substitute_env_vars(content)
            return yaml.safe_load(substituted_content)
    except FileNotFoundError:
        raise Exception("Configuration file 'config.yaml' not found")
    except yaml.YAMLError as e:
        raise Exception(f"Error parsing configuration file: {e}")

def cleanup_previous_test_files():
    """Clean up previous test output files to prevent clutter."""
    test_files_patterns = [
        'tests/comprehensive_analysis_*.csv',
        'tests/comprehensive_analysis_*.md',
        'tests/production_test_report_*.md',
        'tests/production_test_errors_*.md',
        'tests/production_test_results_*.csv',
        'tests/retest_results_*.csv',
        'tests/retest_report_*.md'
    ]
    
    cleaned_count = 0
    for pattern in test_files_patterns:
        files = glob.glob(pattern)
        if files:
            log_and_print(f"Found {len(files)} files matching pattern: {pattern}")
        for file in files:
            try:
                os.remove(file)
                cleaned_count += 1
                log_and_print(f"Cleaned up previous file: {file}")
            except OSError as e:
                log_and_print(f"Could not remove {file}: {e}", 'warning')
    
    if cleaned_count > 0:
        log_and_print(f"Cleaned up {cleaned_count} previous test files")
    else:
        log_and_print("No previous test files found to clean up")

def get_ion_matches_for_tipqa_tools(token: str, config: Dict[str, Any], tipqa_tools: List[Dict], environment: str) -> Dict[str, Any]:
    """
    EFFICIENT APPROACH: Query Ion for SPECIFIC TipQA serial/part combinations.
    
    This function uses targeted queries for both TOOL and PART partTypes,
    matching TipQA serial numbers and part numbers efficiently.
    """
    from utilities.graphql_utils import post_graphql, read_query, organize_ion_data_by_serial
    
    log_and_print(f"Fetching Ion matches for {len(tipqa_tools)} TipQA tools...")
    
    # Get unique serial/part combinations from TipQA tools
    log_and_print("Extracting unique serial/part combinations from TipQA data...")
    combinations = set()
    for tool in tipqa_tools:
        serial_number = tool.get('serial_number', '') or ''
        part_number = tool.get('part_number', '') or ''
        serial_number = serial_number.strip() if serial_number else ''
        part_number = part_number.strip() if part_number else ''
        if serial_number and part_number:
            combinations.add((serial_number, part_number))
    
    log_and_print(f" Found {len(combinations)} unique serial/part combinations")
    log_and_print(f"Starting Ion queries for {len(combinations)} combinations...")
    
    if not combinations:
        log_and_print("No serial/part combinations to query - returning empty result")
        return {"by_serial": {}, "all_tools": []}
    
    # Use targeted query for each combination - no partType filter needed since we're filtering on serial/part
    query = read_query('get_tool_inventory_by_serial_and_part.graphql')
    all_matches = []
    
    for i, (serial_number, part_number) in enumerate(combinations):
        if i % 100 == 0:
            log_and_print(f"Querying Ion for combination {i+1}/{len(combinations)}: {serial_number}/{part_number}")
        
        variables = {
            "serialNumber": serial_number,
            "partNumber": part_number,
            "first": 100,
            "after": None
        }
        
        try:
            response = post_graphql(token, config, {'query': query, 'variables': variables}, environment)
            
            if 'errors' in response:
                log_and_print(f"GraphQL errors for {serial_number}/{part_number}: {response['errors']}", 'error')
                continue
            
            edges = response.get('data', {}).get('partInventories', {}).get('edges', [])
            for edge in edges:
                tool_node = edge.get('node', {})
                all_matches.append(tool_node)
                
        except Exception as e:
            log_and_print(f"Error fetching Ion match for {serial_number}/{part_number}: {str(e)}", 'error')
            continue
    
    log_and_print(f"Found {len(all_matches)} Ion matches for TipQA tools")
    
    # Organize by serial number
    ion_data = organize_ion_data_by_serial(all_matches)
    
    return ion_data

def get_orphaned_ion_tools(token: str, config: Dict[str, Any], tipqa_serial_numbers: set, environment: str) -> Dict[str, Any]:
    """Fetch Ion tools that don't exist in TipQA (orphans)."""
    from utilities.graphql_utils import post_graphql, read_query, organize_ion_data_by_serial
    
    log_and_print("Fetching orphaned Ion tools (tools in Ion but not in TipQA)...")
    
    # Use the consolidated inventory query (only TOOL partType)
    query = read_query('get_all_tool_inventory.graphql')
    orphaned_tools = []
    after_cursor = None
    page_count = 0
    
    while True:
        page_count += 1
        variables = {
            "first": 1000,  # Fetch in batches
            "after": after_cursor
        }
        
        try:
            response = post_graphql(token, config, {'query': query, 'variables': variables}, environment)
            
            if 'errors' in response:
                log_and_print(f"GraphQL errors: {response['errors']}", 'error')
                break
            
            edges = response.get('data', {}).get('partInventories', {}).get('edges', [])
            if not edges:
                break
                
            # Filter for tools that DON'T exist in TipQA and are not fake serial numbers
            for edge in edges:
                tool_node = edge.get('node', {})
                serial_number = tool_node.get('serialNumber', '')
                # Skip fake serial numbers and tools that exist in TipQA
                if (serial_number and 
                    serial_number not in tipqa_serial_numbers and 
                    not is_fake_serial_number(serial_number)):
                    orphaned_tools.append(tool_node)
            
            page_info = response.get('data', {}).get('partInventories', {}).get('pageInfo', {})
            if not page_info.get('hasNextPage', False):
                break
                
            after_cursor = page_info.get('endCursor')
            
            log_and_print(f"Fetched page {page_count} with {len(edges)} total tools")
            
        except Exception as e:
            log_and_print(f"Error fetching orphaned Ion tools: {str(e)}", 'error')
            break
    
    log_and_print(f"Found {len(orphaned_tools)} orphaned Ion tools")
    
    # Organize by serial number
    ion_data = organize_ion_data_by_serial(orphaned_tools)
    
    return ion_data

def is_fake_serial_number(serial_number: str) -> bool:
    """Check if a serial number appears to be fake/test data."""
    if not serial_number:
        return False
    
    # Only filter out clearly fake patterns - be conservative
    fake_patterns = [
        '000000',    # All zeros: 00000021, 00000022, etc.
        'TEST',      # Test serial numbers
        'FAKE',      # Fake serial numbers
        'DUMMY',     # Dummy serial numbers
    ]
    
    serial_upper = serial_number.upper()
    for pattern in fake_patterns:
        if serial_upper.startswith(pattern):
            return True
    
    # Check for all numeric patterns that look fake (all zeros, sequential numbers, etc.)
    if serial_number.isdigit():
        # All zeros or mostly zeros
        if serial_number.count('0') > len(serial_number) * 0.8:
            return True
        # Sequential numbers (like 00000021, 00000022, etc.)
        if len(serial_number) >= 8 and serial_number.startswith('000000'):
            return True
    
    return False

def get_ion_attribute_value(attributes: List[Dict], key: str) -> str:
    """Extract attribute value from Ion attributes list by key."""
    for attr in attributes:
        if attr.get('key') == key:
            return str(attr.get('value', ''))
    return ''

def get_ion_attribute_value_from_both_levels(inventory_data: Dict, key: str) -> str:
    """
    Extract attribute value from both inventory-level and part-level attributes.
    Checks inventory-level attributes first, then part-level attributes.
    """
    # First check inventory-level attributes
    inventory_attrs = inventory_data.get('attributes', [])
    value = get_ion_attribute_value(inventory_attrs, key)
    if value:
        return value
    
    # Then check part-level attributes
    part_attrs = inventory_data.get('part', {}).get('attributes', [])
    return get_ion_attribute_value(part_attrs, key)

def create_master_dataframe(token: str, config: Dict[str, Any], tipqa_tools_df, environment: str, dry_run_mode: bool = False):
    """
    SIMPLIFIED WORKFLOW: Matches manual process for efficiency and correctness.
    
    Manual workflow:
    1. Pull TipQA data (CSV)
    2a. Pull all Ion tools (partType = TOOL) - Query 1
    2b. Pull Ion matches for TipQA serial+part combos (all partTypes) - Query 2
    3. Combine both Ion queries and remove exact duplicates
    4. Merge TipQA and Ion on serial+part (one row per combo, plus orphans)
    5. Compare TipQA vs Ion columns - only act on differences
    
    This uses two optimized queries:
    - Query 1: All partType = TOOL (finds orphaned Ion tools)
    - Query 2: TipQA serial+part combos regardless of partType (catches PART types that need conversion)
    - Exact duplicates are dropped after combining queries
    
    Args:
        dry_run_mode: If True, skip fetching all Ion tools (faster for testing)
    """
    import pandas as pd
    import time
    
    print("Starting master dataframe creation flow (simplified workflow)...", flush=True)
    start_time = time.time()
    
    # Step 1: Get all TipQA tools - already provided as tipqa_tools_df
    print(f"Step 1: TipQA tools dataframe ready ({len(tipqa_tools_df)} tools)", flush=True)
    
    # Step 2a: Get ALL Ion tools (partType = TOOL only) - Query 1
    print("Step 2a: Fetching ALL Ion tools (partType = TOOL only)...", flush=True)
    print("  Query 1: All partType = TOOL (to find orphaned Ion tools)", flush=True)
    all_ion_tools_dict = get_all_ion_tools_optimized(token, config, environment, tipqa_serial_numbers=None)
    
    # Step 2a-filter: In-memory filter to find TipQA combos NOT already matched as TOOL in Step 2a
    # This avoids redundant individual queries for combos already covered by the bulk TOOL fetch
    print("Step 2a-filter: Finding TipQA combos not yet matched in Step 2a results...", flush=True)
    ion_tool_combos = set()
    for serial_number, ion_data_list in all_ion_tools_dict.get('by_serial', {}).items():
        for tool_data in ion_data_list:
            part_number = tool_data.get('part', {}).get('partNumber', '')
            if serial_number and part_number:
                ion_tool_combos.add((serial_number.strip(), part_number.strip()))

    tipqa_combos = set()
    for _, row in tipqa_tools_df.iterrows():
        serial = str(row.get('serial_number', '') or '').strip()
        part = str(row.get('part_number', '') or '').strip()
        if serial and part:
            tipqa_combos.add((serial, part))

    unmatched_combos = tipqa_combos - ion_tool_combos
    matched_count = len(tipqa_combos) - len(unmatched_combos)
    print(f"  {matched_count} of {len(tipqa_combos)} TipQA combos already matched as TOOL in Step 2a", flush=True)
    print(f"  Querying {len(unmatched_combos)} remaining combos for non-TOOL matches (PART types needing conversion)", flush=True)

    # Step 2b: Query Ion for ONLY unmatched TipQA serial+part combos (regardless of partType) - Query 2
    # This finds PART types that need to be converted to TOOL
    print("Step 2b: Fetching Ion matches for unmatched TipQA serial+part combinations (all partTypes)...", flush=True)
    print("  Query 2: Unmatched TipQA serial+part combos regardless of partType (to catch PART types)", flush=True)
    unmatched_tools_list = [
        row.to_dict() for _, row in tipqa_tools_df.iterrows()
        if (str(row.get('serial_number', '') or '').strip(),
            str(row.get('part_number', '') or '').strip()) in unmatched_combos
    ]
    tipqa_matches_dict = get_ion_matches_for_tipqa_tools_optimized(token, config, unmatched_tools_list, environment)
    
    # Step 3: Combine both Ion queries and remove exact duplicates
    print("Step 3: Combining Ion queries and removing exact duplicates...", flush=True)
    combined_ion_dict = combine_ion_dataframes(tipqa_matches_dict, all_ion_tools_dict)
    
    # Convert combined Ion data to dataframe
    print("Step 4: Converting combined Ion data to dataframe...", flush=True)
    ion_df = ion_data_to_dataframe(combined_ion_dict, "combined")
    
    # Step 5: Remove exact duplicates from Ion data (by ion_id)
    print("Step 5: Removing exact duplicates from Ion data (by ion_id)...", flush=True)
    original_ion_count = len(ion_df)
    ion_df = ion_df.drop_duplicates(subset=['ion_id'], keep='first')
    removed_count = original_ion_count - len(ion_df)
    if removed_count > 0:
        print(f"  Removed {removed_count} duplicate Ion records (exact duplicates from combining queries)", flush=True)
    else:
        print(f"  No exact duplicates found (all {original_ion_count} records are unique)", flush=True)
    
    # Step 6: Merge TipQA and Ion on serial+part (composite key)
    print("Step 6: Merging TipQA and Ion dataframes on serial+part composite key...", flush=True)
    master_df = append_ion_to_tipqa(tipqa_tools_df, ion_df)
    
    # Step 7: Final deduplication
    print("Step 7: Final deduplication of master dataframe...", flush=True)
    master_df = remove_duplicates_from_master_dataframe(master_df)
    
    elapsed = time.time() - start_time
    print(f"Master dataframe created with {len(master_df)} records in {elapsed:.1f}s", flush=True)
    return master_df

def combine_ion_dataframes(ion_matches: Dict[str, Any], all_ion_tools: Dict[str, Any]) -> Dict[str, Any]:
    """
    Combine Ion matches (TOOL + PART partTypes) with all Ion tools (TOOL only).
    This ensures we have both the targeted matches and all Ion tools for orphans.
    
    Duplicates are handled by checking the 'id' field - if a tool with the same id
    already exists in ion_matches, it won't be added again from all_ion_tools.
    """
    print("Combining Ion matches with all Ion tools...", flush=True)
    
    # Start with Ion matches (includes both TOOL and PART partTypes from Query 2)
    combined_by_serial = ion_matches.get('by_serial', {}).copy()
    combined_all_tools = ion_matches.get('all_tools', []).copy()
    
    initial_count = len(combined_all_tools)
    
    # Add all Ion tools (TOOL partType only from Query 1) to the combined data
    for serial_number, ion_data_list in all_ion_tools.get('by_serial', {}).items():
        if serial_number not in combined_by_serial:
            # This is an orphaned Ion tool (not in TipQA)
            combined_by_serial[serial_number] = ion_data_list
            combined_all_tools.extend(ion_data_list)
        else:
            # This serial number exists in both - merge the data, checking for exact duplicates by id
            existing_data = combined_by_serial[serial_number]
            for ion_data in ion_data_list:
                # Check if this Ion tool already exists in matches (exact duplicate check)
                tool_exists = any(
                    existing.get('id') == ion_data.get('id') 
                    for existing in existing_data
                )
                if not tool_exists:
                    existing_data.append(ion_data)
                    combined_all_tools.append(ion_data)
    
    added_count = len(combined_all_tools) - initial_count
    print(f"  Combined Ion data: {len(combined_by_serial)} unique serials, {len(combined_all_tools)} total tools", flush=True)
    print(f"  Added {added_count} tools from Query 1 (all partType=TOOL) to Query 2 results", flush=True)
    
    return {
        "by_serial": combined_by_serial,
        "all_tools": combined_all_tools
    }

def filter_ion_tools_for_tipqa_matches(all_ion_tools: Dict[str, Any], tipqa_tools: List[Dict]) -> Dict[str, Any]:
    """
    Filter all Ion tools to find matches with TipQA serial/part combinations.
    This is much more efficient than making individual GraphQL queries.
    """
    log_and_print("Filtering Ion tools for TipQA matches...")
    
    # Create a set of TipQA serial/part combinations for fast lookup
    tipqa_combinations = set()
    for tool in tipqa_tools:
        serial_number = str(tool.get('serial_number', '') or '').strip()
        part_number = str(tool.get('part_number', '') or '').strip()
        if serial_number and part_number:
            tipqa_combinations.add((serial_number, part_number))
    
    log_and_print(f"Looking for matches among {len(tipqa_combinations)} TipQA combinations")
    
    # Filter Ion tools to find matches and orphans
    matched_tools = []
    orphaned_tools = []
    matched_serials = set()
    
    for serial_number, ion_data in all_ion_tools.get('by_serial', {}).items():
        serial_matched = False
        for tool_data in ion_data:
            part_number = tool_data.get('part', {}).get('partNumber', '')
            if (serial_number, part_number) in tipqa_combinations:
                matched_tools.append(tool_data)
                serial_matched = True
                matched_serials.add(serial_number)
        
        # If this serial number doesn't match any TipQA combination, it's an orphan
        if not serial_matched:
            orphaned_tools.extend(ion_data)
    
    log_and_print(f"Found {len(matched_tools)} Ion tools matching TipQA combinations")
    log_and_print(f"Found {len(orphaned_tools)} orphaned Ion tools")
    
    # Combine matched and orphaned tools
    all_filtered_tools = matched_tools + orphaned_tools
    
    # Create filtered by_serial structure
    filtered_by_serial = {}
    for tool_data in all_filtered_tools:
        serial_number = tool_data.get('serialNumber', '')
        if serial_number not in filtered_by_serial:
            filtered_by_serial[serial_number] = []
        filtered_by_serial[serial_number].append(tool_data)
    
    return {
        "by_serial": filtered_by_serial,
        "all_tools": all_filtered_tools
    }

def get_all_ion_tools_optimized(token: str, config: Dict[str, Any], environment: str, tipqa_serial_numbers: set = None) -> Dict[str, Any]:
    """
    Get Ion tools with parallel processing - OPTIMIZED to only fetch tools matching TipQA serial numbers.
    
    This function now only fetches:
    1. Tool parts (partType = TOOL) that match TipQA serial numbers
    2. Parts with serial/part combos that need to be switched to tools (handled by get_ion_matches_for_tipqa_tools_optimized)
    3. Excludes protected parts
    
    Args:
        token: Ion API token
        config: Configuration dictionary
        environment: Environment name (v2_production, etc.)
        tipqa_serial_numbers: Set of TipQA serial numbers to filter by (if None, fetches all - not recommended)
    
    Returns:
        Dictionary of Ion tools organized by serial number
    """
    from utilities.graphql_utils import post_graphql, read_query, organize_ion_data_by_serial
    from utilities.tool_processing_utils import is_part_number_protected
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time
    
    # Get protected part numbers to exclude
    protected_part_numbers = config.get('sync_exceptions', {}).get('protected_part_numbers', [])
    protected_part_numbers = [pn.upper().strip() for pn in protected_part_numbers] if protected_part_numbers else []
    
    if tipqa_serial_numbers is None:
        print("INFO: Fetching ALL Ion tools (partType = TOOL) - this is normal for daily sync", flush=True)
        print("  Reason: Need all tools to identify orphaned Ion tools and ensure complete matching", flush=True)
        print("Fetching all Ion tools with parallel processing...", flush=True)
    else:
        print(f"Fetching Ion tools matching {len(tipqa_serial_numbers)} TipQA serial numbers (optimized)...", flush=True)
    
    start_time = time.time()
    
    # Use the consolidated inventory query (only TOOL partType)
    query = read_query('get_all_tool_inventory.graphql')
    all_tools = []
    
    # Note: totalCount is not available in PartInventoriesConnection, so we'll determine count from actual fetching
    if tipqa_serial_numbers is None:
        print("Fetching all Ion tools (count will be determined as we fetch)...", flush=True)
    else:
        print(f"Fetching Ion tools for {len(tipqa_serial_numbers)} TipQA serial numbers...", flush=True)
    
    # Use parallel processing for pagination
    def fetch_page(page_num, after_cursor):
        variables = {
            "first": 5000,  # Increased batch size from 2000 to 5000 for faster processing
            "after": after_cursor
        }
        
        try:
            response = post_graphql(token, config, {'query': query, 'variables': variables}, environment)
            if 'errors' in response:
                print(f"GraphQL errors on page {page_num}: {response['errors']}")
                return None, None
            
            edges = response.get('data', {}).get('partInventories', {}).get('edges', [])
            page_info = response.get('data', {}).get('partInventories', {}).get('pageInfo', {})
            
            # Filter tools based on TipQA serial numbers and protected parts
            filtered_tools = []
            for edge in edges:
                tool_node = edge.get('node', {})
                serial_number = tool_node.get('serialNumber', '')
                part_number = tool_node.get('part', {}).get('partNumber', '') if tool_node.get('part') else ''
                
                # Filter 1: Only include if serial number matches TipQA (if provided)
                if tipqa_serial_numbers is not None:
                    if not serial_number or serial_number not in tipqa_serial_numbers:
                        continue
                
                # Filter 2: Exclude protected parts
                if part_number and protected_part_numbers:
                    if part_number.upper().strip() in protected_part_numbers:
                        continue
                
                # Filter 3: Only include TOOL type parts (query already filters this, but double-check)
                part_type = tool_node.get('part', {}).get('partType', '').upper() if tool_node.get('part') else ''
                if part_type != 'TOOL':
                    continue
                
                filtered_tools.append(tool_node)
            
            next_cursor = page_info.get('endCursor') if page_info.get('hasNextPage', False) else None
            
            return filtered_tools, next_cursor
            
        except Exception as e:
            print(f"Error fetching page {page_num}: {str(e)}")
            return None, None
    
    # Start with first page
    print("Fetching first page of Ion tools...", flush=True)
    first_tools, next_cursor = fetch_page(1, None)
    if first_tools:
        all_tools.extend(first_tools)
        print(f"Fetched first page: {len(first_tools)} tools", flush=True)
    elif first_tools is None:
        print("Warning: First page fetch returned None (possible error), continuing anyway...", flush=True)
    else:
        print("Warning: First page returned empty results (0 tools)", flush=True)
    
    # Use sequential pagination (simpler and more reliable than parallel for paginated queries)
    # OPTIMIZATION: If filtering by TipQA serial numbers, track found serials and stop early
    found_serials = set()
    if tipqa_serial_numbers is not None:
        # Track which serial numbers we've found
        for tool in all_tools:
            serial = tool.get('serialNumber', '')
            if serial in tipqa_serial_numbers:
                found_serials.add(serial)
    
    if next_cursor:
        if tipqa_serial_numbers is None:
            print(f"Fetching remaining pages (this may take a few minutes)...", flush=True)
        else:
            remaining = len(tipqa_serial_numbers) - len(found_serials)
            print(f"Fetching remaining pages (looking for {remaining} more matching serial numbers)...", flush=True)
        page_count = 1
        page_num = 2
        
        while next_cursor:
            tools, cursor = fetch_page(page_num, next_cursor)
            if tools:
                all_tools.extend(tools)
                page_count += 1
                
                # Update found serials if filtering
                if tipqa_serial_numbers is not None:
                    for tool in tools:
                        serial = tool.get('serialNumber', '')
                        if serial in tipqa_serial_numbers:
                            found_serials.add(serial)
                    
                    # Early stopping: If we've found all matching serial numbers, stop paginating
                    remaining = len(tipqa_serial_numbers) - len(found_serials)
                    if remaining == 0:
                        print(f"Early stopping: Found all {len(tipqa_serial_numbers)} matching TipQA serial numbers after {page_count} pages", flush=True)
                        break
                
                if page_count % 5 == 0:  # Show progress every 5 pages
                    if tipqa_serial_numbers is None:
                        print(f"Progress: Fetched {page_count} pages, {len(all_tools)} tools so far...", flush=True)
                    else:
                        remaining = len(tipqa_serial_numbers) - len(found_serials)
                        print(f"Progress: Fetched {page_count} pages, {len(all_tools)} tools, {len(found_serials)}/{len(tipqa_serial_numbers)} matching serials found...", flush=True)
            elif tools is None:
                print(f"Warning: Page {page_num} returned None (possible error), stopping pagination", flush=True)
                break
            
            next_cursor = cursor
            if not next_cursor:
                break
            page_num += 1
    
    elapsed = time.time() - start_time
    if tipqa_serial_numbers is None:
        print(f"Found {len(all_tools)} total Ion tools in {elapsed:.1f}s", flush=True)
    else:
        print(f"Found {len(all_tools)} Ion tools matching TipQA serial numbers in {elapsed:.1f}s", flush=True)
        print(f"  (Optimized: Filtered from potentially 160K+ tools to only those matching {len(tipqa_serial_numbers)} TipQA serial numbers)", flush=True)
    
    # Organize by serial number
    ion_data = organize_ion_data_by_serial(all_tools)
    
    return ion_data

def get_ion_matches_for_tipqa_tools_optimized(token: str, config: Dict[str, Any], tipqa_tools: List[Dict], environment: str) -> Dict[str, Any]:
    """
    Query Ion for SPECIFIC TipQA serial/part combinations with parallel processing.
    Uses ThreadPoolExecutor for concurrent API calls and batched queries.
    
    This follows the master_data_flow.md specification:
    - Step 2: Targeted Ion Data Collection
    - Query: get_tool_inventory_by_serial_and_part.graphql
    - Finds both TOOL and PART partTypes (no partType filter needed)
    - Uses aggressive parallel processing for targeted queries
    """
    from utilities.graphql_utils import post_graphql, read_query, organize_ion_data_by_serial
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time
    
    print(f"Fetching Ion matches for {len(tipqa_tools)} TipQA tools with parallel processing...", flush=True)
    start_time = time.time()
    
    # Get unique serial/part combinations from TipQA tools
    print("Extracting unique serial/part combinations from TipQA data...", flush=True)
    combinations = set()
    for tool in tipqa_tools:
        serial_number = tool.get('serial_number', '') or ''
        part_number = tool.get('part_number', '') or ''
        serial_number = serial_number.strip() if serial_number else ''
        part_number = part_number.strip() if part_number else ''
        if serial_number and part_number:
            combinations.add((serial_number, part_number))
    
    print(f"Found {len(combinations)} unique serial/part combinations", flush=True)
    
    if not combinations:
        print("No serial/part combinations to query - returning empty result", flush=True)
        return {"by_serial": {}, "all_tools": []}
    
    # Use targeted query for each combination with parallel processing
    # This follows master_data_flow.md Step 2: Targeted Ion Data Collection
    query = read_query('get_tool_inventory_by_serial_and_part.graphql')
    all_matches = []
    
    def fetch_combination(serial_number, part_number):
        variables = {
            "serialNumber": serial_number,
            "partNumber": part_number
        }
        
        try:
            response = post_graphql(token, config, {'query': query, 'variables': variables}, environment)
            if 'errors' in response:
                print(f"GraphQL errors for {serial_number}/{part_number}: {response['errors']}", flush=True)
                return []
            
            edges = response.get('data', {}).get('partInventories', {}).get('edges', [])
            return [edge.get('node', {}) for edge in edges]
            
        except Exception as e:
            print(f"Error fetching {serial_number}/{part_number}: {str(e)}", flush=True)
            return []
    
    # Use ThreadPoolExecutor for parallel processing (as specified in master_data_flow.md)
    print(f"Starting parallel queries for {len(combinations)} combinations (this may take several minutes)...", flush=True)
    completed_count = 0
    with ThreadPoolExecutor(max_workers=20) as executor:  # Parallel processing for speed
        # Submit all combinations
        future_to_combo = {
            executor.submit(fetch_combination, serial_number, part_number): (serial_number, part_number)
            for serial_number, part_number in combinations
        }
        
        # Process results as they complete
        for future in as_completed(future_to_combo):
            serial_number, part_number = future_to_combo[future]
            completed_count += 1
            try:
                matches = future.result()
                all_matches.extend(matches)
                # Print progress every 500 combinations or every 5000 matches
                if completed_count % 500 == 0:
                    print(f"Progress: {completed_count}/{len(combinations)} combinations processed, {len(all_matches)} matches found so far...", flush=True)
                elif len(all_matches) % 5000 == 0:
                    print(f"Progress: {completed_count}/{len(combinations)} combinations processed, {len(all_matches)} matches found so far...", flush=True)
            except Exception as e:
                print(f"Error processing {serial_number}/{part_number}: {str(e)}", flush=True)
    
    print(f"Completed all {len(combinations)} combination queries", flush=True)
    
    elapsed = time.time() - start_time
    print(f"Found {len(all_matches)} Ion matches in {elapsed:.1f}s", flush=True)
    
    # Organize by serial number
    ion_data = organize_ion_data_by_serial(all_matches)
    
    return ion_data

def get_all_ion_tools(token: str, config: Dict[str, Any], environment: str) -> Dict[str, Any]:
    """Get ALL Ion tools (not just orphaned ones)."""
    from utilities.graphql_utils import post_graphql, read_query, organize_ion_data_by_serial
    
    log_and_print("Fetching all Ion tools...")
    
    # Use the consolidated inventory query (only TOOL partType)
    query = read_query('get_all_tool_inventory.graphql')
    all_tools = []
    after_cursor = None
    page_count = 0
    
    while True:
        page_count += 1
        variables = {
            "first": 1000,  # Fetch in batches
            "after": after_cursor
        }
        
        try:
            response = post_graphql(token, config, {'query': query, 'variables': variables}, environment)
            
            if 'errors' in response:
                log_and_print(f"GraphQL errors: {response['errors']}", 'error')
                break
            
            edges = response.get('data', {}).get('partInventories', {}).get('edges', [])
            if not edges:
                break
                
            # Add all tools (no filtering)
            for edge in edges:
                tool_node = edge.get('node', {})
                all_tools.append(tool_node)
            
            page_info = response.get('data', {}).get('partInventories', {}).get('pageInfo', {})
            if not page_info.get('hasNextPage', False):
                break
                
            after_cursor = page_info.get('endCursor')
            
            log_and_print(f"Fetched page {page_count} with {len(edges)} total tools")
            
        except Exception as e:
            log_and_print(f"Error fetching all Ion tools: {str(e)}", 'error')
            break
    
    log_and_print(f"Found {len(all_tools)} total Ion tools")
    
    # Organize by serial number
    ion_data = organize_ion_data_by_serial(all_tools)
    
    return ion_data

def get_ion_attribute_etag(attributes: List[Dict], key: str) -> str:
    """Extract attribute etag for a given key."""
    for attr in attributes:
        if attr.get('key') == key:
            return str(attr.get('Etag', '')) or str(attr.get('etag', ''))
    return ''

def ion_data_to_dataframe(ion_data: Dict, source: str):
    """Convert Ion data dictionary to DataFrame."""
    import pandas as pd
    
    records = []
    for serial_number, ion_records in ion_data.get('by_serial', {}).items():
        for ion_data_item in ion_records:
            # Extract inventory-level attributes
            inventory_attrs = ion_data_item.get('attributes', [])
            inventory_manufacturer = get_ion_attribute_value(inventory_attrs, 'Manufacturer')
            inventory_asset_serial = get_ion_attribute_value(inventory_attrs, 'Asset Serial Number')
            inventory_attr_etag = get_ion_attribute_etag(inventory_attrs, 'Manufacturer') or get_ion_attribute_etag(inventory_attrs, 'Asset Serial Number')
            
            # Extract part-level attributes
            part = ion_data_item.get('part', {})
            part_attrs = part.get('attributes', []) if part else []
            part_asset_type = get_ion_attribute_value(part_attrs, 'Asset Type')
            part_attr_etag = get_ion_attribute_etag(part_attrs, 'Asset Type')
            
            # Use inventory attributes if available, otherwise fall back to part attributes
            manufacturer = inventory_manufacturer or get_ion_attribute_value(part_attrs, 'Manufacturer')
            asset_serial = inventory_asset_serial or get_ion_attribute_value(part_attrs, 'Asset Serial Number')
            
            record = {
                'serial_number': serial_number,
                'source': source,
                # Core inventory fields
                'ion_id': str(ion_data_item.get('id', '')),
                'ion_serialNumber': str(ion_data_item.get('serialNumber', '')),
                'ion__etag': str(ion_data_item.get('_etag', '')),
                'ion_status': str(ion_data_item.get('status', '')),
                'ion_unavailable': bool(ion_data_item.get('unavailable', False)),
                'ion_lastMaintainedDate': str(ion_data_item.get('lastMaintainedDate', '')) if ion_data_item.get('lastMaintainedDate') else '',
                
                # Location fields
                'ion_location': str(ion_data_item.get('location', {}).get('name', '')) if ion_data_item.get('location') else '',
                'ion_location_id': str(ion_data_item.get('location', {}).get('id', '')) if ion_data_item.get('location') else '',
                'ion_location_name': str(ion_data_item.get('location', {}).get('name', '')) if ion_data_item.get('location') else '',
                
                # Inventory attributes (with exact key names)
                'ion_attributes_Asset Serial Number': asset_serial,
                'ion_attributes_Manufacturer': manufacturer,
                'ion_attributes_Etag': inventory_attr_etag,
                
                # Part fields
                'ion_part_id': str(part.get('id', '')) if part else '',
                'ion_part_partNumber': str(part.get('partNumber', '')) if part else '',
                'ion_part_revision': str(part.get('revision', '')) if part else '',
                'ion_part_partType': str(part.get('partType', '')) if part else '',
                'ion_part_trackingType': str(part.get('trackingType', '')) if part else '',
                'ion_part_description': str(part.get('description', '')) if part else '',
                'ion_part_maintenanceIntervalSeconds': str(part.get('maintenanceIntervalSeconds', '')) if (part and part.get('maintenanceIntervalSeconds') is not None) else '',
                'ion_part_etag': str(part.get('_etag', '')) if part else '',
                
                # Part attributes (with exact key names)
                'ion_part_attributes_Asset Type': part_asset_type,
                'ion_part_attributes_Etag': part_attr_etag,
                
                # Installations
                'ion_abomInstallations_id': str(ion_data_item.get('abomInstallations', [{}])[0].get('id', '')) if ion_data_item.get('abomInstallations') else '',
                'ion_buildrequirement_id': str(ion_data_item.get('abomInstallations', [{}])[0].get('buildRequirement', {}).get('id', '')) if ion_data_item.get('abomInstallations') and ion_data_item.get('abomInstallations', [{}])[0].get('buildRequirement') else '',
            }
            records.append(record)
    
    return pd.DataFrame(records)

def append_ion_to_tipqa(tipqa_df, ion_df):
    """
    Merge Ion dataframe with TipQA dataframe with memory-efficient operations.
    Uses vectorized pandas operations and avoids unnecessary copying.
    """
    import pandas as pd
    
    log_and_print("Merging Ion dataframe with TipQA dataframe...")
    
    # Define column mappings upfront to avoid repeated lookups
    # IMPORTANT: This list must match EXACTLY the fields returned by the TipQA query (tipqa_tools.sql)
    # TipQA query returns: serial_number, part_number, description, revision, service_interval_seconds,
    #                     asset_type, location, last_maintenance_date, asset_serial_number, manufacturer,
    #                     maintenance_status, revision_status (12 fields total)
    tipqa_columns = ['serial_number', 'part_number', 'description', 'revision', 'service_interval_seconds', 
                     'asset_type', 'location', 'last_maintenance_date', 
                     'asset_serial_number', 'manufacturer', 'maintenance_status', 'revision_status']
    
    ion_columns = ['ion_id', 'ion_serialNumber', 'ion__etag', 'ion_status', 'ion_unavailable',
                   'ion_lastMaintainedDate', 'ion_location', 'ion_location_id', 'ion_location_name',
                   'ion_attributes_Asset Serial Number', 'ion_attributes_Manufacturer', 'ion_attributes_Etag',
                   'ion_part_id', 'ion_part_partNumber', 'ion_part_revision', 'ion_part_partType',
                   'ion_part_trackingType', 'ion_part_description', 'ion_part_maintenanceIntervalSeconds',
                   'ion_part_etag', 'ion_part_attributes_Asset Type', 'ion_part_attributes_Etag',
                   'ion_abomInstallations_id', 'ion_buildrequirement_id']
    
    # MEMORY OPTIMIZATION: Use vectorized operations instead of loops
    log_and_print("Applying vectorized column prefixing...")
    
    # Add tipqa_ prefix to TipQA columns efficiently
    tipqa_rename_dict = {col: f'tipqa_{col}' for col in tipqa_columns if col in tipqa_df.columns}
    tipqa_df_prefixed = tipqa_df.rename(columns=tipqa_rename_dict)
    
    # MEMORY OPTIMIZATION: Pre-allocate columns with appropriate dtypes
    log_and_print("Pre-allocating Ion columns with optimized dtypes...")
    
    # Create Ion columns with appropriate dtypes for memory efficiency
    ion_dtype_dict = {}
    for col in ion_columns:
        if col not in tipqa_df_prefixed.columns:
            # Use string dtype to avoid categorical issues
            ion_dtype_dict[col] = 'string'
    
    # Add Ion columns efficiently
    for col, dtype in ion_dtype_dict.items():
        tipqa_df_prefixed[col] = pd.Series(dtype=dtype, index=tipqa_df_prefixed.index)
    
    # Add TipQA columns to Ion dataframe efficiently
    tipqa_dtype_dict = {}
    for col in tipqa_columns:
        ion_col = f'tipqa_{col}'
        if ion_col not in ion_df.columns:
            tipqa_dtype_dict[ion_col] = 'string'
    
    for col, dtype in tipqa_dtype_dict.items():
        ion_df[col] = pd.Series(dtype=dtype, index=ion_df.index)
    
    # MEMORY OPTIMIZATION: Use merge instead of manual iteration
    log_and_print("Performing vectorized merge operation...")
    
    # Ensure both dataframes have the same columns efficiently
    all_columns = list(tipqa_df_prefixed.columns) + [col for col in ion_df.columns if col not in tipqa_df_prefixed.columns]
    
    # Reindex efficiently with fill_value
    tipqa_df_prefixed = tipqa_df_prefixed.reindex(columns=all_columns, fill_value='')
    ion_df = ion_df.reindex(columns=all_columns, fill_value='')
    
    # MEMORY OPTIMIZATION: Use pandas merge instead of manual iteration
    # First, merge TipQA tools with Ion matches
    # CRITICAL: Match on BOTH serial_number AND part_number to ensure correct matching
    # This prevents false matches when the same serial number has different part numbers
    ion_df_for_merge = ion_df.copy()
    
    # Ensure serial_number column exists in ion_df_for_merge
    if 'serial_number' not in ion_df_for_merge.columns:
        ion_df_for_merge['serial_number'] = ion_df_for_merge.get('ion_serialNumber', '')
    
    # Ensure part_number column exists in ion_df_for_merge (use ion_part_partNumber)
    if 'part_number' not in ion_df_for_merge.columns:
        ion_df_for_merge['part_number'] = ion_df_for_merge.get('ion_part_partNumber', '')
    
    # Normalize merge keys: convert to string and strip whitespace to match comparison logic
    # This ensures consistent matching with the is_ion_tool_up_to_date function
    tipqa_df_prefixed['merge_serial'] = tipqa_df_prefixed['tipqa_serial_number'].astype(str).str.strip()
    tipqa_df_prefixed['merge_part'] = tipqa_df_prefixed['tipqa_part_number'].astype(str).str.strip()
    ion_df_for_merge['merge_serial'] = ion_df_for_merge['serial_number'].astype(str).str.strip()
    ion_df_for_merge['merge_part'] = ion_df_for_merge['part_number'].astype(str).str.strip()
    
    # Perform left join to merge TipQA with Ion data on BOTH serial_number AND part_number
    merged_df = tipqa_df_prefixed.merge(
        ion_df_for_merge, 
        left_on=['merge_serial', 'merge_part'],
        right_on=['merge_serial', 'merge_part'],
        how='left', 
        suffixes=('', '_ion')
    )
    
    # Clean up temporary merge columns
    merged_df = merged_df.drop(columns=['merge_serial', 'merge_part'], errors='ignore')
    
    # Update Ion columns with merged data
    for col in ion_columns:
        ion_col = f'{col}_ion'
        if ion_col in merged_df.columns:
            # Use vectorized operation to update non-null values
            mask = merged_df[ion_col].notna() & (merged_df[ion_col] != '')
            # Convert to string to avoid categorical dtype issues
            merged_df.loc[mask, col] = merged_df.loc[mask, ion_col].astype(str)
    
    # Clean up temporary columns
    # Note: ion_serialNumber_ion should be copied to ion_serialNumber before dropping
    if 'ion_serialNumber_ion' in merged_df.columns and 'ion_serialNumber' in merged_df.columns:
        mask = merged_df['ion_serialNumber_ion'].notna() & (merged_df['ion_serialNumber_ion'] != '')
        merged_df.loc[mask, 'ion_serialNumber'] = merged_df.loc[mask, 'ion_serialNumber_ion'].astype(str)
    
    # Drop temporary merge columns (no longer using merge_key)
    columns_to_drop = ['serial_number_ion', 'part_number_ion'] + [f'{col}_ion' for col in ion_columns]
    merged_df = merged_df.drop(columns=[col for col in columns_to_drop if col in merged_df.columns])
    
    # SECONDARY MERGE: Handle part number changes in TipQA
    # When a tool's part number changes in TipQA, the primary (serial+part) merge won't
    # match the existing Ion tool (because the part numbers differ). This secondary merge
    # matches on serial number only, so part number changes are detected as UPDATE actions
    # (not CREATE), allowing update_tool() to handle the partId reassignment.
    log_and_print("Checking for unmatched TipQA tools that may have changed part numbers...")
    
    unmatched_mask = merged_df['ion_id'].isna() | (merged_df['ion_id'].astype(str).str.strip().isin(['', 'nan', 'None', '<NA>']))
    unmatched_serials = merged_df.loc[unmatched_mask, 'tipqa_serial_number'].dropna().astype(str).str.strip()
    unmatched_serials = set(unmatched_serials[unmatched_serials != ''])
    
    # Build a set of serials already matched via the primary merge (to avoid double-matching)
    matched_mask = ~(merged_df['ion_id'].isna() | (merged_df['ion_id'].astype(str).str.strip().isin(['', 'nan', 'None', '<NA>'])))
    matched_ion_ids = set(merged_df.loc[matched_mask, 'ion_id'].astype(str).str.strip())
    
    # Track Ion serials that get matched in this secondary pass (used later for orphan filtering)
    secondary_matched_ion_ids = set()
    
    if unmatched_serials and len(ion_df_for_merge) > 0:
        ion_serial_col = ion_df_for_merge['merge_serial'].astype(str).str.strip()
        ion_id_col = ion_df_for_merge['ion_id'].astype(str).str.strip() if 'ion_id' in ion_df_for_merge.columns else pd.Series(dtype='string')
        ion_part_type_col = ion_df_for_merge['ion_part_partType'].astype(str).str.strip().str.upper() if 'ion_part_partType' in ion_df_for_merge.columns else pd.Series([''] * len(ion_df_for_merge), dtype='string')
        
        # Only match TOOL type records. PART type records use (serial, part) as
        # their identity -- reassigning the partId on a PART inventory could
        # affect other serials sharing that part. Unmatched TipQA rows that only
        # have a PART type Ion record will fall through to CREATE, which creates
        # a new TOOL type part+inventory.
        candidate_mask = (
            ion_serial_col.isin(unmatched_serials) &
            ~ion_id_col.isin(matched_ion_ids) &
            (ion_part_type_col == 'TOOL')
        )
        candidate_ion_df = ion_df_for_merge[candidate_mask]
        
        if len(candidate_ion_df) > 0:
            candidate_deduped = candidate_ion_df.drop_duplicates(subset=['merge_serial'], keep='first')
            
            serial_to_ion = {}
            for _, ion_row in candidate_deduped.iterrows():
                serial_key = str(ion_row.get('merge_serial', '')).strip()
                if serial_key:
                    serial_to_ion[serial_key] = ion_row
            
            backfill_count = 0
            for idx in merged_df.index:
                row_serial = str(merged_df.at[idx, 'tipqa_serial_number']).strip() if pd.notna(merged_df.at[idx, 'tipqa_serial_number']) else ''
                row_ion_id = str(merged_df.at[idx, 'ion_id']).strip() if pd.notna(merged_df.at[idx, 'ion_id']) else ''
                
                if row_serial in serial_to_ion and row_ion_id in ('', 'nan', 'None', '<NA>'):
                    ion_row = serial_to_ion[row_serial]
                    for col in ion_columns:
                        if col in ion_row.index:
                            val = ion_row[col]
                            if pd.notna(val) and str(val).strip() not in ('', 'nan', 'None'):
                                merged_df.at[idx, col] = str(val).strip()
                    
                    ion_id_val = str(ion_row.get('ion_id', '')).strip()
                    if ion_id_val:
                        secondary_matched_ion_ids.add(ion_id_val)
                    
                    backfill_count += 1
                    ion_part = str(ion_row.get('ion_part_partNumber', '')).strip()
                    tipqa_part = str(merged_df.at[idx, 'tipqa_part_number']).strip() if pd.notna(merged_df.at[idx, 'tipqa_part_number']) else ''
                    log_and_print(f"Part number change detected: serial {row_serial} - TipQA part '{tipqa_part}' vs Ion part '{ion_part}' (secondary serial-only merge)", 'info')
            
            if backfill_count > 0:
                log_and_print(f"Secondary merge matched {backfill_count} TipQA tools to existing Ion TOOL-type tools with different part numbers", 'info')
    
    # Add Ion-only tools (orphans) efficiently
    log_and_print("Adding Ion-only tools (orphans)...")
    
    # Find orphaned Ion tools - tools that don't have matching TipQA rows
    # A tool is orphaned if it wasn't matched by EITHER the primary (serial+part) merge
    # or the secondary (serial-only) merge for part number changes.
    tipqa_combinations = set()
    for _, row in tipqa_df_prefixed.iterrows():
        serial = str(row.get('tipqa_serial_number', '')).strip()
        part = str(row.get('tipqa_part_number', '')).strip()
        if serial and part:
            tipqa_combinations.add((serial, part))
    
    # Ensure serial_number and part_number columns exist in ion_df
    if 'serial_number' not in ion_df.columns:
        ion_df['serial_number'] = ion_df.get('ion_serialNumber', '')
    if 'part_number' not in ion_df.columns:
        ion_df['part_number'] = ion_df.get('ion_part_partNumber', '')
    
    # Find Ion tools that don't match any TipQA combo AND weren't matched via secondary merge
    def _is_orphan(row):
        serial = str(row.get('serial_number', '')).strip()
        part = str(row.get('part_number', '')).strip()
        ion_id = str(row.get('ion_id', '')).strip()
        if (serial, part) in tipqa_combinations:
            return False
        if ion_id and ion_id in secondary_matched_ion_ids:
            return False
        return True
    
    orphan_mask = ion_df.apply(_is_orphan, axis=1)
    orphan_ion_df = ion_df[orphan_mask].copy()
    
    if len(orphan_ion_df) > 0:
        # Add empty TipQA columns to orphan Ion tools
        for col in tipqa_columns:
            tipqa_col = f'tipqa_{col}'
            if tipqa_col not in orphan_ion_df.columns:
                orphan_ion_df[tipqa_col] = ''
        
        # Ensure orphan dataframe has same columns as merged dataframe
        orphan_ion_df = orphan_ion_df.reindex(columns=merged_df.columns, fill_value='')
        
        # Concatenate efficiently - handle empty dataframes to avoid FutureWarning
        if len(orphan_ion_df) > 0 and orphan_ion_df.notna().any().any():
            import warnings
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore', category=FutureWarning, message='.*concatenation.*')
                master_df = pd.concat([merged_df, orphan_ion_df], ignore_index=True)
        else:
            master_df = merged_df
    else:
        master_df = merged_df
    
    # MEMORY OPTIMIZATION: Convert to appropriate dtypes (avoid categorical for now)
    log_and_print("Optimizing memory usage with efficient dtypes...")
    
    # Convert object columns to string for consistency
    for col in master_df.columns:
        if master_df[col].dtype == 'object':
            master_df[col] = master_df[col].astype('string')
    
    log_and_print(f"Master dataframe created:")
    log_and_print(f"  - {len(tipqa_df)} TipQA tools processed")
    log_and_print(f"  - {len(ion_df)} Ion tools processed")
    log_and_print(f"  - {len(master_df)} total records")
    log_and_print(f"  - Memory usage optimized with efficient dtypes")
    
    return master_df

def create_unified_dataframe(tipqa_df, ion_matches: Dict, ion_tools: Dict):
    """Create unified dataframe from TipQA and Ion data."""
    import pandas as pd
    
    # Convert TipQA to records
    master_records = tipqa_df.to_dict('records')
    
    # Add Ion match data to TipQA records
    for record in master_records:
        serial_number = record.get('serial_number', '')
        ion_records = ion_matches.get('by_serial', {}).get(serial_number, [])
        if ion_records:
            # Merge first Ion match (should be only one for exact matches)
            ion_data = ion_records[0]
            record.update({
                # Core inventory fields
                'ion_id': str(ion_data.get('id', '')),
                'ion_serial_number': str(ion_data.get('serialNumber', '')),
                'ion_etag': str(ion_data.get('_etag', '')),
                'ion_status': str(ion_data.get('status', '')),
                'ion_last_maintained_date': str(ion_data.get('lastMaintainedDate', '')) if ion_data.get('lastMaintainedDate') else '',
                
                # Location fields
                'ion_location_id': str(ion_data.get('location', {}).get('id', '')) if ion_data.get('location') else '',
                'ion_location_name': str(ion_data.get('location', {}).get('name', '')) if ion_data.get('location') else '',
                
                # Part fields
                'ion_part_id': str(ion_data.get('part', {}).get('id', '')) if ion_data.get('part') else '',
                'ion_part_number': str(ion_data.get('part', {}).get('partNumber', '')) if ion_data.get('part') else '',
                'ion_part_revision': str(ion_data.get('part', {}).get('revision', '')) if ion_data.get('part') else '',
                'ion_part_type': str(ion_data.get('part', {}).get('partType', '')) if ion_data.get('part') else '',
                'ion_part_tracking_type': str(ion_data.get('part', {}).get('trackingType', '')) if ion_data.get('part') else '',
                'ion_part_description': str(ion_data.get('part', {}).get('description', '')) if ion_data.get('part') else '',
                'ion_maintenance_interval_seconds': str(ion_data.get('part', {}).get('maintenanceIntervalSeconds', '')) if ion_data.get('part') else '',
                'ion_part_etag': str(ion_data.get('part', {}).get('_etag', '')) if ion_data.get('part') else '',
                
                # Installations
                'ion_installations': len(ion_data.get('abomInstallations', [])),
                # Ion attributes - check both inventory and part levels
                'ion_asset_serial_number': get_ion_attribute_value_from_both_levels(ion_data, 'Asset Serial Number'),
                'ion_manufacturer': get_ion_attribute_value_from_both_levels(ion_data, 'Manufacturer'),
                'ion_asset_type': get_ion_attribute_value_from_both_levels(ion_data, 'Asset Type'),
                'ion_service_interval_seconds': str(ion_data.get('part', {}).get('maintenanceIntervalSeconds', '')) if ion_data.get('part', {}).get('maintenanceIntervalSeconds') else '',
                'ion_last_maintenance_date': str(ion_data.get('lastMaintainedDate', '')) if ion_data.get('lastMaintainedDate') else '',
                'ion_model_number': get_ion_attribute_value_from_both_levels(ion_data, 'Model Number'),
                'ion_condition': get_ion_attribute_value_from_both_levels(ion_data, 'Condition'),
                'ion_status': get_ion_attribute_value_from_both_levels(ion_data, 'Status'),
                'ion_date_added': get_ion_attribute_value_from_both_levels(ion_data, 'Date Added'),
                'ion_last_updated': get_ion_attribute_value_from_both_levels(ion_data, 'Last Updated'),
                'ion_notes': get_ion_attribute_value_from_both_levels(ion_data, 'Notes')
            })
    
    # Add orphaned Ion tools
    for serial_number, ion_records in ion_tools.get('by_serial', {}).items():
        for ion_data in ion_records:
            orphan_record = {
                'serial_number': serial_number,
                'part_number': ion_data.get('part', {}).get('partNumber', '') if ion_data.get('part') else '',
                'revision': ion_data.get('part', {}).get('revision', '') if ion_data.get('part') else '',
                'description': ion_data.get('part', {}).get('description', '') if ion_data.get('part') else '',
                'location': ion_data.get('location', {}).get('name', '') if ion_data.get('location') else '',
                'maintenance_status': '',  # Not in TipQA
                'revision_status': '',     # Not in TipQA
                'model_number': '',        # Not in TipQA
                'condition': '',           # Not in TipQA
                'status': '',              # Not in TipQA
                'date_added': '',          # Not in TipQA
                'last_updated': '',        # Not in TipQA
                'notes': '',               # Not in TipQA
                # Core inventory fields
                'ion_id': str(ion_data.get('id', '')),
                'ion_serial_number': str(ion_data.get('serialNumber', '')),
                'ion_etag': str(ion_data.get('_etag', '')),
                'ion_status': str(ion_data.get('status', '')),
                'ion_last_maintained_date': str(ion_data.get('lastMaintainedDate', '')) if ion_data.get('lastMaintainedDate') else '',
                
                # Location fields
                'ion_location_id': str(ion_data.get('location', {}).get('id', '')) if ion_data.get('location') else '',
                'ion_location_name': str(ion_data.get('location', {}).get('name', '')) if ion_data.get('location') else '',
                
                # Part fields
                'ion_part_id': str(ion_data.get('part', {}).get('id', '')) if ion_data.get('part') else '',
                'ion_part_number': str(ion_data.get('part', {}).get('partNumber', '')) if ion_data.get('part') else '',
                'ion_part_revision': str(ion_data.get('part', {}).get('revision', '')) if ion_data.get('part') else '',
                'ion_part_type': str(ion_data.get('part', {}).get('partType', '')) if ion_data.get('part') else '',
                'ion_part_tracking_type': str(ion_data.get('part', {}).get('trackingType', '')) if ion_data.get('part') else '',
                'ion_part_description': str(ion_data.get('part', {}).get('description', '')) if ion_data.get('part') else '',
                'ion_maintenance_interval_seconds': str(ion_data.get('part', {}).get('maintenanceIntervalSeconds', '')) if ion_data.get('part') else '',
                'ion_part_etag': str(ion_data.get('part', {}).get('_etag', '')) if ion_data.get('part') else '',
                
                # Installations
                'ion_installations': len(ion_data.get('abomInstallations', [])),
                # Ion attributes - check both inventory and part levels
                'ion_asset_serial_number': get_ion_attribute_value_from_both_levels(ion_data, 'Asset Serial Number'),
                'ion_manufacturer': get_ion_attribute_value_from_both_levels(ion_data, 'Manufacturer'),
                'ion_asset_type': get_ion_attribute_value_from_both_levels(ion_data, 'Asset Type'),
                'ion_service_interval_seconds': str(ion_data.get('part', {}).get('maintenanceIntervalSeconds', '')) if ion_data.get('part', {}).get('maintenanceIntervalSeconds') else '',
                'ion_last_maintenance_date': str(ion_data.get('lastMaintainedDate', '')) if ion_data.get('lastMaintainedDate') else '',
                'ion_model_number': get_ion_attribute_value_from_both_levels(ion_data, 'Model Number'),
                'ion_condition': get_ion_attribute_value_from_both_levels(ion_data, 'Condition'),
                'ion_status': get_ion_attribute_value_from_both_levels(ion_data, 'Status'),
                'ion_date_added': get_ion_attribute_value_from_both_levels(ion_data, 'Date Added'),
                'ion_last_updated': get_ion_attribute_value_from_both_levels(ion_data, 'Last Updated'),
                'ion_notes': get_ion_attribute_value_from_both_levels(ion_data, 'Notes')
            }
            master_records.append(orphan_record)
    
    return pd.DataFrame(master_records)

def remove_duplicates_from_master_dataframe(df):
    """Remove exact duplicate rows from master dataframe (excluding timestamp column)."""
    import pandas as pd
    log_and_print("Removing exact duplicate rows from master dataframe...")
    
    # Get all columns except the last one (timestamp column)
    columns_to_check = df.columns[:-1]  # Exclude column 40 (timestamp)
    
    # Remove duplicates based on all columns except timestamp
    # This compares entire rows (both TipQA and Ion data combined)
    deduplicated_df = df.drop_duplicates(subset=columns_to_check, keep='first')
    
    removed_count = len(df) - len(deduplicated_df)
    log_and_print(f"Removed {removed_count} exact duplicate records")
    
    return deduplicated_df

def select_best_ion_tool(ion_records):
    """Select the best Ion tool from multiple options with same serial number."""
    # Priority criteria for selecting best Ion tool:
    # 1. Tool with most complete data (fewest empty fields)
    # 2. Tool with most recent last_updated date
    # 3. Tool with lowest ion_id (oldest created)
    
    best_record = None
    best_score = -1
    
    for _, record in ion_records.iterrows():
        score = 0
        
        # Count non-empty Ion fields
        ion_fields = [col for col in record.index if col.startswith('ion_')]
        non_empty_count = sum(1 for field in ion_fields if record[field] and str(record[field]).strip() != '')
        score += non_empty_count
        
        # Prefer more recent last_updated (if available)
        last_updated = record.get('ion_last_updated', '')
        if last_updated and last_updated != '':
            score += 10
        
        # Prefer lower ion_id (older tool, more established)
        ion_id = record.get('ion_id', 0)
        if ion_id:
            score += (1000000 - int(ion_id))  # Lower ID = higher score
        
        if score > best_score:
            best_score = score
            best_record = record
    
    return best_record

def is_ion_tool_up_to_date(tool_data: dict, debug: bool = False, valid_ion_locations: set = None):
    """
    Check if Ion tool is already up-to-date with TipQA data.
    Compares all TipQA fields that should be synchronized to Ion.
    
    Matching criteria (all must be true):
    1. tipqa serial number matches ion serial number
    2. tipqa part number matches ion part number  
    3. tipqa description matches ion part description
    4. tipqa revision matches ion part revision
    5. tipqa service interval seconds matches ion service interval seconds
    6. tipqa location matches ion location name OR tipqa maintenance_status = L and ion location is lost
       NOTE: Location is always compared when TipQA has a value - changes (e.g. NULL->MAK02) are detected and synced
    7. tipqa last maintenance date matches ion last maintenance date
    8. tipqa asset serial number matches ion asset serial number
    9. tipqa manufacturer matches ion manufacturer
    
    Note: tipqa null = ion null for criteria 3-9
    Note: asset_type is NOT compared (excluded from update checks)
    
    Args:
        tool_data: Dictionary containing tool data with TipQA and Ion fields
        debug: If True, returns tuple (is_up_to_date, mismatches) instead of just bool
        valid_ion_locations: Set of valid Ion location names (case-insensitive). Reserved for future use;
                            location is always compared when TipQA has a value.
    
    Returns:
        bool: True if tool is up-to-date, False otherwise
        OR tuple: (is_up_to_date: bool, mismatches: list) if debug=True
    """
    import pandas as pd
    mismatches = []
    # Core fields that should match between TipQA and Ion
    # Map TipQA field names to actual Ion field names in the master dataframe
    # NOTE: asset_type is intentionally excluded from comparison
    field_mappings = [
        ('serial_number', 'ion_serialNumber'),                    # serial number in TipQA to part inventory serial number in Ion
        ('part_number', 'ion_part_partNumber'),                        # part number in TipQA to part number in Ion
        ('description', 'ion_part_description'),                  # description in TipQA to part description in Ion
        ('revision', 'ion_part_revision'),                        # revision in TipQA to part revision in Ion
        ('service_interval_seconds', 'ion_part_maintenanceIntervalSeconds'), # service interval seconds in TipQA to part maintenance interval seconds in Ion
        # ('asset_type', 'ion_part_attributes_Asset Type'),       # EXCLUDED: asset_type is not compared
        ('location', 'ion_location_name'),                        # location in TipQA to part inventory location name in Ion
        ('last_maintenance_date', 'ion_lastMaintainedDate'),    # last maintenance date in TipQA to part inventory last maintained date in Ion
        ('asset_serial_number', 'ion_attributes_Asset Serial Number'),       # asset serial number in TipQA to part inventory asset serial number in Ion
        ('manufacturer', 'ion_attributes_Manufacturer')                      # manufacturer in TipQA to part inventory manufacturer in Ion
    ]
    
    # DIAGNOSTIC: Check if all expected columns exist in tool_data
    missing_columns = []
    for tipqa_field, ion_field in field_mappings:
        tipqa_key = f'tipqa_{tipqa_field}'
        if tipqa_key not in tool_data:
            missing_columns.append(tipqa_key)
        if ion_field not in tool_data:
            missing_columns.append(ion_field)
    
    # If critical columns are missing, log a warning but continue (might be orphaned tool)
    if missing_columns and debug:
        mismatches.append(f'MISSING_COLUMNS: {", ".join(missing_columns)}')
    
    # Check if all core fields match
    for tipqa_field, ion_field in field_mappings:
        tipqa_key = f'tipqa_{tipqa_field}'
        tipqa_value = tool_data.get(tipqa_key, '')
        ion_value = tool_data.get(ion_field, '')
        
        # CRITICAL FIX: For description field only, skip comparison if Ion already has a value.
        # Description is shared across all serials of the same part, so we shouldn't flag mismatches
        # just because this serial's TipQA value differs from the Ion part value. Only flag if Ion
        # is empty and TipQA has a value. The batch update uses the most common description across
        # all TipQA serials to avoid ping-pong updates.
        #
        # IMPORTANT: Revision and service_interval_seconds are NOT in this list:
        # - Revision: Can legitimately change over time (e.g., part revised from "A" to "B"),
        #   so revisions should ALWAYS be compared and updated when they differ.
        # - Service Interval: Should ALWAYS be compared and updated when TipQA changes,
        #   even if Ion already has a value.
        #
        # IMPORTANT: Inventory-level fields (location, last_maintenance_date, asset_serial_number,
        # manufacturer, serial_number, part_number) are NOT in this list and will ALWAYS be compared.
        # Inventory-level fields are per-serial and should be updated whenever they differ.
        part_level_fields = ['description']
        if tipqa_field in part_level_fields:
            # Handle None/NaN values
            if pd.isna(tipqa_value):
                tipqa_value = ''
            if pd.isna(ion_value):
                ion_value = ''
            
            # Normalize for comparison
            tipqa_str = str(tipqa_value).strip()
            ion_str = str(ion_value).strip()
            
            # Handle special case: if either value is 'nan', 'none', or 'None' (string), treat as empty
            if tipqa_str.lower() in ('nan', 'none', '') or not tipqa_str or pd.isna(tipqa_value):
                tipqa_str = ''
            if ion_str.lower() in ('nan', 'none') or ion_str == 'None' or pd.isna(ion_value):
                ion_str = ''
            
            # For part-level fields: only flag mismatch if Ion is empty and TipQA has a value
            # If Ion already has a value, don't compare (part-level fields are shared)
            if not ion_str and tipqa_str:
                # Ion is empty, TipQA has value - this is a valid mismatch
                # Continue to field-specific handling below
                pass
            elif ion_str:
                # Ion already has a value - skip comparison for part-level fields
                # The part description/service_interval/revision is shared and may differ from
                # this serial's TipQA value, which is expected
                continue
            else:
                # Both empty - match, skip
                continue
        
        # DIAGNOSTIC: If Ion column is missing and TipQA has a value, this could be the issue
        # For optional fields, if Ion column doesn't exist, treat as empty (might be missing attribute)
        if ion_field not in tool_data and tipqa_value:
            # Column doesn't exist - this is likely the root cause of false positives
            # For optional fields, we'll treat missing Ion column as empty
            # But log it in debug mode to help diagnose
            if debug and tipqa_field in ['asset_serial_number', 'manufacturer']:
                mismatches.append(f'{tipqa_field}: TipQA has value but Ion column "{ion_field}" MISSING')
        
        # Handle None/NaN values
        if pd.isna(tipqa_value):
            tipqa_value = ''
        if pd.isna(ion_value):
            ion_value = ''
            
        # Special handling for service_interval_seconds (decimal to integer conversion)
        if tipqa_field == 'service_interval_seconds':
            try:
                # Normalize TipQA value: treat NaN, empty string, None, or string "None"/"null" as empty
                tipqa_normalized = tipqa_value
                if pd.isna(tipqa_value) or tipqa_value == '' or tipqa_value is None:
                    tipqa_normalized = ''
                elif isinstance(tipqa_value, str) and tipqa_value.strip().lower() in ('nan', 'none', 'null'):
                    tipqa_normalized = ''
                
                # Normalize Ion value: treat NaN, empty string, None, or string "None"/"null" as empty
                ion_normalized = ion_value
                if pd.isna(ion_value) or ion_value == '' or ion_value is None:
                    ion_normalized = ''
                elif isinstance(ion_value, str) and ion_value.strip().lower() in ('nan', 'none', 'null'):
                    ion_normalized = ''
                
                # Convert to integers for comparison (empty/null = 0)
                if not tipqa_normalized:
                    tipqa_int = 0
                else:
                    tipqa_int = int(float(tipqa_normalized))
                
                if not ion_normalized:
                    ion_int = 0
                else:
                    ion_int = int(float(ion_normalized))  # Ion might also be a string representation
                
                # maintenanceIntervalSeconds is a PART-level field shared across
                # all serials of the same part.  TipQA is the source of truth:
                # if TipQA has null/0 and Ion has a value, Ion should be cleared.
                if tipqa_int != ion_int:
                    mismatches.append(f'service_interval_seconds: TipQA={tipqa_int}, Ion={ion_int}')
                    if not debug:
                        return False
            except (ValueError, TypeError) as e:
                # If conversion fails, normalize both to strings and compare
                # But first check if both are effectively empty/null
                tipqa_str = str(tipqa_value).strip().lower() if tipqa_value is not None else ''
                ion_str = str(ion_value).strip().lower() if ion_value is not None else ''
                
                # Normalize empty representations
                if tipqa_str in ('nan', 'none', 'null', ''):
                    tipqa_str = ''
                if ion_str in ('nan', 'none', 'null', ''):
                    ion_str = ''
                
                # If TipQA is empty after normalization, skip (no opinion)
                if not tipqa_str:
                    continue
                
                # If both are empty after normalization, they match
                if not tipqa_str and not ion_str:
                    continue  # Skip - both are empty/null, considered a match
                
                # Otherwise, they don't match
                mismatches.append(f'service_interval_seconds: TipQA={repr(tipqa_value)}, Ion={repr(ion_value)} (conversion failed: {str(e)})')
                if not debug:
                    return False
        # Special handling for last_maintenance_date (date format normalization)
        elif tipqa_field == 'last_maintenance_date':
            # Normalize date formats for comparison (TipQA uses space, Ion uses T)
            tipqa_str = str(tipqa_value).strip()
            ion_str = str(ion_value).strip()
            
            # Handle special case: if Ion value is 'nan', 'none', or 'None' (string), treat as empty
            # Also handle if TipQA is empty/null and Ion is 'None' - they should match
            if tipqa_str.lower() in ('nan', 'none', '') or not tipqa_str:
                tipqa_str = ''
            if ion_str.lower() in ('nan', 'none') or ion_str == 'None':
                ion_str = ''
            
            # For maintenance dates, compare only the date portion (YYYY-MM-DD), ignore time
            # This handles timezone differences where TipQA might show 00:00:00 and Ion shows 07:00:00
            if tipqa_str and ion_str:
                # Extract date portion (first 10 characters: YYYY-MM-DD)
                try:
                    tipqa_date_only = tipqa_str[:10] if len(tipqa_str) >= 10 else tipqa_str
                    ion_date_only = ion_str[:10] if len(ion_str) >= 10 else ion_str
                    
                    # Compare only the date portion
                    if tipqa_date_only != ion_date_only:
                        mismatches.append(f'last_maintenance_date: TipQA={tipqa_date_only}, Ion={ion_date_only}')
                        if not debug:
                            return False
                except (IndexError, TypeError):
                    # Fall back to full string comparison if date extraction fails
                    tipqa_normalized = tipqa_str.replace(' ', 'T') if ' ' in tipqa_str else tipqa_str
                    ion_normalized = ion_str.replace(' ', 'T') if ' ' in ion_str else ion_str
                    if tipqa_normalized != ion_normalized:
                        mismatches.append(f'last_maintenance_date: TipQA={tipqa_normalized}, Ion={ion_normalized} (fallback)')
                        if not debug:
                            return False
            elif tipqa_str != ion_str:  # One is empty, other is not
                mismatches.append(f'last_maintenance_date: TipQA={tipqa_str}, Ion={ion_str} (one empty)')
                if not debug:
                    return False
        else:
            # Convert to strings for comparison
            tipqa_str = str(tipqa_value).strip()
            ion_str = str(ion_value).strip()
            
            # Handle special case: if either value is 'nan', 'none', or 'None' (string), treat as empty
            # Also handle null/empty values - if TipQA is null/empty and Ion is 'None', they should match
            if tipqa_str.lower() in ('nan', 'none', '') or not tipqa_str or pd.isna(tipqa_value):
                tipqa_str = ''
            if ion_str.lower() in ('nan', 'none') or ion_str == 'None' or pd.isna(ion_value):
                ion_str = ''
            
            # Special handling: For optional fields (description, revision, location, 
            # asset_serial_number, manufacturer), if both are empty, they match
            # But serial_number and part_number should always match if tool exists
            # Note: asset_type is excluded from comparison entirely
            optional_fields = ['description', 'revision', 'location', 
                             'asset_serial_number', 'manufacturer']
            
            # CRITICAL: For location field - ALWAYS compare when TipQA has a non-empty location.
            # Location is an inventory-level field; when TipQA location changes (e.g. NULL->MAK02),
            # we must detect the mismatch and update Ion. Previously we skipped when TipQA location
            # wasn't in valid_ion_locations, which incorrectly skipped valid locations (e.g. when
            # TipQA uses codes like MAK02 that match Ion names or are mapped via config).
            # Only skip when TipQA location is truly invalid (not in valid_ion_locations AND we're
            # sure it can't be set). Since valid_ion_locations now includes Ion names + config-mapped
            # codes, we only skip when TipQA has an unknown location - but even then, we should
            # compare and let update_tool log a warning if resolution fails, rather than silently
            # skipping and never updating. So we REMOVE the skip - always compare location.
            
            # For optional fields, if both are empty, consider it a match
            # Also, if TipQA is empty and Ion is empty/missing, consider it a match
            # Only flag as mismatch if TipQA has a value and Ion doesn't match
            if tipqa_field in optional_fields:
                # Both empty = match
                if not tipqa_str and not ion_str:
                    continue  # Skip this field - both empty is considered a match
                # TipQA empty but Ion has value = mismatch (Ion should be cleared)
                # TipQA has value but Ion empty = mismatch (Ion should be set)
                # Both have values = compare them below
            
            # Normalize whitespace: replace multiple spaces/tabs with single space
            tipqa_normalized = re.sub(r'\s+', ' ', tipqa_str)
            ion_normalized = re.sub(r'\s+', ' ', ion_str)
            
            # CRITICAL FIX: Use case-insensitive comparison for all fields except serial_number and part_number
            # Serial and part numbers should match exactly (case-sensitive) as they are identifiers
            # Other fields (description, location, manufacturer, etc.) should be case-insensitive
            if tipqa_field in ['serial_number', 'part_number']:
                # Case-sensitive comparison for identifiers
                if tipqa_normalized != ion_normalized:
                    mismatches.append(f'{tipqa_field}: TipQA="{tipqa_str}", Ion="{ion_str}"')
                    if not debug:
                        return False
            else:
                # Case-insensitive comparison for all other fields
                # IMPORTANT: Even if one is empty and the other isn't, we need to detect the mismatch
                tipqa_normalized_lower = tipqa_normalized.lower() if tipqa_normalized else ''
                ion_normalized_lower = ion_normalized.lower() if ion_normalized else ''
                
                if tipqa_normalized_lower != ion_normalized_lower:
                    mismatches.append(f'{tipqa_field}: TipQA="{tipqa_str}", Ion="{ion_str}"')
                    if not debug:
                        return False
    
    # Check special maintenance status mappings
    maintenance_status = tool_data.get('tipqa_maintenance_status', '')
    revision_status = tool_data.get('tipqa_revision_status', '')
    ion_status = tool_data.get('ion_status', '')
    ion_location_name = tool_data.get('ion_location_name', '')
    
    # Handle None/NaN values properly before string operations
    if pd.isna(ion_location_name) or ion_location_name is None:
        ion_location_name = ''
    else:
        ion_location_name = str(ion_location_name).strip()
    
    if pd.isna(ion_status) or ion_status is None:
        ion_status = ''
    else:
        ion_status = str(ion_status).strip()
    
    # maintenance_status = L in TipQA should match lost location in Ion
    if maintenance_status == 'L':
        if 'LOST' not in ion_location_name.upper():
            mismatches.append(f'maintenance_status=L but location not LOST: Ion location="{ion_location_name}"')
            if not debug:
                return False
    
    # maintenance_status = I or revision_status = I in TipQA should match status (UNAVAILABLE) in Ion
    if maintenance_status == 'I' or revision_status == 'I':
        if ion_status != 'UNAVAILABLE':
            mismatches.append(f'maintenance_status=I but Ion status not UNAVAILABLE: Ion status="{ion_status}"')
            if not debug:
                return False
    
    # Check if tool should be AVAILABLE but is currently UNAVAILABLE in Ion
    # Tool should be AVAILABLE if:
    # - NOT inactive (maintenance_status != 'I' and revision_status != 'I')
    # - NOT lost/offsite/quality hold (maintenance_status not in ('L', 'OS', 'OC', 'TO', 'QAHD'))
    # - Has valid location (not empty, not 'lost')
    tipqa_location = tool_data.get('tipqa_location') or tool_data.get('location', '')
    should_be_available = (
        maintenance_status != 'I' and revision_status != 'I' and
        maintenance_status not in ('L', 'OS', 'OC', 'TO', 'QAHD') and
        tipqa_location and str(tipqa_location).strip() and
        'lost' not in str(tipqa_location).lower()
    )
    
    # Check both ion_status and ion_unavailable flag
    # Handle ion_unavailable - it might be a boolean, string, or missing
    ion_unavailable_raw = tool_data.get('ion_unavailable', False)
    if pd.isna(ion_unavailable_raw):
        ion_unavailable = False
    elif isinstance(ion_unavailable_raw, str):
        ion_unavailable = ion_unavailable_raw.lower() in ('true', '1', 'yes')
    else:
        ion_unavailable = bool(ion_unavailable_raw)
    
    # Check if tool is currently unavailable (either via status or unavailable flag)
    # Make status check case-insensitive
    is_currently_unavailable = (str(ion_status).upper() == 'UNAVAILABLE' or ion_unavailable)
    
    # Debug logging for availability-debug serials (e.g. JT00004653 - lost in Ion but not in TipQA)
    serial_number = tool_data.get('tipqa_serial_number') or tool_data.get('serial_number', '')
    if serial_number in ('JT00004887', 'JT00004653'):
        log_and_print(f"DEBUG: is_ion_tool_up_to_date for {serial_number} - should_be_available={should_be_available}, ion_status={ion_status}, ion_unavailable={ion_unavailable}, is_currently_unavailable={is_currently_unavailable}", 'info')
        log_and_print(f"DEBUG: {serial_number} maintenance_status={maintenance_status}, revision_status={revision_status}, tipqa_location={tipqa_location}", 'info')
    
    if should_be_available and is_currently_unavailable:
        mismatches.append(f'Tool should be AVAILABLE (TipQA: maintenance_status={maintenance_status}, revision_status={revision_status}) but Ion shows UNAVAILABLE (status={ion_status}, unavailable={ion_unavailable})')
        if not debug:
            return False
    
    # Note: We're only checking direct field mappings above, not Ion attributes
    # The Ion attributes are already captured in the direct field mappings
    
    # All fields match - tool is up-to-date
    is_up_to_date = len(mismatches) == 0
    if debug:
        return (is_up_to_date, mismatches)
    return is_up_to_date

def determine_update_mutation_complexity(tool_data: dict) -> str:
    """
    Determine if an UPDATE action requires 1 or 2 mutations based on what needs to be updated.
    
    Returns:
        - 'update_inventory': Only inventory-level attributes need updates (1 mutation)
        - 'update_inventory_and_part': Both part and inventory need updates (2 mutations)
    """
    # Check if part-level fields need updates
    part_fields_need_update = False
    
    # Part-level fields that require part updates (from master_data_flow.md):
    # 1. Description
    # CRITICAL FIX: Only update description if Ion doesn't have one and TipQA does.
    # Don't flag mismatches when Ion already has a value, as part descriptions are shared
    # across all serials and may differ from individual serial descriptions in TipQA.
    tipqa_description = tool_data.get('tipqa_description', '')
    ion_description = tool_data.get('ion_part_description', '')
    tipqa_desc_str = str(tipqa_description).strip() if tipqa_description else ''
    ion_desc_str = str(ion_description).strip() if ion_description else ''
    
    # Handle None/NaN values
    import pandas as pd
    if pd.isna(tipqa_description) or tipqa_desc_str.lower() in ('nan', 'none', ''):
        tipqa_desc_str = ''
    if pd.isna(ion_description) or ion_desc_str.lower() in ('nan', 'none', ''):
        ion_desc_str = ''
    
    # Normalize for comparison (case-insensitive, whitespace normalized)
    tipqa_desc_normalized = re.sub(r'\s+', ' ', tipqa_desc_str).lower() if tipqa_desc_str else ''
    ion_desc_normalized = re.sub(r'\s+', ' ', ion_desc_str).lower() if ion_desc_str else ''
    
    # Flag for update ONLY if Ion has no description and TipQA has one
    # Don't compare descriptions when Ion already has a value (part-level fields are shared)
    if not ion_desc_normalized and tipqa_desc_normalized:
        part_fields_need_update = True
    
    # 2. Revision
    # CRITICAL: Revisions can legitimately change over time (e.g., part revised from "A" to "B"),
    # so revisions should ALWAYS be compared and updated when they differ, even if Ion already has a value.
    # Unlike description/service_interval, revisions are expected to change and should be synchronized.
    tipqa_revision = tool_data.get('tipqa_revision', '')
    ion_revision = tool_data.get('ion_part_revision', '')
    
    # Handle None/NaN values
    tipqa_rev_str = str(tipqa_revision).strip() if tipqa_revision else ''
    ion_rev_str = str(ion_revision).strip() if ion_revision else ''
    if pd.isna(tipqa_revision) or tipqa_rev_str.lower() in ('nan', 'none', ''):
        tipqa_rev_str = ''
    if pd.isna(ion_revision) or ion_rev_str.lower() in ('nan', 'none', ''):
        ion_rev_str = ''
    
    # Normalize revisions for comparison (case-insensitive, treat empty as 'A')
    tipqa_rev_normalized = tipqa_rev_str.lower() if tipqa_rev_str else 'a'
    ion_rev_normalized = ion_rev_str.lower() if ion_rev_str else 'a'
    
    # Flag for update if revisions differ (always compare, even if Ion already has a value)
    if tipqa_rev_normalized != ion_rev_normalized:
        part_fields_need_update = True
    
    # 3. Service Interval
    # maintenanceIntervalSeconds is a PART-level field shared across all serials.
    # TipQA is the source of truth: if it differs from Ion (including null vs value), update.
    tipqa_service_interval = tool_data.get('tipqa_service_interval_seconds', '')
    ion_service_interval = tool_data.get('ion_part_maintenanceIntervalSeconds', '')
    
    # Handle None/NaN values
    tipqa_si_str = str(tipqa_service_interval).strip() if tipqa_service_interval else ''
    ion_si_str = str(ion_service_interval).strip() if ion_service_interval else ''
    if pd.isna(tipqa_service_interval) or tipqa_si_str.lower() in ('nan', 'none', ''):
        tipqa_si_str = ''
    if pd.isna(ion_service_interval) or ion_si_str.lower() in ('nan', 'none', ''):
        ion_si_str = ''
    
    # Convert to integers for comparison (empty/null = 0)
    try:
        tipqa_si_int = int(float(tipqa_si_str)) if tipqa_si_str else 0
        ion_si_int = int(float(ion_si_str)) if ion_si_str else 0
        
        if tipqa_si_int != ion_si_int:
            part_fields_need_update = True
    except (ValueError, TypeError):
        if tipqa_si_str != ion_si_str:
            part_fields_need_update = True
    
    # 4. Asset Type (part-level attribute)
    tipqa_asset_type = tool_data.get('tipqa_asset_type', '')
    ion_asset_type = tool_data.get('ion_part_attributes_Asset Type', '')
    if str(tipqa_asset_type).strip() != str(ion_asset_type).strip():
        part_fields_need_update = True
    
    if part_fields_need_update:
        return 'update_inventory_and_part'
    else:
        return 'update_inventory'

def determine_create_mutation_complexity(tool_data: dict, merged_df=None, ion_part_numbers: set = None) -> str:
    """
    Determine if a CREATE action requires 1 or 2 mutations based on what needs to be created.
    
    Args:
        ion_part_numbers: Optional pre-computed set of lowercase Ion part numbers.
                          When provided, avoids an O(n) scan of merged_df per call.
    
    Returns:
        - 'create_new_inventory': Part exists, only need to create inventory (1 mutation)
        - 'new_tool_create': Need to create both part and inventory (2 mutations)
    """
    part_number = tool_data.get('tipqa_part_number', '')
    
    if not part_number:
        return 'new_tool_create'
    
    part_number_str = str(part_number).strip().lower()
    if not part_number_str:
        return 'new_tool_create'
    
    # Fast path: use pre-computed set (O(1) lookup)
    if ion_part_numbers is not None:
        if part_number_str in ion_part_numbers:
            return 'create_new_inventory'
        return 'new_tool_create'
    
    # Fallback: scan merged_df (O(n) per call -- used by subset_test and other callers)
    if merged_df is not None:
        ion_part_exists = merged_df['ion_part_partNumber'].str.strip().str.lower().eq(part_number_str).any()
        if ion_part_exists:
            return 'create_new_inventory'
    
    return 'new_tool_create'

def analyze_tool_using_daily_sync_logic(tool_data: dict, ion_data: dict, stats: dict, config: dict, dry_run: bool = False, merged_df=None, ion_part_numbers: set = None) -> dict:
    """Analyze a single tool using the optimized daily sync logic - shared across all scripts.
    
    Args:
        ion_part_numbers: Optional pre-computed set of lowercase Ion part numbers
                          (speeds up determine_create_mutation_complexity from O(n) to O(1)).
    """
    import pandas as pd
    from utilities.tool_processing_utils import (
        clean_part_number, is_tool_inactive, check_missing_part_number_logic, 
        check_tipqa_revision_problem_logic
    )
    
    from utilities.tool_processing_utils import clean_serial_number
    
    serial_number_raw = tool_data.get('tipqa_serial_number', '')
    serial_number = clean_serial_number(serial_number_raw)
    part_number = clean_part_number(tool_data.get('tipqa_part_number', ''))
    # Serial numbers to log for availability/lost debugging (lost in Ion but not in TipQA, etc.)
    _debug_availability_serials = ('JT00004887', 'JT00004653', 'JT00003025')
    _debug_this = serial_number in _debug_availability_serials or serial_number_raw in _debug_availability_serials

    # CRITICAL DEBUG: Log entry into analysis for tools we're debugging (availability/lost)
    if _debug_this:
        log_and_print(f"DEBUG: {serial_number} ENTERING analyze_tool_using_daily_sync_logic - serial_number={serial_number}, part_number={part_number}", 'info')
        log_and_print(f"DEBUG: {serial_number} - ion_id={tool_data.get('ion_id', 'N/A')}, ion_status={tool_data.get('ion_status', 'N/A')}, ion_unavailable={tool_data.get('ion_unavailable', 'N/A')}", 'info')
        log_and_print(f"DEBUG: {serial_number} - tipqa_maintenance={tool_data.get('tipqa_maintenance_status', 'N/A')}, tipqa_revision={tool_data.get('tipqa_revision_status', 'N/A')}, tipqa_location={tool_data.get('tipqa_location', 'N/A')}", 'info')
    
    # Step 1: Pre-filter checks (reduce analysis overhead)
    
    # CRITICAL: Check for missing or invalid serial number FIRST - skip these immediately
    # Blocked values: N/A, NA (no backslash), NONE, NULL, <NA>, NAN, UNKNOWN
    if not serial_number or serial_number.strip() == '' or str(serial_number_raw).strip().upper() in ('N/A', 'NA', 'NONE', 'NULL', '<NA>', 'NAN', 'UNKNOWN'):
        if _debug_this:
            log_and_print(f"ERROR: {serial_number} being SKIPPED due to missing/invalid serial number!", 'error')
        return {"action": "SKIP", "reason": "missing_serial_number"}
    
    # CRITICAL: Check if tool exists in Ion FIRST
    ion_id = tool_data.get('ion_id', '')
    # Handle nan values properly
    tool_exists_in_ion = False
    if pd.notna(ion_id) and str(ion_id).strip() and str(ion_id).strip() != 'nan':
        tool_exists_in_ion = True
    
    # CRITICAL DEBUG: Log tool_exists_in_ion status for availability-debug serials
    if _debug_this:
        log_and_print(f"DEBUG: {serial_number} - tool_exists_in_ion={tool_exists_in_ion}, ion_id={ion_id}", 'info')
    
    # Get maintenance status for logic
    maintenance_status = tool_data.get('tipqa_maintenance_status', '')
    revision_status = tool_data.get('tipqa_revision_status', '')
    
    # If tool doesn't exist in Ion, determine if we should CREATE or SKIP
    if not tool_exists_in_ion:
        # Check for truly inactive tools (I status) FIRST - skip these regardless of part number
        if maintenance_status == 'I' or revision_status == 'I':
            return {"action": "SKIP", "reason": "inactive_in_tipqa_no_ion"}
        
        # Check for missing part number - skip these
        # Also check for invalid values like "N/A", "NA" (no backslash), etc.
        # Blocked values: N/A, NA, NONE, NULL, <NA>, NAN (case-insensitive)
        # Using .upper() for case-insensitive comparison to catch variations like "na", "Na", "nA", etc.
        if not part_number or str(part_number).strip().upper() in ('N/A', 'NA', 'NONE', 'NULL', '<NA>', 'NAN'):
            return {"action": "SKIP", "reason": "missing_part_number"}
        
        # Check for missing location - skip these (tools without location should not be created)
        tipqa_location = tool_data.get('tipqa_location', '') or tool_data.get('location', '')
        if not tipqa_location or pd.isna(tipqa_location) or str(tipqa_location).strip() == '' or str(tipqa_location).strip().lower() in ('nan', 'none', 'null', '<na>'):
            return {"action": "SKIP", "reason": "missing_location"}
        
        # Check for protected part numbers - skip these
        protected_part_numbers = config.get('sync_exceptions', {}).get('protected_part_numbers', []) if config else []
        if protected_part_numbers:
            protected_part_numbers = [pn.upper().strip() for pn in protected_part_numbers]
            if part_number and part_number.upper().strip() in protected_part_numbers:
                return {"action": "SKIP", "reason": "protected_part_number"}
        
        # Check for revision problems - valid revisions must be:
        # - Up to 3 alphabetical characters (A, B, C, AA, BB, ABC, etc.)
        # - Dash (-)
        # - Empty/null (allowed, defaults to 'A')
        # NOTE: Ion requires ALPHABETICAL revisions only. Numeric revisions (1, 2, 11, 001, etc.)
        # are automatically converted to 'A' by the clean_revision() function before creating parts.
        revision = tool_data.get('tipqa_revision', '')
        if revision and str(revision).strip() != '-':
            revision_str = str(revision).strip()
            # Check if revision is NOT a valid alphanumeric string (1-3 characters)
            # This check allows alphanumeric for TipQA data validation, but numeric revisions
            # will be converted to 'A' by clean_revision() before sending to Ion.
            # Valid: A, B, C, AA, BB, ABC, etc. (alphanumeric, 1-3 characters)
            # Invalid: spaces, periods, special characters, length > 3, etc.
            if not (revision_str.isalnum() and len(revision_str) >= 1 and len(revision_str) <= 3):
                return {"action": "SKIP", "reason": "tipqa_revision_problem"}
        
        # Check for lost tools - SKIP them (don't create tools that are lost)
        if maintenance_status == 'L':
            return {"action": "SKIP", "reason": "lost_tool_not_in_ion"}
        
        # Check for offsite and quality hold tools - SKIP them (don't create tools that are offsite or in QAHD)
        if maintenance_status in ('OS', 'OC', 'TO'):
            return {"action": "SKIP", "reason": "offsite_tool_not_in_ion"}
        elif maintenance_status == 'QAHD':
            return {"action": "SKIP", "reason": "quality_hold_tool_not_in_ion"}
        
        # All other cases - determine if we need to create part or just inventory
        create_reason = determine_create_mutation_complexity(tool_data, merged_df, ion_part_numbers=ion_part_numbers)
        return {"action": "CREATE", "reason": create_reason}
    
    # Tool exists in Ion - check special maintenance statuses FIRST
    else:
        # Check for truly inactive tools (I status) - THIS MUST COME FIRST
        if maintenance_status == 'I' or revision_status == 'I':
            # Check if Ion tool is already UNAVAILABLE - if so, skip (no need to check data matching)
            ion_status = tool_data.get('ion_status', '')
            if ion_status == 'UNAVAILABLE':
                return {"action": "SKIP", "reason": "inactive_tipqa_unavailable_ion"}
            else:
                # Return match info for MARK_UNAVAILABLE action
                ion_id = tool_data.get('ion_id', '')
                # Convert float to int if needed
                if isinstance(ion_id, float) and not pd.isna(ion_id):
                    ion_id = int(ion_id)
                # Get etag - try both single and double underscore versions for compatibility
                ion_etag = tool_data.get('ion_etag', '') or tool_data.get('ion__etag', '')
                ion_part_etag = tool_data.get('ion_part_etag', '')
                
                match_info = {
                    'id': ion_id,
                    '_etag': ion_etag,
                    'part': {
                        'id': tool_data.get('ion_part_id', ''),
                        '_etag': ion_part_etag,
                        'partNumber': tool_data.get('ion_part_partNumber', ''),
                        'description': tool_data.get('ion_part_description', '')
                    }
                }
                return {"action": "MARK_UNAVAILABLE", "reason": "inactive_in_tipqa", "match": match_info}
        
        # Check if Ion tool is already up-to-date with TipQA data (for normal cases)
        # Get valid Ion locations from config if available
        valid_ion_locations = config.get('_valid_ion_locations') if config else None
        is_up_to_date, mismatches = is_ion_tool_up_to_date(tool_data, debug=True, valid_ion_locations=valid_ion_locations)
        
        # Debug logging for availability-debug serials (e.g. lost in Ion but not in TipQA)
        serial_number_check = tool_data.get('tipqa_serial_number') or tool_data.get('serial_number', '')
        if _debug_this:
            log_and_print(f"DEBUG: {serial_number} - is_up_to_date={is_up_to_date}, mismatches={mismatches}", 'info')

        if is_up_to_date:
            # CRITICAL: Even if tool is marked as up-to-date, check if availability needs updating
            # This handles the case where all fields match BUT availability is wrong
            maintenance_status_check = tool_data.get('tipqa_maintenance_status', '')
            revision_status_check = tool_data.get('tipqa_revision_status', '')
            tipqa_location_check = tool_data.get('tipqa_location') or tool_data.get('location', '')
            
            should_be_available_check = (
                maintenance_status_check != 'I' and revision_status_check != 'I' and
                maintenance_status_check not in ('L', 'OS', 'OC', 'TO', 'QAHD') and
                tipqa_location_check and str(tipqa_location_check).strip() and
                'lost' not in str(tipqa_location_check).lower()
            )
            
            ion_status_check = tool_data.get('ion_status', '')
            import pandas as pd
            ion_unavailable_raw_check = tool_data.get('ion_unavailable', False)
            if pd.isna(ion_unavailable_raw_check):
                ion_unavailable_check = False
            elif isinstance(ion_unavailable_raw_check, str):
                ion_unavailable_check = ion_unavailable_raw_check.lower() in ('true', '1', 'yes')
            else:
                ion_unavailable_check = bool(ion_unavailable_raw_check)
            
            is_currently_unavailable_check = (str(ion_status_check).upper() == 'UNAVAILABLE' or ion_unavailable_check)
            
            if _debug_this:
                log_and_print(f"DEBUG: {serial_number} - Even though is_up_to_date=True, checking availability: should_be_available={should_be_available_check}, is_currently_unavailable={is_currently_unavailable_check}", 'info')

            # If tool should be available but is unavailable, override SKIP with MARK_AVAILABLE
            if should_be_available_check and is_currently_unavailable_check:
                ion_id = tool_data.get('ion_id', '')
                if isinstance(ion_id, float) and not pd.isna(ion_id):
                    ion_id = int(ion_id)
                ion_etag = tool_data.get('ion_etag', '') or tool_data.get('ion__etag', '')
                ion_part_etag = tool_data.get('ion_part_etag', '')
                
                match_info = {
                    'id': ion_id,
                    '_etag': ion_etag,
                    'part': {
                        'id': tool_data.get('ion_part_id', ''),
                        '_etag': ion_part_etag,
                        'partNumber': tool_data.get('ion_part_partNumber', ''),
                        'description': tool_data.get('ion_part_description', '')
                    }
                }
                if _debug_this:
                    log_and_print(f"DEBUG: {serial_number} - Overriding SKIP with MARK_AVAILABLE because availability mismatch", 'info')
                return {"action": "MARK_AVAILABLE", "reason": "should_be_available_but_unavailable", "match": match_info}
            
            return {"action": "SKIP", "reason": "already_up_to_date"}
        
        # Store mismatch details for later use in reason
        mismatch_summary = ''
        if mismatches and len(mismatches) > 0:
            mismatch_summary = '; '.join(mismatches[:2])
            if len(mismatches) > 2:
                mismatch_summary += f' (+{len(mismatches) - 2} more)'
        
        # Check if tool should be AVAILABLE but is currently UNAVAILABLE
        # This is a special case that requires MARK_AVAILABLE action (similar to MARK_UNAVAILABLE)
        # IMPORTANT: This check must happen BEFORE checking for special maintenance statuses (L, OS, etc.)
        # because those tools should be unavailable, not available
        maintenance_status = tool_data.get('tipqa_maintenance_status', '')
        revision_status = tool_data.get('tipqa_revision_status', '')
        tipqa_location = tool_data.get('tipqa_location') or tool_data.get('location', '')
        
        # Tool should be AVAILABLE if:
        # - NOT inactive (maintenance_status != 'I' and revision_status != 'I')
        # - NOT lost/offsite/quality hold (maintenance_status not in ('L', 'OS', 'OC', 'TO', 'QAHD'))
        # - Has valid location (not empty, not 'lost')
        should_be_available = (
            maintenance_status != 'I' and revision_status != 'I' and
            maintenance_status not in ('L', 'OS', 'OC', 'TO', 'QAHD') and
            tipqa_location and str(tipqa_location).strip() and
            'lost' not in str(tipqa_location).lower()
        )
        
        # Check if tool is currently unavailable
        ion_status = tool_data.get('ion_status', '')
        import pandas as pd
        ion_unavailable_raw = tool_data.get('ion_unavailable', False)
        if pd.isna(ion_unavailable_raw):
            ion_unavailable = False
        elif isinstance(ion_unavailable_raw, str):
            ion_unavailable = ion_unavailable_raw.lower() in ('true', '1', 'yes')
        else:
            ion_unavailable = bool(ion_unavailable_raw)
        
        is_currently_unavailable = (str(ion_status).upper() == 'UNAVAILABLE' or ion_unavailable)
        
        # Debug logging for availability-debug serials
        serial_number = tool_data.get('tipqa_serial_number') or tool_data.get('serial_number', '')
        if _debug_this:
            log_and_print(f"DEBUG: {serial_number} MARK_AVAILABLE check - should_be_available={should_be_available}, is_currently_unavailable={is_currently_unavailable}, mismatches={mismatches}", 'info')

        # If tool should be available but is currently unavailable, and all other fields match,
        # use MARK_AVAILABLE action (single mutation, efficient)
        if should_be_available and is_currently_unavailable:
            # Check if the ONLY mismatch is availability (all other fields are up-to-date)
            availability_mismatch_only = False
            if mismatches:
                # Filter out availability-related mismatches to see if there are other mismatches
                other_mismatches = [m for m in mismatches if 'AVAILABLE' not in m and 'UNAVAILABLE' not in m]
                if len(other_mismatches) == 0:
                    availability_mismatch_only = True
                    if _debug_this:
                        log_and_print(f"DEBUG: {serial_number} - availability_mismatch_only=True (no other mismatches)", 'info')
                else:
                    if _debug_this:
                        log_and_print(f"DEBUG: {serial_number} - availability_mismatch_only=False (other mismatches: {other_mismatches})", 'info')
            else:
                # No mismatches except availability - this shouldn't happen if is_up_to_date caught it,
                # but check anyway
                availability_mismatch_only = True
                if _debug_this:
                    log_and_print(f"DEBUG: {serial_number} - availability_mismatch_only=True (no mismatches list)", 'info')

            if availability_mismatch_only:
                # Only availability needs updating - use MARK_AVAILABLE action
                ion_id = tool_data.get('ion_id', '')
                if isinstance(ion_id, float) and not pd.isna(ion_id):
                    ion_id = int(ion_id)
                ion_etag = tool_data.get('ion_etag', '') or tool_data.get('ion__etag', '')
                ion_part_etag = tool_data.get('ion_part_etag', '')
                
                match_info = {
                    'id': ion_id,
                    '_etag': ion_etag,
                    'part': {
                        'id': tool_data.get('ion_part_id', ''),
                        '_etag': ion_part_etag,
                        'partNumber': tool_data.get('ion_part_partNumber', ''),
                        'description': tool_data.get('ion_part_description', '')
                    }
                }
                if _debug_this:
                    log_and_print(f"DEBUG: {serial_number} - Returning MARK_AVAILABLE action", 'info')
                return {"action": "MARK_AVAILABLE", "reason": "should_be_available_but_unavailable", "match": match_info}
            elif _debug_this:
                log_and_print(f"DEBUG: {serial_number} - Not using MARK_AVAILABLE (has other mismatches), will use UPDATE instead", 'info')
        
        # Check for TipQA location missing (NULL/empty) - Ion had location, now TipQA doesn't.
        # Move to Lost and mark unavailable. Must run BEFORE maintenance_status check.
        tipqa_location_raw = tool_data.get('tipqa_location') or tool_data.get('location', '')
        tipqa_loc_empty = (
            not tipqa_location_raw or pd.isna(tipqa_location_raw) or
            str(tipqa_location_raw).strip() == '' or
            str(tipqa_location_raw).strip().lower() in ('nan', 'none', 'null', '<na>')
        )
        ion_location_name = tool_data.get('ion_location_name', '')
        ion_has_location = (
            ion_location_name and str(ion_location_name).strip() and
            str(ion_location_name).strip().lower() not in ('nan', 'none', 'null', '') and
            'lost' not in str(ion_location_name).lower()
        )
        if tipqa_loc_empty and ion_has_location:
            # TipQA location is now NULL but Ion had a location - move to Lost
            ion_id = tool_data.get('ion_id', '')
            if isinstance(ion_id, float) and not pd.isna(ion_id):
                ion_id = int(ion_id)
            ion_etag = tool_data.get('ion_etag', '') or tool_data.get('ion__etag', '')
            ion_part_etag = tool_data.get('ion_part_etag', '')
            match_info = {
                'id': ion_id,
                '_etag': ion_etag,
                'part': {
                    'id': tool_data.get('ion_part_id', ''),
                    '_etag': ion_part_etag,
                    'partNumber': tool_data.get('ion_part_partNumber', ''),
                    'description': tool_data.get('ion_part_description', '')
                }
            }
            reason = "location_missing_in_tipqa"
            if mismatch_summary:
                reason += f" (mismatches: {mismatch_summary})"
            return {"action": "UPDATE_THEN_MARK_UNAVAILABLE", "reason": reason, "match": match_info}
        
        # Check for special maintenance statuses that require UPDATE_THEN_MARK_UNAVAILABLE
        if maintenance_status in ('L', 'OS', 'OC', 'TO', 'QAHD'):
            # Tool needs updates, then mark as unavailable
            ion_id = tool_data.get('ion_id', '')
            # Convert float to int if needed
            if isinstance(ion_id, float) and not pd.isna(ion_id):
                ion_id = int(ion_id)
            # Get etag - try both single and double underscore versions for compatibility
            ion_etag = tool_data.get('ion_etag', '') or tool_data.get('ion__etag', '')
            ion_part_etag = tool_data.get('ion_part_etag', '')
            
            match_info = {
                'id': ion_id,
                '_etag': ion_etag,
                'part': {
                    'id': tool_data.get('ion_part_id', ''),
                    '_etag': ion_part_etag,
                    'partNumber': tool_data.get('ion_part_partNumber', ''),
                    'description': tool_data.get('ion_part_description', '')
                }
            }
            if maintenance_status == 'L':
                reason = "lost_location"
                if mismatch_summary:
                    reason += f" (mismatches: {mismatch_summary})"
                return {"action": "UPDATE_THEN_MARK_UNAVAILABLE", "reason": reason, "match": match_info}
            elif maintenance_status in ('OS', 'OC', 'TO'):
                reason = "offsite_in_tipqa"
                if mismatch_summary:
                    reason += f" (mismatches: {mismatch_summary})"
                return {"action": "UPDATE_THEN_MARK_UNAVAILABLE", "reason": reason, "match": match_info}
            elif maintenance_status == 'QAHD':
                reason = "quality_hold_in_tipqa"
                if mismatch_summary:
                    reason += f" (mismatches: {mismatch_summary})"
                return {"action": "UPDATE_THEN_MARK_UNAVAILABLE", "reason": reason, "match": match_info}
        
        # Tool exists in Ion and doesn't match special conditions - check for protected parts and revision problems
        # Check for protected part numbers
        protected_part_numbers = config.get('sync_exceptions', {}).get('protected_part_numbers', []) if config else []
        if protected_part_numbers:
            protected_part_numbers = [pn.upper().strip() for pn in protected_part_numbers]
            if part_number and part_number.upper().strip() in protected_part_numbers:
                return {"action": "SKIP", "reason": "protected_part_number"}
        
        # Check for revision problems - valid revisions must be:
        # - Up to 3 alphabetical characters (A, B, C, AA, BB, ABC, etc.)
        # - Dash (-)
        # - Empty/null (allowed, defaults to 'A')
        # NOTE: Ion requires ALPHABETICAL revisions only. Numeric revisions (1, 2, 11, 001, etc.)
        # are automatically converted to 'A' by the clean_revision() function before creating parts.
        revision = tool_data.get('tipqa_revision', '')
        if revision and str(revision).strip() != '-':
            revision_str = str(revision).strip()
            # Check if revision is NOT a valid alphanumeric string (1-3 characters)
            # This check allows alphanumeric for TipQA data validation, but numeric revisions
            # will be converted to 'A' by clean_revision() before sending to Ion.
            # Valid: A, B, C, AA, BB, ABC, etc. (alphanumeric, 1-3 characters)
            # Invalid: spaces, periods, special characters, length > 3, etc.
            if not (revision_str.isalnum() and len(revision_str) >= 1 and len(revision_str) <= 3):
                return {"action": "SKIP", "reason": "tipqa_revision_problem"}
        
        # Tool exists in Ion - determine action based on part type
        # Note: master dataframe uses 'ion_part_partType', not 'ion_part_type'
        ion_part_type = tool_data.get('ion_part_partType', '') or tool_data.get('ion_part_type', '')
        ion_id = tool_data.get('ion_id', '')
        # Convert float to int if needed
        if isinstance(ion_id, float) and not pd.isna(ion_id):
            ion_id = int(ion_id)
        # Get etag - try both single and double underscore versions for compatibility
        ion_etag = tool_data.get('ion_etag', '') or tool_data.get('ion__etag', '')
        ion_part_etag = tool_data.get('ion_part_etag', '')
        
        match_info = {
            'id': ion_id,
            '_etag': ion_etag,
            'part': {
                'id': tool_data.get('ion_part_id', ''),
                '_etag': ion_part_etag,
                'partNumber': tool_data.get('ion_part_partNumber', ''),
                'description': tool_data.get('ion_part_description', '')
            }
        }
        
        # Handle both TOOL and PART types with the same logic flow
        if ion_part_type in ['TOOL', 'PART']:
            # Reuse is_up_to_date / mismatches from the call at the top of
            # the else block (line ~1922).  The tool already passed the
            # "already_up_to_date" SKIP check, so is_up_to_date is False here.
            # No need to call is_ion_tool_up_to_date a second time.
            
            # For PART types, check if we can convert to TOOL
            if ion_part_type == 'PART':
                # CRITICAL: Check if TipQA part number is empty - skip conversion if so
                tipqa_part_number = tool_data.get('tipqa_part_number', '')
                if not tipqa_part_number or tipqa_part_number.strip() == '':
                    log_and_print(f"Cannot convert PART to TOOL for {serial_number} - TipQA part number is empty", 'warning')
                    return {"action": "SKIP", "reason": "empty_tipqa_part_number"}
                
                # CRITICAL SAFETY CHECK: Verify part number matches TipQA before allowing conversion
                ion_part_number = match_info.get('part', {}).get('partNumber', '')
                
                if ion_part_number.strip().lower() != tipqa_part_number.strip().lower():
                    log_and_print(f"SAFETY CHECK FAILED: Cannot convert PART to TOOL for {serial_number} - Ion part number '{ion_part_number}' does not match TipQA part number '{tipqa_part_number}'", 'error')
                    return {"action": "SKIP", "reason": "part_number_mismatch_conversion_blocked"}
                
                # Part exists and part number matches - safe to convert to tool
                log_and_print(f"SAFETY CHECKS PASSED: Converting PART to TOOL for {serial_number} - exact serial and part number match confirmed", 'info')
                return {"action": "UPDATE", "reason": "part_to_tool_conversion", "match": match_info}
            else:
                # Regular TOOL update
                update_reason = determine_update_mutation_complexity(tool_data)
                if mismatch_summary:
                    update_reason = f"{update_reason} (mismatches: {mismatch_summary})"
                return {"action": "UPDATE", "reason": update_reason, "match": match_info}
        else:
            # Tool exists but unknown part type -- reuse is_up_to_date /
            # mismatches from the earlier call (already False here).
            update_reason = determine_update_mutation_complexity(tool_data)
            if mismatch_summary:
                update_reason = f"{update_reason} (mismatches: {mismatch_summary})"
            return {"action": "UPDATE", "reason": update_reason, "match": match_info}
    
    # This should never be reached - all cases should be handled above
    return {"action": "UNKNOWN", "reason": "unhandled_case"}

def process_orphaned_ion_tools(tipqa_tools_df, ion_data: Dict, stats: Dict, token: str, config: Dict, 
                              lost_location_id: str, environment: str, dry_run: bool = False) -> None:
    """Process orphaned Ion tools (tools in Ion but not in TipQA) - shared logic."""
    
    from utilities.tool_processing_utils import analyze_orphaned_ion_tools, mark_tool_unavailable
    
    log_and_print("Analyzing orphaned Ion tools...")
    
    # Create TipQA data structure for orphaned tool analysis
    tipqa_by_serial = {}
    for _, tool_row in tipqa_tools_df.iterrows():
        serial_number = tool_row.get('serial_number', '')
        if serial_number:
            if serial_number not in tipqa_by_serial:
                tipqa_by_serial[serial_number] = []
            tipqa_by_serial[serial_number].append(tool_row.to_dict())
    
    tipqa_data = {'by_serial': tipqa_by_serial}
    orphaned_actions = analyze_orphaned_ion_tools(tipqa_data, ion_data, stats)
    log_and_print(f"Found {len(orphaned_actions)} orphaned Ion tools")
    
    # Process orphaned tools
    for orphaned_action in orphaned_actions:
        action = orphaned_action.get('action', 'UNKNOWN')
        reason = orphaned_action.get('reason', 'UNKNOWN')
        match_info = orphaned_action.get('match', {})
        
        # Execute the action if not dry run
        success = False
        error_message = None
        
        if not dry_run:
            try:
                if action == 'MARK_UNAVAILABLE':
                    success = mark_tool_unavailable(token, config, orphaned_action, match_info, lost_location_id, environment, dry_run)
                else:
                    success = True  # Other actions are considered successful
            except Exception as e:
                error_message = str(e)
                success = False
        else:
            success = True  # Dry run always succeeds
        
        # Update stats based on action and success
        if success:
            if action == 'MARK_UNAVAILABLE':
                stats['marked_unavailable'] = stats.get('marked_unavailable', 0) + 1
        else:
            stats['errors'] = stats.get('errors', 0) + 1
            if error_message:
                log_and_print(f"Error processing orphaned tool: {error_message}", 'error')

def process_duplicate_tools(stats: Dict, token: str, config: Dict, environment: str, dry_run: bool = False) -> None:
    """Process duplicate tools identified during matching - shared logic."""
    
    from utilities.tool_processing_utils import cleanup_duplicate_tools
    
    # Handle any duplicates that were identified during matching
    duplicates_to_cleanup = stats.get('duplicates_to_cleanup', [])
    if duplicates_to_cleanup:
        log_and_print(f"Cleaning up {len(duplicates_to_cleanup)} duplicate tools")
        cleanup_stats = cleanup_duplicate_tools(token, config, duplicates_to_cleanup, environment, dry_run)
        stats['duplicate_cleanup'] = cleanup_stats
        # Clear the cleanup list to avoid reprocessing
        stats['duplicates_to_cleanup'] = []

