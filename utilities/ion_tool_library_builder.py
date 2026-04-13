#!/usr/bin/env python3
"""
Ion Tool Library Builder for V2 Sandbox and V2 Production (Ion Reloaded)

This script builds the Ion Tool Library (parts) in V2 Sandbox or V2 Production based on TipQA data.
It creates parts that will be used for tool inventory creation.

Requirements:
- Exclude inactive tools (maintenance_status: I or revision_status: I)
- Include offsite tools (maintenance_status: OS)
- Include tools with missing locations
- Only create unique part_number + revision combinations
- Exclude protected part numbers and their serial numbers
- Generate CSV output for dry run analysis
"""

import os
import sys
import pandas as pd
import argparse
import re
import time
import yaml
from datetime import datetime
from typing import Dict, List, Set, Tuple
from dotenv import load_dotenv

# Add the parent directory to the path so we can import utilities
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utilities.database_utils import get_tipqa_connection, get_all_tipqa_tools, read_sql_query
from utilities.graphql_utils import get_token, post_graphql, read_query
from utilities.tool_processing_utils import is_tool_inactive, clean_part_number, is_part_number_protected

def print_status(message: str):
    """Simple print function for status messages."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")

def cleanup_previous_library_files():
    """Clean up previous Ion Tool Library analysis files to prevent clutter."""
    import glob
    import os
    
    test_files_patterns = [
        'tests/ion_tool_library_analysis_*.csv'
    ]
    
    cleaned_count = 0
    for pattern in test_files_patterns:
        files = glob.glob(pattern)
        for file in files:
            try:
                os.remove(file)
                cleaned_count += 1
                print_status(f"Cleaned up previous file: {file}")
            except OSError as e:
                print_status(f"Could not remove {file}: {e}")
    
    if cleaned_count > 0:
        print_status(f"Cleaned up {cleaned_count} previous Ion Tool Library files")
    else:
        print_status("No previous Ion Tool Library files found to clean up")

def load_config_with_env_vars():
    """Load configuration file with environment variable expansion."""
    try:
        with open('config.yaml', 'r') as f:
            content = f.read()
        
        # Replace environment variables in the YAML content
        def replace_env_var(match):
            var_name = match.group(1)
            return os.getenv(var_name, match.group(0))
        
        content = re.sub(r'\$\{([^}]+)\}', replace_env_var, content)
        
        config = yaml.safe_load(content)
        return config
    except Exception as e:
        print_status(f"Error loading config: {e}")
        sys.exit(1)


def get_existing_ion_parts(token: str, config: Dict, environment: str = 'v2_sandbox') -> Dict[str, Dict]:
    """Get all existing parts from V2 Sandbox or V2 Production Ion using approved inventory query."""
    env_name = 'V2 Sandbox' if environment == 'v2_sandbox' else 'V2 Production (Ion Reloaded)'
    print_status(f"Fetching existing parts from {env_name} Ion (via inventory query)...")
    
    # Use approved query: get_all_tool_inventory
    query = read_query('get_all_tool_inventory.graphql')
    
    try:
        # Fetch all inventory items with pagination
        all_parts_data = []
        after_cursor = None
        page_count = 0
        
        while True:
            page_count += 1
            variables = {
                'first': 1000,
                'after': after_cursor
            }
            
            response = post_graphql(token, config, {'query': query, 'variables': variables}, environment)
            
            # Check for GraphQL errors first
            if 'errors' in response:
                error_messages = [error.get('message', 'Unknown error') for error in response['errors']]
                print_status(f"GraphQL errors fetching parts: {error_messages}")
                break
            
            # Get inventory data from the connection
            data = response.get('data', {})
            inventory_connection = data.get('partInventories', {})
            inventory_edges = inventory_connection.get('edges', [])
            
            # Extract unique parts from inventory items
            seen_part_keys = set()
            for edge in inventory_edges:
                inv_node = edge.get('node', {})
                part_node = inv_node.get('part', {})
                if part_node:
                    part_number = part_node.get('partNumber', '').upper().strip()
                    revision = part_node.get('revision', '').upper().strip()
                    key = f"{part_number}|{revision}"
                    
                    # Only add each part once (deduplicate)
                    if key not in seen_part_keys:
                        seen_part_keys.add(key)
                        all_parts_data.append(part_node)
            
            # Check for pagination
            page_info = inventory_connection.get('pageInfo', {})
            if not page_info.get('hasNextPage', False):
                break
            after_cursor = page_info.get('endCursor')
            
            if page_count % 10 == 0:
                print_status(f"Fetched {len(all_parts_data)} unique parts so far (page {page_count})...")
        
        # Index by part_number + revision for fast lookup
        parts_index = {}
        for part in all_parts_data:
            part_number = part.get('partNumber', '').upper().strip()
            revision = part.get('revision', '').upper().strip()
            key = f"{part_number}|{revision}"
            parts_index[key] = part
            
        print_status(f"Retrieved {len(all_parts_data)} existing parts from {env_name} (from inventory)")
        return parts_index
        
    except Exception as e:
        print_status(f"Error fetching existing parts: {e}")
        raise

def analyze_tipqa_parts_for_library(conn, config: Dict) -> pd.DataFrame:
    """Analyze TipQA data to identify parts that should be added to Ion Tool Library."""
    print_status("Analyzing TipQA data for Tool Library parts...")
    
    # Load TipQA data
    tipqa_df = get_all_tipqa_tools(conn, config)
    
    print_status(f"Loaded {len(tipqa_df)} tools from TipQA")
    
    # Deduplicate TipQA data by serial number (keep first occurrence)
    original_count = len(tipqa_df)
    tipqa_df = tipqa_df.drop_duplicates(subset=['serial_number'], keep='first')
    if len(tipqa_df) < original_count:
        print_status(f"Removed {original_count - len(tipqa_df)} duplicate TipQA records")
    
    # Get protected part numbers from config
    protected_part_numbers = config.get('sync_exceptions', {}).get('protected_part_numbers', [])
    protected_part_numbers = [pn.upper().strip() for pn in protected_part_numbers]
    print_status(f"Protected part numbers: {protected_part_numbers}")
    
    # Identify serial numbers with protected parts (for reporting and exclusion)
    if protected_part_numbers:
        tipqa_df['part_number_upper'] = tipqa_df['part_number'].str.upper().str.strip()
        protected_serials = tipqa_df[tipqa_df['part_number_upper'].isin(protected_part_numbers)]['serial_number'].unique().tolist()
        print_status(f"Found {len(protected_serials)} serial numbers with protected part numbers")
        if len(protected_serials) > 0 and len(protected_serials) <= 50:
            print_status(f"Protected serial numbers: {', '.join(sorted(protected_serials))}")
        elif len(protected_serials) > 50:
            print_status(f"Protected serial numbers (first 50): {', '.join(sorted(protected_serials[:50]))} ... and {len(protected_serials) - 50} more")
        tipqa_df = tipqa_df.drop('part_number_upper', axis=1)
    
    # Filter out inactive tools
    active_tools = []
    protected_serial_count = 0
    for _, tool in tipqa_df.iterrows():
        tool_data = tool.to_dict()
        
        # Skip inactive tools
        if is_tool_inactive(tool_data):
            continue
        
        # Skip tools with protected part numbers
        part_num = str(tool_data.get('part_number', '') or '').upper().strip()
        if part_num and is_part_number_protected(part_num, config):
            protected_serial_count += 1
            continue
            
        active_tools.append(tool_data)
    
    if protected_serial_count > 0:
        print_status(f"Excluded {protected_serial_count} active tools with protected part numbers")
    
    active_df = pd.DataFrame(active_tools)
    print_status(f"After filtering inactive tools and protected parts: {len(active_df)} active tools")
    
    # Get unique part combinations (part_number + revision)
    library_parts = active_df.groupby(['part_number', 'revision']).agg({
        'description': 'first',
        'service_interval_seconds': 'first', 
        'asset_type': 'first'
    }).reset_index()
    
    library_parts.columns = ['part_number', 'revision', 'description', 'service_interval_seconds', 'asset_type']
    
    # Clean part numbers
    library_parts['part_number'] = library_parts['part_number'].apply(clean_part_number)
    
    # Filter out protected part numbers
    if protected_part_numbers:
        before_count = len(library_parts)
        library_parts = library_parts[~library_parts['part_number'].isin(protected_part_numbers)]
        after_count = len(library_parts)
        filtered_count = before_count - after_count
        print_status(f"Filtered out {filtered_count} protected part numbers")
    
    # Convert service interval to integer (remove .0)
    library_parts['service_interval_seconds'] = library_parts['service_interval_seconds'].fillna(0).astype(int)
    
    # Remove rows with empty part numbers
    library_parts = library_parts[library_parts['part_number'].notna() & (library_parts['part_number'] != '')]
    
    print_status(f"Identified {len(library_parts)} unique parts for Tool Library")
    
    return library_parts

def create_part_in_ion(token: str, part_data: Dict, config: Dict, environment: str = 'v2_sandbox', max_retries: int = 3) -> Dict:
    """Create a part in V2 Sandbox or V2 Production Ion with retry logic."""
    
    # CRITICAL SAFETY CHECK: Verify we have required data for tool creation
    part_number = part_data.get('part_number', '')
    if not part_number:
        print_status(f"SAFETY CHECK FAILED: Cannot create tool - missing part number")
        return "MISSING_PART_NUMBER"
    
    # CRITICAL SAFETY CHECK: Verify part number is not protected before creating
    if is_part_number_protected(part_number, config):
        print_status(f"CRITICAL SAFETY CHECK FAILED: Part number '{part_number}' is protected and cannot be created as TOOL in Ion. Skipping creation.")
        return "PROTECTED_PART_NUMBER"
    
    # Check if revision is valid (alphabetical only)
    revision = part_data['revision']
    if revision and revision != '-' and not revision.replace(' ', '').isalpha():
        print_status(f"Skipping part with invalid revision format: {part_data['part_number']}-{revision} (revision must be alphabetical)")
        return "INVALID_REVISION"
    
    mutation = read_query('create_tool.graphql')
    
    # Convert service_interval_seconds to integer, or None if missing/invalid/zero
    service_interval = part_data.get('service_interval_seconds')
    if service_interval is not None:
        try:
            # Handle both int and float values, convert to int
            # Only set value if it's non-zero and valid
            interval_value = int(float(service_interval)) if service_interval else None
            maintenance_interval_seconds = interval_value if interval_value and interval_value > 0 else None
        except (ValueError, TypeError):
            maintenance_interval_seconds = None
    else:
        maintenance_interval_seconds = None
    
    # Build attributes array with Asset Type
    attributes = []
    if part_data.get('asset_type'):
        attributes.append({
            "key": "Asset Type",
            "value": str(part_data['asset_type'])
        })
    
    variables = {
        "input": {
            "partNumber": part_data['part_number'],
            "revision": part_data['revision'],
            "description": part_data.get('description', ''),
            "partType": "TOOL",
            "trackingType": "SERIAL",
            "maintenanceIntervalSeconds": maintenance_interval_seconds,
            "attributes": attributes
        }
    }
    
    for attempt in range(max_retries):
        try:
            response = post_graphql(token, config, {'query': mutation, 'variables': variables}, environment)
            
            # Check for GraphQL errors first
            if 'errors' in response:
                error_messages = [error.get('message', 'Unknown error') for error in response['errors']]
                
                # Check if it's a duplicate error (already exists)
                if any('already exists' in error_msg.lower() for error_msg in error_messages):
                    print_status(f"Part already exists: {part_data['part_number']}-{part_data['revision']}")
                    return "ALREADY_EXISTS"  # Return special value to indicate skip
                
                # For other errors, retry if we have attempts left
                if attempt < max_retries - 1:
                    print_status(f"GraphQL errors creating part {part_data['part_number']}-{part_data['revision']} (attempt {attempt + 1}/{max_retries}): {error_messages}")
                    print_status(f"Retrying in 2 seconds...")
                    time.sleep(2)
                    continue
                else:
                    print_status(f"GraphQL errors creating part {part_data['part_number']}-{part_data['revision']} (final attempt): {error_messages}")
                    return {}
            
            # Check if data exists and has the expected structure
            data = response.get('data')
            if not data:
                if attempt < max_retries - 1:
                    print_status(f"No data returned for part {part_data['part_number']}-{part_data['revision']} (attempt {attempt + 1}/{max_retries})")
                    print_status(f"Retrying in 2 seconds...")
                    time.sleep(2)
                    continue
                else:
                    print_status(f"No data returned for part {part_data['part_number']}-{part_data['revision']} (final attempt)")
                    return {}
                
            create_part_result = data.get('createPart')
            if not create_part_result:
                if attempt < max_retries - 1:
                    print_status(f"No createPart result for part {part_data['part_number']}-{part_data['revision']} (attempt {attempt + 1}/{max_retries})")
                    print_status(f"Retrying in 2 seconds...")
                    time.sleep(2)
                    continue
                else:
                    print_status(f"No createPart result for part {part_data['part_number']}-{part_data['revision']} (final attempt)")
                    return {}
                
            part = create_part_result.get('part')
            if not part:
                if attempt < max_retries - 1:
                    print_status(f"No part data returned for {part_data['part_number']}-{part_data['revision']} (attempt {attempt + 1}/{max_retries})")
                    print_status(f"Retrying in 2 seconds...")
                    time.sleep(2)
                    continue
                else:
                    print_status(f"No part data returned for {part_data['part_number']}-{part_data['revision']} (final attempt)")
                    return {}
                
            return part
            
        except Exception as e:
            if attempt < max_retries - 1:
                print_status(f"Error creating part {part_data['part_number']}-{part_data['revision']} (attempt {attempt + 1}/{max_retries}): {e}")
                print_status(f"Retrying in 2 seconds...")
                time.sleep(2)
                continue
            else:
                print_status(f"Error creating part {part_data['part_number']}-{part_data['revision']} (final attempt): {e}")
                return {}
    
    return {}

def generate_invalid_revision_report(tipqa_df: pd.DataFrame):
    """Generate a complete report of tools with invalid revisions."""
    print_status("Generating invalid revision report...")
    
    def is_invalid_revision(revision):
        if not revision or revision == '-':
            return False
        return not str(revision).replace(' ', '').isalpha()
    
    # Filter for tools with invalid revisions
    invalid_mask = tipqa_df['revision'].apply(is_invalid_revision)
    invalid_df = tipqa_df[invalid_mask].copy()
    
    if len(invalid_df) > 0:
        # Generate timestamp for filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f'tests/complete_invalid_revision_tools_{timestamp}.csv'
        
        # Save to CSV with all TipQA fields
        invalid_df.to_csv(csv_filename, index=False)
        
        print_status(f"Complete Invalid Revision Report Generated:")
        print_status(f"==========================================")
        print_status(f"Total tools with invalid revisions: {len(invalid_df)}")
        print_status(f"Report saved to: {csv_filename}")
        
        # Show all columns in the report
        print_status(f"Columns included in report ({len(invalid_df.columns)} total):")
        for i, col in enumerate(invalid_df.columns, 1):
            print_status(f"  {i:2d}. {col}")
        
        # Show some examples
        print_status(f"Examples of invalid revisions:")
        examples = invalid_df[['serial_number', 'part_number', 'revision', 'description', 'location', 'maintenance_status']].head(10)
        for _, row in examples.iterrows():
            print_status(f"  {row['serial_number']} | {row['part_number']} | {row['revision']} | {row['description']} | {row['location']} | {row['maintenance_status']}")
        
        if len(invalid_df) > 10:
            print_status(f"  ... and {len(invalid_df) - 10} more")
            
        # Show revision patterns
        print_status(f"Invalid revision patterns found:")
        revision_patterns = invalid_df['revision'].value_counts().head(10)
        for revision, count in revision_patterns.items():
            print_status(f"  \"{revision}\": {count} tools")
    else:
        print_status("No tools with invalid revisions found!")

def main():
    """Main function to build Ion Tool Library."""
    parser = argparse.ArgumentParser(description='Build Ion Tool Library in V2 Sandbox or V2 Production (Ion Reloaded)')
    parser.add_argument('--dry-run', action='store_true', help='Perform dry run analysis only')
    parser.add_argument('--invalid-revision-report', action='store_true', help='Generate report of tools with invalid revisions')
    parser.add_argument('--environment', choices=['v2_sandbox', 'v2_production', 'v2_staging'], default='v2_sandbox', 
                       help='Ion environment to use: v2_sandbox (default), v2_production (Ion Reloaded), or v2_staging (Ion Reloaded Staging)')
    args = parser.parse_args()
    
    dry_run = args.dry_run
    invalid_report = args.invalid_revision_report
    environment = args.environment
    
    env_name = 'V2 Sandbox' if environment == 'v2_sandbox' else 'V2 Production (Ion Reloaded)'
    print_status("Starting Ion Tool Library Builder...")
    print_status(f"Environment: {env_name}")
    print_status(f"Dry run mode: {dry_run}")
    print_status(f"Invalid revision report: {invalid_report}")
    
    # Clean up previous analysis files
    cleanup_previous_library_files()
    
    try:
        # Load configuration and environment
        load_dotenv()
        config = load_config_with_env_vars()
        
        # Connect to TipQA (via Databricks)
        print_status("Connecting to TipQA (via Databricks)...")
        conn = get_tipqa_connection(config)
        
        # Analyze TipQA data for library parts
        library_parts_df = analyze_tipqa_parts_for_library(conn, config)
        
        # Generate invalid revision report if requested
        if invalid_report:
            # Load complete TipQA data for the report
            tipqa_df = get_all_tipqa_tools(conn, config)
            generate_invalid_revision_report(tipqa_df)
            return  # Exit after generating the report
        
        if dry_run:
            # Dry run - generate CSV analysis
            print_status("Performing dry run analysis...")
            
            # Get existing Ion parts
            token = get_token(config, environment)
            existing_parts = get_existing_ion_parts(token, config, environment)
            
            # Analyze what needs to be created
            analysis_results = []
            
            for _, part in library_parts_df.iterrows():
                part_number = part['part_number'].upper().strip()
                revision = part['revision'].upper().strip()
                key = f"{part_number}|{revision}"
                
                analysis_result = {
                    'part_number': part_number,
                    'revision': revision,
                    'description': part['description'],
                    'service_interval_seconds': part['service_interval_seconds'],
                    'asset_type': part['asset_type'],
                    'partType': 'TOOL',
                    'trackingType': 'SERIAL'
                }
                analysis_results.append(analysis_result)
            
            # Save analysis to CSV
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_filename = f'tests/ion_tool_library_analysis_{timestamp}.csv'
            
            analysis_df = pd.DataFrame(analysis_results)
            analysis_df.to_csv(csv_filename, index=False)
            
            # Generate summary
            total_parts = len(analysis_df)
            
            print_status(f"\nION TOOL LIBRARY ANALYSIS SUMMARY")
            print_status(f"==================================================")
            print_status(f"Total unique parts to create: {total_parts}")
            print_status(f"Analysis saved to: {csv_filename}")
            
        else:
            # Actual execution - create parts
            print_status(f"Creating parts in {env_name} Ion...")
            
            token = get_token(config, environment)
            existing_parts = get_existing_ion_parts(token, config, environment)
            
            created_count = 0
            skipped_count = 0
            failed_count = 0
            total_parts = len(library_parts_df)
            
            print_status(f"Processing {total_parts} parts with retry logic for spotty connections...")
            
            for index, (_, part) in enumerate(library_parts_df.iterrows(), 1):
                part_number = part['part_number'].upper().strip()
                revision = part['revision'].upper().strip()
                key = f"{part_number}|{revision}"
                
                if key in existing_parts:
                    print_status(f"[{index}/{total_parts}] Skipping existing part: {part_number}-{revision}")
                    skipped_count += 1
                    continue
                
                print_status(f"[{index}/{total_parts}] Creating part: {part_number}-{revision}")
                
                part_data = {
                    'part_number': part_number,
                    'revision': revision,
                    'description': part['description'],
                    'asset_type': part['asset_type'],
                    'service_interval_seconds': part['service_interval_seconds']
                }
                
                result = create_part_in_ion(token, part_data, config, environment, max_retries=3)
                if result == "ALREADY_EXISTS":
                    skipped_count += 1
                    print_status(f"Part already exists: {part_number}-{revision}")
                elif result == "PROTECTED_PART_NUMBER":
                    skipped_count += 1
                    # Message already printed in create_part_in_ion
                elif result == "INVALID_REVISION":
                    skipped_count += 1
                    # Message already printed in create_part_in_ion
                elif result and isinstance(result, dict):
                    created_count += 1
                    print_status(f"Created part: {part_number}-{revision} (ID: {result.get('id', 'N/A')})")
                else:
                    failed_count += 1
                    print_status(f"Failed to create part: {part_number}-{revision}")
                
                # Progress update every 50 parts
                if index % 50 == 0:
                    print_status(f"Progress: {index}/{total_parts} processed | Created: {created_count} | Skipped: {skipped_count} | Failed: {failed_count}")
            
            print_status(f"\nION TOOL LIBRARY BUILD COMPLETED!")
            print_status(f"==================================================")
            print_status(f"Parts created: {created_count}")
            print_status(f"Parts skipped (already exist): {skipped_count}")
            print_status(f"Parts failed: {failed_count}")
            print_status(f"Total processed: {created_count + skipped_count + failed_count}")
        
    except Exception as e:
        print_status(f"Error in Ion Tool Library Builder: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
