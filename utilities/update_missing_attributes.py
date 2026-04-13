#!/usr/bin/env python3
'''
Update Missing Attributes Script
=================================

This script finds and updates tools in Ion that are missing "Asset Serial Number" 
or "Manufacturer" attributes, populating them from TipQA data.

Created: 2025-01-28
Author: Jae Osenbach
Purpose: Fix missing inventory attributes in Ion
Version: 1.0
'''

import os
import sys
import yaml
import time
import argparse
import threading
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import pandas as pd

# Import our modular utilities
from utilities.database_utils import get_tipqa_connection, get_all_tipqa_tools
from utilities.graphql_utils import get_token, post_graphql, read_query, AuthenticationError
from utilities.shared_sync_utils import log_and_print, create_master_dataframe
from utilities.tool_processing_utils import build_tipqa_inventory_attributes


def load_config() -> Dict[str, Any]:
    """Load configuration from YAML file with environment variable substitution."""
    import os
    import re
    
    try:
        with open('config.yaml', 'r') as f:
            content = f.read()
        
        # Replace environment variables in the YAML content
        def replace_env_vars(match):
            env_var = match.group(1)
            return os.getenv(env_var, match.group(0))  # Return original if env var not found
        
        content = re.sub(r'\$\{([^}]+)\}', replace_env_vars, content)
        
        return yaml.safe_load(content)
    except Exception as e:
        print(f"Error loading config: {e}")
        sys.exit(1)


def get_ion_attribute_value(attributes: List[Dict], key: str) -> str:
    """Extract attribute value from Ion attributes list."""
    for attr in attributes:
        if attr.get('key') == key:
            return str(attr.get('value', ''))
    return ''


def update_inventory_attributes(token: str, config: Dict, inventory_id: str, inventory_etag: str, 
                                attributes: List[Dict], environment: str) -> bool:
    """Update inventory attributes in Ion."""
    if not attributes:
        return True  # Nothing to update
    
    mutation = read_query('update_inventory_with_attributes.graphql')
    variables = {
        'input': {
            'id': inventory_id,
            'etag': inventory_etag,
            'attributes': attributes
        }
    }
    
    try:
        result = post_graphql(token, config, {'query': mutation, 'variables': variables}, environment)
        
        if 'errors' in result:
            log_and_print(f"Failed to update inventory {inventory_id} attributes: {result['errors']}", 'error')
            return False
        
        return True
    except AuthenticationError:
        # Token expired, will be handled by caller
        raise
    except Exception as e:
        log_and_print(f"Exception updating inventory {inventory_id} attributes: {e}", 'error')
        return False


def process_tool_for_missing_attributes(tool_data: Dict, token: str, config: Dict, 
                                       environment: str, stats: Dict, stats_lock: threading.Lock) -> bool:
    """Process a single tool to update missing attributes."""
    serial_number = tool_data.get('tipqa_serial_number', '') or tool_data.get('serial_number', '')
    part_number = tool_data.get('tipqa_part_number', '') or tool_data.get('part_number', '')
    ion_id = tool_data.get('ion_id', '')
    
    if not serial_number or not part_number or not ion_id:
        return False
    
    # Get Ion data for this tool
    ion_data = tool_data.get('ion_data', {})
    if not ion_data:
        return False
    
    # Check current attributes
    current_attrs = ion_data.get('attributes', [])
    current_asset_serial = get_ion_attribute_value(current_attrs, 'Asset Serial Number')
    current_manufacturer = get_ion_attribute_value(current_attrs, 'Manufacturer')
    
    # Get TipQA values
    tipqa_asset_serial = tool_data.get('tipqa_asset_serial_number', '') or tool_data.get('asset_serial_number', '')
    tipqa_manufacturer = tool_data.get('tipqa_manufacturer', '') or tool_data.get('manufacturer', '')
    
    # Clean values
    if pd.isna(tipqa_asset_serial) or not str(tipqa_asset_serial).strip():
        tipqa_asset_serial = ''
    else:
        tipqa_asset_serial = str(tipqa_asset_serial).strip()
    
    if pd.isna(tipqa_manufacturer) or not str(tipqa_manufacturer).strip():
        tipqa_manufacturer = ''
    else:
        tipqa_manufacturer = str(tipqa_manufacturer).strip()
    
    # Determine what needs to be updated
    needs_update = False
    update_attrs = []
    
    # Check Asset Serial Number
    if not current_asset_serial and tipqa_asset_serial:
        needs_update = True
        update_attrs.append({
            'key': 'Asset Serial Number',
            'value': tipqa_asset_serial
        })
        with stats_lock:
            stats['missing_asset_serial'] = stats.get('missing_asset_serial', 0) + 1
    
    # Check Manufacturer
    if not current_manufacturer and tipqa_manufacturer:
        needs_update = True
        update_attrs.append({
            'key': 'Manufacturer',
            'value': tipqa_manufacturer
        })
        with stats_lock:
            stats['missing_manufacturer'] = stats.get('missing_manufacturer', 0) + 1
    
    if not needs_update:
        return True  # Nothing to update
    
    # Get inventory etag
    inventory_etag = tool_data.get('ion__etag', '') or ion_data.get('_etag', '')
    if not inventory_etag:
        log_and_print(f"Warning: No etag found for {serial_number}, skipping", 'warning')
        return False
    
    # Update attributes
    success = update_inventory_attributes(token, config, ion_id, inventory_etag, update_attrs, environment)
    
    if success:
        with stats_lock:
            stats['updated'] = stats.get('updated', 0) + 1
        log_and_print(f"Updated {serial_number} ({part_number}): Added {[attr['key'] for attr in update_attrs]}", 'info')
    else:
        with stats_lock:
            stats['errors'] = stats.get('errors', 0) + 1
    
    return success


def update_missing_attributes(environment: str):
    """Main function to update missing attributes."""
    print(f"\n{'='*60}")
    print(f"Updating Missing Attributes for {environment}")
    print(f"{'='*60}\n")
    
    # Load configuration
    config = load_config()
    
    # Validate environment
    if environment not in config.get('environments', {}):
        print(f"Error: Environment '{environment}' not found in config.yaml")
        sys.exit(1)
    
    # Get authentication token
    print(f"Authenticating with {environment}...")
    token = get_token(config, environment)
    print(f"Successfully obtained token for {environment}\n")
    
    # Get TipQA data
    print("Fetching TipQA data...")
    conn = get_tipqa_connection(config)
    tipqa_tools = get_all_tipqa_tools(conn, config)
    conn.close()
    print(f"Found {len(tipqa_tools)} tools in TipQA\n")
    
    # Create master dataframe (this gets both TipQA and Ion data)
    print("Creating master dataframe (fetching Ion data)...")
    master_df, ion_data = create_master_dataframe(token, config, pd.DataFrame(tipqa_tools), environment, dry_run_mode=False)
    print(f"Master dataframe created with {len(master_df)} tools\n")
    
    # Find tools missing attributes
    print("Identifying tools missing attributes...")
    
    missing_asset_serial = []
    missing_manufacturer = []
    
    for idx, row in master_df.iterrows():
        tool_data = row.to_dict()
        
        # Get Ion data
        serial_number = tool_data.get('tipqa_serial_number', '') or tool_data.get('serial_number', '')
        ion_id = tool_data.get('ion_id', '')
        
        if not serial_number or not ion_id or pd.isna(ion_id) or str(ion_id).strip() == '':
            continue  # Tool doesn't exist in Ion
        
        # Get Ion attributes
        ion_attrs_key = f"ion_attributes_Asset Serial Number"
        ion_asset_serial = tool_data.get(ion_attrs_key, '')
        
        ion_manufacturer_key = f"ion_attributes_Manufacturer"
        ion_manufacturer = tool_data.get(ion_manufacturer_key, '')
        
        # Check if missing
        tipqa_asset_serial = tool_data.get('tipqa_asset_serial_number', '')
        tipqa_manufacturer = tool_data.get('tipqa_manufacturer', '')
        
        # Clean TipQA values
        if pd.isna(tipqa_asset_serial) or not str(tipqa_asset_serial).strip():
            tipqa_asset_serial = ''
        else:
            tipqa_asset_serial = str(tipqa_asset_serial).strip()
        
        if pd.isna(tipqa_manufacturer) or not str(tipqa_manufacturer).strip():
            tipqa_manufacturer = ''
        else:
            tipqa_manufacturer = str(tipqa_manufacturer).strip()
        
        # Check if missing and TipQA has value
        if (not ion_asset_serial or str(ion_asset_serial).strip() == '') and tipqa_asset_serial:
            missing_asset_serial.append(tool_data)
        
        if (not ion_manufacturer or str(ion_manufacturer).strip() == '') and tipqa_manufacturer:
            missing_manufacturer.append(tool_data)
    
    print(f"Found {len(missing_asset_serial)} tools missing Asset Serial Number")
    print(f"Found {len(missing_manufacturer)} tools missing Manufacturer\n")
    
    # Combine unique tools that need updates
    tools_to_update = {}
    for tool in missing_asset_serial + missing_manufacturer:
        ion_id = tool.get('ion_id', '')
        if ion_id and pd.notna(ion_id) and str(ion_id).strip():
            tools_to_update[str(ion_id)] = tool
    
    print(f"Total unique tools to update: {len(tools_to_update)}\n")
    
    if not tools_to_update:
        print("No tools need attribute updates!")
        return
    
    # Process tools in parallel
    stats = {
        'updated': 0,
        'errors': 0,
        'missing_asset_serial': 0,
        'missing_manufacturer': 0
    }
    stats_lock = threading.Lock()
    token_lock = threading.Lock()
    current_token = token
    
    def process_tool_wrapper(tool_tuple):
        """Wrapper for parallel processing."""
        nonlocal current_token
        ion_id, tool_data = tool_tuple
        
        # Get current token (thread-safe)
        with token_lock:
            current_tok = current_token
        
        try:
            # Get Ion data from the ion_data dict
            serial_number = tool_data.get('tipqa_serial_number', '') or tool_data.get('serial_number', '')
            part_number = tool_data.get('tipqa_part_number', '') or tool_data.get('part_number', '')
            
            ion_tool_data = None
            if serial_number in ion_data.get('by_serial', {}):
                for inv in ion_data['by_serial'][serial_number]:
                    if inv.get('part', {}).get('partNumber', '') == part_number:
                        ion_tool_data = inv
                        break
            
            if not ion_tool_data:
                return False
            
            # Build tool_data dict with Ion data
            full_tool_data = tool_data.copy()
            full_tool_data['ion_data'] = ion_tool_data
            
            success = process_tool_for_missing_attributes(
                full_tool_data, current_tok, config, environment, stats, stats_lock
            )
            
            return success
        except AuthenticationError:
            # Token expired, refresh and retry once
            try:
                new_token = get_token(config, environment)
                with token_lock:
                    current_token = new_token
                
                # Rebuild tool_data with Ion data
                serial_number = tool_data.get('tipqa_serial_number', '') or tool_data.get('serial_number', '')
                part_number = tool_data.get('tipqa_part_number', '') or tool_data.get('part_number', '')
                
                ion_tool_data = None
                if serial_number in ion_data.get('by_serial', {}):
                    for inv in ion_data['by_serial'][serial_number]:
                        if inv.get('part', {}).get('partNumber', '') == part_number:
                            ion_tool_data = inv
                            break
                
                if not ion_tool_data:
                    return False
                
                full_tool_data = tool_data.copy()
                full_tool_data['ion_data'] = ion_tool_data
                
                return process_tool_for_missing_attributes(
                    full_tool_data, new_token, config, environment, stats, stats_lock
                )
            except Exception as e:
                with stats_lock:
                    stats['errors'] = stats.get('errors', 0) + 1
                log_and_print(f"Error processing tool {ion_id} after token refresh: {e}", 'error')
                return False
        except Exception as e:
            with stats_lock:
                stats['errors'] = stats.get('errors', 0) + 1
            log_and_print(f"Error processing tool {ion_id}: {e}", 'error')
            return False
    
    # Process in batches
    tools_list = list(tools_to_update.items())
    batch_size = 200
    max_workers = 15
    
    print(f"Processing {len(tools_list)} tools in parallel (batch size: {batch_size}, workers: {max_workers})...\n")
    
    start_time = time.time()
    
    for batch_start in range(0, len(tools_list), batch_size):
        batch_end = min(batch_start + batch_size, len(tools_list))
        batch = tools_list[batch_start:batch_end]
        
        # Refresh token before each batch
        if batch_start > 0:
            print(f"Progress: {batch_start}/{len(tools_list)} tools processed. Refreshing token...")
            try:
                new_token = get_token(config, environment)
                with token_lock:
                    current_token = new_token
                print("Token refreshed successfully\n")
            except Exception as e:
                print(f"Failed to refresh token: {e}\n")
        
        # Process batch
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_tool_wrapper, tool_tuple): tool_tuple for tool_tuple in batch}
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    with stats_lock:
                        stats['errors'] = stats.get('errors', 0) + 1
                    print(f"Error in parallel processing: {e}")
        
        print(f"Completed batch: {batch_end}/{len(tools_list)} tools processed\n")
    
    duration = time.time() - start_time
    
    # Print summary
    print(f"\n{'='*60}")
    print("UPDATE SUMMARY")
    print(f"{'='*60}")
    print(f"Tools missing Asset Serial Number: {stats['missing_asset_serial']}")
    print(f"Tools missing Manufacturer: {stats['missing_manufacturer']}")
    print(f"Tools updated successfully: {stats['updated']}")
    print(f"Errors: {stats['errors']}")
    print(f"Total duration: {duration:.2f} seconds")
    print(f"{'='*60}\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Update missing attributes in Ion')
    parser.add_argument('--environment', required=True, help='Environment (e.g., v2_production)')
    
    args = parser.parse_args()
    
    # Load environment variables
    load_dotenv()
    
    update_missing_attributes(args.environment)


if __name__ == '__main__':
    main()

