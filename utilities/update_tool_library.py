#!/usr/bin/env python3
"""
Update Tool Library Script for V2 Sandbox

This script updates all tool parts in V2 Sandbox to ensure they have:
1. trackingType = SERIAL
2. partType = TOOL
3. Asset Type attribute set correctly

It reads tool data from TipQA and updates the corresponding parts in Ion.
"""

import sys
import os
import yaml
import re
import argparse
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utilities.graphql_utils import get_token, post_graphql, log_and_print
from utilities.database_utils import get_all_tipqa_tools, get_tipqa_connection
from utilities.logging_config import setup_logging


def read_query(filename: str) -> str:
    """Read a GraphQL query from the queries directory."""
    query_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'queries', filename)
    with open(query_path, 'r') as f:
        return f.read()


def load_config() -> Dict:
    """Load configuration from config.yaml with environment variable substitution."""
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.yaml')
    with open(config_path, 'r') as f:
        content = f.read()
    
    # Replace environment variables
    def replace_env_var(match):
        var_name = match.group(1)
        return os.getenv(var_name, match.group(0))
    
    content = re.sub(r'\$\{([^}]+)\}', replace_env_var, content)
    return yaml.safe_load(content)


def get_existing_tools(token: str, config: Dict, environment: str) -> Dict[str, Dict]:
    """Get all existing tool parts from Ion, keyed by part number only (using approved inventory query)."""
    log_and_print(f"Fetching existing tool parts from {environment.upper()} Ion (via inventory query)...", 'info')
    
    # Use approved query: get_all_tool_inventory
    query = read_query('get_all_tool_inventory.graphql')
    
    try:
        # Fetch all inventory items with pagination
        tools = {}
        after_cursor = None
        page_count = 0
        
        while True:
            page_count += 1
            variables = {
                'first': 1000,
                'after': after_cursor
            }
            
            response = post_graphql(token, config, {'query': query, 'variables': variables}, environment)
            
            if 'errors' in response:
                log_and_print(f"GraphQL errors: {response['errors']}", 'error')
                break
            
            # Extract unique parts from inventory items
            inventory_edges = response.get('data', {}).get('partInventories', {}).get('edges', [])
            for edge in inventory_edges:
                inv_node = edge.get('node', {})
                tool_node = inv_node.get('part', {})
                
                if not tool_node:
                    continue
                
                # Only include tools with partType=TOOL (should already be filtered by query, but double-check)
                if tool_node.get('partType') != 'TOOL':
                    continue
                    
                part_number = tool_node.get('partNumber', '')
                
                # Key by part number only (no serial numbers) - only keep first occurrence
                if part_number and part_number not in tools:
                    tools[part_number] = {
                        'id': tool_node.get('id'),
                        'partNumber': part_number,
                        'revision': tool_node.get('revision', ''),
                        'description': tool_node.get('description', ''),
                        'partType': tool_node.get('partType', ''),
                        'trackingType': tool_node.get('trackingType', ''),
                        'attributes': tool_node.get('attributes', []),
                        '_etag': tool_node.get('_etag', '')
                    }
            
            # Check for pagination
            page_info = response.get('data', {}).get('partInventories', {}).get('pageInfo', {})
            if not page_info.get('hasNextPage', False):
                break
            after_cursor = page_info.get('endCursor')
            
            if page_count % 10 == 0:
                log_and_print(f"Fetched {len(tools)} unique tool parts so far (page {page_count})...", 'info')
        
        log_and_print(f"Found {len(tools)} existing tool parts in Ion", 'info')
        return tools
        
    except Exception as e:
        log_and_print(f"Error fetching existing tools: {str(e)}", 'error')
        return {}


def get_asset_type_from_attributes(attributes: List[Dict]) -> Optional[str]:
    """Extract asset type from attributes list."""
    for attr in attributes:
        if attr.get('key') == 'Asset Type':
            return attr.get('value')
    return None


def needs_update(tool: Dict, tipqa_data: Dict) -> tuple[bool, List[str]]:
    """
    Check if a tool needs updating and return reasons.
    
    Returns:
        tuple: (needs_update: bool, reasons: List[str])
    """
    reasons = []
    
    # Check trackingType
    if tool.get('trackingType') != 'SERIAL':
        reasons.append('trackingType not SERIAL')
    
    # Check asset type
    current_asset_type = get_asset_type_from_attributes(tool.get('attributes', []))
    expected_asset_type = tipqa_data.get('asset_type', '')
    
    if current_asset_type != expected_asset_type:
        reasons.append(f'asset_type mismatch: current="{current_asset_type}", expected="{expected_asset_type}"')
    
    return len(reasons) > 0, reasons


def update_tool_part(token: str, config: Dict, tool: Dict, tipqa_data: Dict, environment: str) -> bool:
    """Update a single tool part in Ion."""
    tool_id = tool['id']
    part_number = tool['partNumber']
    revision = tool['revision']
    
    log_and_print(f"Updating tool part {part_number}-{revision}", 'info')
    
    # Prepare attributes with etags
    attributes = []
    asset_type = tipqa_data.get('asset_type', '')
    if asset_type:
        # Find existing Asset Type attribute to get its etag
        existing_asset_attr = None
        for attr in tool.get('attributes', []):
            if attr.get('key') == 'Asset Type':
                existing_asset_attr = attr
                break
        
        if existing_asset_attr:
            # Update existing attribute
            # Handle both 'Etag' (capital E from GraphQL) and 'etag' (lowercase)
            attr_etag = existing_asset_attr.get('Etag', '') or existing_asset_attr.get('etag', '')
            attributes.append({
                'key': 'Asset Type',
                'value': asset_type,
                'etag': attr_etag
            })
        else:
            # Create new attribute (no etag needed for new attributes)
            attributes.append({
                'key': 'Asset Type',
                'value': asset_type
            })
    
    # Update mutation
    mutation = read_query('update_tool.graphql')
    variables = {
        'input': {
            'id': tool_id,
            'etag': tool['_etag'],
            'trackingType': 'SERIAL',
            'attributes': attributes
        }
    }
    
    try:
        response = post_graphql(token, config, {'query': mutation, 'variables': variables}, environment)
        
        if 'errors' in response:
            log_and_print(f"Error updating tool {part_number}-{revision}: {response['errors']}", 'error')
            return False
        
        log_and_print(f"Successfully updated tool part {part_number}-{revision}", 'info')
        return True
        
    except Exception as e:
        log_and_print(f"Exception updating tool {part_number}-{revision}: {str(e)}", 'error')
        return False


def main():
    """Main function to update tool library."""
    # Load environment variables from .env file
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
    load_dotenv(env_path)
    
    parser = argparse.ArgumentParser(description='Update tool library in Ion')
    parser.add_argument('--environment', '-e', 
                       choices=['v1_production', 'v1_sandbox', 'v2_sandbox'],
                       default='v2_sandbox',
                       help='Ion environment to update (default: v2_sandbox)')
    
    args = parser.parse_args()
    environment = args.environment
    
    setup_logging()
    
    log_and_print(f"Starting Tool Library Update for {environment.upper()}", 'info')
    log_and_print("=" * 60, 'info')
    
    # Load configuration
    config = load_config()
    
    # Get authentication token
    token = get_token(config, environment)
    if not token:
        log_and_print(f"Failed to get authentication token for {environment}", 'error')
        return
    
    # Get TipQA tools data
    log_and_print("Fetching tool data from TipQA...", 'info')
    
    # Get database connection (Databricks)
    conn = get_tipqa_connection(config)
    
    if not conn:
        log_and_print("Failed to connect to Databricks", 'error')
        return
    
    try:
        tipqa_df = get_all_tipqa_tools(conn, config)
    finally:
        conn.close()
    
    if tipqa_df.empty:
        log_and_print("No TipQA tools found", 'error')
        return
    
    log_and_print(f"Found {len(tipqa_df)} tools in TipQA", 'info')
    
    # Create TipQA lookup by part number
    tipqa_lookup = {}
    for _, row in tipqa_df.iterrows():
        part_number = row.get('part_number', '')
        if part_number:
            tipqa_lookup[part_number] = row.to_dict()
    
    log_and_print(f"Created TipQA lookup with {len(tipqa_lookup)} unique part numbers", 'info')
    
    # Get existing tools from Ion
    ion_tools = get_existing_tools(token, config, environment)
    
    if not ion_tools:
        log_and_print("No tools found in Ion", 'error')
        return
    
    log_and_print(f"Found {len(ion_tools)} tools in Ion", 'info')
    
    # Process tools
    updated_count = 0
    skipped_count = 0
    error_count = 0
    
    log_and_print("Processing tools...", 'info')
    
    for part_number, tool in ion_tools.items():
        # Find matching TipQA data by part number
        if part_number not in tipqa_lookup:
            log_and_print(f"Part number {part_number} not found in TipQA, skipping", 'warning')
            skipped_count += 1
            continue
        
        tipqa_data = tipqa_lookup[part_number]
        
        # Check if update is needed
        needs_update_flag, reasons = needs_update(tool, tipqa_data)
        
        if not needs_update_flag:
            log_and_print(f"Tool {part_number} is up to date, skipping", 'info')
            skipped_count += 1
            continue
        
        log_and_print(f"Tool {part_number} needs update: {', '.join(reasons)}", 'info')
        
        # Update the tool
        if update_tool_part(token, config, tool, tipqa_data, environment):
            updated_count += 1
        else:
            error_count += 1
    
    # Summary
    log_and_print("=" * 60, 'info')
    log_and_print("Tool Library Update Summary:", 'info')
    log_and_print(f"  Total Ion tools processed: {len(ion_tools)}", 'info')
    log_and_print(f"  Successfully updated: {updated_count}", 'info')
    log_and_print(f"  Skipped (no changes needed): {skipped_count}", 'info')
    log_and_print(f"  Errors: {error_count}", 'error' if error_count > 0 else 'info')
    
    if error_count == 0:
        log_and_print("Tool library update completed successfully!", 'info')
    else:
        log_and_print(f"Tool library update completed with {error_count} errors", 'warning')


if __name__ == "__main__":
    main()
