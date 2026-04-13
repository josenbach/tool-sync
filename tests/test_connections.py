#!/usr/bin/env python3
"""
Connection Test Script
=====================

This script tests both TipQA database and Ion API connections separately
to identify connection issues.

Usage: python tests/test_connections.py
"""

import os
import sys
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utilities.database_utils import get_tipqa_connection, get_all_tipqa_tools, read_sql_query
from utilities.graphql_utils import get_token, post_graphql, read_query
from utilities.shared_sync_utils import log_and_print, load_config

def test_tipqa_connection():
    """Test TipQA database connection and basic query."""
    print("=" * 50)
    print("TESTING TIPQA CONNECTION")
    print("=" * 50)
    
    try:
        # Load config
        config = load_config()
        print(" Configuration loaded")
        
        # Test connection
        print("Connecting to TipQA (via Databricks)...")
        conn = get_tipqa_connection(config)
        print(" Connected to Databricks")
        
        # Test simple query
        print("Testing simple count query...")
        db_cfg = config['tipqa_databricks']
        count_query = f"SELECT COUNT(*) as tool_count FROM {db_cfg['catalog']}.{db_cfg['schema']}.{db_cfg['table']} WHERE BUSINESS_UNIT = 'JAI'"
        cursor = conn.cursor()
        cursor.execute(count_query)
        tool_count = cursor.fetchone()[0]
        cursor.close()
        print(f" Query successful - Found {tool_count} tools in TipQA")
        
        # Test main query with LIMIT
        print("Testing main Databricks query with LIMIT 10...")
        from utilities.database_utils import get_tipqa_tools_by_serials
        sample_data = get_tipqa_tools_by_serials(conn, ['JT00005389'], config)
        print(f" Sample query successful - Retrieved {len(sample_data)} records")
        
        if len(sample_data) > 0:
            print("Sample data preview:")
            print(sample_data.head(3).to_string())
        
        conn.close()
        print(" Database connection closed")
        return True
        
    except Exception as e:
        print(f" TipQA connection test failed: {str(e)}")
        import traceback
        print(f" Traceback: {traceback.format_exc()}")
        return False

def test_ion_connection():
    """Test Ion API connection and basic query."""
    print("\n" + "=" * 50)
    print("TESTING ION API CONNECTION")
    print("=" * 50)
    
    try:
        # Load config
        config = load_config()
        print(" Configuration loaded")
        
        # Test token generation
        print("Getting Ion API token...")
        token = get_token(config, environment='v1_production')
        if not token:
            print(" Failed to get authentication token")
            return False
        print(" Ion API token obtained")
        
        # Test simple GraphQL query
        print("Testing simple GraphQL query...")
        simple_query = """
        query TestQuery {
          __schema {
            types {
              name
            }
          }
        }
        """
        
        response = post_graphql(token, config, {'query': simple_query}, 'v1_production')
        if 'errors' in response:
            print(f" GraphQL errors: {response['errors']}")
            return False
        print(" Simple GraphQL query successful")
        
        # Test schema query
        print("Testing schema query...")
        schema_query = read_query('get_schema.graphql')
        schema_response = post_graphql(token, config, {'query': schema_query}, 'v1_production')
        if 'errors' in schema_response:
            print(f" Schema query errors: {schema_response['errors']}")
            return False
        print(" Schema query successful")
        
        # Test tool inventory query
        print("Testing tool inventory query...")
        tool_query = read_query('get_all_tool_inventory.graphql')
        tool_variables = {
            "first": 5,  # Just get 5 records for testing
            "after": None
        }
        
        tool_response = post_graphql(token, config, {
            'query': tool_query, 
            'variables': tool_variables
        }, 'v1_production')
        
        if 'errors' in tool_response:
            print(f" Tool inventory query errors: {tool_response['errors']}")
            return False
        
        edges = tool_response.get('data', {}).get('partInventories', {}).get('edges', [])
        print(f" Tool inventory query successful - Retrieved {len(edges)} records")
        
        if len(edges) > 0:
            print("Sample tool data preview:")
            sample_tool = edges[0]['node']
            print(f"  ID: {sample_tool.get('id', 'N/A')}")
            print(f"  Serial: {sample_tool.get('serialNumber', 'N/A')}")
            print(f"  Part Type: {sample_tool.get('part', {}).get('partType', 'N/A')}")
        
        return True
        
    except Exception as e:
        print(f" Ion connection test failed: {str(e)}")
        import traceback
        print(f" Traceback: {traceback.format_exc()}")
        return False

def main():
    """Run all connection tests."""
    print("CONNECTION TEST SCRIPT")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load environment variables
    load_dotenv()
    
    # Test TipQA connection
    tipqa_success = test_tipqa_connection()
    
    # Test Ion connection
    ion_success = test_ion_connection()
    
    # Summary
    print("\n" + "=" * 50)
    print("CONNECTION TEST SUMMARY")
    print("=" * 50)
    print(f"TipQA Connection: {' PASS' if tipqa_success else ' FAIL'}")
    print(f"Ion API Connection: {' PASS' if ion_success else ' FAIL'}")
    
    if tipqa_success and ion_success:
        print("\nAll connections successful! The issue may be elsewhere.")
    else:
        print("\nWARNING: Connection issues detected. Please fix these before running the main script.")
    
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == '__main__':
    main()
