#!/usr/bin/env python3
"""
DRY RUN ANALYSIS SCRIPT
=======================

This script analyzes all tools from TipQA and Ion for the specified environment
and generates a comprehensive CSV report showing what actions
would be taken by the daily sync script.

This is a READ-ONLY analysis script that does not make any changes.

Usage:
    python tests/dry_run_test.py --environment v2_production
"""

import os
import sys
import pandas as pd
import argparse
from datetime import datetime
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utilities.database_utils import get_tipqa_connection, get_all_tipqa_tools, read_sql_query
from utilities.graphql_utils import get_token, organize_ion_data_by_serial
from utilities.shared_sync_utils import (
    log_and_print, load_config, cleanup_previous_test_files, 
    create_master_dataframe, analyze_tool_using_daily_sync_logic, 
    process_orphaned_ion_tools, determine_update_mutation_complexity,
    determine_create_mutation_complexity
)

def generate_analysis_report(analysis_df: pd.DataFrame, timestamp: str, environment: str):
    """Generate MD report from comprehensive analysis."""
    
    # Generate MD report
    md_filename = f"tests/comprehensive_analysis_{timestamp}.md"
    with open(md_filename, 'w') as f:
        f.write(f"# Production Tool Sync Analysis Report\n\n")
        f.write(f"**Analysis Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**Environment:** {environment.upper()}\n")
        f.write(f"**Total Tools Analyzed:** {len(analysis_df)}\n\n")
        
        # Summary statistics
        f.write("## Summary Statistics\n\n")
        f.write("### Actions Required\n")
        action_counts = analysis_df['action_in_ion'].value_counts()
        for action, count in action_counts.items():
            f.write(f"- **{action}:** {count}\n")
        
        f.write("\n### Reasons\n")
        reason_counts = analysis_df['reason'].value_counts()
        for reason, count in reason_counts.items():
            f.write(f"- **{reason}:** {count}\n")
        
        # Detailed breakdown by reason
        f.write("\n## Detailed Analysis by Reason\n\n")
        for reason in sorted(analysis_df['reason'].unique()):
            reason_data = analysis_df[analysis_df['reason'] == reason]
            f.write(f"### {reason} ({len(reason_data)} tools)\n\n")
            
            # Show sample tools for this reason
            sample_tools = reason_data.head(5)
            for _, row in sample_tools.iterrows():
                f.write(f"- **{row['tipqa_serial_number']}** ({row['tipqa_part_number']}) - {row['action_in_ion']}\n")
            
            if len(reason_data) > 5:
                f.write(f"- ... and {len(reason_data) - 5} more\n")
            f.write("\n")
    
    log_and_print(f"Generated analysis report: {md_filename}")

def main():
    """Main test function."""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run dry run analysis (read-only)')
    parser.add_argument('--environment', required=True,
                       help='Environment to use (e.g., v1_production, v2_production, v1_sandbox, v2_sandbox)')
    
    args = parser.parse_args()
    
    # This is always a dry run analysis script
    dry_run = True
    
    log_and_print(f"Starting dry run analysis...")
    log_and_print(f"Environment: {args.environment}")
    
    # Clean up previous test files first
    log_and_print("Cleaning up previous test files...")
    cleanup_previous_test_files()
    
    # Load environment variables first
    load_dotenv()
    
    # Load configuration
    config = load_config()
    log_and_print('Configuration loaded successfully')
    
    # Connect to database
    log_and_print("Connecting to TipQA (via Databricks)...")
    try:
        conn = get_tipqa_connection(config)
        log_and_print("Connected to Databricks successfully")
    except Exception as e:
        log_and_print(f"Failed to connect to Databricks: {str(e)}", 'error')
        sys.exit(1)
    
    # Fetch tools from TipQA
    log_and_print("1) Starting to obtain all TipQA tools...")
    log_and_print("This may take a few minutes for large datasets...")
    try:
        tipqa_tools_df = get_all_tipqa_tools(conn, config)
        log_and_print(f"4) Completed all TipQA tools - Retrieved {len(tipqa_tools_df)} tools")
    except Exception as e:
        log_and_print(f"Failed to fetch TipQA data: {str(e)}", 'error')
        conn.close()
        sys.exit(1)
    finally:
        conn.close()
        log_and_print("Database connection closed")
    
    # Filter out fake/test serial numbers (starting with 0000)
    log_and_print("Filtering out fake/test serial numbers...")
    original_count = len(tipqa_tools_df)
    tipqa_tools_df = tipqa_tools_df[~tipqa_tools_df['serial_number'].str.startswith('0000', na=False)]
    filtered_count = len(tipqa_tools_df)
    if original_count > filtered_count:
        log_and_print(f"Filtered out {original_count - filtered_count} fake/test serial numbers (starting with 0000)")
    else:
        log_and_print("No fake/test serial numbers found")
    
    log_and_print(f"Final TipQA dataset: {filtered_count} tools")
    
    # Get Ion API token
    log_and_print("Getting Ion API token...")
    log_and_print(f"Using environment: {args.environment}")
    try:
        token = get_token(config, environment=args.environment)
        if not token:
            log_and_print("Failed to get authentication token", 'error')
            sys.exit(1)
        log_and_print("Ion API token obtained successfully")
    except Exception as e:
        log_and_print(f"Error getting authentication token: {str(e)}", 'error')
        sys.exit(1)
    
    # Create master dataframe using the ULTRA-OPTIMIZED centralized implementation
    log_and_print("Creating master dataframe using ULTRA-OPTIMIZED centralized implementation...")
    log_and_print("This uses the proper 5-step flow with caching, parallel processing, and memory optimization!")
    try:
        master_df = create_master_dataframe(token, config, tipqa_tools_df, environment=args.environment, dry_run_mode=True)
        log_and_print(f"ULTRA-OPTIMIZED master dataframe created successfully with {len(master_df)} records")
    except Exception as e:
        log_and_print(f"Error creating master dataframe: {str(e)}", 'error')
        import traceback
        log_and_print(f"Traceback: {traceback.format_exc()}", 'error')
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
    
    # CRITICAL: Run analysis logic to determine actions and reasons
    log_and_print("Running comprehensive analysis logic...")
    log_and_print("This determines what actions would be taken for each tool...")
    
    # Initialize analysis columns
    master_df['action_in_ion'] = ''
    master_df['reason'] = ''
    master_df['mutation_complexity'] = ''
    
    # Run analysis for each tool
    analysis_stats = {
        'total_tools': len(master_df),
        'skipped': 0,
        'created': 0,
        'updated': 0,
        'marked_unavailable': 0,
        'marked_available': 0,
        'update_then_mark_unavailable': 0
    }
    
    log_and_print(f"Analyzing {len(master_df)} tools...")
    
    # CRITICAL DEBUG: Check if JT00004887 is in master dataframe BEFORE analysis loop
    jt_rows_before = master_df[master_df['tipqa_serial_number'] == 'JT00004887']
    if len(jt_rows_before) == 0:
        log_and_print(f"ERROR: JT00004887 NOT FOUND in master dataframe BEFORE analysis! Total rows: {len(master_df)}", 'error')
        # Try alternative column names
        for col in master_df.columns:
            if 'serial' in col.lower():
                matches = master_df[master_df[col].astype(str).str.contains('JT00004887', case=False, na=False)]
                if len(matches) > 0:
                    log_and_print(f"DEBUG: Found JT00004887 in column {col}", 'info')
    else:
        log_and_print(f"DEBUG: JT00004887 found in master dataframe BEFORE analysis: {len(jt_rows_before)} row(s)", 'info')
        for idx, jt_row in jt_rows_before.iterrows():
            log_and_print(f"DEBUG: JT00004887 BEFORE analysis - tipqa_maintenance={jt_row.get('tipqa_maintenance_status', 'N/A')}, tipqa_location={jt_row.get('tipqa_location', 'N/A')}, ion_id={jt_row.get('ion_id', 'N/A')}, ion_status={jt_row.get('ion_status', 'N/A')}, ion_unavailable={jt_row.get('ion_unavailable', 'N/A')}", 'info')
    
    for index, row in master_df.iterrows():
        if index % 1000 == 0:
            log_and_print(f"Analyzed {index}/{len(master_df)} tools...")
        
        # Convert row to dict for analysis
        tool_data = row.to_dict()
        
        # CRITICAL DEBUG: Log JT00004887 when we encounter it in the loop
        serial_number = tool_data.get('tipqa_serial_number', '')
        if serial_number == 'JT00004887':
            log_and_print(f"DEBUG: JT00004887 found in analysis loop at index {index}", 'info')
            log_and_print(f"DEBUG: JT00004887 tool_data keys: {list(tool_data.keys())}", 'info')
        
        # Get Ion data for this tool
        ion_data = {}
        if tool_data.get('ion_id'):
            ion_data = {
                'id': tool_data.get('ion_id'),
                '_etag': tool_data.get('ion_etag'),
                'serialNumber': tool_data.get('ion_serial_number'),
                'status': tool_data.get('ion_status'),
                'part': {
                    'id': tool_data.get('ion_part_id'),
                    '_etag': tool_data.get('ion_part__etag'),
                    'partNumber': tool_data.get('ion_part_partNumber'),
                    'partType': tool_data.get('ion_part_partType'),
                    'trackingType': tool_data.get('ion_part_trackingType'),
                    'description': tool_data.get('ion_part_description'),
                    'revision': tool_data.get('ion_part_revision'),
                    'maintenanceIntervalSeconds': tool_data.get('ion_part_maintenanceIntervalSeconds'),
                    'attributes': []
                }
            }
        
        # Run analysis logic
        try:
            result = analyze_tool_using_daily_sync_logic(
                tool_data=tool_data,
                ion_data=ion_data,
                stats=analysis_stats,
                config=config,
                dry_run=True,
                merged_df=master_df
            )
            
            # Update the dataframe with analysis results
            action = result.get('action', 'UNKNOWN')
            reason = result.get('reason', 'unknown_reason')
            master_df.at[index, 'action_in_ion'] = action
            master_df.at[index, 'reason'] = reason
            
            # CRITICAL DEBUG: Log JT00004887 analysis result
            if serial_number == 'JT00004887':
                log_and_print(f"DEBUG: JT00004887 analysis result - action={action}, reason={reason}", 'info')
                if action == 'SKIP':
                    log_and_print(f"ERROR: JT00004887 was assigned SKIP! This will prevent it from being marked available!", 'error')
                elif action == 'MARK_AVAILABLE':
                    log_and_print(f"SUCCESS: JT00004887 was assigned MARK_AVAILABLE!", 'info')
            
            # Determine mutation complexity
            action = result.get('action', '')
            if action == 'UPDATE':
                mutation_complexity = determine_update_mutation_complexity(tool_data)
                master_df.at[index, 'mutation_complexity'] = mutation_complexity
            elif action == 'CREATE':
                mutation_complexity = determine_create_mutation_complexity(tool_data, master_df)
                master_df.at[index, 'mutation_complexity'] = mutation_complexity
            else:
                master_df.at[index, 'mutation_complexity'] = 'N/A'
            
            # Update stats
            if action == 'SKIP':
                analysis_stats['skipped'] += 1
            elif action == 'CREATE':
                analysis_stats['created'] += 1
            elif action == 'UPDATE':
                analysis_stats['updated'] += 1
            elif action == 'MARK_UNAVAILABLE':
                analysis_stats['marked_unavailable'] += 1
            elif action == 'MARK_AVAILABLE':
                analysis_stats['marked_available'] += 1
            elif action == 'UPDATE_THEN_MARK_UNAVAILABLE':
                analysis_stats['update_then_mark_unavailable'] += 1
                
        except Exception as e:
            log_and_print(f"Error analyzing tool {tool_data.get('tipqa_serial_number', 'UNKNOWN')}: {str(e)}", 'error')
            master_df.at[index, 'action_in_ion'] = 'ERROR'
            master_df.at[index, 'reason'] = f'analysis_error: {str(e)}'
            master_df.at[index, 'mutation_complexity'] = 'ERROR'
    
    log_and_print("Analysis completed!")
    log_and_print(f"Analysis Summary:")
    log_and_print(f"  - Total tools: {analysis_stats['total_tools']}")
    log_and_print(f"  - SKIP: {analysis_stats['skipped']}")
    log_and_print(f"  - CREATE: {analysis_stats['created']}")
    log_and_print(f"  - UPDATE: {analysis_stats['updated']}")
    log_and_print(f"  - MARK_UNAVAILABLE: {analysis_stats['marked_unavailable']}")
    log_and_print(f"  - MARK_AVAILABLE: {analysis_stats.get('marked_available', 0)}")
    log_and_print(f"  - UPDATE_THEN_MARK_UNAVAILABLE: {analysis_stats['update_then_mark_unavailable']}")
    
    # CRITICAL DEBUG: Check JT00004887 AFTER analysis
    jt_rows_after = master_df[master_df['tipqa_serial_number'] == 'JT00004887']
    if len(jt_rows_after) > 0:
        for idx, jt_row in jt_rows_after.iterrows():
            action = jt_row.get('action_in_ion', 'N/A')
            reason = jt_row.get('reason', 'N/A')
            log_and_print(f"DEBUG: JT00004887 AFTER analysis - action={action}, reason={reason}", 'info')
            if action == 'SKIP':
                log_and_print(f"ERROR: JT00004887 was assigned SKIP! Reason: {reason}", 'error')
    else:
        log_and_print(f"ERROR: JT00004887 NOT FOUND in master dataframe AFTER analysis!", 'error')
    
    # Use the analyzed master dataframe as our analysis results
    analysis_df = master_df.copy()
    log_and_print(f"Analysis dataframe ready - {len(analysis_df)} records analyzed")
    
    # Select exactly the 36 standard columns in the specified order
    from utilities.shared_sync_utils import select_standard_columns
    analysis_df = select_standard_columns(analysis_df)
    log_and_print(f"Selected 36 standard columns for CSV output")
    
    # Save analysis to CSV with quoting (original format)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    analysis_csv = f"tests/comprehensive_analysis_{timestamp}.csv"
    analysis_df.to_csv(analysis_csv, index=False, quoting=1, escapechar='\\')
    log_and_print(f" Analysis saved to CSV: {analysis_csv}")
    
    # Generate analysis report
    generate_analysis_report(analysis_df, timestamp, args.environment)
    log_and_print(f" Analysis complete! Files saved:")
    log_and_print(f"  - CSV: {analysis_csv}")
    log_and_print(f"  - MD Report: tests/comprehensive_analysis_{timestamp}.md")
    
    log_and_print("Dry run analysis completed successfully!")

if __name__ == "__main__":
    main()
