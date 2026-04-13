#!/usr/bin/env python3
"""
LIVE TEST - ACTUAL TOOL UPDATES
===============================

WARNING: THIS SCRIPT MAKES ACTUAL CHANGES TO THE SPECIFIED ENVIRONMENT

This script will:
1. Use optimized master dataframe creation from master_data_flow.md
2. Select 100 tools per reason category ordered by mutation complexity (0→1→2→3+ mutations)
3. Run the daily sync logic on these tools in LIVE mode (ACTUAL UPDATES)
4. Generate detailed MD report and error CSV/MD with mutation complexity breakdown (NO master dataframe CSV)

CRITICAL SAFETY NOTES:
- This script makes ACTUAL CHANGES to the specified environment
- Use --small-batch flag for functionality testing (5 tools per reason)
- Use --environment flag to specify which environment (e.g., v2_production)
- Uses same optimized master dataframe creation as dry run for consistency

Usage:
    python tests/live_test.py --environment v2_production
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

from utilities.database_utils import get_tipqa_connection, get_all_tipqa_tools, read_sql_query
from utilities.graphql_utils import get_token
from utilities.shared_sync_utils import (
    log_and_print, load_config, cleanup_previous_test_files,
    analyze_tool_using_daily_sync_logic, determine_update_mutation_complexity,
    determine_create_mutation_complexity
)
from utilities.tool_processing_utils import (
    create_tool, update_tool, update_then_mark_unavailable,
    mark_tool_unavailable, mark_tool_available, convert_part_to_tool
)

def get_mutation_complexity(reason: str) -> int:
    """Get mutation complexity for a given reason.
    OPTIMIZATION: UPDATE_THEN_MARK_UNAVAILABLE reasons can be 1 or 2 mutations depending on part updates.
    """
    
    # Zero mutations (SKIP operations)
    zero_mutation_reasons = {
        'already_up_to_date', 'inactive_in_tipqa_no_ion', 'inactive_tipqa_unavailable_ion',
        'missing_part_number', 'protected_part_number', 'tipqa_revision_problem'
    }
    
    # Single mutations
    single_mutation_reasons = {
        'inactive_in_tipqa', 'update_inventory', 'create_new_inventory'
    }
    
    # Two mutations
    two_mutation_reasons = {
        'lost_tool_create', 'offsite_tool_create', 'quality_hold_create',
        'lost_location', 'offsite_in_tipqa', 'quality_hold_in_tipqa',
        'update_inventory_and_part', 'new_tool_create'
    }
    
    # Multiple mutations (3+)
    multiple_mutation_reasons = {
        'serial_match_update_part', 'part_to_tool_conversion'
    }
    
    if reason in zero_mutation_reasons:
        return 0
    elif reason in single_mutation_reasons:
        return 1
    elif reason in two_mutation_reasons:
        return 2
    elif reason in multiple_mutation_reasons:
        return 3
    else:
        return 99  # Unknown complexity - process last

def select_tools_for_testing(analysis_df: pd.DataFrame, tools_per_reason: int = 100, small_batch: bool = False) -> pd.DataFrame:
    """Select tools for testing - 100 tools per reason category, ordered by mutation complexity."""
    
    if small_batch:
        tools_per_reason = 5  # Small batch for functionality testing
        log_and_print(f"SMALL BATCH MODE: Selecting {tools_per_reason} tools per reason category for functionality testing...")
    else:
        log_and_print(f"Selecting {tools_per_reason} tools per reason category for testing...")
    log_and_print("OPTIMIZATION: Processing tools by mutation complexity for maximum efficiency!")
    
    # Get unique reasons and their counts
    reason_counts = analysis_df['reason'].value_counts()
    log_and_print(f"Found {len(reason_counts)} unique reason categories:")
    
    # Add mutation complexity to each reason
    reason_complexity = {}
    for reason, count in reason_counts.items():
        complexity = get_mutation_complexity(reason)
        reason_complexity[reason] = complexity
        log_and_print(f"  {reason}: {count} tools available (Complexity: {complexity} mutations)")
    
    # Sort reasons by mutation complexity (0 → 1 → 2 → 3+)
    sorted_reasons = sorted(reason_counts.index, key=lambda r: reason_complexity.get(r, 99))
    
    log_and_print("\nProcessing order by mutation complexity:")
    complexity_groups = {0: [], 1: [], 2: [], 3: []}
    for reason in sorted_reasons:
        complexity = reason_complexity.get(reason, 99)
        if complexity <= 3:
            complexity_groups[complexity].append(reason)
    
    for complexity in [0, 1, 2, 3]:
        if complexity_groups[complexity]:
            log_and_print(f"  {complexity} mutations: {', '.join(complexity_groups[complexity])}")
    
    selected_tools = []
    
    for reason in sorted_reasons:
        # Skip header row and invalid reasons
        if reason in ['reason', '29371.0', '- Desk top scale', '- 0 TO 10 LBS']:
            continue
            
        reason_tools = analysis_df[analysis_df['reason'] == reason]
        available_count = len(reason_tools)
        
        # Select up to tools_per_reason tools for this reason
        select_count = min(tools_per_reason, available_count)
        selected_reason_tools = reason_tools.head(select_count)
        
        selected_tools.append(selected_reason_tools)
        complexity = reason_complexity.get(reason, 99)
        log_and_print(f"  Selected {select_count} tools for reason '{reason}' ({complexity} mutations)")
    
    if selected_tools:
        test_df = pd.concat(selected_tools, ignore_index=True)
        log_and_print(f"Total tools selected for testing: {len(test_df)}")
        return test_df
    else:
        log_and_print("No valid tools found for testing", 'warning')
        return pd.DataFrame()

def run_sync_test_on_selected_tools(selected_tools: pd.DataFrame, config: dict, 
                                  environment: str, master_df: pd.DataFrame = None) -> dict:
    """Run the sync test on selected tools."""
    
    log_and_print(f"Running sync test on {len(selected_tools)} tools in LIVE mode")
    
    # Get Ion API token
    token = get_token(config, environment=environment)
    
    # Get lost location ID
    from utilities.graphql_utils import get_lost_location_id
    lost_location_id = get_lost_location_id(token, config, environment)
    
    # Initialize stats
    stats = {}
    test_results = []
    
    # Process each tool
    for idx, tool_row in selected_tools.iterrows():
        # Refresh token every 50 tools to prevent expiration during long runs
        if idx % 50 == 0 and idx > 0:
            log_and_print(f"Refreshing authentication token at tool {idx+1}/{len(selected_tools)}", 'info')
            try:
                token = get_token(config, environment=environment)
                log_and_print("Token refreshed successfully", 'info')
            except Exception as e:
                log_and_print(f"Failed to refresh token: {e}", 'error')
                # Continue with existing token - it might still work
        
        tool_data = tool_row.to_dict()
        serial_number = tool_data.get('tipqa_serial_number', '')
        
        # Add serial_number to tool_data for processing functions
        tool_data['serial_number'] = serial_number
        
        # Map TipQA column names to expected processing function names
        tool_data['part_number'] = tool_data.get('tipqa_part_number', '')
        tool_data['revision'] = tool_data.get('tipqa_revision', '')
        tool_data['description'] = tool_data.get('tipqa_description', '')
        
        log_and_print(f"Processing tool {idx+1}/{len(selected_tools)}: {serial_number}")
        
        try:
            # Analyze the tool using the master dataframe data
            analysis_result = analyze_tool_using_daily_sync_logic(tool_data, {}, stats, config, dry_run=False)
            
            # Handle case where analysis_result might be a string or None
            if isinstance(analysis_result, dict):
                action = analysis_result.get('action', 'UNKNOWN')
                reason = analysis_result.get('reason', 'UNKNOWN')
                match_info = analysis_result.get('match', {})
            else:
                action = 'UNKNOWN'
                reason = 'UNKNOWN'
                match_info = {}
            
            # Execute the action (LIVE mode - actual changes)
            success = False
            error_message = None
            
            try:
                if action == 'SKIP':
                    success = True  # SKIP actions are considered successful - no API calls
                elif action == 'CREATE':
                    try:
                        success = create_tool(token, config, tool_data, environment, dry_run=False, merged_df=master_df)
                        if not success:
                            error_message = f"Failed to create tool {serial_number} - check logs for detailed error"
                    except Exception as create_error:
                        success = False
                        error_message = f"Exception creating tool {serial_number}: {str(create_error)}"
                elif action == 'UPDATE':
                    try:
                        # Wrap match_info in the expected format
                        wrapped_match_info = {'match': match_info} if match_info else {}
                        success = update_tool(token, config, tool_data, wrapped_match_info, environment, dry_run=False)
                        if not success:
                            error_message = f"Failed to update tool {serial_number} - check logs for detailed error"
                    except Exception as update_error:
                        success = False
                        error_message = f"Exception updating tool {serial_number}: {str(update_error)}"
                elif action == 'CONVERT_PART_TO_TOOL':
                    try:
                        # Wrap match_info in the expected format
                        wrapped_match_info = {'match': match_info} if match_info else {}
                        success = convert_part_to_tool(token, config, tool_data, wrapped_match_info, environment, dry_run=False)
                        if not success:
                            error_message = f"Failed to convert part to tool {serial_number} - check logs for detailed error"
                    except Exception as convert_error:
                        success = False
                        error_message = f"Exception converting part to tool {serial_number}: {str(convert_error)}"
                elif action == 'UPDATE_THEN_MARK_UNAVAILABLE':
                    try:
                        # Wrap match_info in the expected format
                        wrapped_match_info = {'match': match_info} if match_info else {}
                        success = update_then_mark_unavailable(token, config, tool_data, wrapped_match_info, lost_location_id, environment, dry_run=False)
                        if not success:
                            error_message = f"Failed to update then mark unavailable tool {serial_number} - check logs for detailed error"
                    except Exception as update_unavailable_error:
                        success = False
                        error_message = f"Exception updating then marking unavailable tool {serial_number}: {str(update_unavailable_error)}"
                elif action == 'MARK_UNAVAILABLE':
                    try:
                        wrapped_match_info = {'match': match_info} if match_info else {}
                        success = mark_tool_unavailable(token, config, tool_data, wrapped_match_info, lost_location_id, environment, dry_run=False)
                        if not success:
                            error_message = f"Failed to mark tool unavailable {serial_number} - check logs for detailed error"
                    except Exception as mark_unavailable_error:
                        success = False
                        error_message = f"Exception marking tool unavailable {serial_number}: {str(mark_unavailable_error)}"
                elif action == 'MARK_AVAILABLE':
                    try:
                        wrapped_match_info = {'match': match_info} if match_info else {}
                        success = mark_tool_available(token, config, tool_data, wrapped_match_info, environment, dry_run=False)
                        if not success:
                            error_message = f"Failed to mark tool available {serial_number} - check logs for detailed error"
                    except Exception as mark_available_error:
                        success = False
                        error_message = f"Exception marking tool available {serial_number}: {str(mark_available_error)}"
                else:
                    success = True  # Unknown actions are considered successful
            except Exception as e:
                error_message = f"General exception processing tool {serial_number}: {str(e)}"
                success = False
            
            # Record result
            # Extract brief error message (up to 5 words)
            result_reason = None
            if error_message:
                words = error_message.split()[:5]
                result_reason = ' '.join(words)
            
            result = {
                'action': action,
                'reason': reason,
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
                'result': 'SUCCESS' if success else 'FAILURE',
                'result_reason': result_reason if result_reason else '',
                'error_message': error_message,
                'timestamp': datetime.now().isoformat()
            }
            test_results.append(result)
            
        except Exception as e:
            log_and_print(f"Exception processing tool {serial_number}: {e}", 'error')
            error_str = str(e)
            words = error_str.split()[:5]
            result_reason = ' '.join(words)
            
            result = {
                'action': 'ERROR',
                'reason': 'exception',
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

def generate_reports(test_results: list, stats: dict, timestamp: str, environment: str):
    """Generate CSV, MD, and error reports with mutation complexity breakdown."""
    
    # Create results DataFrame
    results_df = pd.DataFrame(test_results)
    
    if results_df.empty or 'reason' not in results_df.columns:
        log_and_print("No results to report.")
        return
    
    # Add mutation complexity column
    results_df['mutation_complexity'] = results_df['reason'].apply(get_mutation_complexity)
    
    # Reorder columns: action, reason, then TipQA fields, then result, result_reason, timestamp, mutation_complexity at end
    tipqa_cols = [col for col in results_df.columns if col.startswith('tipqa_')]
    ion_cols = [col for col in results_df.columns if col.startswith('ion_')]
    other_cols = ['serial_number', 'part_number', 'revision', 'description', 'location', 
                  'maintenance_status', 'revision_status', 'model_number', 'manufacturer', 
                  'condition', 'status', 'date_added', 'last_updated', 'notes']
    extra_cols = [col for col in results_df.columns if col not in ['action', 'reason', 'result', 'result_reason', 
                  'error_message', 'timestamp', 'mutation_complexity'] 
                  and not col.startswith('tipqa_') and not col.startswith('ion_') and col not in other_cols]
    
    # New column order: action, reason, TipQA fields, Ion fields, result, result_reason, error_message, timestamp, then mutation_complexity last
    column_order = ['action', 'reason'] + tipqa_cols + ion_cols + other_cols + ['result', 'result_reason', 
                     'error_message', 'timestamp']
    if 'mutation_complexity' in results_df.columns:
        column_order.append('mutation_complexity')
    
    # Reorder the dataframe (only include columns that actually exist)
    column_order = [c for c in column_order if c in results_df.columns]
    results_df = results_df[column_order]
    
    # Generate CSV report (no quoting for standard CSV)
    csv_filename = f"tests/production_test_results_{timestamp}.csv"
    results_df.to_csv(csv_filename, index=False)
    log_and_print(f"Generated CSV report: {csv_filename}")
    
    # Generate MD report
    md_filename = f"tests/production_test_report_{timestamp}.md"
    with open(md_filename, 'w') as f:
        f.write(f"# Production Tool Sync Test Report\n\n")
        f.write(f"**Test Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**Environment:** {environment.upper()}\n")
        f.write(f"**Total Tools Tested:** {len(test_results)}\n\n")
        
        # Summary statistics
        f.write("## Summary Statistics\n\n")
        action_counts = results_df['action'].value_counts()
        f.write("### Actions Performed\n")
        for action, count in action_counts.items():
            f.write(f"- **{action}:** {count}\n")
        f.write("\n")
        
        reason_counts = results_df['reason'].value_counts()
        f.write("### Reasons\n")
        for reason, count in reason_counts.items():
            f.write(f"- **{reason}:** {count}\n")
        f.write("\n")
        
        success_count = (results_df['result'] == 'SUCCESS').sum()
        failure_count = (results_df['result'] == 'FAILURE').sum()
        f.write(f"### Success Rate\n")
        f.write(f"- **Successful:** {success_count}\n")
        f.write(f"- **Failed:** {failure_count}\n")
        f.write(f"- **Success Rate:** {(success_count/len(results_df)*100):.1f}%\n\n")
        
        # Mutation complexity breakdown
        f.write("## Mutation Complexity Analysis\n\n")
        complexity_counts = results_df['mutation_complexity'].value_counts().sort_index()
        f.write("### Tools by Mutation Complexity\n")
        for complexity, count in complexity_counts.items():
            if complexity == 0:
                f.write(f"- **Zero Mutations (SKIP):** {count} tools\n")
            elif complexity == 1:
                f.write(f"- **Single Mutation:** {count} tools\n")
            elif complexity == 2:
                f.write(f"- **Two Mutations:** {count} tools\n")
            elif complexity == 3:
                f.write(f"- **Multiple Mutations (3+):** {count} tools\n")
            else:
                f.write(f"- **Unknown Complexity ({complexity}):** {count} tools\n")
        f.write("\n")
        
        # Success rate by mutation complexity
        f.write("### Success Rate by Mutation Complexity\n")
        for complexity in sorted(results_df['mutation_complexity'].unique()):
            complexity_df = results_df[results_df['mutation_complexity'] == complexity]
            complexity_success = (complexity_df['result'] == 'SUCCESS').sum()
            complexity_total = len(complexity_df)
            success_rate = (complexity_success/complexity_total*100) if complexity_total > 0 else 0
            
            if complexity == 0:
                f.write(f"- **Zero Mutations (SKIP):** {complexity_success}/{complexity_total} ({success_rate:.1f}%)\n")
            elif complexity == 1:
                f.write(f"- **Single Mutation:** {complexity_success}/{complexity_total} ({success_rate:.1f}%)\n")
            elif complexity == 2:
                f.write(f"- **Two Mutations:** {complexity_success}/{complexity_total} ({success_rate:.1f}%)\n")
            elif complexity == 3:
                f.write(f"- **Multiple Mutations (3+):** {complexity_success}/{complexity_total} ({success_rate:.1f}%)\n")
            else:
                f.write(f"- **Unknown Complexity ({complexity}):** {complexity_success}/{complexity_total} ({success_rate:.1f}%)\n")
        f.write("\n")
        
        # Detailed results by mutation complexity
        f.write("## Detailed Results by Mutation Complexity\n\n")
        for complexity in sorted(results_df['mutation_complexity'].unique()):
            complexity_df = results_df[results_df['mutation_complexity'] == complexity]
            
            if complexity == 0:
                f.write(f"### Zero Mutations (SKIP) - {len(complexity_df)} tools\n\n")
            elif complexity == 1:
                f.write(f"### Single Mutation - {len(complexity_df)} tools\n\n")
            elif complexity == 2:
                f.write(f"### Two Mutations - {len(complexity_df)} tools\n\n")
            elif complexity == 3:
                f.write(f"### Multiple Mutations (3+) - {len(complexity_df)} tools\n\n")
            else:
                f.write(f"### Unknown Complexity ({complexity}) - {len(complexity_df)} tools\n\n")
            
            # Group by reason within complexity
            for reason in complexity_df['reason'].unique():
                reason_tools = complexity_df[complexity_df['reason'] == reason]
                f.write(f"#### {reason} ({len(reason_tools)} tools)\n\n")
                
                for _, tool in reason_tools.iterrows():
                    status = "SUCCESS" if tool['result'] == 'SUCCESS' else "FAILED"
                    f.write(f"- **{tool['serial_number']}** ({tool['action']}) - {status}\n")
                    if tool['result'] == 'FAILURE' and tool['error_message']:
                        f.write(f"  - Error: {tool['error_message']}\n")
                f.write("\n")
    
    log_and_print(f"Generated MD report: {md_filename}")
    
    # Generate error report
    error_results = results_df[results_df['result'] == 'FAILURE']
    if len(error_results) > 0:
        error_filename = f"tests/production_test_errors_{timestamp}.md"
        with open(error_filename, 'w') as f:
            f.write(f"# Production Tool Sync Test - Error Report\n\n")
            f.write(f"**Test Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Environment:** {environment.upper()}\n")
            f.write(f"**Total Errors:** {len(error_results)}\n\n")
            
            # Error breakdown by mutation complexity
            f.write("## Error Breakdown by Mutation Complexity\n\n")
            error_complexity_counts = error_results['mutation_complexity'].value_counts().sort_index()
            for complexity, count in error_complexity_counts.items():
                if complexity == 0:
                    f.write(f"- **Zero Mutations (SKIP):** {count} errors\n")
                elif complexity == 1:
                    f.write(f"- **Single Mutation:** {count} errors\n")
                elif complexity == 2:
                    f.write(f"- **Two Mutations:** {count} errors\n")
                elif complexity == 3:
                    f.write(f"- **Multiple Mutations (3+):** {count} errors\n")
                else:
                    f.write(f"- **Unknown Complexity ({complexity}):** {count} errors\n")
            f.write("\n")
            
            f.write("## Failed Tools\n\n")
            for _, tool in error_results.iterrows():
                f.write(f"### {tool['serial_number']}\n")
                f.write(f"- **Part Number:** {tool['part_number']}\n")
                f.write(f"- **Revision:** {tool['revision']}\n")
                f.write(f"- **Action:** {tool['action']}\n")
                f.write(f"- **Reason:** {tool['reason']}\n")
                f.write(f"- **Mutation Complexity:** {tool['mutation_complexity']}\n")
                f.write(f"- **Error:** {tool['error_message']}\n")
                f.write(f"- **Timestamp:** {tool['timestamp']}\n\n")
        
        log_and_print(f"Generated error report: {error_filename}")
    else:
        log_and_print("No errors to report!")

def main():
    """Main test function."""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run LIVE test (makes actual changes)')
    parser.add_argument('--small-batch', action='store_true', 
                       help='Run in small batch mode (5 tools per reason) for functionality testing')
    parser.add_argument('--environment', required=True,
                       help='Environment to use (e.g., v1_production, v2_production, v1_sandbox, v2_sandbox)')
    
    args = parser.parse_args()
    
    # Determine small batch mode
    small_batch = args.small_batch
    
    # Safety check for live mode
    log_and_print("RUNNING IN LIVE MODE - ACTUAL CHANGES WILL BE MADE!", 'warning')
    log_and_print(f"This will update tools in {args.environment.upper()}!", 'warning')
    
    if small_batch:
        log_and_print("SMALL BATCH MODE: Testing functionality with 5 tools per reason category", 'info')
    
    log_and_print(f"Starting LIVE test...")
    log_and_print(f"Environment: {args.environment}")
    
    # Clean up previous test files first
    log_and_print("Cleaning up previous test files...")
    cleanup_previous_test_files()
    
    # Load environment variables first
    load_dotenv()
    
    # Load configuration
    config = load_config()
    log_and_print('Configuration loaded successfully')
    
    # Connect to TipQA (via Databricks)
    conn = get_tipqa_connection(config)
    
    # Fetch tools from TipQA
    log_and_print("Fetching tools from TipQA...")
    tipqa_tools_df = get_all_tipqa_tools(conn, config)
    log_and_print(f"Fetched {len(tipqa_tools_df)} tools from TipQA")
    
    # Get Ion API token
    token = get_token(config, environment=args.environment)
    
    # Create master dataframe using the CORRECT implementation (not the old optimized one)
    log_and_print("Creating master dataframe using CORRECT implementation...")
    log_and_print("This uses the proper 5-step flow that only includes necessary fields!")
    try:
        from utilities.shared_sync_utils import create_master_dataframe
        master_df = create_master_dataframe(token, config, tipqa_tools_df, environment=args.environment, dry_run_mode=True)
        log_and_print(f"Correct master dataframe created successfully with {len(master_df)} records")
    except Exception as e:
        log_and_print(f"Error creating correct master dataframe: {str(e)}", 'error')
        log_and_print("Falling back to old optimized approach...", 'warning')
        try:
            master_df = create_ultra_optimized_master_dataframe(token, config, tipqa_tools_df, environment=args.environment, dry_run_mode=True)
            log_and_print(f"Fallback master dataframe created successfully with {len(master_df)} records")
        except Exception as e2:
            log_and_print(f"Error creating fallback master dataframe: {traceback.format_exc()}", 'error')
            sys.exit(1)
    
    # Use the master dataframe as our analysis data (NO CSV creation for live test)
    analysis_df = master_df
    log_and_print(f"Using master dataframe with {len(analysis_df)} records for live testing")
    
    # Perform analysis on the master dataframe
    log_and_print("Performing analysis on master dataframe...")
    log_and_print("This will determine actions, reasons, and mutation complexity for each tool")
    
    # Initialize analysis columns
    analysis_df['action_in_ion'] = ''
    analysis_df['reason'] = ''
    analysis_df['mutation_complexity'] = ''
    
    # Perform analysis for each tool
    log_and_print(f"Analyzing {len(analysis_df)} tools...")
    
    # Initialize stats dictionary for analysis
    stats = {
        'skipped': 0,
        'created': 0,
        'updated': 0,
        'marked_unavailable': 0,
        'update_then_mark_unavailable': 0,
        'errors': 0
    }
    
    for idx, row in analysis_df.iterrows():
        try:
            # Convert row to dict for analysis
            tool_data = row.to_dict()
            
            # Create empty ion_data dict (not used in this context)
            ion_data = {}
            
            # Perform analysis using daily sync logic
            analysis_result = analyze_tool_using_daily_sync_logic(tool_data, ion_data, stats, config, dry_run=True)
            action = analysis_result.get('action', 'UNKNOWN')
            reason = analysis_result.get('reason', 'unknown')
            
            # Determine mutation complexity based on action and reason
            if action == 'SKIP':
                mutation_complexity = 'N/A'
            elif action == 'CREATE':
                mutation_complexity = determine_create_mutation_complexity(tool_data)
            elif action in ['UPDATE', 'MARK_UNAVAILABLE', 'UPDATE_THEN_MARK_UNAVAILABLE']:
                mutation_complexity = determine_update_mutation_complexity(tool_data)
            else:
                mutation_complexity = 'unknown'
            
            # Update the dataframe
            analysis_df.at[idx, 'action_in_ion'] = action
            analysis_df.at[idx, 'reason'] = reason
            analysis_df.at[idx, 'mutation_complexity'] = mutation_complexity
            
            # Progress logging
            if (idx + 1) % 1000 == 0:
                log_and_print(f"Analyzed {idx + 1}/{len(analysis_df)} tools...")
                
        except Exception as e:
            log_and_print(f"Error analyzing tool at index {idx}: {str(e)}", 'error')
            analysis_df.at[idx, 'action_in_ion'] = 'ERROR'
            analysis_df.at[idx, 'reason'] = 'analysis_error'
            analysis_df.at[idx, 'mutation_complexity'] = 'unknown'
    
    log_and_print(f"Analysis complete! Processed {len(analysis_df)} tools")
    
    # Show analysis summary
    action_counts = analysis_df['action_in_ion'].value_counts()
    log_and_print("Analysis Summary:")
    for action, count in action_counts.items():
        log_and_print(f"  {action}: {count} tools")
    
    # Select tools for testing - OPTIMIZED BY MUTATION COMPLEXITY
    log_and_print("Selecting tools for testing...")
    log_and_print("OPTIMIZATION: Tools will be processed in order of mutation complexity for maximum efficiency!")
    selected_tools = select_tools_for_testing(analysis_df, tools_per_reason=100, small_batch=small_batch)
    
    # Run sync test
    log_and_print("Running sync test on selected tools...")
    test_results = run_sync_test_on_selected_tools(selected_tools, config, args.environment, master_df)
    
    # Generate reports
    log_and_print("Generating reports...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    generate_reports(test_results['test_results'], test_results['stats'], timestamp, args.environment)
    
    log_and_print("Test completed successfully!")

if __name__ == "__main__":
    main()
