# System Update Execution Flow - Version 3.0

This document defines how scripts execute system updates based on the master dataframe. It covers the actual mutations, API calls, and update processes that perform the actions determined during analysis.

**CRITICAL REFERENCE**: 
- **Master Data Flow**: `queries/master_data_flow.md` - Source of truth for master dataframe creation and analysis logic (determines actions/reasons)
- **Tool Sync Logic**: `utilities/tool_sync_logic_flow.md` - Additional tool synchronization details
- **Analysis Logic**: `utilities/shared_sync_utils.py` - Proven `analyze_tool_using_daily_sync_logic` function

**Scope**: This document focuses on **execution** - how scripts perform the actions determined in the master dataframe. The master dataframe (with actions/reasons) is created using the process documented in `queries/master_data_flow.md`.

## Overview

This document covers how scripts **execute** the actions determined in the master dataframe. The master dataframe (created per `queries/master_data_flow.md`) contains TipQA data, Ion data, and actions/reasons. This document explains how those actions are performed.

### Execution Flow Overview

1. **Master DataFrame Input** - Scripts receive master dataframe with TipQA data, Ion data, and actions/reasons (from `queries/master_data_flow.md`)
2. **Action Processing** - Scripts iterate through tools and execute actions based on `action_in_ion` and `reason`
3. **Mutation Execution** - Appropriate GraphQL mutations are called based on action type
4. **Error Handling** - Failed operations are logged and reported
5. **Results Reporting** - Summary statistics and diagnostic reports are generated

---

# PART I: ACTION EXECUTION

## Purpose
Document how each action type is executed in Ion, including which mutations are called, how data is transformed, and how errors are handled.

## Execution by Action Type

### SKIP Actions
**Execution**: No Ion mutations performed. Tool is skipped entirely.
**Process**: Script moves to next tool without any API calls.

### CREATE Actions
**Execution**: Creates new tools in Ion
**Mutations Used**:
- `create_tool.graphql` - Creates part (if needed)
- `create_tool_inventory.graphql` - Creates inventory item

**Process**:
1. Check if part exists in Ion
2. If part doesn't exist, create part using `create_tool.graphql`
3. Create inventory item using `create_tool_inventory.graphql`
4. Set all TipQA fields (description, location, attributes, etc.)

**Functions**: `create_tool()` in `utilities/tool_processing_utils.py`

### UPDATE Actions
**Execution**: Updates existing tools in Ion
**Mutations Used**:
- `update_tool.graphql` - Updates part-level fields
- `update_inventory_with_attributes.graphql` - Updates inventory-level fields

**Process**:
1. Determine which fields need updating (part-level vs inventory-level)
2. If part-level updates needed: Call `update_tool.graphql` with part etag
3. If inventory-level updates needed: Call `update_inventory_with_attributes.graphql` with inventory etag
4. Handle attribute etags correctly (required for existing attributes)

**Functions**: `update_tool()` in `utilities/tool_processing_utils.py`

**CRITICAL**: Attribute etags must be fetched before updating attributes. See recent fix for `update_then_mark_unavailable` function.

### MARK_UNAVAILABLE Actions
**Execution**: Marks tools as unavailable in Ion
**Mutations Used**:
- `update_inventory_with_attributes.graphql` - Updates inventory status

**Process**:
1. Get fresh inventory etag
2. Update inventory with `unavailable: true`
3. Optionally move to lost location (location ID 10043)

**Functions**: `mark_tool_unavailable()` in `utilities/tool_processing_utils.py`

### UPDATE_THEN_MARK_UNAVAILABLE Actions
**Execution**: Updates tool data then marks as unavailable
**Mutations Used**:
- `update_tool.graphql` + `update_inventory_with_attributes.graphql` (if part updates needed)
- OR `update_inventory_with_attributes.graphql` only (if only inventory updates needed)

**Process**:
1. Determine mutation complexity (part updates needed vs inventory-only)
2. If part updates needed: Update part first, then mark unavailable
3. If inventory-only: Combine update + mark unavailable into single mutation (optimization)
4. **CRITICAL**: Must fetch attribute etags before updating attributes

**Functions**: `update_then_mark_unavailable()` in `utilities/tool_processing_utils.py`

### CONVERT_PART_TO_TOOL Actions
**Execution**: Converts existing Ion parts to tools
**Mutations Used**: Multiple mutations to change part type and update attributes

**Process**:
1. Verify part exists and is PART type
2. Convert part type from PART to TOOL
3. Update all tool-specific attributes
4. Create inventory items if needed

**Functions**: Part conversion logic in `utilities/tool_processing_utils.py`

## Field Update Execution Details

### Part-Level Field Updates
**Fields**: `description`, `revision`, `service_interval_seconds`
**Mutation**: `update_tool.graphql`
**Process**:
- Batch update optimization: Groups tools by part_number+revision
- Uses most common description across all TipQA serials
- Updates each unique part once before processing individual tools
- Only updates if Ion field is empty (for description)

**CRITICAL**: Revision and service_interval_seconds are always compared and updated when they differ.

### Inventory-Level Field Updates
**Fields**: `location`, `last_maintenance_date`, `asset_serial_number`, `manufacturer`
**Mutation**: `update_inventory_with_attributes.graphql`
**Process**:
- Each serial updated individually (never aggregated)
- Always compared and updated if they differ
- Attribute etags must be included for existing attributes

**CRITICAL**: Must fetch current inventory data to get attribute etags before updating.

## Error Handling During Execution

### ETag Refresh
- If mutation fails due to stale etag, automatically refresh and retry
- Uses `post_graphql_with_etag_refresh()` function

### Concurrency Handling
- Retry logic for concurrent update conflicts
- Exponential backoff for retries

### Error Reporting
- Failed operations logged with full context
- Diagnostic reports generated with error details
- Error CSVs include all TipQA and Ion data for failed tools

---

# PART II: TESTING METHODOLOGY

## Purpose
Document the testing methodology for validating that execution works correctly. This section covers how to test the execution process.

### v1_production_dry_run_test.py
- **Purpose**: Comprehensive analysis of all tools (NO CHANGES)
- **Safety**: DRY-RUN mode by default
- **Output**: Complete CSV with all TipQA and Ion tools analyzed
- **Master Data**: Uses optimized master dataframe creation from `master_data_flow.md`
- **Performance**: Ultra-optimized with batched queries, caching, and connection pooling

### v1_production_live_test.py  
- **Purpose**: Live testing with actual changes to V1 Production
- **Safety**: LIVE mode by default - makes actual changes
- **Selection**: 100 tools per reason category (optimized distribution)
- **Master Data**: Uses same optimized master dataframe creation as dry run
- **Output**: MD report + error CSV/MD (NO master dataframe CSV)

### retest_errored_tools.py
- **Purpose**: Retest tools that failed in previous runs
- **Safety**: Can run in either dry-run or live mode

### v1_production_subset_test.py
- **Purpose**: Process a subset of tools from a CSV file with actual updates
- **Safety**: LIVE mode by default - makes actual changes to V1 Production
- **Input**: CSV file with serial numbers (default: `tests/subset_tools.csv`)
- **Output**: 
  - CSV report with TipQA data, Ion data, actions, reasons, success/failure
  - MD report with summary statistics
  - Error report (if any failures)

#### Subset Test Detailed Flow

**Step 1: Load Serial Numbers**
- Reads serial numbers from CSV file
- Validates serial number format and filters out invalid entries
- Creates initial DataFrame with serial numbers

**Step 2: Check TipQA Existence**
- Queries TipQA database using `tipqa_tools_subset.sql` query
- Filters to only tools that exist in TipQA (prevents adding non-existent tools to Ion)
- Creates warning messages for serial numbers not found in TipQA

**Step 3: Get TipQA Data**
- Fetches complete TipQA data for existing tools
- Includes all fields: part_number, revision, description, location, maintenance_status, etc.
- Adds TipQA fields to subset CSV with `tipqa_` prefix (e.g., `tipqa_serial_number`, `tipqa_part_number`)

**Step 4: Get Ion Data**
- Fetches Ion data ONLY for tools in the subset using `get_tool_inventory_by_serial_and_part.graphql`
- Gets all Ion fields: ion_id, ion_etag, ion_status, ion_location_name, etc.
- Includes Ion part data: ion_part_id, ion_part_partNumber, ion_part_description, etc.
- Adds Ion fields to subset CSV with `ion_` prefix (e.g., `ion_id`, `ion_status`)

**Step 5: Merge Data**
- Combines TipQA + Ion data into analysis dataframe using `append_ion_to_tipqa()` function
- Creates unified dataframe with both TipQA and Ion data columns
- Ensures proper field prefixing (tipqa_* and ion_*)

**Step 6: Analyze Tools**
- Runs `analyze_tool_using_daily_sync_logic()` for each tool in the subset
- Determines action and reason for each tool BEFORE executing
- Populates `action_in_ion`, `reason`, and `mutation_complexity` columns
- Uses same analysis logic as dry run and live test

**Step 7: Execute Updates**
- Shows pre-execution analysis summary with breakdown by action and reason
- Filters out tools with SKIP actions (no API calls needed - already synchronized)
- Only executes updates on tools that actually need changes
- Executes CREATE, UPDATE, MARK_UNAVAILABLE, etc. based on pre-analyzed actions
- Tracks success/failure for each update attempt
- Adds SKIP results to final report (marked as successful)

**Step 8: Generate Reports**
- Creates CSV report with ALL TipQA data, ALL Ion data, actions, reasons, success/failure
- Generates MD report with summary statistics
- Creates error report if any failures occur
- Reports include complete data context for each tool

#### CSV Column Order

**For Dry Run and Comprehensive Analysis (36 columns total):**
- **Column 1**: action_in_ion (UPDATE, CREATE, MARK_UNAVAILABLE, etc.)
- **Column 2**: reason (exact_match_update, missing_in_tipqa, etc.)
- **Columns 3-14**: TipQA fields (tipqa_serial_number, tipqa_part_number, tipqa_description, tipqa_revision, tipqa_service_interval_seconds, tipqa_asset_type, tipqa_location, tipqa_last_maintenance_date, tipqa_asset_serial_number, tipqa_manufacturer, tipqa_maintenance_status, tipqa_revision_status)
- **Columns 15-36**: Ion fields (ion_id, ion_serialNumber, ion__etag, ion_status, ion_lastMaintainedDate, ion_location, ion_location_id, ion_location_name, ion_attributes_Asset Serial Number, ion_attributes_Manufacturer, ion_attributes_Etag, ion_part_id, ion_part_partNumber, ion_part_revision, ion_part_partType, ion_part_trackingType, ion_part_description, ion_part_maintenanceIntervalSeconds, ion_part_attributes_Asset Type, ion_part_attributes_Etag, ion_abomInstallations_id, ion_buildrequirement_id)

**Note**: See `queries/master_data_flow.md` for the complete 36-column specification that must be maintained.

**For Live Test Results (38 columns total):**
- **Columns 1-36**: Same as Dry Run (action_in_ion, reason, TipQA fields, Ion fields)
- **Column 37**: `update_status` - Either SUCCESS or FAIL, depending on how the update went
- **Column 38**: `status_reason` - Brief explanation of why the update failed (if applicable)

**For Subset Test CSV (38 columns total):**
- **Columns 1-36**: Same as Dry Run (action_in_ion, reason, TipQA fields, Ion fields)
- **Column 37**: `update_status` - Either SUCCESS or FAIL, depending on how the update went
- **Column 38**: `status_reason` - Brief explanation of why the update failed (if applicable)

**Note**: CSV files are standard format (no quoting around values)

#### Key Features
- **TipQA Existence Check**: Only processes tools that exist in TipQA (prevents adding non-existent tools to Ion)
- **Optimized Ion Queries**: Only fetches Ion data for tools in the subset (not all Ion tools)
- **Serial + Part Matching**: Matches on both serial number AND part number for safety
- **Pre-Analysis**: Determines actions and reasons BEFORE executing updates
- **Standard Analysis Logic**: Uses same `analyze_tool_using_daily_sync_logic` as dry run and live test
- **Live Updates**: Makes actual changes to V1 Production environment
- **Comprehensive Reporting**: CSV includes ALL TipQA data + ALL Ion data + results

#### Usage
```bash
# Use default CSV file
python tests/v1_production_subset_test.py

# Specify custom CSV file
python tests/v1_production_subset_test.py --csv-file path/to/your/tools.csv
```

## Test Scripts

## Testing Process

### Step 1: Dry Run Analysis
```bash
python tests/v1_production_dry_run_test.py
```
- Analyzes ALL tools from TipQA and Ion using optimized master dataframe creation
- Generates comprehensive CSV report for review
- Shows what actions would be taken for each tool
- NO actual changes made
- **Master Data**: Uses `master_data_flow.md` optimized process
- **Performance**: Ultra-optimized with batched queries, caching, and connection pooling

### Step 2: Live Testing (Optimized by Mutation Complexity)
```bash
python tests/v1_production_live_test.py
```
- Uses same optimized master dataframe creation as dry run
- Selects 100 tools per reason category found in dry run
- **OPTIMIZATION**: Processes tools in order of mutation complexity for maximum efficiency:
  1. **Zero Mutations** (SKIP operations) - Process first (no API calls)
  2. **Single Mutations** (Simple updates) - Process second (1 API call each)
  3. **Two Mutations** (Multiple updates) - Process third (2 API calls each)
  4. **Multiple Mutations** (Complex updates) - Process last (3+ API calls each)
- Makes actual changes to V1 Production
- **Output**: MD report + error CSV/MD (NO master dataframe CSV)
- **Creates error document**: `tests/production_test_errors_YYYYMMDD_HHMMSS.md` (if errors occur)
- **Creates error CSV**: `tests/production_test_errors_YYYYMMDD_HHMMSS.csv` (if errors occur) - Contains all TipQA and Ion data for failed tools

### Step 3: Error Analysis
**Manual Process**: Review generated error reports
- **Read error document**: `tests/production_test_errors_YYYYMMDD_HHMMSS.md`
- **Review error CSV**: `tests/production_test_errors_YYYYMMDD_HHMMSS.csv` - Contains complete TipQA and Ion data for failed tools
- Analyze why each tool failed
- Identify patterns in failures
- Document root causes
- Plan fixes for scripts and logic flows
- **Share with TipQA admins**: Error CSV can be sent to TipQA team for data validation

### Step 4: Logic Updates
**Manual Process**: Update system based on error analysis
- Update scripts (`v1_production_live_test.py`, `daily_tool_sync_v1.py`)
- Update logic flow documents (`utilities/tool_sync_logic_flow.md`)
- Update utility functions (`utilities/tool_processing_utils.py`)
- Test fixes in dry-run mode first

### Step 5: Retest Failed Tools
```bash
python tests/retest_errored_tools.py
```
- Retests only the tools that failed in previous runs
- Validates that fixes work for specific error cases
- Generates new error reports if failures persist
- **Creates new error document**: `tests/retest_errors_YYYYMMDD_HHMMSS.md` (if errors persist)
- **Creates new error CSV**: `tests/retest_errors_YYYYMMDD_HHMMSS.csv` (if errors persist) - Contains all TipQA and Ion data for still-failing tools

### Step 6: Iterate Until 100% Success
**Repeat Steps 3-5** until all tools process successfully:
- If errors remain: Go back to Step 3 (Error Analysis)
- If 100% success: Proceed to full production deployment
- **Goal**: Every single tool must process without errors

## Environment Safety

**CRITICAL**: Only use V1 Production environment. V2 environments have incorrect location configurations and will break the system.

## File Cleanup

**CRITICAL**: Before running new tests, clean up old test files to avoid confusion:
```bash
rm tests/*_analysis_*.csv
rm tests/*_report_*.md
rm tests/*_results_*.csv
rm tests/*_errors_*.md
rm tests/*_errors_*.csv
```

**Why cleanup is essential**:
- Prevents confusion about which results are current
- Ensures only the latest error documents are reviewed
- Avoids mixing old error reports with new test runs
- Clear identification of the most recent test session

## Expected Results

- **Dry Run**: Complete analysis of all tools with no changes + CSV for review
- **Live Testing**: 100 tools per reason category tested with actual changes (NO CSV creation)
- **Master Data**: Both scripts use same optimized master dataframe creation from `master_data_flow.md`
- **Error Reports**: Detailed analysis of every failure with root cause identification
- **Error CSVs**: Complete TipQA and Ion data for failed tools (shareable with TipQA admins)
- **Success Rate**: Must achieve 100% - iterate and fix logic until all tools process successfully
- **Coverage**: All action/reason combinations from the main logic flow
- **Iteration**: Continue fixing and retesting until zero errors remain
- **Performance**: Ultra-optimized with batched queries, caching, and connection pooling
- **Holistic Approach**: Same functions used across dry run, live test, and daily sync

## Troubleshooting

Common issues and solutions are documented in the main logic flow document. Check console output for detailed error messages.

---

**Last Updated**: 2026-01-23
**Version**: 3.0
**Status**: Active
**Scope**: This document focuses on execution - how scripts perform the actions determined in the master dataframe. For master dataframe creation and analysis logic, see `queries/master_data_flow.md`.

**Changes**: 
- **Version 3.0**: Restructured to focus on execution details
- **Version 3.0**: Added Part I: Action Execution (how each action type is executed)
- **Version 3.0**: Clarified that master dataframe creation is documented in `queries/master_data_flow.md`
- **Version 2.1**: Added testing methodology documentation
- **Version 2.1**: Updated to reference `master_data_flow.md` as source of truth

