# Tool Synchronization Logic Flow

## Overview
This document outlines the step-by-step logic flow for synchronizing tool data between TipQA and Ion systems. The process uses a "matching-first" approach to identify relationships between tools before applying business rules.

## System Architecture Overview

### TipQA System
- **Architecture**: Only tool part inventory level (serialized tools)
- **Data**: Serial numbers, part numbers, locations, maintenance status, etc.
- **Limitation**: No tool part library functionality - only inventory instances

### Ion System  
- **Architecture**: Two-level system
  - **Tool Part Library**: Part definitions with `partType = 'tool'`
  - **Tool Part Inventory**: Serialized instances linked to library
- **Complexity**: Requires handling both library and inventory levels

### Synchronization Challenge
- **TipQA → Ion**: Must determine if tool part library exists in Ion before creating inventory
- **Ion → TipQA**: Only inventory level exists in TipQA, so library-level data is ignored
- **Critical Decision Point**: Does tool part library exist in Ion for this TipQA tool?

**CRITICAL PRINCIPLE**: TipQA data ALWAYS overwrites Ion data when there is a serial number + part number match. The system NEVER creates duplicate tools - it always updates existing tools or cleans up duplicates before processing.

## Data Capture Phase (Master DataFrame Creation)

**CRITICAL**: All analysis and processing must be based on a single master dataframe containing all necessary data from both systems. No separate system queries should occur during analysis.

**MANDATORY 5-STEP FLOW**: The master dataframe creation MUST follow this exact sequence. Any deviation from this flow will cause data inconsistencies and incorrect analysis results.

### Step 0: Master Data Collection (Required Before Any Analysis)

#### Step 0.1: Get All Tools from TipQA (DataFrame 1)
- **Source**: SQL Server database using `tipqa_tools.sql` query
- **Purpose**: Complete inventory of all tools in TipQA system
- **Fields Required**: ALL fields needed for script functionality
- **Result**: TipQA tools dataframe (27,557+ tools)
- **Implementation**: `tipqa_tools_df` parameter passed to `create_master_dataframe()`

#### Step 0.2: Get Ion Matches for Unmatched TipQA Combos (DataFrame 2)
- **Source**: Ion GraphQL API using `get_tool_inventory_by_serial_and_part.graphql`
- **Purpose**: Find Ion inventory matching TipQA serial+part combos that were NOT found as partType=TOOL in Step 0.3
- **Filter**: Serial+part match (no partType filter) — catches PART types needing conversion
- **Pre-filter**: In-memory set difference eliminates combos already matched in Step 0.3, reducing ~16K queries to ~3-4K
- **Fields Required**: ALL fields needed for script functionality
- **Result**: Ion matches dataframe for unmatched combos only
- **Implementation**: `get_ion_matches_for_tipqa_tools_optimized()` function (receives only unmatched tools)

#### Step 0.3: Get All Ion Tools (DataFrame 3)
- **Source**: Ion GraphQL API using `get_all_tool_inventory.graphql`
- **Purpose**: Complete inventory of ALL tools in Ion system (not just orphaned ones)
- **Filter**: `partType = 'tool'` only, NO other filtering
- **Fields Required**: ALL fields needed for CSV output and script functionality
- **Result**: All Ion tools dataframe (12,000+ tools)
- **Implementation**: `get_all_ion_tools()` function

#### Step 0.4: Combine the Two Ion DataFrames and Remove Duplicates
- **Purpose**: Merge Ion matches (Step 0.2) and all Ion tools (Step 0.3) into single Ion dataframe
- **Deduplication**: Remove duplicates based on `serial_number` and `ion_id`
- **Result**: Combined Ion dataframe (12,000+ unique tools)
- **Implementation**: `combine_ion_dataframes()` function

#### Step 0.5: Merge/Append Ion DataFrame to TipQA DataFrame
- **Purpose**: Create master dataframe containing all data from both systems
- **Structure**: 
  - Some rows have only TipQA data (tools not in Ion)
  - Some rows have only Ion data (tools not in TipQA) 
  - Some rows have both TipQA and Ion data (tools in both systems)
- **Field Prefixing**: TipQA fields prefixed with `tipqa_`, Ion fields prefixed with `ion_`
- **Result**: Master dataframe (40,000+ total records)
- **Implementation**: `append_ion_to_tipqa()` function

**CRITICAL IMPLEMENTATION NOTES**:
- Steps 0.2 and 0.3 both query Ion but serve different purposes
- Step 0.2 finds tools that exist in both systems (matches)
- Step 0.3 gets ALL Ion tools regardless of TipQA existence
- Step 0.4 removes duplicates between the two Ion queries
- Step 0.5 creates the final master dataframe for analysis
- This flow ensures complete data capture from both systems

### Master DataFrame Creation Implementation

**PRIMARY FUNCTION**: `create_master_dataframe(token, config, tipqa_tools_df, environment)`

**SUPPORTING FUNCTIONS**:
- `get_ion_matches_for_tipqa_tools()` - Step 0.2 implementation
- `get_all_ion_tools()` - Step 0.3 implementation  
- `combine_ion_dataframes()` - Step 0.4 implementation
- `append_ion_to_tipqa()` - Step 0.5 implementation
- `ion_data_to_dataframe()` - Helper for converting Ion data to DataFrames

**DEPRECATED/INCORRECT APPROACHES** (DO NOT USE):
- `get_orphaned_ion_tools()` - Only gets tools not in TipQA, missing complete Ion dataset
- `create_unified_dataframe()` - Incorrect approach that doesn't follow 5-step flow
- Any approach that skips Step 0.3 (getting all Ion tools)
- Any approach that combines steps or changes the sequence

### Expected Master DataFrame Results

**VALIDATION CRITERIA** (Use these to verify correct implementation):
- **Total Records**: ~40,000+ records (27,557 TipQA + 12,000+ Ion after deduplication)
- **TipQA Records**: All 27,557+ TipQA tools with `tipqa_` prefixed fields
- **Ion Records**: All 12,000+ Ion tools with `ion_` prefixed fields
- **Field Structure**: Both `tipqa_*` and `ion_*` fields present in all records
- **Data Completeness**: No missing data due to incomplete Ion queries
- **Deduplication**: No duplicate Ion tools in final dataframe

**COMMON ERRORS TO AVOID**:
- Master dataframe with only ~14,000 records (indicates incomplete Ion data)
- Missing `ion_*` fields in records (indicates incorrect field mapping)
- Fake serial numbers appearing in analysis (indicates incorrect filtering)
- Analysis results that don't match expected business logic (indicates incomplete data)
- **Critical**: All analysis must work from this dataframe, not separate system queries

#### Step 0.5: Remove Duplicates from Master DataFrame
- **Purpose**: Eliminate duplicate records and create unified master dataset
- **Logic**: 
  - **TipQA tools NOT in Ion**: No duplicates (only appear in TipQA)
  - **TipQA tools IN Ion**: Duplicates (appear in both TipQA and Ion matches)
  - **Ion tools NOT in TipQA**: No duplicates (orphans, only appear in Ion tools)
- **Deduplication Strategy**:
  - **For TipQA tools**: Keep TipQA record as primary, merge Ion data when available
  - **For Ion orphans**: Keep Ion record (no TipQA equivalent)
  - **For duplicates**: Merge TipQA + Ion data into single record
- **Process**:
  1. **Identify TipQA-Ion matches**: Match by serial number + part number
  2. **Merge matched records**: Combine TipQA + Ion data into single record
  3. **Keep TipQA-only records**: Tools in TipQA but not in Ion
  4. **Keep Ion-only records**: Orphaned tools in Ion but not in TipQA
  5. **Remove duplicate Ion records**: Eliminate standalone Ion records that were merged
- **Result**: Master dataframe with:
  - TipQA tools with merged Ion data (when available)
  - TipQA-only tools (not in Ion)
  - Ion-only tools (orphans not in TipQA)
  - No duplicate records

**Data Capture Requirements**:
- Each query must include ALL fields needed for CSV output
- Each query must include ALL fields needed for script functionality
- No additional system queries during analysis phase
- All processing based on master dataframe only
- Master dataframe must be deduplicated and unified before analysis
- Master dataframe must be deduplicated before analysis

## Sequential Logic Flow (Optimized for Performance)

### Section 1: Skip Operations (No Changes in Ion)
**Purpose**: Tools that require no Ion mutations - maximum efficiency

#### Step 1: Pre-Filter TipQA Tools (Skip Operations)
**Purpose**: Skip tools that don't need Ion processing

##### Step 1.1: Skip Inactive Tools Not Existing in Ion
- **Logic**: Tools with `maintenance_status: I` OR `revision_status: I` that don't exist in Ion
- **Action**: `SKIP` → No Ion mutations required
- **Reason**: `inactive_in_tipqa_no_ion`
- **Benefit**: Eliminates processing overhead for inactive tools
- **Overhead**: None - no Ion API calls

##### Step 1.2: Skip Tools with Bad Part Numbers
- **Logic**: Tools with missing/invalid part numbers that don't exist in Ion
- **Action**: `SKIP` → No Ion mutations required
- **Reason**: `missing_part_number`
- **Benefit**: Prevents invalid data from reaching Ion
- **Overhead**: None - no Ion API calls

##### Step 1.3: Skip Tools with Revision Problems
- **Logic**: Tools with invalid revision formats (e.g., "5000 lbs", "123.45")
- **Action**: `SKIP` → No Ion mutations required
- **Reason**: `tipqa_revision_problem`
- **Benefit**: Prevents Ion validation errors
- **Overhead**: None - no Ion API calls
- **Valid Revisions**: Alphabetical only (A, B, C, AA, BB, ABC, etc.) or '-' (dash). Ion requires ALPHABETICAL revisions only - numeric revisions are automatically converted to 'A'.
- **Invalid Examples**: "5000 lbs" (spaces), "123.45" (periods), "001" (numeric - converted to 'A'), "1" (numeric - converted to 'A')

##### Step 1.4: Skip Protected Part Numbers
- **Logic**: Tools that match protected part numbers (must remain as regular parts)
- **Action**: `SKIP` → No Ion mutations required
- **Reason**: `protected_part_number`
- **Benefit**: Preserves critical part configurations
- **Overhead**: None - no Ion API calls
- **Configuration**: Defined in `config.yaml` under `sync_exceptions.protected_part_numbers` (part numbers are universal across all environments, unlike part IDs which differ)

##### Step 1.5: Skip Tools Already Up-to-Date (Performance Optimization)
- **Logic**: Tools that exist in Ion and are already synchronized with TipQA data
- **Action**: `SKIP` → No Ion mutations required
- **Reason**: `already_up_to_date`
- **Benefit**: Avoids unnecessary API calls and processing overhead
- **Overhead**: None - no Ion API calls
- **Comparison Fields**: All TipQA fields that should be synchronized to Ion:
  - Core fields: `part_number`, `revision`, `description`, `service_interval_seconds`, `asset_type`, `location`, `last_maintenance_date`, `asset_serial_number`, `manufacturer`, `model_number`, `condition`, `status`, `date_added`, `last_updated`, `notes`
  - Ion attributes: Asset Serial Number, Manufacturer, Location, Asset Type, Service Interval Seconds, Last Maintenance Date, Model Number, Condition, Status, Date Added, Last Updated, Notes
- **Implementation**: Compares TipQA values with corresponding Ion field values and attributes
- **Performance Impact**: Significantly reduces processing time for tools that don't need updates

### Section 2: Single Mutation Operations (Low Overhead)
**Purpose**: Tools requiring exactly one Ion mutation - efficient processing

#### Step 2: Mark Orphaned Ion Tools Unavailable (Single Mutation)
- **Purpose**: Handle tools that exist in Ion but not in TipQA
- **Logic**: Tools in Ion that don't exist in TipQA
- **Action**: `MARK_UNAVAILABLE` → Mark unavailable + move to Lost location
- **Reason**: `missing_in_tipqa` (tools in Ion but not in TipQA)
- **Benefit**: Simple Ion mutation (mark unavailable + move location)
- **Overhead**: Low - single Ion mutation

#### Step 3: Simple Updates (Single Mutation)
**Purpose**: Direct updates with no additional processing required

##### Step 3.1: Exact Match Updates (Serial + Part Number)
- **Logic**: Tools that exist in both TipQA and Ion with exact serial+part match
- **Condition**: `partType = 'tool'` in Ion tool part inventory, no location/status issues
- **Action**: `UPDATE` → Standard field updates only
- **Reason**: `exact_match_update`
- **Benefit**: Single Ion mutation (update inventory fields)
- **Overhead**: Lowest - direct field updates only

##### Step 3.2: Mark Inactive Tools Unavailable (Single Mutation)
- **Purpose**: Mark tools unavailable when they are inactive in TipQA (no TipQA updates needed)
- **Logic**: Tools with `maintenance_status: I` OR `revision_status: I`
- **Action**: `MARK_UNAVAILABLE` → Mark unavailable + move to Lost location
- **Reason**: `inactive_in_tipqa`
- **Benefit**: Simple Ion mutation (mark unavailable + move location)
- **Overhead**: Low - no TipQA data processing required

##### Step 3.3: Create New Tool Part Inventory (Single Mutation)
- **Purpose**: Create tool part inventory for tools that exist in TipQA but not in Ion
- **Logic**: Tools that exist in TipQA but not in Ion (tool part library exists, no inventory)
- **Action**: `CREATE` → Create new tool part inventory (tool part library already exists)
- **Benefit**: Simple Ion mutation (create inventory only)
- **Overhead**: Low - single inventory creation

### Section 3: Multiple Mutation Operations (High Overhead)
**Purpose**: Tools requiring multiple Ion mutations OR complex logic flows

#### Step 4: Complex Multi-Step Operations (Multiple Mutations)
**Purpose**: Operations requiring multiple mutations OR complex logic to determine action

##### Step 4.1: Create New Tool Part Library + Inventory (Two Mutations)
- **Purpose**: Create complete tool parts from scratch (requires two mutations)
- **Logic**: Tools that exist in TipQA but not in Ion AND tool part library doesn't exist
- **TipQA Challenge**: TipQA has no library concept, so we must create Ion library from TipQA inventory data
- **Action**: `CREATE` → Create new tool part library + create new tool part inventory
- **Benefit**: Complete tool creation from scratch
- **Overhead**: Medium - requires tool part library creation and tool part inventory creation
- **Process**:
  1. **Create Tool Part Library**: Create the part definition with `partType = 'tool'` (derived from TipQA inventory data)
  2. **Create Tool Part Inventory**: Create the inventory instance linked to the library

##### Step 4.2: Update + Mark Unavailable (Single Mutation, Complex Logic)
- **Purpose**: Tools requiring TipQA updates AND status changes and location moves
- **Logic**: Tools with `maintenance_status: L, OS, OC, TO, QAHD` OR location issues in TipQA
- **Action**: `UPDATE_THEN_MARK_UNAVAILABLE` → Update fields to match TipQA + mark unavailable + move location
- **Reasons**: 
  - `offsite_in_tipqa` (for OS, OC, TO status)
  - `lost_location` (for L status)
  - `location_missing_in_tipqa` (missing location in TipQA)
  - `no_matching_location_in_ion` (location in TipQA doesn't exist in Ion)
  - `quality_hold_in_tipqa` (for QAHD status)
- **Benefit**: Complete synchronization with TipQA data plus status handling
- **Overhead**: Medium - single mutation but complex logic for TipQA field matching
- **Complexity**: Complex logic to determine what fields need updating from TipQA
- **Maintenance Status Mapping**:
  - **L** → `lost_location`
  - **OS, OC, TO** → `offsite_in_tipqa`
  - **QAHD** → `quality_hold_in_tipqa`

##### Step 4.3: Serial+PartType Match, Different Part Number (Tool Update)
- **Case**: Serial number matches TipQA and `partType = 'tool'` in Ion, but part number differs
- **Step 1**: Identify serial number matches between TipQA and Ion
- **Step 2**: Check `partType` in Ion
  - **If `partType = 'tool'`**: Update tool to correct part number
    - **If correct part exists in Ion**: Update inventory to reference existing part (single mutation)
    - **If correct part doesn't exist**: Create new part + update inventory (multiple mutations)
  - **If `partType = 'part'`**: Skip (cannot update parts with different part numbers)
- **Reason**: `serial_match_update_part`
- **Overhead**: Medium to High - depends on whether correct part already exists

##### Step 4.4: Serial+Part Match, Different PartType (Part-to-Tool Conversion)
- **Case**: Serial number AND part number match TipQA exactly, but `partType = 'part'` in Ion
- **Step 1**: CRITICAL SAFETY CHECK - Verify exact serial number AND part number match between TipQA and Ion
  - **If part number mismatch**: Skip conversion (action: `SKIP`, reason: `part_number_mismatch_conversion_blocked`)
  - **If part number matches**: Proceed to Step 2
- **Step 2**: Check for installations that must be uninstalled first
  - **If installed**: Attempt to uninstall tool part
    - **If uninstall succeeds**: Proceed to Step 3
    - **If uninstall fails**: Log error and skip tool (action: `UPDATE`, reason: `part_to_tool_conversion`, error: "Cannot uninstall - conversion blocked")
  - **If not installed**: Proceed to Step 3
- **Step 3**: Change `trackingType` to "SERIAL" FIRST
- **Step 4**: Get NEW etag from Step 3 response
- **Step 5**: Change `partType` to "TOOL" SECOND (using new etag)
- **Step 6**: Update all other fields to match TipQA
- **CRITICAL ERROR PREVENTION**: 
  - **Part number must match TipQA exactly** - prevents incorrect conversions
  - `trackingType` must be changed BEFORE `partType`
  - Fresh etag required for `partType` update to prevent concurrency errors
- **Action**: `UPDATE` (even when conversion fails due to uninstall issues)
- **Reason**: `part_to_tool_conversion`
- **Overhead**: Highest - requires uninstall attempts, trackingType changes, etag handling, partType changes, and field updates

##### Step 4.5: Duplicate Cleanup (Highest Overhead)
- **Purpose**: Handle multiple tools with same serial number but different part numbers (both `partType = 'tool'`)
- **Reason**: `duplicate_cleanup`
- **Process**: 
  1. **Identify**: Find all tools with same serial number and `partType = 'tool'`
  2. **Match TipQA**: Determine which tool matches TipQA exactly (serial + part number)
  3. **Delete Incorrect**: Try to delete the tool with incorrect part number
  4. **Fallback Safety**: If deletion fails, mark incorrect tool as "- DO NOT USE" (append to description)
  5. **Update Correct**: Update the tool with correct part number to match TipQA
- **Overhead**: High - requires analysis, scoring, and multiple deletions
- **Safety Protection**: "- DO NOT USE" suffix prevents accidental use of incorrect duplicates
- **CRITICAL**: Only applies to `partType = 'tool'` - regular parts (`partType = 'part'`) can have same serial with different part numbers
- **Scoring System**: Rate tools based on data completeness and installations
- **Safe Deletion**: Uninstall tools before deletion if they're installed

## Summary: All Processing Actions and Reasons (Matching CSV Data)

Based on the logic flow above and actual CSV data, here are **all actions and reasons** that appear in the synchronization process:

### SKIP Actions (No Ion Changes)
1. **Skip tools with bad part numbers** - `missing_part_number`
2. **Skip tools with revision problems** - `tipqa_revision_problem`
3. **Skip protected part numbers** - `protected_part_number`
4. **Skip inactive tools not existing in Ion** - `inactive_in_tipqa_no_ion`
5. **Skip tools already up-to-date** - `already_up_to_date`
6. **Skip part-to-tool conversion due to part number mismatch** - `part_number_mismatch_conversion_blocked`
7. **Skip part-to-tool conversion due to empty TipQA part number** - `empty_tipqa_part_number`

### UPDATE Actions
5. **Update existing tool (exact match)** - `exact_match_update`
11. **Serial match updates (different part number)** - `serial_match_update_part`
12. **Part-to-tool conversion** - `part_to_tool_conversion`
13. **Duplicate cleanup** - `duplicate_cleanup`

### UPDATE_THEN_MARK_UNAVAILABLE Actions
6. **Update + mark unavailable (offsite)** - `offsite_in_tipqa`
7. **Update + mark unavailable (lost)** - `lost_location`
8. **Update + mark unavailable (location missing)** - `location_missing_in_tipqa`
9. **Update + mark unavailable (no matching location)** - `no_matching_location_in_ion`
10. **Update + mark unavailable (quality hold)** - `quality_hold_in_tipqa`

### MARK_UNAVAILABLE Actions
14. **Mark orphaned Ion tools unavailable** - `missing_in_tipqa` (tools in Ion but not in TipQA)
15. **Mark inactive tools unavailable** - `inactive_in_tipqa`

### CREATE Actions
16. **Create new tool part inventory** - `CREATE` (tool part library exists, create inventory only)
17. **Create new tool part library + inventory** - `CREATE` (create both library and inventory)

---

## CRITICAL IMPLEMENTATION REQUIREMENTS

### Master DataFrame Creation - MANDATORY COMPLIANCE

**WARNING**: The master dataframe creation flow (Steps 0.1-0.5) is CRITICAL to the correct operation of all tool synchronization scripts. Any modification to this flow will cause:

1. **Data Inconsistencies**: Missing tools from analysis
2. **Incorrect Business Logic**: Wrong actions and reasons assigned
3. **Failed Synchronization**: Tools not properly synchronized between systems
4. **Data Loss**: Tools marked unavailable when they shouldn't be
5. **Duplicate Creation**: Tools created when they already exist

### Implementation Compliance Checklist

Before modifying any script that uses master dataframe creation, verify:

- [ ] **Step 0.1**: TipQA tools fetched completely (27,557+ tools)
- [ ] **Step 0.2**: Ion matches fetched for TipQA serial numbers (11,000+ matches)
- [ ] **Step 0.3**: ALL Ion tools fetched (12,000+ tools) - NOT just orphaned ones
- [ ] **Step 0.4**: Ion dataframes combined and deduplicated properly
- [ ] **Step 0.5**: Master dataframe created with both TipQA and Ion data (40,000+ records)
- [ ] **Field Structure**: All `tipqa_*` and `ion_*` fields present
- [ ] **No Separate Queries**: Analysis uses only the master dataframe

### Scripts That Must Follow This Flow

- `tests/v1_production_dry_run_test.py`
- `tests/v1_production_live_test.py` 
- `daily_tool_sync_v1.py`
- `tests/update_subset_tools.py`
- Any future scripts that perform tool synchronization

### Change Control

**REQUIRED**: Any changes to the master dataframe creation flow must:
1. Be documented in this file
2. Include validation criteria
3. Be tested with all affected scripts
4. Maintain backward compatibility with existing CSV outputs
5. Be approved by the system owner

**PROHIBITED**: 
- Skipping any of the 5 steps
- Changing the sequence of steps
- Using deprecated functions (`get_orphaned_ion_tools`, `create_unified_dataframe`)
- Modifying field prefixing (`tipqa_*`, `ion_*`)
- Reducing the scope of Ion data fetching

This documentation serves as the single source of truth for master dataframe creation. All implementations must comply with this specification.