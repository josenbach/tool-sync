# Master Data Flow Documentation - Version 4.4

## Overview
This document defines the standardized process for creating the master dataset used in tool synchronization analysis. The master dataset combines TipQA and Ion data to provide a comprehensive view for analysis and decision-making.

**Scope**: This document focuses exclusively on **master dataframe creation** - how TipQA and Ion data are extracted, combined, and structured into a unified dataset. For details on analysis logic and update execution, see `tests/test_logic_flow.md`.

**Current Status**: Production-ready with optimized performance.

**IMPORTANT NOTE**: The revision validation logic in `utilities/shared_sync_utils.py` is CRITICAL and must NEVER be removed or modified. Ion requires ALPHABETICAL revisions only (A, B, C, AA, BB, ABC, etc.) or a dash (-). Numeric revisions (1, 2, 11, 001, etc.) are NOT accepted and will cause Ion API validation errors. Any numeric revisions are automatically converted to 'A' by the `clean_revision()` function.

## Performance Optimizations (Implemented)

### Ultra-Optimized Master Dataframe Creation
The current implementation includes several performance optimizations:

1. **Batched GraphQL Queries**: Groups multiple individual queries into batches for reduced API overhead
2. **Smart Caching**: Caches Ion data for 30 minutes to avoid redundant API calls
3. **Connection Pooling**: Reuses HTTP connections for multiple API calls
4. **Parallel Processing**: Uses ThreadPoolExecutor for concurrent API calls
5. **Memory Optimization**: Efficient pandas operations and data flattening

### Expected Performance
- **Targeted Ion Queries**: ~10,653 records in ~2-3 minutes
- **All Ion Tools Query**: ~12,750 records in ~2-3 minutes  
- **Total Processing Time**: ~5-6 minutes for complete master dataframe creation
- **Cache Benefits**: Subsequent runs use cached data (30-minute cache duration)

---

# MASTER DATASET CREATION

## Purpose
Create a comprehensive master dataset that combines TipQA and Ion data with proper flattening and deduplication. This dataset serves as the foundation for all analysis operations.

## CRITICAL IMPLEMENTATION REQUIREMENTS

**MANDATORY STEPS - DO NOT OPTIMIZE AWAY**:

1. **Step 3 MUST fetch ALL Ion tools** (partType = TOOL) with **NO filtering**
   - **DO NOT** filter by TipQA serial numbers
   - **DO NOT** use early stopping optimizations
   - **REQUIRED** for orphan detection (tools in Ion but not in TipQA)
   - Pass `tipqa_serial_numbers=None` to `get_all_ion_tools_optimized()`

2. **Step 4 MUST combine and deduplicate Ion dataframes BEFORE Step 5**
   - **DO NOT** skip deduplication step
   - **DO NOT** append Ion dataframes directly to TipQA without combining first
   - **REQUIRED** to prevent duplicate tools in master dataframe
   - Use `combine_ion_dataframes(ion_matches, all_ion_tools)` function

**These requirements are NON-NEGOTIABLE** - Any optimization that breaks these steps will cause:
- Missing orphaned tools (tools in Ion but not in TipQA)
- Duplicate tools in master dataframe
- Incorrect analysis results
- Data integrity issues

**When reviewing code for optimization, verify these steps are intact.**

## Critical Query Protection
**NEVER MODIFY THESE QUERIES** - They are proven and stable:

### Protected SQL Queries
- `tipqa_tools.sql` - Core TipQA tool data extraction
- `tipqa_tools_subset.sql` - Subset version for specific serial numbers

### Protected GraphQL Queries  
- `get_tool_inventory_by_serial_and_part.graphql` - Targeted Ion inventory lookup by serial and part number
- `get_all_tool_inventory.graphql` - Complete Ion tool inventory (partType = TOOL only)

**Note**: `get_part_inventory.graphql` was removed to prevent fetching all parts (160K+ records)

## Step 1: TipQA Data Extraction
**Purpose**: Extract all tool data from TipQA database
**Query**: `tipqa_tools.sql`
**Output**: DataFrame with TipQA tool records

**Key Fields**:
- `serial_number` (primary key)
- `part_number`
- `description`
- `revision`
- `service_interval_seconds` (converted to integer from decimal)
- `asset_type`
- `location`
- `location_name`
- `stock_room`
- `last_maintenance_date`
- `asset_serial_number`
- `manufacturer`
- `maintenance_status`
- `revision_status`

**Process**:
1. Connect to TipQA database
2. Execute `tipqa_tools.sql` query
3. Filter out fake/test serial numbers (starting with '0000')
4. Validate data integrity

## Step 2a: All Ion Tools Collection (Bulk Fetch)
**Purpose**: Get complete inventory of all Ion tools (partType = TOOL) - single paginated query
**Query**: `get_all_tool_inventory.graphql`
**Output**: Dict organized by serial number with all TOOL-type Ion inventory

**Process**:
1. Fetch ALL Ion tools (partType = TOOL) in a single paginated query (~13K items, ~20s)
2. No filtering by TipQA data - fetches everything for orphan detection

## Step 2a-filter: In-Memory Pre-Filter (No API Calls)
**Purpose**: Identify which TipQA serial+part combos are NOT already matched in Step 2a results
**Process**: Set difference between TipQA combos and Step 2a (serial, partNumber) pairs

**Process**:
1. Build set of `(serialNumber, partNumber)` from Step 2a results
2. Build set of `(serial_number, part_number)` from TipQA dataframe
3. Compute `unmatched = tipqa_combos - ion_tool_combos`
4. Only unmatched combos proceed to Step 2b

**Why This Is Critical**:
- ~80% of TipQA combos already match a TOOL in Step 2a
- Without this filter, Step 2b would make ~16K redundant individual API calls
- With this filter, Step 2b only queries ~3-4K unmatched combos
- Reduces total processing time by several minutes

## Step 2b: Targeted Non-TOOL Matches (Individual Queries)
**Purpose**: Find Ion inventory matching unmatched TipQA serial+part combos regardless of partType
**Query**: `get_tool_inventory_by_serial_and_part.graphql` (no partType filter)
**Output**: Dict of Ion matches for combos not found as TOOL in Step 2a

**Process**:
1. Query Ion for each unmatched TipQA serial+part combination (parallel, 20 workers)
2. Catches PART types that need conversion to TOOL (Step 4.4 in tool_sync_logic_flow.md)
3. Results combined with Step 2a data in Step 3

## Step 3: Filter and Combine Ion Data (IN MEMORY)
**Purpose**: Combine Step 2a (all TOOL types) and Step 2b (unmatched non-TOOL matches), deduplicate
**Process**: Merge both Ion result sets, remove exact duplicates by ion_id

**Process**:
1. Combine Step 2b matches with Step 2a data using `combine_ion_dataframes()`
2. Identify orphans (Ion TOOL-type tools NOT matching any TipQA combo)
3. Deduplicate by `ion_id` to prevent double-counting
4. Result: Complete Ion dataset with matches and orphans

## Step 4: Remove Duplicates
**Purpose**: Remove duplicate Ion records before merging
**Process**: Use `drop_duplicates()` based on `ion_id`

**Why This Is Critical**:
- Prevents duplicate tools in master dataframe
- Ensures one row per Ion tool
- Maintains data integrity

**Process**:
1. Remove duplicates based on `ion_id` (one Ion tool = one row)
2. Keep first occurrence of each duplicate
3. Result: Deduplicated Ion dataframe ready for merge

## Step 5: Merge TipQA and Ion Data (REMOVED - Now Step 5)

## Step 5: Master Dataset Assembly
**Purpose**: Create final master dataset combining TipQA and Ion data
**Process**:
1. Rename TipQA columns with `tipqa_` prefix
2. Rename Ion columns with `ion_` prefix  
3. Perform merge on serial_number + part_number (composite key)
4. One row per TipQA serial+part combo (with Ion data if match exists)
5. Additional rows for orphaned Ion tools (not in TipQA)
6. Add action_in_ion and reason columns (initialized as empty)
7. Select final column set for analysis

**CRITICAL**: Merge must use serial_number + part_number as composite key (not just serial_number)
- This ensures correct matching (serial alone is not unique)
- Prevents false matches when same serial has different parts

## Step 6: Batch Part Update Optimization (Daily Tool Sync Only)
**Purpose**: Optimize part-level field updates by aggregating and updating each unique part once
**Process**: (Implemented in `daily_tool_sync.py` before individual tool processing)

1. **Group tools by part_number+revision** that need part-level updates
2. **Collect all descriptions** for each part from all TipQA serials
3. **Determine most common description** (normalized, case-insensitive) for each part
4. **Update each unique part once** using the most common description
5. **Cache updated parts** to skip redundant updates during individual tool processing

**CRITICAL RULES**:
- **ONLY part-level fields** are aggregated (description, revision, service_interval_seconds)
- **Inventory-level fields** are NEVER aggregated - always updated individually per serial
- Part updates only occur if Ion part field is empty and TipQA has a value
- Prevents ping-pong updates when TipQA serials have different descriptions for the same part

**Why This Is Critical**:
- TipQA is serial-based: Serial A might have "Torque Wrench", Serial B might have "Torque Driver" for the same part
- Ion is part-based: One part has one description shared by all serials
- Without aggregation: Serial A updates part to "Torque Wrench", then Serial B updates it to "Torque Driver", then Serial A updates again → infinite loop
- With aggregation: Most common description is used once, then no more updates needed

## Master Dataset Structure

### Record Format
Each record in the master dataset contains:

**TipQA Fields** (prefixed with `tipqa_`):
- All fields from Step 1 TipQA extraction
- Maintains original TipQA data integrity

**Ion Fields** (prefixed with `ion_`):
- Core inventory fields: `id`, `_etag`, `status`, `unavailable`, `lotNumber`
- Location fields: `location_id`, `location_name`
- Part fields: `part_id`, `part_partNumber`, `part_revision`, `part_partType`, `part_trackingType`, `part_description`, `part__etag`, `part_maintenanceIntervalSeconds`
- Part attributes: `part_attr_Asset Type`, `part_attr_Primary Location`, etc.
- Inventory attributes: `attr_Manufacturer`, `attr_Asset Serial Number`, etc.
- Installation fields: `abom_id`, `abom_buildRequirement_id`

**Analysis Fields**:
- `action_in_ion`: Primary action to take in Ion system
- `reason`: Detailed reason for the action

## Field Mapping: TipQA → Ion

### Primary Keys
- **TipQA**: `serial_number` + `part_number` (composite key)
- **Ion**: `serialNumber` + `partNumber` (composite key)

### CRITICAL: Part-Level vs Inventory-Level Fields

**IMPORTANT DISTINCTION**: Ion has a one-to-many relationship between parts and inventory items (serials). TipQA is serial-based (each serial can have different values). This creates a fundamental mismatch that requires special handling:

#### Part-Level Fields (Shared Across All Serials)
These fields are stored at the **part level** in Ion and are **shared** across all serials of the same part:
- `description` → `part.description`
- `revision` → `part.revision`
- `service_interval_seconds` → `part.maintenanceIntervalSeconds`

**Update Behavior**:

**Description**:
- **Batch Update**: Aggregated using the **most common value** across all TipQA serials for the same part
- **Comparison Logic**: Only flagged for update if Ion part field is **empty** and TipQA has a value
- **Rationale**: Since TipQA serials can have different descriptions for the same part, but Ion shares one description per part, we:
  1. Use the most common TipQA description for the part
  2. Don't flag mismatches when Ion already has a value (prevents ping-pong updates)
  3. Only update when Ion is empty (initial population)

**Revision**:
- **Individual Update**: Handled at the tool update level (NOT batch aggregated)
- **Comparison Logic**: **ALWAYS compared** and updated when TipQA and Ion revisions differ, even if Ion already has a value
- **Rationale**: Revisions can legitimately change over time (e.g., part revised from "A" to "B"), so they must always be synchronized. When revision changes, `update_tool()` finds or creates the correct part with the new revision.

**Service Interval**:
- **Individual Update**: Handled at the tool update level (NOT batch aggregated)
- **Comparison Logic**: **ALWAYS compared** and updated when TipQA and Ion service intervals differ, even if Ion already has a value
- **Rationale**: Service intervals can legitimately change over time in TipQA, so they must always be synchronized when TipQA changes

#### Inventory-Level Fields (Per Serial)
These fields are stored at the **inventory level** in Ion and are **unique per serial**:
- `location` → `inventory.locationId` / `inventory.location.name`
- `last_maintenance_date` → `inventory.lastMaintainedDate`
- `asset_serial_number` → `inventory.attributes.Asset Serial Number`
- `manufacturer` → `inventory.attributes.Manufacturer`
- `serial_number` → `inventory.serialNumber`

**Update Behavior**:
- **Individual Updates**: Each serial's inventory-level fields are updated **individually** (never aggregated)
- **Comparison Logic**: Always compared and updated if they differ from TipQA
- **Rationale**: These fields are per-serial in both TipQA and Ion, so they should always match exactly

### Core Field Mapping
| TipQA Field | Ion Field | Level | Update Behavior |
|-------------|-----------|-------|-----------------|
| `serial_number` | `serialNumber` | Inventory | Always matched (primary key) |
| `part_number` | `partNumber` | Part | Always matched (primary key) |
| `description` | `part.description` | **Part** | **Aggregated (most common), only if Ion empty** |
| `revision` | `part.revision` | **Part** | **Aggregated (most common), only if Ion empty** |
| `service_interval_seconds` | `part.maintenanceIntervalSeconds` | **Part** | **Aggregated (most common), only if Ion empty** |
| `last_maintenance_date` | `lastMaintainedDate` | **Inventory** | **Always compared and updated per serial** |
| `location` | `locationId` / `location.name` | **Inventory** | **Always compared and updated per serial** |
| `asset_serial_number` | `inventory.attributes.Asset Serial Number` | **Inventory** | **Always compared and updated per serial** |
| `manufacturer` | `inventory.attributes.Manufacturer` | **Inventory** | **Always compared and updated per serial** |

### Attribute Field Mapping
| TipQA Field | Ion Part Attribute | Ion Inventory Attribute | Level | Update Behavior |
|-------------|-------------------|----------------------|-------|-----------------|
| `asset_type` | `part.attributes.Asset Type` | - | Part | Part-level (aggregated) |
| `manufacturer` | `part.attributes.Manufacturer` | `inventory.attributes.Manufacturer` | **Inventory** | **Always updated per serial** |
| `asset_serial_number` | `part.attributes.Asset Serial Number` | `inventory.attributes.Asset Serial Number` | **Inventory** | **Always updated per serial** |
| `location` | `part.attributes.Location` | `inventory.attributes.Location` | **Inventory** | **Always updated per serial** |

### Field Comparison Notes
The master dataframe includes all fields needed for comparison. The actual comparison logic and update behavior are documented in `tests/test_logic_flow.md`. Key points:

- **Part-level fields** (description, revision, service_interval_seconds): Stored at part level in Ion, shared across all serials
- **Inventory-level fields** (location, last_maintenance_date, asset_serial_number, manufacturer): Stored at inventory level in Ion, unique per serial
- All fields are prefixed appropriately (`tipqa_` and `ion_`) for clear identification

### Data Flattening Process
The optimized approach includes comprehensive flattening of nested Ion data structures:

**Location Flattening**:
- `location.id` → `location_id`
- `location.name` → `location_name`

**Attributes Flattening**:
- Inventory-level attributes → `attr_{key}` columns
- Part-level attributes → `part_attr_{key}` columns

**Part Flattening**:
- All part fields → `part_{field}` columns
- Nested part attributes → `part_attr_{key}` columns

**Installation Flattening**:
- `abomInstallations` → `abom_{field}` columns
- Nested installation fields → `abom_{field}_{nested_field}` columns

### Data Integrity Rules
1. **No Data Modification**: Master dataset records are never modified after creation
2. **Complete Field Mapping**: All TipQA and Ion fields are preserved
3. **Consistent Prefixing**: Clear separation between TipQA and Ion data
4. **Comprehensive Flattening**: All nested structures are flattened for analysis
5. **Duplicate Handling**: Duplicates are removed during combination, not after

### Query Organization
**CRITICAL**: All queries are properly containerized:
- **GraphQL queries**: Stored in `queries/` folder, loaded via `read_query()` function
- **SQL queries**: Stored in `queries/` folder, loaded via `read_sql_query()` function
- **No hardcoded queries**: No queries are embedded directly in Python scripts
- **Benefits**: Easy to maintain, version control, and update queries without modifying code

**Query Files**:
- GraphQL: `create_tool.graphql`, `update_tool.graphql`, `update_inventory_with_attributes.graphql`, `get_all_tool_inventory.graphql`, etc.
- SQL: `tipqa_tools.sql`, `tipqa_tools_subset.sql`

---

---

# PART II: ANALYSIS LOGIC APPLICATION

## Purpose
Apply comprehensive business logic to determine the appropriate action and reason for each tool in the master dataset. This process categorizes tools based on their current state and required Ion operations. The result is a master dataframe with TipQA data, Ion data, AND action/reason columns populated.

## Purpose
Apply comprehensive business logic to determine the appropriate action and reason for each tool in the master dataset. This process categorizes tools based on their current state and required Ion operations.

## Analysis Logic Overview
The analysis logic uses the **proven `analyze_tool_using_daily_sync_logic` function** from `shared_sync_utils.py`. This sophisticated logic has been tested and validated in production, ensuring consistent and reliable action/reason assignment.

The logic follows a **4-category operational approach** that groups actions by Ion mutation complexity:

1. **SKIP OPERATIONS** - No Ion mutations required (highest efficiency)
2. **SIMPLE UPDATES** - Single Ion mutation required (low overhead)
3. **MULTIPLE UPDATES** - Two Ion mutations required (medium overhead)
4. **COMPLEX UPDATES** - Multiple Ion mutations and queries required (high overhead)

Each category is processed in order, with higher efficiency operations taking precedence.

---

## 1. SKIP OPERATIONS (No Ion Mutations Required)

### Purpose
Identify tools that require no Ion operations, maximizing efficiency by avoiding unnecessary API calls.

### Skip Operations Logic (from analyze_tool_using_daily_sync_logic)

The skip operations are processed in the following priority order:

### Skip Condition 1: Inactive Tools NOT in Ion
**Criteria**: `tipqa_maintenance_status = 'I'` OR `tipqa_revision_status = 'I'` AND `ion_serialNumber` is null
**Action**: `SKIP`
**Reason**: `inactive_in_tipqa_no_ion`
**Rationale**: Inactive tools not in Ion require no action - already effectively unavailable

### Skip Condition 2: Missing Part Number
**Criteria**: `tipqa_part_number` is empty or invalid
**Action**: `SKIP`
**Reason**: `missing_part_number`
**Rationale**: Cannot process tools without valid part numbers - no Ion operation possible

### Skip Condition 3: Revision Problem
**Criteria**: `tipqa_revision` contains invalid characters or format
**Action**: `SKIP`
**Reason**: `tipqa_revision_problem`
**Rationale**: Ion requires ALPHABETICAL revisions only (not numeric). Invalid revisions will cause Ion API validation errors. Numeric revisions are automatically converted to 'A' by the `clean_revision()` function.
**Valid Revisions**: 
- Up to 3 alphabetical characters (A, B, C, AA, BB, ABC, etc.)
- Dash (-)
- Empty/null (defaults to 'A')
**Invalid Revisions**:
- Numeric revisions (1, 2, 11, 001, etc.) - automatically converted to 'A'
- Length greater than 3 characters
- Containing spaces (e.g., "5000 lbs", "A B")
- Containing periods (e.g., "1.5", "123.45")
- Containing special characters (!, @, #, $, %, etc.)
- Containing numbers mixed with letters (e.g., "A1", "2B")

### Skip Condition 4: Protected Part Numbers
**Criteria**: `tipqa_part_number` is in the protected part numbers list
**Action**: `SKIP`
**Reason**: `protected_part_number`
**Rationale**: Protected parts cannot be modified - no Ion operation possible

**CRITICAL: Ion Data Requirement for Protected Part Numbers**
Even though protected part numbers are marked as SKIP, **Ion data MUST be included** in the CSV output. This allows verification that:
- The Ion `partType` is still `PART` (not `TOOL`)
- The Ion record exists and matches the TipQA data
- Protected parts remain properly configured in Ion

The master dataframe creation process ensures Ion data is fetched for all TipQA tools, including protected part numbers, by querying Ion matches that include both `partType = TOOL` and `partType = PART`. This Ion data is preserved in the final CSV output even when the action is SKIP.

**Current Protected Part Numbers** (from `config.yaml`):
- `356A45` - Part ID 2798 - ICP® Accelerometer
- `TLD339A37` - Part ID 2916 - ACCELEROMETER, TRIAXIAL ICP, TITANIUM HOUSING  
- `356A17` - Part ID - Accelerometer (should remain as part, not tool)

**Expected Count**: ~32 tools categorized as `protected_part_number`

### Skip Condition 5: Inactive Tools Already Unavailable
**Criteria**: `tipqa_maintenance_status = 'I'` OR `tipqa_revision_status = 'I'` AND `ion_status = 'UNAVAILABLE'`
**Action**: `SKIP`
**Reason**: `inactive_tipqa_unavailable_ion`
**Rationale**: Already marked unavailable in Ion - no action needed

### Skip Condition 6: Tools Already Synchronized
**Criteria**: Tools exist in both TipQA and Ion with identical "updateable fields"
**Action**: `SKIP`
**Reason**: `already_up_to_date` or `already_matches_tipqa`
**Rationale**: Tools are already synchronized - no updates needed

### Skip Condition 7: Lost Tools NOT in Ion
**Criteria**: `tipqa_maintenance_status = 'L'` AND `ion_serialNumber` is null
**Action**: `SKIP`
**Reason**: `lost_tool_not_in_ion`
**Rationale**: Lost tools not in Ion should not be created. They will only be created when they are found and have a valid location in Ion.

### Skip Condition 8: Offsite Tools NOT in Ion
**Criteria**: `tipqa_maintenance_status` in ['OS', 'OC', 'TO'] AND `ion_serialNumber` is null
**Action**: `SKIP`
**Reason**: `offsite_tool_not_in_ion`
**Rationale**: Offsite tools not in Ion should not be created. They will only be created when they return onsite and have a valid location in Ion.

### Skip Condition 9: Quality Hold Tools NOT in Ion
**Criteria**: `tipqa_maintenance_status = 'QAHD'` AND `ion_serialNumber` is null
**Action**: `SKIP`
**Reason**: `quality_hold_tool_not_in_ion`
**Rationale**: Quality hold tools not in Ion should not be created. They will only be created when they are released from quality hold and have a valid location in Ion.

### Skip Condition 10: Invalid Location NOT in Ion
**Criteria**: `tipqa_location` does not match any Ion location name AND `ion_serialNumber` is null
**Action**: `SKIP`
**Reason**: `invalid_location_not_in_ion`
**Rationale**: Tools with locations in TipQA that don't exist in Ion should not be created. They will only be created when the location is fixed in TipQA and matches a valid Ion location.

### Skip Operations Benefits
- **Zero API Calls**: No Ion mutations required
- **Maximum Efficiency**: Fastest processing category
- **Data Integrity**: Preserves existing synchronized state
- **Resource Conservation**: Minimizes Ion API load

---

## 2. SIMPLE UPDATES (Single Ion Mutation Required)

### Purpose
Identify tools that require exactly one Ion mutation, providing efficient updates with minimal API overhead.

### Simple Update Logic (from analyze_tool_using_daily_sync_logic)

### Simple Update 1: Mark Inactive Tools Unavailable
**Criteria**: `tipqa_maintenance_status = 'I'` OR `tipqa_revision_status = 'I'` AND `ion_serialNumber` is not null AND `ion_status != 'UNAVAILABLE'`
**Action**: `MARK_UNAVAILABLE`
**Reason**: `inactive_in_tipqa`
**Rationale**: Single mutation to mark inactive tools unavailable

### Simple Update 5: Move Lost Tools to Lost Location
**Criteria**: `tipqa_maintenance_status = 'L'` AND `ion_serialNumber` is not null
**Action**: `UPDATE`
**Reason**: `lost_tool_move_to_lost_location`
**Rationale**: Single mutation to move existing lost tools to the lost location in Ion (location ID 10043)

### Simple Update 6: Mark Offsite Tools Unavailable
**Criteria**: `tipqa_maintenance_status` in ['OS', 'OC', 'TO'] AND `ion_serialNumber` is not null AND `ion_status != 'UNAVAILABLE'`
**Action**: `MARK_UNAVAILABLE`
**Reason**: `offsite_tool_mark_unavailable`
**Rationale**: Single mutation to mark existing offsite tools unavailable

### Simple Update 7: Mark Quality Hold Tools Unavailable
**Criteria**: `tipqa_maintenance_status = 'QAHD'` AND `ion_serialNumber` is not null AND `ion_status != 'UNAVAILABLE'`
**Action**: `MARK_UNAVAILABLE`
**Reason**: `quality_hold_tool_mark_unavailable`
**Rationale**: Single mutation to mark existing quality hold tools unavailable

### Simple Update 8: Mark Invalid Location Tools Unavailable
**Criteria**: `tipqa_location` does not match any Ion location name AND `ion_serialNumber` is not null AND `ion_status != 'UNAVAILABLE'`
**Action**: `MARK_UNAVAILABLE`
**Reason**: `invalid_location_mark_unavailable`
**Rationale**: Single mutation to mark existing tools with invalid TipQA locations unavailable until the location is fixed in TipQA

### Simple Update 2: Mark Up-to-Date Tools Unavailable
**Criteria**: Tools already up-to-date but need to be marked unavailable
**Action**: `MARK_UNAVAILABLE`
**Reason**: `up_to_date_but_needs_unavailable`
**Rationale**: Single mutation to mark synchronized tools unavailable

### Simple Update 2a: Mark Tools Available
**Criteria**: Tools that should be AVAILABLE based on TipQA status (NOT inactive, NOT lost/offsite/quality hold, has valid location) but are currently UNAVAILABLE in Ion, and all other fields are up-to-date
**Action**: `MARK_AVAILABLE`
**Reason**: `should_be_available_but_unavailable`
**Rationale**: Single mutation to mark tools as available when they should be available but are currently unavailable. This is the counterpart to `MARK_UNAVAILABLE` - used when a tool's status in TipQA indicates it should be available (e.g., calibration completed, returned from offsite) but Ion still shows it as unavailable.
**Mutation**: `update_inventory_with_attributes.graphql` with `unavailable: False`

### Simple Update 3: Inventory-Only Updates
**Criteria**: Tools where part data is correct, only inventory attributes need updating
**Action**: `UPDATE`
**Reason**: `update_inventory`
**Rationale**: Single mutation to update inventory-level attributes only
**Mutation**: `update_inventory_with_attributes.graphql`

### Simple Update 4: Simple Inventory Creation
**Criteria**: Tools where part library exists, only need inventory creation
**Action**: `CREATE`
**Reason**: `create_new_inventory`
**Rationale**: Single mutation to create inventory instance for existing part
**Mutation**: `create_tool_inventory.graphql`

### Simple Updates Benefits
- **Low API Overhead**: Only one Ion mutation required
- **Efficient Processing**: Fast execution with minimal complexity
- **Clear Actions**: Straightforward single-operation updates
- **Predictable Results**: Simple, reliable operations

---

## 3. MULTIPLE UPDATES (Two Ion Mutations Required)

### Purpose
Identify tools that require exactly two Ion mutations, handling more complex scenarios that need sequential operations.

### Multiple Update Logic (from analyze_tool_using_daily_sync_logic)

### Multiple Update 1: Update Lost Tool Location (Complex)
**Criteria**: `tipqa_maintenance_status = 'L'` AND `ion_serialNumber` is not null AND location needs update
**Action**: `UPDATE`
**Reason**: `lost_tool_update_location`
**Rationale**: Two mutations - update tool data (including location to lost location) then update inventory attributes if needed
**Note**: Lost tools that already exist in Ion should be moved to the lost location (ID 10043) via Simple Update 5. This case handles additional data updates beyond just location.

### Multiple Update 2: Update Offsite Tool (Complex)
**Criteria**: `tipqa_maintenance_status` in ['OS', 'OC', 'TO'] AND `ion_serialNumber` is not null AND tool data needs update
**Action**: `UPDATE`
**Reason**: `offsite_tool_update`
**Rationale**: Two mutations - update tool data then mark unavailable
**Note**: Offsite tools that already exist in Ion should be marked unavailable via Simple Update 6. This case handles additional data updates beyond just marking unavailable.

### Multiple Update 3: Update Quality Hold Tool (Complex)
**Criteria**: `tipqa_maintenance_status = 'QAHD'` AND `ion_serialNumber` is not null AND tool data needs update
**Action**: `UPDATE`
**Reason**: `quality_hold_tool_update`
**Rationale**: Two mutations - update tool data then mark unavailable
**Note**: Quality hold tools that already exist in Ion should be marked unavailable via Simple Update 7. This case handles additional data updates beyond just marking unavailable.

### Multiple Update 4: Update Invalid Location Tool (Complex)
**Criteria**: `tipqa_location` does not match any Ion location name AND `ion_serialNumber` is not null AND tool data needs update
**Action**: `UPDATE`
**Reason**: `invalid_location_tool_update`
**Rationale**: Two mutations - update tool data then mark unavailable
**Note**: Tools with invalid locations that already exist in Ion should be marked unavailable via Simple Update 8. This case handles additional data updates beyond just marking unavailable.

### Multiple Update 7: Part and Inventory Updates
**Criteria**: Tools that need both part data and inventory attributes updated
**Action**: `UPDATE`
**Reason**: `update_inventory_and_part`
**Rationale**: Two mutations - update part data then update inventory attributes
**Mutations**: `update_tool.graphql` + `update_inventory_with_attributes.graphql`

**CRITICAL: Part-Level Field Update Logic**:
- **Part-level fields** (description, revision, service_interval_seconds) are only flagged for update if Ion part field is **empty** and TipQA has a value
- This prevents ping-pong updates when TipQA serials have different descriptions for the same part
- Batch update phase aggregates part-level fields using the **most common value** across all TipQA serials
- **Inventory-level fields** (location, last_maintenance_date, asset_serial_number, manufacturer) are **always** compared and updated individually per serial (never aggregated)

### Multiple Update 8: Complete Tool Creation
**Criteria**: Tools where neither part nor inventory exists
**Action**: `CREATE`
**Reason**: `new_tool_create`
**Rationale**: Two mutations - create part then create inventory
**Mutations**: `create_tool.graphql` + `create_tool_inventory.graphql`

### Multiple Updates Benefits
- **Sequential Operations**: Handles complex scenarios requiring multiple steps
- **Controlled Complexity**: Limited to exactly two mutations
- **Specialized Handling**: Addresses specific business cases
- **Predictable Workflow**: Clear two-step process

---

## 4. COMPLEX UPDATES (Multiple Ion Mutations and Queries Required)

### Purpose
Identify tools that require multiple Ion mutations and potentially additional queries, handling the most complex synchronization scenarios.

### Complex Update Logic (from analyze_tool_using_daily_sync_logic)

### Complex Update 1: Part Number Changes
**Criteria**: Tools with same serial but different part number
**Action**: `UPDATE`
**Reason**: `serial_match_update_part`
**Rationale**: Multiple mutations - query existing parts + create new part (if needed) + update inventory to reference new part + update inventory attributes
**Mutations**: Query + `create_tool.graphql` (if needed) + `update_inventory_part.graphql` + `update_inventory_with_attributes.graphql`

### Complex Update 2: Part-to-Tool Conversion
**Criteria**: Ion part exists but needs to be converted to tool
**Action**: `UPDATE`
**Reason**: `part_to_tool_conversion`
**Rationale**: Multiple mutations to convert part type and update all attributes
**Mutations**: Multiple mutations to convert part type and update all attributes

### Complex Updates Benefits
- **Comprehensive Synchronization**: Handles all data differences
- **Flexible Operations**: Adapts to complex business requirements
- **Complete Coverage**: Ensures full data consistency
- **Advanced Scenarios**: Handles edge cases and complex workflows

---

## Exact Match Criteria
**Purpose**: Determine if TipQA and Ion data are identical

**Matching Criteria (all must be true):**
1. **Part Number Match** (required): `tipqa_part_number` ↔ `ion_part_partNumber`
2. **Description Match** (tipqa null = ion null): `tipqa_description` ↔ `ion_part_description`
3. **Revision Match** (tipqa null = ion null): `tipqa_revision` ↔ `ion_part_revision`
4. **Service Interval Match** (tipqa null = ion null): `tipqa_service_interval_seconds` ↔ `ion_part_maintenanceIntervalSeconds`
5. **Location Match** (tipqa null = ion null): `tipqa_location` ↔ `ion_location_name`
6. **Last Maintenance Date Match** (tipqa null = ion null): `tipqa_last_maintenance_date` ↔ `ion_lastMaintainedDate`
7. **Asset Serial Number Match** (tipqa null = ion null): `tipqa_asset_serial_number` ↔ `ion_attr_Asset Serial Number`
8. **Manufacturer Match** (tipqa null = ion null): `tipqa_manufacturer` ↔ `ion_attr_Manufacturer`
9. **Asset Type Match** (tipqa null = ion null): `tipqa_asset_type` ↔ `ion_part_attr_Asset Type`
10. **Serial Number Match** (required): `tipqa_serial_number` ↔ `ion_serialNumber`

**Special Cases**:
- **Service Intervals**: Converted to integers, null/0 treated as equivalent
- **Dates**: Normalized to T-separated format for consistent comparison
- **Maintenance Status**: Special mappings for lost, offsite, and quality hold statuses

---

# PART III: MASTER DATAFRAME OUTPUT STRUCTURE

## Purpose
Document the structure and content of the master dataframe after analysis logic has been applied. This dataframe contains all TipQA data, all Ion data, and the determined actions/reasons for each tool.

### Master DataFrame Structure
The master dataframe is the result of combining TipQA and Ion data (Part I) and applying analysis logic (Part II). It serves as the input for script execution (see `tests/test_logic_flow.md` for execution details).

### Column Structure (36 columns total)

**CRITICAL**: This exact column structure must be maintained in all CSV outputs. The `select_standard_columns()` function in `utilities/shared_sync_utils.py` ensures these 36 columns are always included in the specified order. No other fields should be added unless explicitly specified by the user.

#### Action/Reason Columns (2)
1. `action_in_ion` - Primary action to take in Ion system
2. `reason` - Detailed reason for the action

#### TipQA Columns (12) - Prefixed with `tipqa_`
3. `tipqa_serial_number` - Primary key from TipQA
4. `tipqa_part_number` - Part number from TipQA
5. `tipqa_description` - Tool description from TipQA
6. `tipqa_revision` - Revision from TipQA
7. `tipqa_service_interval_seconds` - Service interval (converted to Int64)
8. `tipqa_asset_type` - Asset type from TipQA
9. `tipqa_location` - Location from TipQA
10. `tipqa_last_maintenance_date` - Last maintenance date (T-normalized)
11. `tipqa_asset_serial_number` - Asset serial number from TipQA
12. `tipqa_manufacturer` - Manufacturer from TipQA
13. `tipqa_maintenance_status` - Maintenance status from TipQA
14. `tipqa_revision_status` - Revision status from TipQA

#### Ion Columns (22) - Prefixed with `ion_`
15. `ion_id` - Primary key from Ion
16. `ion_serialNumber` - Serial number from Ion
17. `ion__etag` - ETag for Ion inventory updates (note: double underscore)
18. `ion_status` - Status from Ion
19. `ion_lastMaintainedDate` - Last maintenance date (T-normalized)
20. `ion_location` - Location name from Ion (same as ion_location_name)
21. `ion_location_id` - Location ID from Ion
22. `ion_location_name` - Location name from Ion
23. `ion_attributes_Asset Serial Number` - Asset serial number attribute value from Ion inventory attributes
24. `ion_attributes_Manufacturer` - Manufacturer attribute value from Ion inventory attributes
25. `ion_attributes_Etag` - ETag for Ion inventory attributes
26. `ion_part_id` - Part ID from Ion
27. `ion_part_partNumber` - Part number from Ion
28. `ion_part_revision` - Part revision from Ion
29. `ion_part_partType` - Part type from Ion (TOOL/PART)
30. `ion_part_trackingType` - Tracking type from Ion
31. `ion_part_description` - Part description from Ion
32. `ion_part_maintenanceIntervalSeconds` - Service interval (converted to Int64)
33. `ion_part_attributes_Asset Type` - Asset type attribute value from Ion part attributes
34. `ion_part_attributes_Etag` - ETag for Ion part attributes
35. `ion_abomInstallations_id` - aBOM installation ID
36. `ion_buildrequirement_id` - Build requirement ID

### Action and Reason Columns
After analysis logic is applied, each row in the master dataframe contains:
- `action_in_ion`: The action to be performed (SKIP, CREATE, UPDATE, MARK_UNAVAILABLE, UPDATE_THEN_MARK_UNAVAILABLE, CONVERT_PART_TO_TOOL)
- `reason`: Detailed reason explaining why this action was determined

### Data Integrity Features
- **Service Intervals**: Converted to integers (Int64) for both TipQA and Ion
- **Date Normalization**: All dates normalized to T-separated format for consistent comparison
- **Null Handling**: TipQA null/0 equivalent to Ion null/0 for matching logic
- **Protected Parts**: Correctly categorized tools with protected part numbers
- **Deduplication**: Removes duplicate records while preserving legitimate one-to-many relationships

**Note**: For details on how scripts execute these actions (mutations, updates, creates), see `tests/test_logic_flow.md`.

---

**Last Updated**: 2026-01-23
**Version**: 4.4
**Status**: Production Ready
**Scope**: This document focuses on master dataframe creation and analysis logic. For execution details (how scripts perform updates), see `tests/test_logic_flow.md`.

**Changes**: 
- **Version 4.4**: Clarified document scope - focuses on dataframe creation and analysis logic, not execution
- **Version 4.3**: Added CRITICAL distinction between part-level and inventory-level fields
- **Version 4.3**: Documented batch update optimization that aggregates part-level fields using most common value
- **Version 4.3**: Documented comparison logic that skips part-level fields when Ion already has a value (prevents ping-pong updates)
- **Version 4.3**: Clarified that inventory-level fields are NEVER aggregated and always updated individually per serial
- **Version 4.2**: Added CRITICAL IMPLEMENTATION REQUIREMENTS section to prevent optimization mistakes
- **Version 4.2**: Clarified Step 3 MUST fetch ALL Ion tools (no filtering) for orphan detection
- **Version 4.2**: Clarified Step 4 MUST combine and deduplicate Ion dataframes before Step 5
- **Version 4.1**: Added skip conditions for lost, offsite, quality hold, and invalid location tools NOT in Ion