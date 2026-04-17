#!/usr/bin/env python3
'''
Tool Processing Utilities Module
===============================

Handles core tool processing logic following the flow document.
Separated from main sync logic for better maintainability.

Created: 2025-01-28
Author: Jae Osenbach
Purpose: Tool processing utilities and flow logic
'''

import time
import math
import random
import json
import re
import pandas as pd
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from utilities.logging_config import get_logger
from utilities.graphql_utils import post_graphql, read_query, post_graphql_with_etag_refresh, get_part_etag, get_inventory_etag, get_lost_location_id, get_ion_location_id_for_tipqa

def is_part_number_protected(part_number: str, config: Dict[str, Any]) -> bool:
    """
    CRITICAL SAFETY FUNCTION: Check if a part number is protected from PART-to-TOOL conversion.
    This prevents accidental conversion of parts that should remain as parts.
    """
    if not part_number or not config:
        return False
    
    protected_part_numbers = config.get('sync_exceptions', {}).get('protected_part_numbers', [])
    if not protected_part_numbers:
        return False
    
    # Normalize both the part number and protected list for comparison
    part_number_normalized = part_number.upper().strip()
    protected_normalized = [pn.upper().strip() for pn in protected_part_numbers]
    
    return part_number_normalized in protected_normalized

def format_date_for_ion(date_value: Any) -> Optional[str]:
    """
    Format a date value for Ion GraphQL mutations.
    Ion expects DateTime format (ISO 8601 with time), not just date.
    
    Args:
        date_value: Can be a string, datetime object, or pandas Timestamp
        
    Returns:
        DateTime string in ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ), or None if invalid/empty
    """
    import pandas as pd
    from datetime import datetime
    
    if not date_value:
        return None
    
    # Handle pandas NaN/None values
    if pd.isna(date_value):
        return None
    
    # Convert to string and clean
    date_str = str(date_value).strip()
    if not date_str or date_str.lower() in ('nan', 'none', 'null', '<na>', ''):
        return None
    
    try:
        parsed_date = None
        was_date_only = False  # Track if original input was date-only
        
        # If it's already a datetime object or pandas Timestamp
        if isinstance(date_value, (datetime, pd.Timestamp)):
            parsed_date = date_value
        else:
            # Check if original string was date-only (no time component)
            original_has_time = 'T' in date_str or ' ' in date_str
            was_date_only = not original_has_time
            
            # Parse string date - handle various formats
            # First, try to extract date portion if it has time
            if 'T' in date_str:
                # ISO format with time: YYYY-MM-DDTHH:MM:SS
                try:
                    # Try parsing as ISO format
                    if date_str.endswith('Z'):
                        parsed_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    elif '+' in date_str or date_str.count('-') > 2:
                        # Has timezone info
                        parsed_date = datetime.fromisoformat(date_str)
                    else:
                        # No timezone, add it
                        parsed_date = datetime.fromisoformat(date_str)
                except ValueError:
                    # Fall back to date-only parsing
                    date_str = date_str.split('T')[0]
                    was_date_only = True
            elif ' ' in date_str:
                # Space-separated: YYYY-MM-DD HH:MM:SS
                try:
                    parsed_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        parsed_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f')
                    except ValueError:
                        # Just get the date part
                        date_str = date_str.split(' ')[0]
                        was_date_only = True
            
            # If we still don't have a parsed date, try parsing as date-only
            if parsed_date is None:
                was_date_only = True
                # Try to parse as date-only
                if len(date_str) >= 10:
                    date_part = date_str[:10]
                    try:
                        parsed_date = datetime.strptime(date_part, '%Y-%m-%d')
                    except ValueError:
                        pass
                
                if parsed_date is None:
                    # Try common date formats
                    for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d']:
                        try:
                            parsed_date = datetime.strptime(date_str, fmt)
                            break
                        except ValueError:
                            continue
        
        if parsed_date is None:
            log_and_print(f"Warning: Could not parse date '{date_value}'", 'warning')
            return None
        
        # Format as ISO 8601 DateTime with UTC timezone (Z suffix)
        # Ion requires DateTime format, so always include time component
        # If original value was date-only, set to midnight UTC
        if was_date_only:
            # It was a date-only string, set to midnight UTC
            return parsed_date.strftime('%Y-%m-%dT00:00:00Z')
        
        # It had time info or is a datetime object, format with time
        # Ensure it has timezone (Z for UTC)
        iso_str = parsed_date.strftime('%Y-%m-%dT%H:%M:%S')
        if not iso_str.endswith('Z') and '+' not in iso_str:
            iso_str += 'Z'
        return iso_str
            
    except (ValueError, TypeError) as e:
        # If all parsing fails, log warning and return None
        log_and_print(f"Warning: Could not parse date '{date_value}': {e}", 'warning')
        return None

def build_tipqa_inventory_attributes(tool_data: Dict) -> List[Dict]:
    """
    Build inventory-level attributes list from TipQA data for Ion inventory attributes.
    These are attributes that belong to the inventory level, not the part level.
    
    NOTE: Location is NOT included here - it's set via locationId field, not as an attribute.
    """
    import pandas as pd
    attributes = []
    
    # Map TipQA fields to Ion inventory-level attributes
    # NOTE: Location is handled via locationId field, not as an attribute
    
    # Asset Serial Number - check both prefixed and non-prefixed versions
    asset_serial = tool_data.get('asset_serial_number') or tool_data.get('tipqa_asset_serial_number')
    if asset_serial and pd.notna(asset_serial) and str(asset_serial).strip():
        attributes.append({
            'key': 'Asset Serial Number',
            'value': str(asset_serial).strip()
        })
    
    # Manufacturer - check both prefixed and non-prefixed versions
    manufacturer = tool_data.get('manufacturer') or tool_data.get('tipqa_manufacturer')
    if manufacturer and pd.notna(manufacturer) and str(manufacturer).strip():
        attributes.append({
            'key': 'Manufacturer',
            'value': str(manufacturer).strip()
        })
    
    # NOTE: Last Maintenance Date is NOT an inventory attribute
    # It's a field (lastMaintainedDate) that should be set directly in the mutation
    # Do NOT add it as an attribute here
    
    # Note: model_number, condition, status, date_added, last_updated, and notes 
    # are not available in the TipQA query - they were removed because they don't exist
    
    return attributes

def build_tipqa_attributes(tool_data: Dict) -> List[Dict]:
    """
    Build attributes list from TipQA data for Ion part attributes.
    
    IMPORTANT: In v2_production, "Asset Serial Number" and "Manufacturer" are NOT valid 
    part-level attributes - they are inventory-level attributes only.
    These should be set via build_tipqa_inventory_attributes() when creating inventory.
    
    Valid part-level attributes:
    - Asset Type
    - Service Interval Seconds (also set as maintenanceIntervalSeconds field)
    - Last Maintenance Date (also set as lastMaintainedDate field)
    
    NOT valid for parts (inventory-level only):
    - Asset Serial Number
    - Manufacturer
    - Location (set via locationId field, not as attribute)
    """
    attributes = []
    
    # Only include attributes that are valid at the part level
    # Asset Serial Number and Manufacturer are inventory-level only in v2_production
    if tool_data.get('asset_type'):
        attributes.append({
            'key': 'Asset Type',
            'value': str(tool_data.get('asset_type', ''))
        })
    
    # Note: service_interval_seconds is set as maintenanceIntervalSeconds field, not as attribute
    # Note: last_maintenance_date is set as lastMaintainedDate field, not as attribute
    # Note: location is set via locationId field, not as attribute
    
    return attributes

def build_tipqa_attributes_for_conversion(tool_data: Dict, part_id, token: str, config: Dict, environment: str) -> List[Dict]:
    """
    Build TipQA attributes merged with existing attribute etags from Ion.
    Ion's updatePart mutation requires etag on each attribute; this fetches
    the current attribute etags from the part and attaches them.
    """
    tipqa_attrs = build_tipqa_attributes(tool_data)
    if not tipqa_attrs:
        return tipqa_attrs

    part_query = read_query('get_part_etag.graphql')
    part_result = post_graphql(token, config, {'query': part_query, 'variables': {'id': part_id}}, environment)
    if 'errors' not in part_result:
        part_data = part_result.get('data', {}).get('part', {})
        existing_etags = {}
        for attr in part_data.get('attributes', []):
            attr_key = attr.get('key', '')
            attr_etag = attr.get('Etag', '') or attr.get('etag', '')
            if attr_key and attr_etag:
                existing_etags[attr_key] = attr_etag
        for attr in tipqa_attrs:
            etag = existing_etags.get(attr['key'])
            if etag:
                attr['etag'] = etag

    return tipqa_attrs

def safe_convert_service_interval(service_interval_value: Any) -> Optional[int]:
    """
    Safely convert service_interval_seconds to an integer for GraphQL mutations.
    Returns None if the value is missing, NaN, None, empty, or invalid.
    Only returns an integer if the value is a valid positive number.
    """
    if service_interval_value is None:
        return None
    
    # Check for NaN (pandas NaN)
    if pd.isna(service_interval_value):
        return None
    
    # Check for empty string
    if isinstance(service_interval_value, str) and not service_interval_value.strip():
        return None
    
    # Try to convert to int
    try:
        # Convert to float first to handle string numbers, then to int
        interval_float = float(service_interval_value)
        
        # Check for NaN or infinity
        if pd.isna(interval_float) or not math.isfinite(interval_float):
            return None
        
        # Convert to int, but only return if positive
        interval_int = int(interval_float)
        return interval_int if interval_int > 0 else None
    except (ValueError, TypeError):
        # If conversion fails, return None (field will be omitted)
        return None


def normalize_service_interval_for_comparison(value: Any) -> int:
    """
    Normalize service_interval_seconds to an integer for TipQA vs Ion comparison.
    Returns 0 for None, NaN, empty string, or invalid values; otherwise the integer value.
    Used to detect when the value has changed so we know to update Ion's part.maintenanceIntervalSeconds.
    """
    if value is None or pd.isna(value):
        return 0
    if isinstance(value, str) and not value.strip():
        return 0
    if isinstance(value, str) and value.strip().lower() in ('nan', 'none', 'null'):
        return 0
    try:
        n = int(float(value))
        return n if math.isfinite(n) else 0
    except (ValueError, TypeError):
        return 0


def _sync_part_service_interval_after_create(token: str, config: Dict, part_id: str, part_number: str, tool_data: Dict, environment: str) -> None:
    """
    After creating inventory for a pre-existing part, sync TipQA service_interval_seconds to the part's
    maintenanceIntervalSeconds in Ion if they differ. TipQA is the source of truth — sends null to clear.
    Non-blocking: logs warnings on failure.
    """
    tipqa_si_raw = tool_data.get('service_interval_seconds') or tool_data.get('tipqa_service_interval_seconds', '')
    service_interval = safe_convert_service_interval(tipqa_si_raw)
    refresh_query = read_query('get_part_etag.graphql')
    refresh_result = post_graphql(token, config, {'query': refresh_query, 'variables': {'id': part_id}}, environment)
    if 'errors' in refresh_result:
        log_and_print(f"Could not fetch part {part_number} to sync service interval: {refresh_result['errors']}", 'warning')
        return
    part_data = refresh_result.get('data', {}).get('part', {})
    current_interval = part_data.get('maintenanceIntervalSeconds')
    current_norm = None if (current_interval is None or pd.isna(current_interval) or str(current_interval).strip() == '') else int(float(current_interval))
    if current_norm == service_interval:
        return
    part_etag = part_data.get('_etag')
    if not part_etag:
        log_and_print(f"Cannot update service interval for part {part_number}: no etag", 'warning')
        return
    existing_attrs = part_data.get('attributes', [])
    updated_attributes = []
    for a in existing_attrs:
        if not a.get('key'):
            continue
        attr_etag = a.get('Etag', '') or a.get('etag', '')
        if attr_etag:
            updated_attributes.append({'key': a.get('key'), 'value': a.get('value', ''), 'etag': attr_etag})
        else:
            updated_attributes.append({'key': a.get('key'), 'value': a.get('value', '')})
    part_mutation = read_query('update_tool.graphql')
    part_variables_input = {
        'id': part_id,
        'etag': part_etag,
        'description': part_data.get('description', '') or '',
        'attributes': updated_attributes,
        'maintenanceIntervalSeconds': service_interval
    }
    part_result = post_graphql(token, config, {'query': part_mutation, 'variables': {'input': part_variables_input}}, environment)
    if 'errors' in part_result:
        log_and_print(f"Could not update maintenanceIntervalSeconds for part {part_number} after create: {part_result['errors']}", 'warning')
        return
    log_and_print(f"Synced maintenanceIntervalSeconds for part {part_number} to {service_interval} seconds (from TipQA after creating inventory)", 'info')


@dataclass
class ToolOperationResult:
    """Structured result for tool operations with explicit success/failure and error details."""
    success: bool
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    error_category: Optional[str] = None
    tool_id: Optional[str] = None
    inventory_id: Optional[str] = None
    action_taken: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    
    def __str__(self):
        if self.success:
            return f"SUCCESS: {self.action_taken or 'Operation completed'}"
        else:
            return f"ERROR [{self.error_code}]: {self.error_message}"
    
    @classmethod
    def success_result(cls, action: str, tool_id: str = None, inventory_id: str = None, details: Dict = None):
        """Create a successful result."""
        return cls(
            success=True,
            action_taken=action,
            tool_id=tool_id,
            inventory_id=inventory_id,
            details=details
        )
    
    @classmethod
    def error_result(cls, error_code: str, error_message: str, error_category: str = None, details: Dict = None):
        """Create an error result."""
        return cls(
            success=False,
            error_code=error_code,
            error_message=error_message,
            error_category=error_category,
            details=details
        )

def clean_serial_number(serial_number: str) -> str:
    """Clean and validate serial number."""
    # Handle NaN values and convert to string
    if pd.isna(serial_number) or serial_number is None:
        return ''
    
    # Convert to string and strip whitespace
    serial_number = str(serial_number)
    if not serial_number or serial_number.strip() == '':
        return ''
    
    # Remove extra whitespace and convert to string
    cleaned = str(serial_number).strip()
    
    # Handle common invalid values - treat "N/A", "NA" (no backslash), "n/a", etc. as missing
    # Blocked values: N/A, NA, NONE, NULL, <NA>, NAN, UNKNOWN
    cleaned_upper = cleaned.upper()
    if cleaned_upper in ('N/A', 'NA', 'NONE', 'NULL', '<NA>', 'NAN', 'UNKNOWN'):
        return ''
    
    # Handle common invalid values
    if cleaned.lower() in ['nan', 'none', 'null', '', 'unknown']:
        return ''
    
    return cleaned

def clean_part_number(part_number: str) -> str:
    """Clean and validate part number."""
    # Handle NaN values and convert to string
    if pd.isna(part_number) or part_number is None:
        return ''
    
    # Convert to string and strip whitespace
    part_number = str(part_number)
    if not part_number:
        return ''
    
    # Remove extra whitespace and convert to string
    cleaned = str(part_number).strip()
    
    # CRITICAL: Block whitespace-only values (spaces, tabs, newlines, carriage returns, etc.)
    # This prevents creating parts with blank/whitespace-only part numbers
    if not cleaned or cleaned.isspace():
        return ''
    
    # Handle common invalid values - treat "N/A", "NA" (no backslash), "n/a", etc. as missing
    # Blocked values: N/A, NA, NONE, NULL, <NA>, NAN (case-insensitive)
    # Convert to uppercase for case-insensitive comparison
    cleaned_upper = cleaned.upper().strip()
    if cleaned_upper in ('N/A', 'NA', 'NONE', 'NULL', '<NA>', 'NAN'):
        return ''
    
    return cleaned

def is_obsolete_serial_format(serial_number: str) -> bool:
    """Check if serial number is in obsolete XXXX-XXXX format."""
    if not serial_number:
        return False
    
    # Check for XXXX-XXXX pattern
    import re
    pattern = r'^\d{4}-\d{4}$'
    return bool(re.match(pattern, serial_number.strip()))

def is_tool_inactive(tool_data: Dict) -> bool:
    """Check if tool is inactive based on maintenance and revision status.
    
    A tool is considered inactive if:
    - maintenance_status == 'I' (Inactive)
    - revision_status == 'I' (Inactive)
    
    Note: Tools with maintenance_status 'L' (Lost) or 'QAHD' (QA Hold) are NOT considered inactive
    and should be included in the library.
    """
    maintenance_status = tool_data.get('maintenance_status', '')
    revision_status = tool_data.get('revision_status', '')
    
    # Check maintenance status - only 'I' is considered inactive
    if maintenance_status == 'I':
        return True
    
    # Check revision status - only 'I' is considered inactive
    if revision_status == 'I':
        return True
    
    return False

def escape_graphql_string(value: str) -> str:
    """Escape string for GraphQL queries."""
    if not value:
        return ''
    
    # Escape GraphQL special characters
    escaped = str(value).replace('\\', '\\\\')
    escaped = escaped.replace('"', '\\"')
    escaped = escaped.replace('\n', '\\n')
    escaped = escaped.replace('\r', '\\r')
    escaped = escaped.replace('\t', '\\t')
    
    return escaped

def check_tipqa_revision_problem_logic(tool_data: Dict, ion_data: Dict, stats: Dict, dry_run: bool = False) -> Optional[str]:
    """
    Check for TipQA revision problems that should cause tools to be skipped.
    Returns action to take or None if revision is valid.
    """
    serial_number = tool_data.get('serial_number', 'UNKNOWN')
    revision = tool_data.get('revision', '')
    
    # Check if revision is valid for Ion
    if not is_valid_revision(revision):
        log_and_print(f"Invalid revision '{revision}' in TipQA for {serial_number}. Skipping tool.", 'warning')
        return "SKIP"
    
    return None  # Revision is valid, continue with normal processing

def check_missing_part_number_logic(tool_data: Dict, ion_data: Dict, stats: Dict, dry_run: bool = False) -> Optional[str]:
    """
    Implement Step 2.1: Missing Part Number Check logic from flow document.
    Returns action to take or None if part number exists.
    """
    serial_number = tool_data.get('serial_number', 'UNKNOWN')
    part_number = clean_part_number(tool_data.get('part_number', ''))
    
    if not part_number:
        # Apply Step 2.1 logic: Check serial match and handle accordingly
        existing_records = ion_data['by_serial'].get(serial_number, []) if ion_data else []
        
        if existing_records:
            # Serial Match Found - check partType
            for record in existing_records:
                part_type = record.get('part', {}).get('partType', '')
                if part_type == 'TOOL':
                    log_and_print(f"No Part Number in TipQA for {serial_number}, but exists as TOOL in Ion. Marking as unavailable.", 'warning')
                    return "MARK_UNAVAILABLE"
                elif part_type == 'PART':
                    log_and_print(f"No Part Number in TipQA for {serial_number}, exists as PART in Ion. Skipping.", 'warning')
                    return "SKIP"
        else:
            # No Serial Match
            log_and_print(f"No Part Number in TipQA for {serial_number}, no tool in Ion. Skipping.", 'warning')
            return "SKIP"
    
    return None  # Part number exists, continue with normal processing

def handle_duplicate_tools_for_serial(serial_number: str, part_number: str, ion_data: Dict, stats: Dict) -> Dict[str, Any]:
    """
    Handle duplicate tools with the same serial number and part number.
    Returns the best tool to keep and marks others for cleanup.
    """
    existing_records = ion_data['by_serial'].get(serial_number, []) if ion_data else []
    
    if not existing_records:
        return {"best_tool": None, "duplicates_to_cleanup": []}
    
    # Find all tools with matching serial + part number (case-insensitive)
    exact_matches = []
    for inv in existing_records:
        if (inv['part']['partNumber'].strip().lower() == part_number.strip().lower() 
            and inv['serialNumber'].strip().lower() == serial_number.strip().lower()):
            exact_matches.append(inv)
    
    if len(exact_matches) <= 1:
        return {"best_tool": exact_matches[0] if exact_matches else None, "duplicates_to_cleanup": []}
    
    # Score tools to determine which one to keep
    scored_tools = []
    for tool in exact_matches:
        score = 0
        part = tool.get('part', {})
        
        # Higher score for more complete data
        if part.get('partNumber'):
            score += 10
        if part.get('revision'):
            score += 5
        if part.get('description'):
            score += 3
        if tool.get('serialNumber'):
            score += 10
        
        # Check for installations (tools with installations should be kept)
        installations = tool.get('abomInstallations', [])
        if installations:
            score += 20
            
        scored_tools.append((score, tool))
    
    # Sort by score (highest first) and keep the best one
    scored_tools.sort(key=lambda x: x[0], reverse=True)
    
    best_tool = scored_tools[0][1]
    duplicates_to_cleanup = [tool for score, tool in scored_tools[1:]]
    
    # Log the duplicate cleanup
    if duplicates_to_cleanup:
        log_and_print(f"Found {len(duplicates_to_cleanup)} duplicate tools for {serial_number} - will cleanup duplicates", 'warning')
        stats['duplicates_found'] = stats.get('duplicates_found', 0) + len(duplicates_to_cleanup)
    
    return {"best_tool": best_tool, "duplicates_to_cleanup": duplicates_to_cleanup}

def check_exact_match_logic(tool_data: Dict, ion_data: Dict, stats: Dict, config: Dict = None, ion_locations: list = None) -> Dict[str, Any]:
    """
    Implement the specific business logic flow with meaningful reasons.
    Returns match information and action to take.
    
    CRITICAL: This function ensures that TipQA data ALWAYS overwrites Ion data
    when there is a serial number + part number match, preventing duplicates.
    
    Args:
        tool_data: Tool data from TipQA
        ion_data: Ion tool data dictionary
        stats: Statistics dictionary
        config: Configuration dictionary
        ion_locations: List of Ion location dictionaries with 'name' field
    """
    serial_number = tool_data.get('serial_number', 'UNKNOWN')
    part_number = clean_part_number(tool_data.get('part_number', ''))
    location = (tool_data.get('location') or '').strip()
    maintenance_status = tool_data.get('maintenance_status', '')
    
    existing_records = ion_data['by_serial'].get(serial_number, []) if ion_data else []
    
    # Handle duplicates first - find the best tool to keep
    duplicate_result = handle_duplicate_tools_for_serial(serial_number, part_number, ion_data, stats)
    exact_match = duplicate_result['best_tool']
    duplicates_to_cleanup = duplicate_result['duplicates_to_cleanup']
    
    # Store cleanup information for later processing
    if duplicates_to_cleanup:
        stats['duplicates_to_cleanup'] = stats.get('duplicates_to_cleanup', []) + duplicates_to_cleanup
    
    if exact_match:
        # Exact match found (Serial + Part) - ALWAYS UPDATE, never create duplicate
        part_type = exact_match.get('part', {}).get('partType', '').upper()
        
        if part_type == 'TOOL':
            # CRITICAL: Check if tool is inactive FIRST (before any other logic)
            if is_tool_inactive(tool_data):
                # Check if Ion tool is already UNAVAILABLE - if so, skip
                ion_status = exact_match.get('status', '')
                if ion_status == 'UNAVAILABLE':
                    return {"match": exact_match, "action": "SKIP", "reason": "already_matches_tipqa"}
                else:
                    return {"match": exact_match, "action": "MARK_UNAVAILABLE", "reason": "inactive_in_tipqa"}
            
            # Check for specific conditions that require UPDATE_THEN_MARK_UNAVAILABLE
            if maintenance_status == 'OS':
                # Check if Ion tool is already up-to-date with TipQA data
                from utilities.shared_sync_utils import is_ion_tool_up_to_date
                if is_ion_tool_up_to_date(tool_data):
                    # Tool is up-to-date, just mark as unavailable
                    ion_status = exact_match.get('status', '')
                    if ion_status == 'UNAVAILABLE':
                        return {"match": exact_match, "action": "SKIP", "reason": "already_matches_tipqa"}
                    else:
                        return {"match": exact_match, "action": "MARK_UNAVAILABLE", "reason": "up_to_date_but_needs_unavailable"}
                else:
                    return {"match": exact_match, "action": "UPDATE_THEN_MARK_UNAVAILABLE", "reason": "offsite_in_tipqa"}
            elif location.lower() == 'lost' or 'lost' in location.lower():
                # Check if Ion tool is already up-to-date with TipQA data
                from utilities.shared_sync_utils import is_ion_tool_up_to_date
                if is_ion_tool_up_to_date(tool_data):
                    # Tool is up-to-date, just mark as unavailable
                    ion_status = exact_match.get('status', '')
                    if ion_status == 'UNAVAILABLE':
                        return {"match": exact_match, "action": "SKIP", "reason": "already_matches_tipqa"}
                    else:
                        return {"match": exact_match, "action": "MARK_UNAVAILABLE", "reason": "up_to_date_but_needs_unavailable"}
                else:
                    return {"match": exact_match, "action": "UPDATE_THEN_MARK_UNAVAILABLE", "reason": "lost_in_tipqa"}
            elif not location or location.strip() == '':
                # Check if Ion tool is already up-to-date with TipQA data
                from utilities.shared_sync_utils import is_ion_tool_up_to_date
                if is_ion_tool_up_to_date(tool_data):
                    # Tool is up-to-date, just mark as unavailable
                    ion_status = exact_match.get('status', '')
                    if ion_status == 'UNAVAILABLE':
                        return {"match": exact_match, "action": "SKIP", "reason": "already_matches_tipqa"}
                    else:
                        return {"match": exact_match, "action": "MARK_UNAVAILABLE", "reason": "up_to_date_but_needs_unavailable"}
                else:
                    return {"match": exact_match, "action": "UPDATE_THEN_MARK_UNAVAILABLE", "reason": "location_missing_in_tipqa"}
            else:
                # Check if TipQA location exists in Ion
                ion_location = exact_match.get('location', {}).get('name', '') if exact_match.get('location') else ''
                if ion_location and location.lower() != ion_location.lower():
                    # CRITICAL: Validate if TipQA location exists in Ion before moving to lost
                    tipqa_location_exists_in_ion = False
                    if ion_locations:
                        for ion_loc in ion_locations:
                            if isinstance(ion_loc, dict):
                                location_name = ion_loc.get('name', '') or ion_loc.get('name')
                            else:
                                location_name = str(ion_loc)
                            if location_name and location.lower() == location_name.lower():
                                tipqa_location_exists_in_ion = True
                                break
                    
                    if not tipqa_location_exists_in_ion:
                        # TipQA location doesn't exist in Ion - move to lost
                        from utilities.shared_sync_utils import is_ion_tool_up_to_date
                        if is_ion_tool_up_to_date(tool_data):
                            # Tool is up-to-date, just mark as unavailable
                            ion_status = exact_match.get('status', '')
                            if ion_status == 'UNAVAILABLE':
                                return {"match": exact_match, "action": "SKIP", "reason": "already_matches_tipqa"}
                            else:
                                return {"match": exact_match, "action": "MARK_UNAVAILABLE", "reason": "up_to_date_but_needs_unavailable"}
                        else:
                            return {"match": exact_match, "action": "UPDATE_THEN_MARK_UNAVAILABLE", "reason": "no_matching_location_in_ion"}
                    else:
                        # TipQA location exists in Ion - normal UPDATE
                        return {"match": exact_match, "action": "UPDATE", "reason": "exact_match_update"}
                else:
                    # CRITICAL: Always UPDATE existing tool when serial + part match
                    return {"match": exact_match, "action": "UPDATE", "reason": "exact_match_update"}
                    
        elif part_type == 'PART':
            # CRITICAL: Check if TipQA part number is empty - skip conversion if so
            if not part_number or part_number.strip() == '':
                log_and_print(f"Cannot convert PART to TOOL for {serial_number} - TipQA part number is empty", 'warning')
                return {"match": exact_match, "action": "SKIP", "reason": "empty_tipqa_part_number"}
            
            # CRITICAL SAFETY CHECK: Verify part number matches TipQA before allowing conversion
            ion_part_number = exact_match.get('part', {}).get('partNumber', '')
            if ion_part_number.strip().lower() != part_number.strip().lower():
                log_and_print(f"SAFETY CHECK FAILED: Cannot convert PART to TOOL for {serial_number} - Ion part number '{ion_part_number}' does not match TipQA part number '{part_number}'", 'error')
                return {"match": exact_match, "action": "SKIP", "reason": "part_number_mismatch_conversion_blocked"}
            
            # CRITICAL SAFETY CHECK: Check if part number is protected (should remain as part)
            # Note: We use part numbers (not part IDs) because IDs differ between environments
            if is_part_number_protected(part_number, config):
                log_and_print(f"CRITICAL SAFETY CHECK FAILED: Part number '{part_number}' is protected and cannot be converted to TOOL for {serial_number}. Skipping conversion.", 'error')
                return {"match": exact_match, "action": "SKIP", "reason": "protected_part_number"}
            
            # Convert PART to TOOL for exact matches (serial + part number match confirmed)
            log_and_print(f"SAFETY CHECKS PASSED: Converting PART to TOOL for {serial_number} - exact serial and part number match confirmed", 'info')
            return {"match": exact_match, "action": "CONVERT_PART_TO_TOOL", "reason": "exact_match_convert_part"}
        else:
            return {"match": exact_match, "action": "UPDATE", "reason": "exact_match_update"}
    else:
        # No exact match - check for serial-only match
        serial_match = None
        if existing_records:
            # Find the best serial match (prefer TOOL type, then first available)
            tool_matches = [inv for inv in existing_records if inv.get('part', {}).get('partType', '').upper() == 'TOOL']
            if tool_matches:
                serial_match = tool_matches[0]  # Take first TOOL match
            else:
                serial_match = existing_records[0]  # Take first match if no TOOL
        
        if serial_match:
            part_type = serial_match.get('part', {}).get('partType', '').upper()
            if part_type == 'TOOL':
                # CRITICAL: Check if tool is inactive FIRST (before any other logic)
                if is_tool_inactive(tool_data):
                    return {"match": serial_match, "action": "MARK_UNAVAILABLE", "reason": "inactive_in_tipqa"}
                
                # Serial matches but different part number - UPDATE the tool with new part number
                return {"match": serial_match, "action": "UPDATE", "reason": "serial_match_update_part"}
            else:
                # It's a PART in Ion but no exact match
                if not part_number or part_number.strip() == '':
                    return {"match": serial_match, "action": "SKIP", "reason": "no_part_number_part"}
                else:
                    return {"match": serial_match, "action": "SKIP", "reason": "part_mismatch_in_ion"}
        else:
            # No match found - check if tool is inactive before creating
            if is_tool_inactive(tool_data):
                return {"match": None, "action": "SKIP", "reason": "inactive_in_tipqa_no_ion"}
            else:
                # Will create new tool - check location status for proper reason
                if location.lower() == 'lost' or 'lost' in location.lower():
                    return {"match": None, "action": "SKIP", "reason": "lost_in_tipqa"}
                elif maintenance_status == 'OS':
                    return {"match": None, "action": "CREATE", "reason": "offsite_in_tipqa_mark_unavailable"}
                elif not location or location.strip() == '':
                    return {"match": None, "action": "CREATE", "reason": "location_missing_in_tipqa_mark_unavailable"}
                else:
                    return {"match": None, "action": "CREATE", "reason": "missing_in_ion"}

def analyze_orphaned_ion_tools(tipqa_data: Dict, ion_data: Dict, stats: Dict) -> List[Dict]:
    """
    Analyze Ion tools that don't exist in TipQA.
    Returns a list of actions for orphaned Ion tools.
    """
    orphaned_actions = []
    
    if not ion_data or 'by_serial' not in ion_data:
        return orphaned_actions
    
    # Create a set of TipQA serial numbers for fast lookup
    tipqa_serials = set()
    if tipqa_data and 'by_serial' in tipqa_data:
        tipqa_serials = set(tipqa_data['by_serial'].keys())
    
    # Check each Ion tool to see if it exists in TipQA
    for serial_number, ion_tools in ion_data['by_serial'].items():
        if serial_number not in tipqa_serials:
            # This Ion tool doesn't exist in TipQA
            for ion_tool in ion_tools:
                part_type = ion_tool.get('part', {}).get('partType', '').upper()
                
                # Only process TOOL type inventory (not PARTS)
                if part_type == 'TOOL':
                    # Check if tool has constraints that might prevent deletion
                    installations = ion_tool.get('abomInstallations', [])
                    has_installations = len(installations) > 0
                    
                    # Mark orphaned tools unavailable (never delete unless duplicate)
                    if has_installations:
                        action = "MARK_UNAVAILABLE"
                        reason = "missing_in_tipqa_with_installations"
                    else:
                        action = "MARK_UNAVAILABLE"
                        reason = "missing_in_tipqa"
                    
                    orphaned_action = {
                        'action_in_ion': action,
                        'reason': reason,
                        'serial_number': serial_number,
                        'part_number': ion_tool.get('part', {}).get('partNumber', ''),
                        'revision': ion_tool.get('part', {}).get('revision', ''),
                        'description': ion_tool.get('part', {}).get('description', ''),
                        'location': ion_tool.get('location', {}).get('name', '') if ion_tool.get('location') else '',
                        'maintenance_status': '',  # Not applicable for Ion-only tools
                        'model_number': '',
                        'manufacturer': '',
                        'condition': '',
                        'status': '',
                        'date_added': '',
                        'last_updated': '',
                        'notes': '',
                        # Ion fields
                        'ion_tool_id': ion_tool.get('id', ''),
                        'ion_serial_number': ion_tool.get('serialNumber', ''),
                        'ion_part_id': ion_tool.get('part', {}).get('id', '') if ion_tool.get('part') else '',
                        'ion_part_number': ion_tool.get('part', {}).get('partNumber', '') if ion_tool.get('part') else '',
                        'ion_revision': ion_tool.get('part', {}).get('revision', '') if ion_tool.get('part') else '',
                        'ion_description': ion_tool.get('part', {}).get('description', '') if ion_tool.get('part') else '',
                        'ion_part_type': ion_tool.get('part', {}).get('partType', '') if ion_tool.get('part') else '',
                        'ion_tracking_type': ion_tool.get('part', {}).get('trackingType', '') if ion_tool.get('part') else '',
                        'ion_location_id': ion_tool.get('location', {}).get('id', '') if ion_tool.get('location') else '',
                        'ion_location_name': ion_tool.get('location', {}).get('name', '') if ion_tool.get('location') else '',
                        'ion_status': ion_tool.get('status', ''),
                        'ion_unavailable': ion_tool.get('unavailable', False),
                        'ion_etag': ion_tool.get('_etag', ''),
                        'ion_installations': len(installations)
                    }
                    orphaned_actions.append(orphaned_action)
                    
                    # Update stats
                    stats['orphaned_ion_tools'] = stats.get('orphaned_ion_tools', 0) + 1
    
    return orphaned_actions

def mark_duplicate_as_unavailable(token: str, config: Dict[str, Any], duplicate_tool: Dict, 
                                 environment: str = 'v1_sandbox', dry_run: bool = True) -> bool:
    """
    Mark a duplicate tool as unavailable by appending '- DO NOT USE' to its description
    and moving it to the lost location.
    """
    from utilities.graphql_utils import get_lost_location_id
    
    tool_id = duplicate_tool.get('id')
    serial_number = duplicate_tool.get('serialNumber', 'UNKNOWN')
    etag = duplicate_tool.get('_etag')
    part = duplicate_tool.get('part', {})
    part_id = part.get('id')
    part_etag = part.get('_etag')
    
    if not tool_id or not etag or not part_id or not part_etag:
        log_and_print(f"Cannot mark tool {serial_number} as unavailable - missing required data", 'warning')
        return False
    
    try:
        if dry_run:
            log_and_print(f"DRY RUN: Would mark duplicate tool {serial_number} as unavailable with '- DO NOT USE' suffix", 'info')
            return True
        
        # Get lost location ID
        lost_location_id = get_lost_location_id(token, config, environment)
        if not lost_location_id:
            log_and_print(f"Cannot mark tool {serial_number} as unavailable - no lost location found", 'error')
            return False
        
        # Update the part description to append "- DO NOT USE"
        current_description = part.get('description', '')
        if '- DO NOT USE' not in current_description:
            new_description = f"{current_description} - DO NOT USE" if current_description else "- DO NOT USE"
            
            # Update the part description
            mutation = read_query('update_tool.graphql')
            variables = {
                'id': part_id,
                'etag': part_etag,
                'description': new_description
            }
            
            result = post_graphql(token, config, {'query': mutation, 'variables': variables}, environment)
            
            if 'errors' in result:
                log_and_print(f"Failed to update description for tool {serial_number}: {result['errors']}", 'error')
                return False
        
        # Update the inventory to mark as unavailable and move to lost location
            mutation = read_query('update_inventory_with_attributes.graphql')
        variables = {
            'id': tool_id,
            'etag': etag,
            'available': False,
            'locationId': lost_location_id
        }
        
        result = post_graphql(token, config, {'query': mutation, 'variables': variables}, environment)
        
        if 'errors' in result:
            log_and_print(f"Failed to mark tool {serial_number} as unavailable: {result['errors']}", 'error')
            return False
        
        log_and_print(f"Successfully marked duplicate tool {serial_number} as unavailable", 'info')
        return True
        
    except Exception as e:
        log_and_print(f"Exception marking tool {serial_number} as unavailable: {e}", 'error')
        return False

def cleanup_duplicate_tools(token: str, config: Dict[str, Any], duplicates_to_cleanup: List[Dict], 
                           environment: str = 'v1_sandbox', dry_run: bool = True) -> Dict[str, int]:
    """
    Clean up duplicate tools by deleting the lower-scored duplicates.
    Returns statistics about the cleanup operation.
    """
    from utilities.safety_utils import uninstall_tool_if_installed
    
    cleanup_stats = {
        'total_duplicates': len(duplicates_to_cleanup),
        'successful_deletions': 0,
        'failed_deletions': 0,
        'uninstalled_tools': 0,
        'marked_unavailable': 0
    }
    
    if not duplicates_to_cleanup:
        return cleanup_stats
    
    log_and_print(f"Starting cleanup of {len(duplicates_to_cleanup)} duplicate tools", 'info')
    
    for duplicate_tool in duplicates_to_cleanup:
        tool_id = duplicate_tool.get('id')
        serial_number = duplicate_tool.get('serialNumber', 'UNKNOWN')
        etag = duplicate_tool.get('_etag')
        
        if not tool_id or not etag:
            log_and_print(f"Skipping duplicate tool {serial_number} - missing ID or etag", 'warning')
            cleanup_stats['failed_deletions'] += 1
            continue
        
        try:
            # First, uninstall the tool if it's installed
            if not dry_run:
                uninstalled = uninstall_tool_if_installed(token, config, tool_id, serial_number, environment)
                if uninstalled:
                    cleanup_stats['uninstalled_tools'] += 1
                    log_and_print(f"Uninstalled duplicate tool {serial_number} before deletion", 'info')
            
            if dry_run:
                log_and_print(f"DRY RUN: Would delete duplicate tool {serial_number} (ID: {tool_id})", 'info')
                cleanup_stats['successful_deletions'] += 1
            else:
                # Delete the duplicate tool
                mutation = read_query('delete_inventory.graphql')
                variables = {
                    'id': tool_id,
                    'etag': etag
                }
                
                result = post_graphql(token, config, {'query': mutation, 'variables': variables}, environment)
                
                if 'errors' in result:
                    log_and_print(f"Failed to delete duplicate tool {serial_number}: {result['errors']}", 'error')
                    cleanup_stats['failed_deletions'] += 1
                    
                    # Mark as unavailable with "- DO NOT USE" suffix
                    mark_result = mark_duplicate_as_unavailable(token, config, duplicate_tool, environment, dry_run)
                    if mark_result:
                        cleanup_stats['marked_unavailable'] = cleanup_stats.get('marked_unavailable', 0) + 1
                        log_and_print(f"Marked duplicate tool {serial_number} as unavailable with '- DO NOT USE' suffix", 'info')
                else:
                    deleted_id = result.get('data', {}).get('deletePartInventory', {}).get('id')
                    if deleted_id:
                        log_and_print(f"Successfully deleted duplicate tool {serial_number}", 'info')
                        cleanup_stats['successful_deletions'] += 1
                    else:
                        log_and_print(f"Unexpected response when deleting duplicate tool {serial_number}", 'error')
                        cleanup_stats['failed_deletions'] += 1
                        
        except Exception as e:
            log_and_print(f"Exception deleting duplicate tool {serial_number}: {e}", 'error')
            cleanup_stats['failed_deletions'] += 1
    
    log_and_print(f"Duplicate cleanup completed: {cleanup_stats['successful_deletions']} deleted, {cleanup_stats['marked_unavailable']} marked unavailable, {cleanup_stats['failed_deletions']} failed", 'info')
    return cleanup_stats

def is_valid_revision(revision: str) -> bool:
    """
    Check if revision follows Ion format requirements.
    Ion requires ALPHABETICAL revisions only (not numeric).
    Ion accepts:
    - Empty revisions (will use default)
    - '-' (dash)
    - Alphabetical characters only (A-Z, uppercase preferred)
    - Cannot include numbers, periods, or other special characters
    """
    if not revision or revision.strip() == '':
        return True  # Empty revision is valid
    
    revision = revision.strip()
    
    # '-' is a valid revision in Ion
    if revision == '-':
        return True
    
    # Ion requires ALPHABETICAL revisions only - reject pure numeric revisions
    if revision.isdigit():
        return False  # Pure numbers are NOT valid - Ion requires alphabetical
    
    # Check if revision contains only alphabetic characters (no numbers, periods, or special chars)
    if not revision.replace(' ', '').isalpha():
        return False
    
    # For alphabetical revisions, uppercase is preferred but lowercase is also acceptable
    # Ion will accept both, but we prefer uppercase
    return True

def clean_revision(revision: str) -> str:
    """
    Clean revision to make it valid for Ion.
    Ion requires ALPHABETICAL revisions only.
    Converts all numeric revisions to 'A' (e.g., 001 -> A, 002 -> A, etc.)
    For invalid revisions, return 'A' as default (first alphabetical revision).
    """
    if not revision or revision.strip() == '':
        return 'A'  # Use 'A' as default revision
    
    revision = revision.strip()
    
    # Handle numeric revisions - convert all to 'A'
    # Ion requires alphabetical revisions only, so all numeric revisions become 'A'
    if revision.isdigit():
        log_and_print(f"Numeric revision '{revision}' converted to 'A' for Ion compatibility (Ion requires alphabetical revisions)", 'info')
        return 'A'
    
    # If it's already valid alphabetical, return as is (uppercase preferred)
    if is_valid_revision(revision):
        return revision.upper() if revision.isalpha() else revision
    
    # For invalid revisions (contains numbers, special chars, etc.), return 'A' as default
    # Ion requires alphabetical revisions only
    log_and_print(f"Invalid revision '{revision}' - using default revision 'A'", 'warning')
    return 'A'

def is_lot_tracked_part(part_number: str, config: Dict = None) -> bool:
    """
    Check if a part number requires lot tracking.
    This is based on business rules - some parts must be lot-tracked.
    """
    if config:
        lot_tracked_parts = config.get('sync_exceptions', {}).get('lot_tracked_parts', [])
        return part_number in lot_tracked_parts
    
    # Fallback to hardcoded list if config not provided
    lot_tracked_parts = [
        '6308-4200',  # Known from previous errors
        # Add more part numbers here as needed
    ]
    
    return part_number in lot_tracked_parts

def create_tool(token: str, config: Dict[str, Any], tool_data: Dict, environment: str = 'v1_sandbox', dry_run: bool = True, merged_df: pd.DataFrame = None) -> tuple[bool, str]:
    """
    Create a new tool in Ion following the flow document requirements.
    
    Returns:
        tuple[bool, str]: (success, error_message) - success is True if tool was created successfully,
                         error_message contains detailed error information if success is False
    """
    
    serial_number_raw = tool_data.get('serial_number', 'UNKNOWN')
    serial_number = clean_serial_number(serial_number_raw)
    part_number = clean_part_number(tool_data.get('part_number', ''))
    revision = tool_data.get('revision', '')
    
    # CRITICAL SAFETY CHECK: Do not create tools without valid serial numbers
    if not serial_number or serial_number.strip() == '':
        error_msg = f"Cannot create tool - serial_number is missing or invalid (was: {repr(serial_number_raw)})"
        log_and_print(f"ERROR: {error_msg}", 'error')
        return False, error_msg
    
    # CRITICAL SAFETY CHECK: Do not create tools without valid part numbers
    if not part_number or part_number.strip() == '':
        error_msg = f"Cannot create tool {serial_number} - part_number is missing or invalid (was: {repr(tool_data.get('part_number', ''))})"
        log_and_print(f"ERROR: {error_msg}", 'error')
        return False, error_msg
    
    # Handle edge cases
    cleaned_revision = clean_revision(revision)
    
    if dry_run:
        log_and_print(f"DRY RUN: Would create new tool {serial_number} with part {part_number}, revision '{cleaned_revision}'", 'info')
        return True, ''
    
    try:
        # Initialize variables to avoid scope issues
        inventory_id = None
        part_type = None
        
        # Check if this part requires lot tracking - if so, we'll update it to serial tracking
        requires_lot_tracking = is_lot_tracked_part(part_number, config)
        if requires_lot_tracking:
            log_and_print(f"Part {part_number} currently requires lot tracking - will update to serial tracking for {serial_number}", 'info')
        
        # Check if the part already exists using master dataframe first
        part_id = None
        part_etag = None
        part_was_pre_existing = False  # True if we used an existing part (so we may need to sync service_interval)
        
        if merged_df is not None:
            # Use master dataframe to find existing part
            part_matches = merged_df[
                (merged_df['ion_part_partNumber'].str.lower() == part_number.lower()) &
                (merged_df['ion_part_revision'].str.lower() == cleaned_revision.lower())
            ]
            
            if not part_matches.empty:
                # Get the first match
                part_match = part_matches.iloc[0]
                part_type = part_match.get('ion_part_partType', '')
                
                # CRITICAL SAFETY CHECK: Verify the existing part doesn't have an invalid part number
                existing_part_number = part_match.get('ion_part_partNumber', '').strip()
                if existing_part_number:
                    existing_part_cleaned = clean_part_number(existing_part_number)
                    if not existing_part_cleaned or existing_part_cleaned.strip() == '' or existing_part_cleaned.isspace():
                        error_msg = f"Found existing part with invalid or whitespace-only part_number '{repr(existing_part_number)}' for tool {serial_number}. Blocking use of this part."
                        log_and_print(f"ERROR: {error_msg}", 'error')
                        return False, error_msg
                    # Case-insensitive check for blocked values (catches "na", "Na", "nA", "N/A", "n/a", etc.)
                    existing_part_upper = existing_part_cleaned.upper().strip()
                    if existing_part_upper in ('N/A', 'NA', 'NONE', 'NULL', '<NA>', 'NAN'):
                        error_msg = f"Found existing part with blocked part_number '{existing_part_number}' for tool {serial_number}. Blocking use of this part."
                        log_and_print(f"ERROR: {error_msg}", 'error')
                        return False, error_msg
                
                # If it's a PART type, we need to convert it to TOOL (this is allowed)
                if part_type == 'PART':
                    # CRITICAL SAFETY CHECK: Verify part number matches TipQA exactly before conversion
                    ion_part_number = part_match.get('ion_part_partNumber', '').strip()
                    tipqa_part_number = part_number.strip()
                    
                    if ion_part_number.lower() != tipqa_part_number.lower():
                        error_msg = f"SAFETY CHECK FAILED: Cannot convert PART to TOOL - Ion part number '{ion_part_number}' does not match TipQA part number '{tipqa_part_number}'. PROTECTION: Skipping conversion to prevent incorrect part modification"
                        log_and_print(f"SAFETY CHECK FAILED: Cannot convert PART to TOOL - Ion part number '{ion_part_number}' does not match TipQA part number '{tipqa_part_number}'", 'error')
                        log_and_print(f"PROTECTION: Skipping conversion to prevent incorrect part modification", 'error')
                        return False, error_msg
                    
                    # Additional safety check: Verify revision matches
                    ion_revision = part_match.get('ion_part_revision', '').strip()
                    tipqa_revision = cleaned_revision.strip()
                    
                    if ion_revision.lower() != tipqa_revision.lower():
                        error_msg = f"SAFETY CHECK FAILED: Cannot convert PART to TOOL - Ion revision '{ion_revision}' does not match TipQA revision '{tipqa_revision}'. PROTECTION: Skipping conversion to prevent incorrect part modification"
                        log_and_print(f"SAFETY CHECK FAILED: Cannot convert PART to TOOL - Ion revision '{ion_revision}' does not match TipQA revision '{tipqa_revision}'", 'error')
                        log_and_print(f"PROTECTION: Skipping conversion to prevent incorrect part modification", 'error')
                        return False, error_msg
                    
                    log_and_print(f"SAFETY CHECKS PASSED: Converting PART {part_number} to TOOL for {serial_number}", 'info')
                
                part_id = part_match['ion_part_id']
                part_etag = part_match.get('ion_part_etag', '') or part_match.get('ion_part__etag', '')
                part_was_pre_existing = True
                log_and_print(f"Found existing {part_type} part {part_number} with ID {part_id} using master dataframe for tool {serial_number}", 'info')
                
                # If it's a PART type, convert it to TOOL
                if part_type == 'PART':
                    log_and_print(f"Converting PART {part_number} to TOOL for {serial_number}", 'info')
                    
                    # Get fresh etag before conversion
                    fresh_part_etag = get_part_etag(token, config, part_id, environment)
                    if not fresh_part_etag:
                        error_msg = f"Failed to get fresh etag for part {part_number}"
                        log_and_print(f"{error_msg}", 'error')
                        return False, error_msg
                    
                    # Step 1: Update trackingType to SERIAL first
                    log_and_print(f"Step 1: Updating trackingType to SERIAL for part {part_number}", 'info')
                    tracking_mutation = read_query('update_tool.graphql')
                    tracking_variables = {
                        'input': {
                            'id': part_id,
                            'etag': fresh_part_etag,
                            'trackingType': 'SERIAL'
                        }
                    }
                    
                    tracking_result = post_graphql_with_etag_refresh(token, config, {'query': tracking_mutation, 'variables': tracking_variables}, environment)
                    
                    if 'errors' in tracking_result:
                        error_msg = f"Failed to update trackingType for part {part_number}: {tracking_result['errors']}"
                        log_and_print(f"{error_msg}", 'error')
                        return False, error_msg
                    
                    # Get fresh etag after trackingType update
                    fresh_part_etag = tracking_result.get('data', {}).get('updatePart', {}).get('part', {}).get('_etag')
                    if not fresh_part_etag:
                        error_msg = f"Cannot get fresh etag after trackingType update for part {part_number}"
                        log_and_print(f"{error_msg}", 'error')
                        return False, error_msg
                    
                    # Step 2: Update any existing inventory items to be serial-tracked
                    log_and_print(f"Step 2: Ensuring all inventory items are serial-tracked for part {part_number}", 'info')
                    
                    # Get all inventory items for this part using approved query
                    # Use get_all_tool_inventory and filter by part number in code
                    get_inventory_query = read_query('get_all_tool_inventory.graphql')
                    inventory_variables = {'first': 1000, 'after': None}
                    inventory_result = post_graphql(token, config, {'query': get_inventory_query, 'variables': inventory_variables}, environment)
                    
                    if 'errors' in inventory_result:
                        log_and_print(f"Error querying inventory for part {part_number}: {inventory_result['errors']}", 'error')
                        inventory_items = []
                    else:
                        # Filter inventory items by part number and revision
                        all_inventory_edges = inventory_result.get('data', {}).get('partInventories', {}).get('edges', [])
                        inventory_items = []
                        for edge in all_inventory_edges:
                            inv_node = edge.get('node', {})
                            part_info = inv_node.get('part', {})
                            if part_info:
                                part_num = part_info.get('partNumber', '').lower()
                                part_rev = part_info.get('revision', '').lower()
                                # Match by part number and revision, and also check if part ID matches
                                if (part_num == part_number.lower() and 
                                    part_rev == cleaned_revision.lower() and
                                    part_info.get('id') == part_id):
                                    inventory_items.append(edge)
                        log_and_print(f"Found {len(inventory_items)} inventory items for part {part_number}", 'info')
                        
                        if len(inventory_items) == 0:
                            log_and_print(f"No inventory items found for part {part_number} - this might be why conversion is failing", 'warning')
                        
                        for edge in inventory_items:
                            inventory_item = edge.get('node', {})
                            inventory_id = inventory_item.get('id')
                            inventory_etag = inventory_item.get('_etag')
                            current_serial = inventory_item.get('serialNumber', '')
                            
                            log_and_print(f"Updating inventory item {inventory_id} (current serial: '{current_serial}') to be serial-tracked", 'info')
                            
                            # Update inventory item to ensure it's serial-tracked
                            update_inventory_mutation = read_query('update_inventory_with_attributes.graphql')
                            update_inventory_variables = {
                                'input': {
                                    'id': inventory_id,
                                    'etag': inventory_etag,
                                    'serialNumber': inventory_item.get('serialNumber', '')  # Ensure serial number is set
                                }
                            }
                            
                            inventory_update_result = post_graphql_with_etag_refresh(token, config, {'query': update_inventory_mutation, 'variables': update_inventory_variables}, environment)
                            
                            if 'errors' in inventory_update_result:
                                log_and_print(f"Warning: Failed to update inventory item {inventory_id} for part {part_number}: {inventory_update_result['errors']}", 'warning')
                            else:
                                log_and_print(f"Updated inventory item {inventory_id} to be serial-tracked", 'info')
                    
                    # CRITICAL SAFETY CHECK: Verify part number is not protected before conversion
                    if is_part_number_protected(part_number, config):
                        error_msg = f"CRITICAL SAFETY CHECK FAILED: Part number '{part_number}' is protected and cannot be converted to TOOL. Skipping conversion."
                        log_and_print(f"{error_msg}", 'error')
                        return False, error_msg
                    
                    # Step 3: Update partType to TOOL and other attributes
                    log_and_print(f"Step 3: Updating partType to TOOL for part {part_number}", 'info')
                    conversion_mutation = read_query('update_tool.graphql')
                    conversion_variables_input = {
                            'id': part_id,
                            'etag': fresh_part_etag,
                            'partType': 'TOOL',
                            'description': tool_data.get('description', ''),
                            'attributes': build_tipqa_attributes_for_conversion(tool_data, part_id, token, config, environment)
                        }
                    # Only include maintenanceIntervalSeconds if we have a valid integer value
                    service_interval = safe_convert_service_interval(tool_data.get('service_interval_seconds'))
                    if service_interval is not None:
                        conversion_variables_input['maintenanceIntervalSeconds'] = service_interval
                    conversion_variables = {'input': conversion_variables_input}
                    
                    conversion_result = post_graphql_with_etag_refresh(token, config, {'query': conversion_mutation, 'variables': conversion_variables}, environment)
                    
                    if 'errors' in conversion_result:
                        error_msg = f"Failed to convert PART {part_number} to TOOL: {conversion_result['errors']}"
                        log_and_print(f"{error_msg}", 'error')
                        return False, error_msg
                    
                    log_and_print(f"Successfully converted PART {part_number} to TOOL", 'info')
                    # Update part_etag to use the converted part's etag
                    part_etag = conversion_result.get('data', {}).get('updatePart', {}).get('part', {}).get('_etag')
        
        if part_id is None:
            # Fallback to API search if not found in master dataframe
            log_and_print(f"Part {part_number} not found in master dataframe, searching via API for tool {serial_number}", 'info')
            
            # Use approved query: get_tool_inventory_by_serial_and_part
            find_inventory_query = read_query('get_tool_inventory_by_serial_and_part.graphql')
            find_variables = {'serialNumber': serial_number, 'partNumber': part_number.lower(), 'first': 10, 'after': None}
            
            find_result = post_graphql(token, config, {'query': find_inventory_query, 'variables': find_variables}, environment)
            
            if 'errors' in find_result:
                error_msg = f"Failed to search for part {part_number}: {find_result['errors']}"
                log_and_print(f"{error_msg}", 'error')
                return False, error_msg
            
            inventory_edges = find_result.get('data', {}).get('partInventories', {}).get('edges', [])
            existing_parts = []
            # Extract part info from inventory and filter by revision
            for edge in inventory_edges:
                inv_node = edge.get('node', {})
                part_info = inv_node.get('part', {})
                if part_info and part_info.get('revision', '').lower() == cleaned_revision.lower():
                    existing_parts.append({'node': part_info})
            
            if existing_parts:
                part_node = existing_parts[0]['node']
                part_type = part_node.get('partType', '')
                
                # CRITICAL SAFETY CHECK: Verify the existing part doesn't have an invalid part number
                existing_part_number = part_node.get('partNumber', '').strip()
                if existing_part_number:
                    existing_part_cleaned = clean_part_number(existing_part_number)
                    if not existing_part_cleaned or existing_part_cleaned.strip() == '' or existing_part_cleaned.isspace():
                        error_msg = f"Found existing part with invalid or whitespace-only part_number '{repr(existing_part_number)}' for tool {serial_number}. Blocking use of this part."
                        log_and_print(f"ERROR: {error_msg}", 'error')
                        return False, error_msg
                    # Case-insensitive check for blocked values (catches "na", "Na", "nA", "N/A", "n/a", etc.)
                    existing_part_upper = existing_part_cleaned.upper().strip()
                    if existing_part_upper in ('N/A', 'NA', 'NONE', 'NULL', '<NA>', 'NAN'):
                        error_msg = f"Found existing part with blocked part_number '{existing_part_number}' for tool {serial_number}. Blocking use of this part."
                        log_and_print(f"ERROR: {error_msg}", 'error')
                        return False, error_msg
                
                # If it's a PART type, we need to convert it to TOOL (this is allowed)
                if part_type == 'PART':
                    log_and_print(f"Found PART type {part_number} - will convert to TOOL for {serial_number}", 'info')
                
                part_id = part_node['id']
                part_etag = part_node['_etag']
                log_and_print(f"Found existing {part_type} part {part_number} with ID {part_id} via API for tool {serial_number}", 'info')
        
        if part_id is None:
            # Part doesn't exist, create it
            log_and_print(f"Part {part_number} doesn't exist, creating new part for tool {serial_number}", 'info')
            
            # CRITICAL SAFETY CHECK: Verify part number is valid before creating part
            # This prevents creating parts with invalid part numbers like "NA" or whitespace-only
            cleaned_part_check = clean_part_number(part_number)
            if not cleaned_part_check or cleaned_part_check.strip() == '' or cleaned_part_check.isspace():
                error_msg = f"Cannot create part - part_number is missing, invalid, or whitespace-only (was: {repr(part_number)}). Blocking part creation."
                log_and_print(f"ERROR: {error_msg}", 'error')
                return False, error_msg
            
            # Additional check: Ensure the part number is not "NA" or any other blocked value (case-insensitive)
            # This catches variations like "na", "Na", "nA", "N/A", "n/a", "N/a", etc.
            part_upper_check = cleaned_part_check.upper().strip()
            if part_upper_check in ('N/A', 'NA', 'NONE', 'NULL', '<NA>', 'NAN'):
                error_msg = f"Cannot create part - part_number '{part_number}' is blocked (invalid value). Blocking part creation."
                log_and_print(f"ERROR: {error_msg}", 'error')
                return False, error_msg
            
            part_mutation = read_query('create_tool.graphql')
            
            # Build attributes from TipQA data
            attributes = build_tipqa_attributes(tool_data)
            
            # Determine tracking type - use SERIAL for lot-tracked parts, otherwise use SERIAL
            tracking_type = 'SERIAL'  # Always use SERIAL for tools
            
            # CRITICAL SAFETY CHECK: Verify part number is not protected before creating as TOOL
            if is_part_number_protected(part_number, config):
                error_msg = f"CRITICAL SAFETY CHECK FAILED: Part number '{part_number}' is protected and cannot be created as TOOL. Skipping creation."
                log_and_print(f"{error_msg}", 'error')
                return False, error_msg
            
            part_variables_input = {
                    'partNumber': part_number,
                    'revision': cleaned_revision,
                    'partType': 'TOOL',
                    'description': tool_data.get('description', ''),
                    'trackingType': tracking_type,
                    'attributes': attributes
                }
            # Only include maintenanceIntervalSeconds if we have a valid integer value
            service_interval = safe_convert_service_interval(tool_data.get('service_interval_seconds'))
            if service_interval is not None:
                part_variables_input['maintenanceIntervalSeconds'] = service_interval
            part_variables = {'input': part_variables_input}
            
            # Add a small random delay to prevent race conditions when creating the same part
            delay = random.uniform(0.1, 0.5)
            time.sleep(delay)
            
            part_result = post_graphql(token, config, {'query': part_mutation, 'variables': part_variables}, environment)
            
            if 'errors' in part_result:
                # Check if this is a part already exists error
                error_messages = [str(error) for error in part_result['errors']]
                if any('already exists' in msg for msg in error_messages):
                    log_and_print(f"Part {part_number} already exists (created by another process), searching for it...", 'warning')
                    # Try to find the existing part with multiple search strategies
                    part_id = None
                    part_etag = None
                    part_was_pre_existing = True
                    
                    # Strategy 0: Check master dataframe first (most efficient)
                    if merged_df is not None:
                        log_and_print(f"Strategy 0: Checking master dataframe for part {part_number} (revision {cleaned_revision})...", 'info')
                        part_matches = merged_df[
                            (merged_df['ion_part_partNumber'].str.lower() == part_number.lower()) &
                            (merged_df['ion_part_revision'].str.lower() == cleaned_revision.lower())
                        ]
                        
                        if not part_matches.empty:
                            part_match = part_matches.iloc[0]
                            part_id = part_match.get('ion_part_id', '')
                            part_etag = part_match.get('ion_part_etag', '')
                            found_part_type = part_match.get('ion_part_partType', '')
                            if part_id and part_etag:
                                log_and_print(f"Found existing part {part_number} in master dataframe: ID {part_id}, type {found_part_type}", 'info')
                                part_type = found_part_type if found_part_type else 'TOOL'
                    
                    # Strategy 1: Search for TOOL type inventory using serial/part combo (approved query)
                    if not part_id:
                        log_and_print(f"Strategy 1: Searching Ion API for TOOL type inventory with serial {serial_number} and part {part_number}...", 'info')
                        find_inventory_query = read_query('get_tool_inventory_by_serial_and_part.graphql')
                        # Use lowercase for search to match Ion's storage format
                        find_variables = {'serialNumber': serial_number, 'partNumber': part_number.lower(), 'first': 10, 'after': None}
                        
                        find_result = post_graphql(token, config, {'query': find_inventory_query, 'variables': find_variables}, environment)
                        
                        if 'errors' in find_result:
                            log_and_print(f"Strategy 1 failed: GraphQL errors: {find_result['errors']}", 'warning')
                        else:
                            inventory_edges = find_result.get('data', {}).get('partInventories', {}).get('edges', [])
                            if inventory_edges:
                                # Filter for TOOL type parts and matching revision
                                for edge in inventory_edges:
                                    inv_node = edge.get('node', {})
                                    part_info = inv_node.get('part', {})
                                    if part_info:
                                        part_type_check = part_info.get('partType', '').upper()
                                        part_revision = part_info.get('revision', '').lower()
                                        if part_type_check == 'TOOL' and part_revision == cleaned_revision.lower():
                                            part_id = part_info['id']
                                            part_etag = part_info['_etag']
                                            log_and_print(f"Found existing TOOL part {part_number} with ID {part_id} (created by another process)", 'info')
                                            part_type = 'TOOL'
                                            break
                                
                                if not part_id:
                                    log_and_print(f"Strategy 1: Found inventory but no matching TOOL part with revision '{cleaned_revision}'", 'warning')
                            else:
                                log_and_print(f"Strategy 1: No inventory found with serialNumber='{serial_number}' and partNumber='{part_number.lower()}'", 'warning')
                    
                    # Strategy 2: If not found as TOOL, search for PART type inventory using serial/part combo (approved query)
                    if not part_id:
                        log_and_print(f"Strategy 2: Searching for PART type inventory with serial {serial_number} and part {part_number} that can be converted...", 'info')
                        # Use approved query: get_tool_inventory_by_serial_and_part (works for both TOOL and PART types)
                        find_inventory_query = read_query('get_tool_inventory_by_serial_and_part.graphql')
                        # Use lowercase for search to match Ion's storage format
                        part_find_variables = {'serialNumber': serial_number, 'partNumber': part_number.lower(), 'first': 10, 'after': None}
                        part_find_result = post_graphql(token, config, {'query': find_inventory_query, 'variables': part_find_variables}, environment)
                        
                        existing_part_parts = []
                        if 'errors' in part_find_result:
                            log_and_print(f"Strategy 2 failed: GraphQL errors: {part_find_result['errors']}", 'warning')
                        else:
                            inventory_edges = part_find_result.get('data', {}).get('partInventories', {}).get('edges', [])
                            # Filter for PART type only and matching revision
                            for edge in inventory_edges:
                                inv_node = edge.get('node', {})
                                part_info = inv_node.get('part', {})
                                if part_info:
                                    part_type_check = part_info.get('partType', '').upper()
                                    part_revision = part_info.get('revision', '').lower()
                                    if part_type_check == 'PART' and part_revision == cleaned_revision.lower():
                                        existing_part_parts.append(edge)
                            
                            if existing_part_parts:
                                log_and_print(f"Strategy 2: Found {len(existing_part_parts)} PART type inventory item(s)", 'info')
                            else:
                                log_and_print(f"Strategy 2: No PART type inventory found with serialNumber='{serial_number}', partNumber='{part_number.lower()}' and revision='{cleaned_revision.lower()}'", 'warning')
                        
                        if existing_part_parts:
                            # Extract part info from inventory edge
                            inv_node = existing_part_parts[0].get('node', {})
                            part_node = inv_node.get('part', {})
                            part_id = part_node['id']
                            part_etag = part_node['_etag']
                            # Guard: if already TOOL/SERIAL, skip conversion
                            existing_part_type = (part_node.get('partType') or '').upper()
                            existing_tracking_type = (part_node.get('trackingType') or '').upper()
                            if existing_part_type == 'TOOL' and existing_tracking_type == 'SERIAL':
                                log_and_print(f"Part {part_number} (ID {part_id}) already TOOL/SERIAL - skipping conversion", 'info')
                                return True, ''
                            log_and_print(f"Found existing PART {part_number} with ID {part_id} - converting to TOOL", 'info')
                            
                            # CRITICAL SAFETY CHECK: Verify part number matches TipQA exactly before conversion
                            ion_part_number = part_node.get('partNumber', '').strip()
                            tipqa_part_number = part_number.strip()
                            
                            if ion_part_number.lower() != tipqa_part_number.lower():
                                error_msg = f"SAFETY CHECK FAILED: Cannot convert PART to TOOL - Ion part number '{ion_part_number}' does not match TipQA part number '{tipqa_part_number}'. PROTECTION: Skipping conversion to prevent incorrect part modification"
                                log_and_print(f"SAFETY CHECK FAILED: Cannot convert PART to TOOL - Ion part number '{ion_part_number}' does not match TipQA part number '{tipqa_part_number}'", 'error')
                                log_and_print(f"PROTECTION: Skipping conversion to prevent incorrect part modification", 'error')
                                return False, error_msg
                            
                            # Additional safety check: Verify revision matches
                            ion_revision = part_node.get('revision', '').strip()
                            tipqa_revision = cleaned_revision.strip()
                            
                            if ion_revision.lower() != tipqa_revision.lower():
                                error_msg = f"SAFETY CHECK FAILED: Cannot convert PART to TOOL - Ion revision '{ion_revision}' does not match TipQA revision '{tipqa_revision}'. PROTECTION: Skipping conversion to prevent incorrect part modification"
                                log_and_print(f"SAFETY CHECK FAILED: Cannot convert PART to TOOL - Ion revision '{ion_revision}' does not match TipQA revision '{tipqa_revision}'", 'error')
                                log_and_print(f"PROTECTION: Skipping conversion to prevent incorrect part modification", 'error')
                                return False, error_msg
                            
                            log_and_print(f"SAFETY CHECKS PASSED: Converting PART {part_number} to TOOL", 'info')
                            
                            # Get fresh etag before conversion
                            fresh_part_etag = get_part_etag(token, config, part_id, environment)
                            if not fresh_part_etag:
                                error_msg = f"Failed to get fresh etag for part {part_number}"
                                log_and_print(f"{error_msg}", 'error')
                                return False, error_msg
                            
                            # Step 0: Update any existing inventory items to be serial-tracked FIRST
                            log_and_print(f"Step 0: Ensuring all inventory items are serial-tracked for part {part_number}", 'info')
                            
                            # Get all inventory items for this part using approved query
                            # Use get_all_tool_inventory and filter by part number in code
                            get_inventory_query = read_query('get_all_tool_inventory.graphql')
                            inventory_variables = {'first': 1000, 'after': None}
                            inventory_result = post_graphql(token, config, {'query': get_inventory_query, 'variables': inventory_variables}, environment)
                            
                            if 'errors' in inventory_result:
                                log_and_print(f"Error querying inventory for part {part_number}: {inventory_result['errors']}", 'error')
                                inventory_items = []
                            else:
                                # Filter inventory items by part number and revision
                                all_inventory_edges = inventory_result.get('data', {}).get('partInventories', {}).get('edges', [])
                                inventory_items = []
                                for edge in all_inventory_edges:
                                    inv_node = edge.get('node', {})
                                    part_info = inv_node.get('part', {})
                                    if part_info:
                                        part_num = part_info.get('partNumber', '').lower()
                                        part_rev = part_info.get('revision', '').lower()
                                        # Match by part number and revision, and also check if part ID matches
                                        if (part_num == part_number.lower() and 
                                            part_rev == cleaned_revision.lower() and
                                            part_info.get('id') == part_id):
                                            inventory_items.append(edge)
                                log_and_print(f"Found {len(inventory_items)} inventory items for part {part_number}", 'info')
                                
                                if len(inventory_items) == 0:
                                    log_and_print(f"No inventory items found for part {part_number} - this might be why conversion is failing", 'warning')
                                
                                for edge in inventory_items:
                                    inventory_item = edge.get('node', {})
                                    inventory_id = inventory_item.get('id')
                                    inventory_etag = inventory_item.get('_etag')
                                    current_serial = inventory_item.get('serialNumber', '')
                                    
                                    # Check if inventory item already has a serial number
                                    original_serial = inventory_item.get('serialNumber', '')
                                    # Handle various representations of "no serial number"
                                    if (original_serial and 
                                        original_serial != 'None' and 
                                        original_serial != 'null' and 
                                        original_serial.strip() != '' and
                                        original_serial.lower() != 'none'):
                                        # Inventory item already has a serial number - skip updating it
                                        log_and_print(f"Inventory item {inventory_id} already has serial number '{original_serial}' - skipping update", 'info')
                                        continue
                                    else:
                                        # Inventory item has no serial number - skip conversion entirely
                                        log_and_print(f"Inventory item {inventory_id} has no serial number (current: '{original_serial}') - skipping PART-to-TOOL conversion", 'warning')
                                        log_and_print(f"Cannot convert PART {part_number} to TOOL due to inventory item without serial number", 'error')
                                        return False
                                    
                                    log_and_print(f"Updating inventory item {inventory_id} (current serial: '{current_serial}') to be serial-tracked", 'info')
                                    
                                    # Update inventory item to ensure it's serial-tracked
                                    update_inventory_mutation = read_query('update_inventory_with_attributes.graphql')
                                    update_inventory_variables = {
                                        'input': {
                                            'id': inventory_id,
                                            'etag': inventory_etag,
                                            'serialNumber': current_serial,  # Use the existing serial number
                                            'quantity': 1  # Serialized parts must have quantity = 1
                                        }
                                    }
                                    
                                    inventory_update_result = post_graphql_with_etag_refresh(token, config, {'query': update_inventory_mutation, 'variables': update_inventory_variables}, environment)
                                    
                                    if 'errors' in inventory_update_result:
                                        log_and_print(f"Warning: Failed to update inventory item {inventory_id} for part {part_number}: {inventory_update_result['errors']}", 'warning')
                                    else:
                                        log_and_print(f"Updated inventory item {inventory_id} to be serial-tracked", 'info')
                            
                            # Step 1: Update trackingType to SERIAL first
                            log_and_print(f"Step 1: Updating trackingType to SERIAL for part {part_number}", 'info')
                            tracking_mutation = read_query('update_tool.graphql')
                            tracking_variables = {
                                'input': {
                                    'id': part_id,
                                    'etag': fresh_part_etag,
                                    'trackingType': 'SERIAL'
                                }
                            }
                            
                            tracking_result = post_graphql_with_etag_refresh(token, config, {'query': tracking_mutation, 'variables': tracking_variables}, environment)
                            
                            if 'errors' in tracking_result:
                                error_msg = f"Failed to update trackingType for part {part_number}: {tracking_result['errors']}"
                                log_and_print(f"{error_msg}", 'error')
                                return False, error_msg
                            
                            # Get fresh etag after trackingType update
                            fresh_part_etag = tracking_result.get('data', {}).get('updatePart', {}).get('part', {}).get('_etag')
                            if not fresh_part_etag:
                                error_msg = f"Cannot get fresh etag after trackingType update for part {part_number}"
                                log_and_print(f"{error_msg}", 'error')
                                return False, error_msg
                            
                            # CRITICAL SAFETY CHECK: Verify part number is not protected before conversion
                            if is_part_number_protected(part_number, config):
                                error_msg = f"CRITICAL SAFETY CHECK FAILED: Part number '{part_number}' is protected and cannot be converted to TOOL. Skipping conversion."
                                log_and_print(f"{error_msg}", 'error')
                                return False, error_msg
                            
                            # Step 2: Update partType to TOOL and other attributes
                            log_and_print(f"Step 2: Updating partType to TOOL for part {part_number}", 'info')
                            conversion_mutation = read_query('update_tool.graphql')
                            conversion_variables_input = {
                                    'id': part_id,
                                    'etag': fresh_part_etag,
                                    'partType': 'TOOL',
                                    'description': tool_data.get('description', ''),
                                    'attributes': build_tipqa_attributes_for_conversion(tool_data, part_id, token, config, environment)
                                }
                            # Only include maintenanceIntervalSeconds if we have a valid integer value
                            service_interval = safe_convert_service_interval(tool_data.get('service_interval_seconds'))
                            if service_interval is not None:
                                conversion_variables_input['maintenanceIntervalSeconds'] = service_interval
                            conversion_variables = {'input': conversion_variables_input}
                            
                            conversion_result = post_graphql_with_etag_refresh(token, config, {'query': conversion_mutation, 'variables': conversion_variables}, environment)
                            
                            if 'errors' in conversion_result:
                                error_msg = f"Failed to convert PART {part_number} to TOOL: {conversion_result['errors']}"
                                log_and_print(f"{error_msg}", 'error')
                                return False, error_msg
                            
                            log_and_print(f"Successfully converted PART {part_number} to TOOL", 'info')
                            # Update part_etag to use the converted part's etag
                            part_etag = conversion_result.get('data', {}).get('updatePart', {}).get('part', {}).get('_etag')
                            # Set part_type to TOOL after successful conversion
                            part_type = 'TOOL'
                    
                    # Strategy 3: If not found, try searching without revision filter using approved query
                    # Note: get_tool_inventory_by_serial_and_part doesn't filter by revision, so we'll filter in code
                    if not part_id:
                        log_and_print(f"Strategy 3: Searching inventory without revision filter for serial {serial_number} and part {part_number}...", 'info')
                        # Use approved query: get_tool_inventory_by_serial_and_part (doesn't filter by revision)
                        find_inventory_query = read_query('get_tool_inventory_by_serial_and_part.graphql')
                        find_variables_no_revision = {'serialNumber': serial_number, 'partNumber': part_number.lower(), 'first': 10, 'after': None}
                        find_result_no_revision = post_graphql(token, config, {'query': find_inventory_query, 'variables': find_variables_no_revision}, environment)
                        
                        if 'errors' in find_result_no_revision:
                            log_and_print(f"Strategy 3 failed: GraphQL errors: {find_result_no_revision['errors']}", 'warning')
                        else:
                            inventory_edges = find_result_no_revision.get('data', {}).get('partInventories', {}).get('edges', [])
                            if inventory_edges:
                                # Find the part with matching revision (case-insensitive) - check both TOOL and PART types
                                for edge in inventory_edges:
                                    inv_node = edge.get('node', {})
                                    part_info = inv_node.get('part', {})
                                    if part_info and part_info.get('revision', '').lower() == cleaned_revision.lower():
                                        found_part_type = part_info.get('partType', '').upper()
                                        part_id = part_info['id']
                                        part_etag = part_info['_etag']
                                        log_and_print(f"Strategy 3: Found existing {found_part_type} part {part_number} with ID {part_id} (no revision filter)", 'info')
                                        part_type = found_part_type if found_part_type else 'TOOL'
                                        break
                                
                                if not part_id:
                                    log_and_print(f"Strategy 3: Found {len(inventory_edges)} inventory items but none match revision '{cleaned_revision}'", 'warning')
                            else:
                                log_and_print(f"Strategy 3: No inventory found with serialNumber='{serial_number}' and partNumber='{part_number.lower()}'", 'warning')
                        
                        # Strategy 3 already checks both TOOL and PART types, so no separate PART search needed
                    
                    # Strategy 5: Search for parts through ALL inventory items (paginated) - NEW STRATEGY
                    # This handles cases where the part exists but our serial doesn't have inventory for it yet
                    # We search through inventory items from OTHER serials to find the part
                    if not part_id:
                        log_and_print(f"Strategy 5: Searching for part through all inventory items (any serial) for part {part_number} (revision {cleaned_revision})...", 'info')
                        # Use get_all_tool_inventory with pagination to search through more items
                        get_all_inventory_query = read_query('get_all_tool_inventory.graphql')
                        found_parts = {}
                        page_count = 0
                        max_pages = 10  # Search up to 10,000 inventory items (10 pages * 1000)
                        cursor = None
                        
                        while page_count < max_pages and not found_parts:
                            all_inventory_variables = {'first': 1000, 'after': cursor}
                            all_inventory_result = post_graphql(token, config, {'query': get_all_inventory_query, 'variables': all_inventory_variables}, environment)
                            
                            if 'errors' in all_inventory_result:
                                log_and_print(f"Strategy 5 failed on page {page_count + 1}: GraphQL errors: {all_inventory_result['errors']}", 'warning')
                                break
                            
                            all_inventory_edges = all_inventory_result.get('data', {}).get('partInventories', {}).get('edges', [])
                            if not all_inventory_edges:
                                break
                            
                            # Search through inventory items to find parts matching our part number and revision
                            for edge in all_inventory_edges:
                                inv_node = edge.get('node', {})
                                part_info = inv_node.get('part', {})
                                if part_info:
                                    part_num = part_info.get('partNumber', '').lower()
                                    part_rev = part_info.get('revision', '').lower()
                                    part_info_id = part_info.get('id')
                                    
                                    # Match part number and revision (case-insensitive)
                                    if (part_num == part_number.lower() and 
                                        part_rev == cleaned_revision.lower() and
                                        part_info_id not in found_parts):
                                        # Found matching part - store it
                                        found_parts[part_info_id] = {
                                            'id': part_info_id,
                                            '_etag': part_info.get('_etag', ''),
                                            'partType': part_info.get('partType', 'TOOL').upper()
                                        }
                            
                            # Check if there are more pages
                            page_info = all_inventory_result.get('data', {}).get('partInventories', {}).get('pageInfo', {})
                            if page_info.get('hasNextPage', False):
                                cursor = page_info.get('endCursor')
                                page_count += 1
                            else:
                                break
                            
                            # If we found the part, stop paginating
                            if found_parts:
                                            break
                    
                        if found_parts:
                            # Use the first matching part
                            part_info_found = list(found_parts.values())[0]
                            part_id = part_info_found['id']
                            part_etag = part_info_found['_etag']
                            part_type = part_info_found['partType']
                            log_and_print(f"Strategy 5: Found existing {part_type} part {part_number} with ID {part_id} after searching {page_count + 1} page(s) of inventory", 'info')
                        else:
                            log_and_print(f"Strategy 5: Searched {page_count + 1} page(s) of inventory but no part matches partNumber='{part_number.lower()}' and revision='{cleaned_revision.lower()}'", 'warning')
                    
                    # Strategy 4: If still not found, try to extract part ID from the creation error response
                    if not part_id:
                        log_and_print(f"Strategy 4: Checking if part ID is in error response...", 'info')
                        # Sometimes the error response contains the part ID that was created
                        for error in part_result['errors']:
                            error_str = str(error)
                            if 'part' in error_str.lower() and 'id' in error_str.lower():
                                # Try to extract part ID from error message
                                import re
                                id_match = re.search(r'id[:\s]+(\d+)', error_str, re.IGNORECASE)
                                if id_match:
                                    potential_part_id = id_match.group(1)
                                    log_and_print(f"Found potential part ID {potential_part_id} in error response, attempting to use it...", 'info')
                                    # Try to get the part details using this ID
                                    test_etag = get_part_etag(token, config, potential_part_id, environment)
                                    if test_etag:
                                        part_id = potential_part_id
                                        part_etag = test_etag
                                        log_and_print(f"Successfully found part {part_number} with ID {part_id} from error response", 'info')
                                        # Set part_type to TOOL (assuming it's a TOOL since it was created)
                                        part_type = 'TOOL'
                                        break
                    
                    # Strategy 6: Query Ion directly for the part by part number and revision (partType = TOOL only)
                    # This queries parts directly (not through inventory), so it works even if the part has no inventory
                    if not part_id:
                        log_and_print(f"Strategy 6: Querying Ion directly for part {part_number} (revision {cleaned_revision}) with partType = TOOL...", 'info')
                        get_part_query = read_query('get_part_by_number_and_revision.graphql')
                        # Use lowercase for part number to match Ion's storage format
                        part_variables = {
                            'partNumber': part_number.lower(),
                            'revision': cleaned_revision.lower(),
                            'first': 10,  # We only need one match
                            'after': None
                        }
                        
                        part_search_result = post_graphql(token, config, {'query': get_part_query, 'variables': part_variables}, environment)
                        
                        if 'errors' in part_search_result:
                            log_and_print(f"Strategy 6 failed: GraphQL errors: {part_search_result['errors']}", 'warning')
                        else:
                            # Query uses 'parts' not 'partInventories', so access data.parts
                            part_edges = part_search_result.get('data', {}).get('parts', {}).get('edges', [])
                            if part_edges:
                                # Extract part info from the first match (node is the part directly, not nested)
                                first_edge = part_edges[0]
                                part_info = first_edge.get('node', {})
                                if part_info:
                                    part_id = part_info.get('id')
                                    part_etag = part_info.get('_etag', '')
                                    part_type = part_info.get('partType', 'TOOL').upper()
                                    log_and_print(f"Strategy 6: Found existing {part_type} part {part_number} with ID {part_id} via direct parts query", 'info')
                            else:
                                log_and_print(f"Strategy 6: No part found with partNumber='{part_number.lower()}', revision='{cleaned_revision.lower()}', and partType=TOOL", 'warning')
                    
                    # Strategy 7: Query parts directly WITHOUT partType filter (catches PART-type parts with no inventory)
                    # Only auto-converts PART→TOOL if the serial number confirms it's a tool (e.g., JT prefix)
                    if not part_id:
                        log_and_print(f"Strategy 7: Querying Ion directly for part {part_number} (revision {cleaned_revision}) with ANY partType...", 'info')
                        get_part_any_type_query = read_query('get_part_by_number_and_revision_any_type.graphql')
                        part_variables_any = {
                            'partNumber': part_number.lower(),
                            'revision': cleaned_revision.lower(),
                            'first': 10,
                            'after': None
                        }
                        
                        part_search_any = post_graphql(token, config, {'query': get_part_any_type_query, 'variables': part_variables_any}, environment)
                        
                        if 'errors' in part_search_any:
                            log_and_print(f"Strategy 7 failed: GraphQL errors: {part_search_any['errors']}", 'warning')
                        else:
                            part_edges_any = part_search_any.get('data', {}).get('parts', {}).get('edges', [])
                            if part_edges_any:
                                first_edge_any = part_edges_any[0]
                                part_info_any = first_edge_any.get('node', {})
                                if part_info_any:
                                    found_part_type = part_info_any.get('partType', '').upper()
                                    found_part_id = part_info_any.get('id')
                                    log_and_print(f"Strategy 7: Found part {part_number} with ID {found_part_id}, partType={found_part_type}", 'info')
                                    
                                    if found_part_type == 'PART':
                                        log_and_print(f"Strategy 7: Found PART-type part {part_number} (ID {found_part_id}) - will convert to TOOL for serial {serial_number}", 'info')
                                        part_id = found_part_id
                                        part_etag = part_info_any.get('_etag', '')
                                        part_type = 'PART'
                                    else:
                                        part_id = found_part_id
                                        part_etag = part_info_any.get('_etag', '')
                                        part_type = found_part_type
                            else:
                                log_and_print(f"Strategy 7: No part found with partNumber='{part_number.lower()}', revision='{cleaned_revision.lower()}' (any type)", 'warning')
                    
                    if not part_id:
                        error_messages = [str(error) for error in part_result.get('errors', [])]
                        log_and_print(f"ERROR: Part {part_number} (revision {cleaned_revision}) creation failed and not found in any search strategy", 'error')
                        log_and_print(f"  Attempted strategies:", 'error')
                        log_and_print(f"    - Strategy 0: Master dataframe check", 'error')
                        log_and_print(f"    - Strategy 1: Ion API search for TOOL type (case-insensitive)", 'error')
                        log_and_print(f"    - Strategy 2: Ion API search for PART type (case-insensitive)", 'error')
                        log_and_print(f"    - Strategy 3: Search without revision filter (case-insensitive)", 'error')
                        log_and_print(f"    - Strategy 4: Extract part ID from error response", 'error')
                        log_and_print(f"    - Strategy 5: Paginated search through all inventory (up to 10K items)", 'error')
                        log_and_print(f"    - Strategy 6: Direct query for part by part number and revision (partType = TOOL only)", 'error')
                        log_and_print(f"    - Strategy 7: Direct query for part by part number and revision (any partType)", 'error')
                        log_and_print(f"  Part creation errors: {error_messages}", 'error')
                        log_and_print(f"  This suggests the part exists in Ion but has no inventory items, or is beyond the search limit.", 'error')
                        log_and_print(f"  Possible causes: Part exists but has no inventory, or part is beyond first 10K inventory items.", 'error')
                        error_msg = f"ACTION: Skipping tool {serial_number} - cannot create inventory without part ID. Part creation errors: {error_messages}"
                        log_and_print(f"  {error_msg}", 'error')
                        return False, error_msg
                else:
                    error_messages = [str(error) for error in part_result.get('errors', [])]
                    error_msg = f"Failed to create part {part_number} for tool {serial_number}: {error_messages}"
                    log_and_print(f"{error_msg}", 'error')
                    return False, error_msg
            else:
                part_id = part_result.get('data', {}).get('createPart', {}).get('part', {}).get('id')
                part_was_pre_existing = False
                if not part_id:
                    error_msg = f"Unexpected response when creating part for tool {serial_number} - no part ID in response"
                    log_and_print(f"{error_msg}", 'error')
                    return False, error_msg
                log_and_print(f"Successfully created part {part_number} with ID {part_id} for tool {serial_number}", 'info')
        else:
            # Part exists - check if we need to update it or just use it
            # If part is already TOOL type, skip update (parts from library are already correct)
            # Only update if part is PART type (needs conversion) or if we need to sync attributes
            log_and_print(f"Part {part_number} already exists with ID {part_id} for tool {serial_number}", 'info')
            
            # Only update part if it's PART type (needs conversion to TOOL)
            # Otherwise, just use the existing part to create inventory
            if part_type == 'PART':
                log_and_print(f"Part {part_number} is PART type - will convert to TOOL (handled below)", 'info')
                # Conversion will be handled in the section below
            else:
                # Part is already TOOL type - skip update, just use it
                log_and_print(f"Part {part_number} is already TOOL type - using existing part without update", 'info')
        
        # CRITICAL: Check if the part is PART type and convert it to TOOL BEFORE checking inventory
        if part_id and part_type == 'PART':
            log_and_print(f"Part {part_number} is PART type - converting to TOOL before processing inventory", 'info')
            
            # Get fresh etag before conversion
            fresh_part_etag = get_part_etag(token, config, part_id, environment)
            if not fresh_part_etag:
                log_and_print(f"Failed to get fresh etag for part {part_number}", 'error')
                return False
            
            # Step 0: Update any existing inventory items to be serial-tracked FIRST
            log_and_print(f"Step 0: Ensuring all inventory items are serial-tracked for part {part_number}", 'info')
            
            # Get all inventory items for this part using approved query
            # Use get_all_tool_inventory and filter by part number in code
            get_inventory_query = read_query('get_all_tool_inventory.graphql')
            inventory_variables = {'first': 1000, 'after': None}
            inventory_result = post_graphql(token, config, {'query': get_inventory_query, 'variables': inventory_variables}, environment)
            
            if 'errors' in inventory_result:
                log_and_print(f"Error querying inventory for part {part_number}: {inventory_result['errors']}", 'error')
                inventory_items = []
            else:
                # Filter inventory items by part number and revision
                all_inventory_edges = inventory_result.get('data', {}).get('partInventories', {}).get('edges', [])
                inventory_items = []
                for edge in all_inventory_edges:
                    inv_node = edge.get('node', {})
                    part_info = inv_node.get('part', {})
                    if part_info:
                        part_num = part_info.get('partNumber', '').lower()
                        part_rev = part_info.get('revision', '').lower()
                        # Match by part number and revision, and also check if part ID matches
                        if (part_num == part_number.lower() and 
                            part_rev == cleaned_revision.lower() and
                            part_info.get('id') == part_id):
                            inventory_items.append(edge)
                log_and_print(f"Found {len(inventory_items)} inventory items for part {part_number}", 'info')
                
                if len(inventory_items) == 0:
                    log_and_print(f"No inventory items found for part {part_number} - this might be why conversion is failing", 'warning')
                
                for edge in inventory_items:
                    inventory_item = edge.get('node', {})
                    inventory_id = inventory_item.get('id')
                    inventory_etag = inventory_item.get('_etag')
                    current_serial = inventory_item.get('serialNumber', '')
                    
                    log_and_print(f"Updating inventory item {inventory_id} (current serial: '{current_serial}') to be serial-tracked", 'info')
                    
                    # Update inventory item to ensure it's serial-tracked
                    update_inventory_mutation = read_query('update_inventory_with_attributes.graphql')
                    update_inventory_variables = {
                        'input': {
                            'id': inventory_id,
                            'etag': inventory_etag,
                            'serialNumber': inventory_item.get('serialNumber', '')  # Ensure serial number is set
                        }
                    }
                    
                    inventory_update_result = post_graphql_with_etag_refresh(token, config, {'query': update_inventory_mutation, 'variables': update_inventory_variables}, environment)
                    
                    if 'errors' in inventory_update_result:
                        log_and_print(f"Warning: Failed to update inventory item {inventory_id} for part {part_number}: {inventory_update_result['errors']}", 'warning')
                    else:
                        log_and_print(f"Updated inventory item {inventory_id} to be serial-tracked", 'info')
            
            # Step 1: Update trackingType to SERIAL first
            log_and_print(f"Step 1: Updating trackingType to SERIAL for part {part_number}", 'info')
            tracking_mutation = read_query('update_tool.graphql')
            tracking_variables = {
                'input': {
                    'id': part_id,
                    'etag': fresh_part_etag,
                    'trackingType': 'SERIAL'
                }
            }
            
            tracking_result = post_graphql_with_etag_refresh(token, config, {'query': tracking_mutation, 'variables': tracking_variables}, environment)
            
            if 'errors' in tracking_result:
                error_msg = f"Failed to update trackingType for part {part_number}: {tracking_result['errors']}"
                log_and_print(f"{error_msg}", 'error')
                return False, error_msg
            
            # Get fresh etag after trackingType update
            fresh_part_etag = tracking_result.get('data', {}).get('updatePart', {}).get('part', {}).get('_etag')
            if not fresh_part_etag:
                error_msg = f"Cannot get fresh etag after trackingType update for part {part_number}"
                log_and_print(f"{error_msg}", 'error')
                return False, error_msg
            
            # Step 2: Update partType to TOOL and other attributes
            log_and_print(f"Step 2: Updating partType to TOOL for part {part_number}", 'info')
            # CRITICAL SAFETY CHECK: Verify part number is not protected before conversion
            if is_part_number_protected(part_number, config):
                error_msg = f"CRITICAL SAFETY CHECK FAILED: Part number '{part_number}' is protected and cannot be converted to TOOL. Skipping conversion."
                log_and_print(f"{error_msg}", 'error')
                return False, error_msg
            
            conversion_mutation = read_query('update_tool.graphql')
            conversion_variables_input = {
                    'id': part_id,
                    'etag': fresh_part_etag,
                    'partType': 'TOOL',
                    'description': tool_data.get('description', ''),
                    'attributes': build_tipqa_attributes_for_conversion(tool_data, part_id, token, config, environment)
                }
            # Only include maintenanceIntervalSeconds if we have a valid integer value
            service_interval = safe_convert_service_interval(tool_data.get('service_interval_seconds'))
            if service_interval is not None:
                conversion_variables_input['maintenanceIntervalSeconds'] = service_interval
            conversion_variables = {'input': conversion_variables_input}
            
            conversion_result = post_graphql_with_etag_refresh(token, config, {'query': conversion_mutation, 'variables': conversion_variables}, environment)
            
            if 'errors' in conversion_result:
                log_and_print(f"Failed to convert PART {part_number} to TOOL: {conversion_result['errors']}", 'error')
                return False
            
            log_and_print(f"Successfully converted PART {part_number} to TOOL", 'info')
            # Update part_etag to use the converted part's etag
            part_etag = conversion_result.get('data', {}).get('updatePart', {}).get('part', {}).get('_etag')
        
        # Now check if inventory already exists for this serial number and part
        # Use lowercase for part number to match Ion's storage format (case-insensitive)
        check_inventory_query = read_query('get_tool_inventory_by_serial_and_part.graphql')
        check_variables = {
            'serialNumber': serial_number,
            'partNumber': part_number.lower(),  # Use lowercase for case-insensitive matching
            'first': 1
        }
        
        check_result = post_graphql(token, config, {'query': check_inventory_query, 'variables': check_variables}, environment)
        
        if 'errors' in check_result:
            log_and_print(f"Failed to check existing inventory for {serial_number}: {check_result['errors']}", 'error')
            return False
        
        existing_inventory = check_result.get('data', {}).get('partInventories', {}).get('edges', [])
        if existing_inventory:
            log_and_print(f"Inventory already exists for serial {serial_number} with part {part_number} - updating existing inventory", 'info')
            
            # Get the existing inventory data
            inventory_data = existing_inventory[0]['node']
            inventory_id = inventory_data['id']
            inventory_etag = inventory_data['_etag']
            
            # Get existing inventory attributes with their etags (CRITICAL: Ion requires etags for existing attributes)
            existing_inventory_attributes = {}
            if 'attributes' in inventory_data:
                for attr in inventory_data.get('attributes', []):
                    # GraphQL returns 'Etag' (capital E)
                    attr_etag = attr.get('Etag', '')
                    existing_inventory_attributes[attr.get('key', '')] = {
                        'value': attr.get('value', ''),
                        'etag': attr_etag
                    }
            
            # Build TipQA attributes and merge with existing attributes (preserving etags)
            tipqa_inventory_attributes = build_tipqa_inventory_attributes(tool_data)
            updated_inventory_attributes = []
            
            if existing_inventory_attributes:
                # Inventory has existing attributes - preserve etags for existing ones
                for tipqa_attr in tipqa_inventory_attributes:
                    key = tipqa_attr['key']
                    value = tipqa_attr['value']
                    
                    if key in existing_inventory_attributes:
                        # This attribute exists - update with its etag if we have one
                        attr_etag = existing_inventory_attributes[key].get('etag', '')
                        if attr_etag:
                            # Update existing attribute with its etag (REQUIRED by Ion)
                            updated_inventory_attributes.append({
                                'key': key,
                                'value': value,
                                'etag': attr_etag
                            })
                        else:
                            # Attribute exists but no etag - omit etag field (don't include null)
                            updated_inventory_attributes.append({
                                'key': key,
                                'value': value
                            })
                    else:
                        # New attribute - add without etag
                        updated_inventory_attributes.append({
                            'key': key,
                            'value': value
                        })
            else:
                # Inventory has no existing attributes - add all TipQA attributes without etags
                updated_inventory_attributes = tipqa_inventory_attributes
            
            # Format last maintenance date properly (None means TipQA cleared it)
            last_maintenance_date_raw = tool_data.get('tipqa_last_maintenance_date') or tool_data.get('last_maintenance_date')
            last_maintenance_date = format_date_for_ion(last_maintenance_date_raw)
            
            # Determine if TipQA intends to clear the date (raw value is null/empty)
            tipqa_date_is_null = not last_maintenance_date_raw or str(last_maintenance_date_raw).strip().lower() in ('', 'nan', 'none', 'nat')
            needs_date_update = last_maintenance_date or tipqa_date_is_null
            
            if updated_inventory_attributes or needs_date_update:
                # Use robust etag handling with retry logic
                max_retries = 3
                update_success = False
                
                for attempt in range(max_retries):
                    # Refresh etag before each attempt
                    fresh_etag = get_inventory_etag(token, config, inventory_id, environment)
                    if not fresh_etag:
                        log_and_print(f"Failed to get fresh etag for tool {serial_number}", 'error')
                        if attempt < max_retries - 1:
                            time.sleep(0.5 * (attempt + 1))
                            continue
                        return False
                    
                    inventory_update_mutation = read_query('update_inventory_with_attributes.graphql')
                    inventory_update_variables = {
                        'input': {
                            'id': inventory_id,
                            'etag': fresh_etag,
                            'attributes': updated_inventory_attributes if updated_inventory_attributes else None
                        }
                    }
                    
                    # Always sync lastMaintainedDate — send null to clear if TipQA cleared it
                    if last_maintenance_date:
                        inventory_update_variables['input']['lastMaintainedDate'] = last_maintenance_date
                    elif tipqa_date_is_null:
                        inventory_update_variables['input']['lastMaintainedDate'] = None
                    
                    inventory_update_result = post_graphql(token, config, {'query': inventory_update_mutation, 'variables': inventory_update_variables}, environment)
                    
                    if 'errors' in inventory_update_result:
                        error_messages = [str(error) for error in inventory_update_result['errors']]
                        has_concurrency_error = any(
                            'CONCURRENCY_ERROR' in msg or 'Etag does not match' in msg
                            for msg in error_messages
                        )
                        
                        if has_concurrency_error and attempt < max_retries - 1:
                            wait_time = 0.5 * (2 ** attempt) + random.uniform(0, 0.5)
                            log_and_print(f"Concurrency error updating inventory for {serial_number}, retrying in {wait_time:.1f}s (attempt {attempt + 2}/{max_retries})...", 'warning')
                            time.sleep(wait_time)
                            continue
                        else:
                            log_and_print(f"Failed to update inventory attributes for tool {serial_number}: {inventory_update_result['errors']}", 'error')
                            return False
                    else:
                        update_success = True
                        break
                
                if not update_success:
                    log_and_print(f"Failed to update inventory attributes for tool {serial_number} after {max_retries} attempts", 'error')
                    return False
                
                log_and_print(f"Successfully updated inventory attributes for tool {serial_number}", 'info')
            
            log_and_print(f"Successfully updated existing tool {serial_number}", 'info')
            return True
        else:
            # COMPLEX SCENARIO: Serial exists but is tied to wrong part
            # Check if serial exists with a different part number
            log_and_print(f"No inventory found for serial {serial_number} with part {part_number}, checking if serial exists with different part...", 'info')
            
            # Search for any inventory with this serial number (regardless of part) using targeted query
            # This is much more efficient than paginating through all inventory items
            serial_search_query = read_query('get_inventory_by_serial.graphql')
            serial_search_variables = {
                'serialNumber': serial_number,
                'first': 1,  # We only need to know if it exists, so 1 result is enough
                'after': None
            }
            
            serial_search_result = post_graphql(token, config, {'query': serial_search_query, 'variables': serial_search_variables}, environment)
            
            if 'errors' in serial_search_result:
                log_and_print(f"Failed to search for serial {serial_number}: {serial_search_result['errors']}", 'error')
                serial_inventories = []
            else:
                serial_inventories = serial_search_result.get('data', {}).get('partInventories', {}).get('edges', [])
                if serial_inventories:
                    log_and_print(f"Found existing inventory for serial {serial_number}", 'info')
            
            if serial_inventories:
                # Serial exists but with different part - this is our complex scenario
                existing_inventory_data = serial_inventories[0]['node']
                existing_part = existing_inventory_data.get('part', {})
                existing_part_number = existing_part.get('partNumber', '')
                existing_part_id = existing_part.get('id')
                
                log_and_print(f"COMPLEX SCENARIO: Serial {serial_number} exists but is tied to part {existing_part_number} instead of {part_number}", 'warning')
                
                # Now we need to find the correct part (which might be PART type)
                # CRITICAL: Check if we already created/found the part earlier in this function
                correct_part_id = None
                correct_part_etag = None
                correct_part_type = None
                part_was_pre_existing = True
                
                # Strategy 0: Use part_id if it was already set (e.g., part was just created)
                if part_id:
                    log_and_print(f"Strategy 0: Using part_id {part_id} that was already found/created for {part_number}", 'info')
                    correct_part_id = part_id
                    correct_part_etag = part_etag if part_etag else get_part_etag(token, config, part_id, environment)
                    correct_part_type = part_type if part_type else 'TOOL'  # Default to TOOL if not set
                    if correct_part_etag:
                        log_and_print(f"Found correct part {part_number} using previously created/found part: ID {correct_part_id}, type {correct_part_type}", 'info')
                
                # Strategy 1: Check master dataframe first (most efficient - only works if part has inventory)
                if merged_df is not None:
                    log_and_print(f"Strategy 1: Checking master dataframe for part {part_number} (revision {cleaned_revision})...", 'info')
                    part_matches = merged_df[
                        (merged_df['ion_part_partNumber'].str.lower() == part_number.lower()) &
                        (merged_df['ion_part_revision'].str.lower() == cleaned_revision.lower())
                    ]
                    
                    if not part_matches.empty:
                        part_match = part_matches.iloc[0]
                        correct_part_id = part_match.get('ion_part_id', '')
                        correct_part_etag = part_match.get('ion_part_etag', '')
                        correct_part_type = part_match.get('ion_part_partType', '')
                        if correct_part_id and correct_part_etag:
                            log_and_print(f"Found correct part {part_number} in master dataframe: ID {correct_part_id}, type {correct_part_type}", 'info')
                
                # Strategy 2: Search through all tool inventory for this part number (approved query)
                if not correct_part_id:
                    log_and_print(f"Strategy 2: Searching all tool inventory for part {part_number} (revision {cleaned_revision})...", 'info')
                    # Use approved query: get_all_tool_inventory (only gets TOOL parts, but we'll check)
                    all_inventory_query = read_query('get_all_tool_inventory.graphql')
                    all_inventory_variables = {'first': 1000, 'after': None}
                    all_inventory_result = post_graphql(token, config, {'query': all_inventory_query, 'variables': all_inventory_variables}, environment)
                    
                    if 'errors' not in all_inventory_result:
                        inventory_edges = all_inventory_result.get('data', {}).get('partInventories', {}).get('edges', [])
                        # Search through inventory for matching part number and revision
                        for edge in inventory_edges:
                            inv_node = edge.get('node', {})
                            part_info = inv_node.get('part', {})
                            if part_info:
                                part_num = part_info.get('partNumber', '').lower()
                                part_rev = part_info.get('revision', '').lower()
                                if part_num == part_number.lower() and part_rev == cleaned_revision.lower():
                                    correct_part_id = part_info['id']
                                    correct_part_etag = part_info['_etag']
                                    correct_part_type = part_info.get('partType', 'TOOL')
                                    log_and_print(f"Found correct part {part_number} in inventory: ID {correct_part_id}, type {correct_part_type}", 'info')
                                    break
                    else:
                        log_and_print(f"Failed to search for correct part {part_number}: {all_inventory_result.get('errors', [])}", 'error')
                        return False
                
                # Strategy 3: Query Ion directly for the part by part number and revision (partType = TOOL only)
                # This queries parts directly (not through inventory), so it works even if the part has no inventory
                # This is critical for newly created parts that don't have inventory yet
                if not correct_part_id:
                    log_and_print(f"Strategy 3: Querying Ion directly for part {part_number} (revision {cleaned_revision}) with partType = TOOL...", 'info')
                    get_part_query = read_query('get_part_by_number_and_revision.graphql')
                    part_variables = {
                        'partNumber': part_number.lower(),
                        'revision': cleaned_revision.lower(),
                        'first': 10,
                        'after': None
                    }
                    
                    part_search_result = post_graphql(token, config, {'query': get_part_query, 'variables': part_variables}, environment)
                    
                    if 'errors' in part_search_result:
                        log_and_print(f"Strategy 3 failed: GraphQL errors: {part_search_result['errors']}", 'warning')
                    else:
                        part_edges = part_search_result.get('data', {}).get('parts', {}).get('edges', [])
                        if part_edges:
                            first_edge = part_edges[0]
                            part_info = first_edge.get('node', {})
                            if part_info:
                                correct_part_id = part_info.get('id')
                                correct_part_etag = part_info.get('_etag', '')
                                correct_part_type = part_info.get('partType', 'TOOL').upper()
                                log_and_print(f"Strategy 3: Found existing {correct_part_type} part {part_number} with ID {correct_part_id} via direct parts query", 'info')
                        else:
                            log_and_print(f"Strategy 3: No part found with partNumber='{part_number.lower()}', revision='{cleaned_revision.lower()}', and partType=TOOL", 'warning')
                
                # Strategy 3b: Query parts directly WITHOUT partType filter (catches PART-type parts with no inventory)
                # Only auto-converts PART→TOOL if the serial number confirms it's a tool (e.g., JT prefix)
                if not correct_part_id:
                    log_and_print(f"Strategy 3b: Querying Ion directly for part {part_number} (revision {cleaned_revision}) with ANY partType...", 'info')
                    get_part_any_type_query = read_query('get_part_by_number_and_revision_any_type.graphql')
                    part_variables_any = {
                        'partNumber': part_number.lower(),
                        'revision': cleaned_revision.lower(),
                        'first': 10,
                        'after': None
                    }
                    
                    part_search_any = post_graphql(token, config, {'query': get_part_any_type_query, 'variables': part_variables_any}, environment)
                    
                    if 'errors' in part_search_any:
                        log_and_print(f"Strategy 3b failed: GraphQL errors: {part_search_any['errors']}", 'warning')
                    else:
                        part_edges_any = part_search_any.get('data', {}).get('parts', {}).get('edges', [])
                        if part_edges_any:
                            first_edge_any = part_edges_any[0]
                            part_info_any = first_edge_any.get('node', {})
                            if part_info_any:
                                found_part_type = part_info_any.get('partType', '').upper()
                                found_part_id = part_info_any.get('id')
                                log_and_print(f"Strategy 3b: Found part {part_number} with ID {found_part_id}, partType={found_part_type}", 'info')
                                
                                if found_part_type == 'PART':
                                    log_and_print(f"Strategy 3b: Found PART-type part {part_number} (ID {found_part_id}) - will convert to TOOL for serial {serial_number}", 'info')
                                    correct_part_id = found_part_id
                                    correct_part_etag = part_info_any.get('_etag', '')
                                    correct_part_type = 'PART'
                                else:
                                    correct_part_id = found_part_id
                                    correct_part_etag = part_info_any.get('_etag', '')
                                    correct_part_type = found_part_type
                        else:
                            log_and_print(f"Strategy 3b: No part found with partNumber='{part_number.lower()}', revision='{cleaned_revision.lower()}' (any type)", 'warning')
                
                # Strategy 4: If still not found, the part doesn't exist and we need to create it
                if not correct_part_id:
                    log_and_print(f"Strategy 4: Part {part_number} not found in any queries - it doesn't exist yet", 'info')
                    log_and_print(f"Creating new TOOL part {part_number}...", 'info')
                    
                    # CRITICAL SAFETY CHECK: Verify part number is not protected before creating as TOOL
                    if is_part_number_protected(part_number, config):
                        error_msg = f"CRITICAL SAFETY CHECK FAILED: Part number '{part_number}' is protected and cannot be created as TOOL. Skipping creation."
                        log_and_print(f"{error_msg}", 'error')
                        return False, error_msg
                    
                    # Create the part using the same logic as the main part creation
                    create_part_mutation = read_query('create_tool.graphql')
                    attributes = build_tipqa_attributes(tool_data)
                    description = tool_data.get('description', '')
                    
                    create_variables_input = {
                        'partNumber': part_number,
                        'revision': cleaned_revision,
                        'partType': 'TOOL',
                        'description': description,
                        'trackingType': 'SERIAL',
                        'attributes': attributes
                    }
                    # Only include maintenanceIntervalSeconds if we have a valid integer value
                    service_interval = safe_convert_service_interval(tool_data.get('service_interval_seconds'))
                    if service_interval is not None:
                        create_variables_input['maintenanceIntervalSeconds'] = service_interval
                    create_variables = {'input': create_variables_input}
                    
                    # Add a small random delay to prevent race conditions
                    delay = random.uniform(0.1, 0.5)
                    time.sleep(delay)
                    
                    create_result = post_graphql(token, config, {'query': create_part_mutation, 'variables': create_variables}, environment)
                    
                    if 'errors' in create_result:
                        # Check if this is a part already exists error
                        error_messages = [str(error) for error in create_result['errors']]
                        if any('already exists' in msg for msg in error_messages):
                            log_and_print(f"Part {part_number} already exists (created by another process), searching for it...", 'warning')
                            # Try Strategy 3 again (direct part query) - it might exist now
                            get_part_query = read_query('get_part_by_number_and_revision.graphql')
                            part_variables = {
                                'partNumber': part_number.lower(),
                                'revision': cleaned_revision.lower(),
                                'first': 10,
                                'after': None
                            }
                            part_search_result = post_graphql(token, config, {'query': get_part_query, 'variables': part_variables}, environment)
                            if 'errors' not in part_search_result:
                                part_edges = part_search_result.get('data', {}).get('parts', {}).get('edges', [])
                                if part_edges:
                                    first_edge = part_edges[0]
                                    part_info = first_edge.get('node', {})
                                    if part_info:
                                        correct_part_id = part_info.get('id')
                                        correct_part_etag = part_info.get('_etag', '')
                                        correct_part_type = part_info.get('partType', 'TOOL').upper()
                                        log_and_print(f"Found existing part {part_number} with ID {correct_part_id} after creation attempt", 'info')
                            # Fallback: try without partType filter (part may exist as PART type)
                            if not correct_part_id:
                                log_and_print(f"Retrying with any partType filter after 'already exists' error...", 'info')
                                get_part_any_type_query = read_query('get_part_by_number_and_revision_any_type.graphql')
                                part_variables_any = {
                                    'partNumber': part_number.lower(),
                                    'revision': cleaned_revision.lower(),
                                    'first': 10,
                                    'after': None
                                }
                                part_search_any = post_graphql(token, config, {'query': get_part_any_type_query, 'variables': part_variables_any}, environment)
                                if 'errors' not in part_search_any:
                                    part_edges_any = part_search_any.get('data', {}).get('parts', {}).get('edges', [])
                                    if part_edges_any:
                                        part_info_any = part_edges_any[0].get('node', {})
                                        if part_info_any:
                                            found_part_type = part_info_any.get('partType', '').upper()
                                            if found_part_type == 'PART':
                                                log_and_print(f"Found PART-type part {part_number} (ID {part_info_any.get('id')}) - will convert to TOOL for serial {serial_number}", 'info')
                                                correct_part_id = part_info_any.get('id')
                                                correct_part_etag = part_info_any.get('_etag', '')
                                                correct_part_type = 'PART'
                                            else:
                                                correct_part_id = part_info_any.get('id')
                                                correct_part_etag = part_info_any.get('_etag', '')
                                                correct_part_type = found_part_type
                                                log_and_print(f"Found existing part {part_number} with ID {correct_part_id} (type: {correct_part_type}) after creation attempt", 'info')
                            if not correct_part_id:
                                error_msg = f"Failed to find existing part {part_number} after creation attempt. Cannot proceed."
                                log_and_print(f"{error_msg}", 'error')
                                return False, error_msg
                        else:
                            error_msg = f"Failed to create part {part_number} (revision {cleaned_revision}): {create_result['errors']}"
                            log_and_print(f"{error_msg}", 'error')
                            return False, error_msg
                    else:
                        correct_part_id = create_result.get('data', {}).get('createPart', {}).get('part', {}).get('id')
                        part_was_pre_existing = False
                        if not correct_part_id:
                            error_msg = f"Unexpected response when creating part {part_number} - no part ID in response"
                            log_and_print(f"{error_msg}", 'error')
                            return False, error_msg
                        correct_part_etag = create_result.get('data', {}).get('createPart', {}).get('part', {}).get('_etag', '')
                        correct_part_type = 'TOOL'
                        log_and_print(f"Successfully created part {part_number} with ID {correct_part_id}", 'info')
                        
                # If we found the part, verify it and proceed with conversion if needed
                if correct_part_id and correct_part_etag:
                    log_and_print(f"Found correct part {part_number} with ID {correct_part_id}, type: {correct_part_type}", 'info')
                    
                    # CRITICAL SAFETY CHECK: Verify part type is valid
                    if correct_part_type not in ['TOOL', 'PART']:
                        log_and_print(f"SAFETY CHECK FAILED: Part {part_number} has invalid partType '{correct_part_type}'", 'error')
                        return False
                    
                    # If it's a PART type, convert it to TOOL
                    if correct_part_type == 'PART':
                        log_and_print(f"Converting PART {part_number} to TOOL for serial {serial_number}", 'info')
                        
                        # Get fresh etag before conversion
                        fresh_part_etag = get_part_etag(token, config, correct_part_id, environment)
                        if not fresh_part_etag:
                            log_and_print(f"Failed to get fresh etag for part {part_number}", 'error')
                            return False
                        
                        # Step 0: Update any existing inventory items to be serial-tracked FIRST
                        log_and_print(f"Step 0: Ensuring all inventory items are serial-tracked for part {part_number}", 'info')
                        
                        # Get all inventory items for this part using approved query
                        # Use get_all_tool_inventory and filter by part number in code
                        get_inventory_query = read_query('get_all_tool_inventory.graphql')
                        inventory_variables = {'first': 1000, 'after': None}
                        inventory_result = post_graphql(token, config, {'query': get_inventory_query, 'variables': inventory_variables}, environment)
                        
                        if 'errors' in inventory_result:
                            log_and_print(f"Error querying inventory for part {part_number}: {inventory_result['errors']}", 'error')
                            inventory_items = []
                        else:
                            # Filter inventory items by part number and revision
                            all_inventory_edges = inventory_result.get('data', {}).get('partInventories', {}).get('edges', [])
                            inventory_items = []
                            for edge in all_inventory_edges:
                                inv_node = edge.get('node', {})
                                part_info = inv_node.get('part', {})
                                if part_info:
                                    part_num = part_info.get('partNumber', '').lower()
                                    part_rev = part_info.get('revision', '').lower()
                                    # Match by part number and revision, and also check if part ID matches
                                    if (part_num == part_number.lower() and 
                                        part_rev == cleaned_revision.lower() and
                                        part_info.get('id') == correct_part_id):
                                        inventory_items.append(edge)
                            
                            log_and_print(f"Found {len(inventory_items)} inventory items for part {part_number}", 'info')
                            
                            if len(inventory_items) == 0:
                                log_and_print(f"No inventory items found for part {part_number} - this might be why conversion is failing", 'warning')
                            
                            for edge in inventory_items:
                                    inventory_item = edge.get('node', {})
                                    inventory_id = inventory_item.get('id')
                                    inventory_etag = inventory_item.get('_etag')
                                    current_serial = inventory_item.get('serialNumber', '')
                                    
                                    # Check if inventory item already has a serial number
                                    original_serial = inventory_item.get('serialNumber', '')
                                    # Handle various representations of "no serial number"
                                    if (original_serial and 
                                        original_serial != 'None' and 
                                        original_serial != 'null' and 
                                        original_serial.strip() != '' and
                                        original_serial.lower() != 'none'):
                                        # Inventory item already has a serial number - skip updating it
                                        log_and_print(f"Inventory item {inventory_id} already has serial number '{original_serial}' - skipping update", 'info')
                                        continue
                                    else:
                                        # Inventory item has no serial number - skip conversion entirely
                                        log_and_print(f"Inventory item {inventory_id} has no serial number (current: '{original_serial}') - skipping PART-to-TOOL conversion", 'warning')
                                        log_and_print(f"Cannot convert PART {part_number} to TOOL due to inventory item without serial number", 'error')
                                        return False
                                    
                                    log_and_print(f"Updating inventory item {inventory_id} (current serial: '{current_serial}') to be serial-tracked", 'info')
                                    
                                    # Update inventory item to ensure it's serial-tracked
                                    update_inventory_mutation = read_query('update_inventory_with_attributes.graphql')
                                    update_inventory_variables = {
                                        'input': {
                                            'id': inventory_id,
                                            'etag': inventory_etag,
                                            'serialNumber': current_serial,  # Use the existing serial number
                                            'quantity': 1  # Serialized parts must have quantity = 1
                                        }
                                    }
                                    
                                    inventory_update_result = post_graphql_with_etag_refresh(token, config, {'query': update_inventory_mutation, 'variables': update_inventory_variables}, environment)
                                    
                                    if 'errors' in inventory_update_result:
                                        log_and_print(f"Warning: Failed to update inventory item {inventory_id} for part {part_number}: {inventory_update_result['errors']}", 'warning')
                                    else:
                                        log_and_print(f"Updated inventory item {inventory_id} to be serial-tracked", 'info')
                            
                            # Step 1: Update trackingType to SERIAL first
                            log_and_print(f"Step 1: Updating trackingType to SERIAL for part {part_number}", 'info')
                            tracking_mutation = read_query('update_tool.graphql')
                            tracking_variables = {
                                'input': {
                                    'id': correct_part_id,
                                    'etag': fresh_part_etag,
                                    'trackingType': 'SERIAL'
                                }
                            }
                            
                            tracking_result = post_graphql_with_etag_refresh(token, config, {'query': tracking_mutation, 'variables': tracking_variables}, environment)
                            
                            if 'errors' in tracking_result:
                                log_and_print(f"Failed to update trackingType for part {part_number}: {tracking_result['errors']}", 'error')
                                return False
                            
                            # Get fresh etag after trackingType update
                            fresh_part_etag = tracking_result.get('data', {}).get('updatePart', {}).get('part', {}).get('_etag')
                            if not fresh_part_etag:
                                log_and_print(f"Cannot get fresh etag after trackingType update for part {part_number}", 'error')
                                return False
                            
                            # Step 2: Update partType to TOOL and other attributes
                            log_and_print(f"Step 2: Updating partType to TOOL for part {part_number}", 'info')
                            # CRITICAL SAFETY CHECK: Verify part number is not protected before conversion
                            if is_part_number_protected(part_number, config):
                                log_and_print(f"CRITICAL SAFETY CHECK FAILED: Part number '{part_number}' is protected and cannot be converted to TOOL. Skipping conversion.", 'error')
                                return False
                            
                            conversion_mutation = read_query('update_tool.graphql')
                            conversion_variables_input = {
                                    'id': correct_part_id,
                                    'etag': fresh_part_etag,
                                    'partType': 'TOOL',
                                    'description': tool_data.get('description', ''),
                                    'attributes': build_tipqa_attributes_for_conversion(tool_data, correct_part_id, token, config, environment)
                                }
                            # Only include maintenanceIntervalSeconds if we have a valid integer value
                            service_interval = safe_convert_service_interval(tool_data.get('service_interval_seconds'))
                            if service_interval is not None:
                                conversion_variables_input['maintenanceIntervalSeconds'] = service_interval
                            conversion_variables = {'input': conversion_variables_input}
                            
                            conversion_result = post_graphql_with_etag_refresh(token, config, {'query': conversion_mutation, 'variables': conversion_variables}, environment)
                            
                            if 'errors' in conversion_result:
                                log_and_print(f"Failed to convert PART {part_number} to TOOL: {conversion_result['errors']}", 'error')
                                return False
                            
                            log_and_print(f"Successfully converted PART {part_number} to TOOL", 'info')
                            # Update part_id to use the converted part
                            part_id = correct_part_id
                            part_etag = conversion_result.get('data', {}).get('updatePart', {}).get('part', {}).get('_etag')
                    else:
                        # It's already a TOOL type
                        part_id = correct_part_id
                        part_etag = correct_part_etag
                        log_and_print(f"Correct part {part_number} is already TOOL type", 'info')
                        
                        # Now update the serial to point to the correct part
                        log_and_print(f"Updating serial {serial_number} to point to correct part {part_number}", 'info')
                        
                        # Get fresh inventory etag
                        inventory_etag = existing_inventory_data['_etag']
                        
                        # Update inventory to point to correct part
                        update_inventory_mutation = read_query('update_inventory_with_attributes.graphql')
                        update_inventory_variables = {
                            'input': {
                                'id': existing_inventory_data['id'],
                                'etag': inventory_etag,
                                'partId': part_id
                            }
                        }
                        
                        update_inventory_result = post_graphql_with_etag_refresh(token, config, {'query': update_inventory_mutation, 'variables': update_inventory_variables}, environment)
                        
                        if 'errors' in update_inventory_result:
                            log_and_print(f"Failed to update serial {serial_number} to correct part: {update_inventory_result['errors']}", 'error')
                            return False
                        
                        log_and_print(f"Successfully updated serial {serial_number} to correct part {part_number}", 'info')
                        
                        # Set inventory attributes -- must include etags for any
                        # existing attributes, otherwise Ion rejects the mutation.
                        inventory_attributes = build_tipqa_inventory_attributes(tool_data)
                        if inventory_attributes:
                            # Fetch current inventory to get fresh etag + attribute etags
                            inv_id = existing_inventory_data['id']
                            inv_refresh_query = read_query('get_inventory_etag.graphql')
                            inv_refresh_result = post_graphql(token, config, {'query': inv_refresh_query, 'variables': {'id': inv_id}}, environment)
                            fresh_inv = inv_refresh_result.get('data', {}).get('partInventory', {}) if 'errors' not in inv_refresh_result else {}
                            fresh_inv_etag = fresh_inv.get('_etag') or update_inventory_result.get('data', {}).get('updatePartInventory', {}).get('partInventory', {}).get('_etag')
                            
                            # Build a map of existing attribute etags
                            existing_attr_etags = {}
                            for attr in fresh_inv.get('attributes', []):
                                attr_key = attr.get('key', '')
                                attr_etag = attr.get('Etag', '') or attr.get('etag', '')
                                if attr_key and attr_etag:
                                    existing_attr_etags[attr_key] = attr_etag
                            
                            # Merge etags into the new attributes
                            for attr in inventory_attributes:
                                etag = existing_attr_etags.get(attr['key'])
                                if etag:
                                    attr['etag'] = etag
                            
                            inventory_update_mutation = read_query('update_inventory_with_attributes.graphql')
                            inventory_update_variables = {
                                'input': {
                                    'id': inv_id,
                                    'etag': fresh_inv_etag,
                                    'attributes': inventory_attributes
                                }
                            }
                            
                            inventory_update_result = post_graphql_with_etag_refresh(token, config, {'query': inventory_update_mutation, 'variables': inventory_update_variables}, environment)
                            
                            if 'errors' in inventory_update_result:
                                log_and_print(f"Failed to set inventory attributes for tool {serial_number}: {inventory_update_result['errors']}", 'error')
                                return False
                            
                            log_and_print(f"Successfully set inventory attributes for tool {serial_number}", 'info')
                        
                        # After fixing serial-to-part relationship, update inventory with all TipQA data
                        log_and_print(f"Updating inventory with complete TipQA data for tool {serial_number}", 'info')
                        
                        inventory_update_mutation = read_query('update_inventory_with_attributes.graphql')
                        
                        # Get location ID for TipQA location
                        tipqa_location = tool_data.get('tipqa_location', '')
                        location_id = None
                        if tipqa_location:
                            from utilities.graphql_utils import get_locations
                            locations_result = get_locations(token, config, environment)
                            if 'errors' not in locations_result:
                                for location in locations_result.get('data', {}).get('locations', {}).get('edges', []):
                                    if location['node'].get('name', '').strip().lower() == tipqa_location.strip().lower():
                                        location_id = location['node']['id']
                                        break
                        
                        # Fetch fresh inventory state to get attribute etags
                        inv_id = existing_inventory_data['id']
                        inv_refresh_query = read_query('get_inventory_etag.graphql')
                        inv_refresh_result = post_graphql(token, config, {'query': inv_refresh_query, 'variables': {'id': inv_id}}, environment)
                        fresh_inv = inv_refresh_result.get('data', {}).get('partInventory', {}) if 'errors' not in inv_refresh_result else {}
                        fresh_inv_etag = fresh_inv.get('_etag') or inventory_update_result.get('data', {}).get('updatePartInventory', {}).get('partInventory', {}).get('_etag')
                        
                        # Build inventory-level attributes and merge existing attribute etags
                        inventory_attributes = build_tipqa_inventory_attributes(tool_data)
                        existing_attr_etags = {}
                        for attr in fresh_inv.get('attributes', []):
                            attr_key = attr.get('key', '')
                            attr_etag = attr.get('Etag', '') or attr.get('etag', '')
                            if attr_key and attr_etag:
                                existing_attr_etags[attr_key] = attr_etag
                        for attr in inventory_attributes:
                            etag = existing_attr_etags.get(attr['key'])
                            if etag:
                                attr['etag'] = etag
                        
                        update_variables = {
                            'input': {
                                'id': inv_id,
                                'etag': fresh_inv_etag,
                                'attributes': inventory_attributes
                            }
                        }
                        
                        if location_id:
                            update_variables['input']['locationId'] = location_id
                        
                        last_maintenance_date_raw = tool_data.get('tipqa_last_maintenance_date') or tool_data.get('last_maintenance_date')
                        last_maintenance_date = format_date_for_ion(last_maintenance_date_raw)
                        if last_maintenance_date:
                            update_variables['input']['lastMaintainedDate'] = last_maintenance_date
                        elif not last_maintenance_date_raw or str(last_maintenance_date_raw).strip().lower() in ('', 'nan', 'none', 'nat'):
                            update_variables['input']['lastMaintainedDate'] = None
                        
                        complete_update_result = post_graphql_with_etag_refresh(token, config, {'query': inventory_update_mutation, 'variables': update_variables}, environment)
                        
                        if 'errors' in complete_update_result:
                            log_and_print(f"Failed to complete inventory update for tool {serial_number}: {complete_update_result['errors']}", 'error')
                            return False
                        
                        log_and_print(f"Successfully completed inventory update with TipQA data for tool {serial_number}", 'info')
                        log_and_print(f"Successfully handled complex scenario for tool {serial_number}", 'info')
                        return True
                else:
                    log_and_print(f"Correct part {part_number} not found for serial {serial_number}", 'error')
                    return False
            else:
                # Serial doesn't exist at all - proceed with normal creation
                log_and_print(f"Serial {serial_number} doesn't exist - proceeding with normal creation", 'info')
                # Reset inventory_id since we're creating new inventory
                inventory_id = None
            
            # Check TipQA status to determine correct action
            tipqa_location = tool_data.get('tipqa_location', '').lower()
            tipqa_maintenance_status = tool_data.get('tipqa_maintenance_status', '').lower()
            
            # Determine if tool should be marked unavailable based on TipQA status
            should_mark_unavailable = (
                'lost' in tipqa_location or 
                'obsolete' in tipqa_location or
                'inactive' in tipqa_maintenance_status or
                'unavailable' in tipqa_maintenance_status
            )
            
            if should_mark_unavailable:
                log_and_print(f"TipQA shows {serial_number} as {tipqa_location}/{tipqa_maintenance_status} - marking as unavailable in Ion", 'info')
                
                # Get lost location ID
                from utilities.graphql_utils import get_locations
                locations_result = get_locations(token, config, environment)
                if 'errors' in locations_result:
                    log_and_print(f"Failed to get locations: {locations_result['errors']}", 'error')
                    return False
                
                lost_location_id = None
                for location in locations_result.get('data', {}).get('locations', {}).get('edges', []):
                    location_name = location['node'].get('name', '').lower()
                    if 'lost' in location_name or 'obsolete' in location_name:
                        lost_location_id = location['node']['id']
                        break
                
                if not lost_location_id:
                    log_and_print(f"Could not find lost location for {serial_number}", 'error')
                    return False
                
                # Mark as unavailable and move to lost location
                mark_unavailable_mutation = read_query('update_inventory_with_attributes.graphql')
                mark_unavailable_variables = {
                    'input': {
                        'id': inventory_id,
                        'etag': inventory_etag,
                        'unavailable': True,
                        'locationId': lost_location_id
                    }
                }
                
                mark_result = post_graphql(token, config, {'query': mark_unavailable_mutation, 'variables': mark_unavailable_variables}, environment)
                
                if 'errors' in mark_result:
                    log_and_print(f"Failed to mark existing inventory as unavailable for tool {serial_number}: {mark_result['errors']}", 'error')
                    return False
                
                log_and_print(f"Successfully marked existing inventory as unavailable for tool {serial_number}", 'info')
                return True
            
            else:
                # Tool is active in TipQA - proceed with normal inventory creation
                log_and_print(f"TipQA shows {serial_number} as active - proceeding with normal inventory creation", 'info')
        
        # Then create the inventory item with all TipQA data
        inventory_mutation = read_query('create_tool_inventory.graphql')
        
        # Build inventory-level attributes from TipQA data
        inventory_attributes = build_tipqa_inventory_attributes(tool_data)
                
        # Get location ID for TipQA location - following master data flow document
        # tipqa_location (TipQA LOCATION_CODE) → match to Ion location name → get Ion location id
        import pandas as pd
        tipqa_location = tool_data.get('tipqa_location') or tool_data.get('location')
        location_id, location_source = get_ion_location_id_for_tipqa(token, config, environment, tipqa_location)
        if not (location_id and location_source) and tipqa_location and pd.notna(tipqa_location) and str(tipqa_location).strip() and str(tipqa_location).strip().lower() not in ('nan', 'none', 'null', '<na>'):
            log_and_print(f"Warning: TipQA location '{tipqa_location}' not found in Ion for tool {serial_number}. Add sync_exceptions.location_code_to_ion_name in config if needed.", 'warning')
        
        # Determine status based on TipQA data (for logging only - not sent to GraphQL)
        maintenance_status = tool_data.get('maintenance_status', '')
        revision_status = tool_data.get('revision_status', '')
        location = tool_data.get('location', '')
        
        # Determine if tool should be AVAILABLE or UNAVAILABLE (for logging only)
        if (maintenance_status == 'I' or revision_status == 'I' or 
            maintenance_status == 'L' or maintenance_status in ['OS', 'OC', 'TO', 'QAHD'] or
            (location and (location.lower() == 'lost' or 'lost' in location.lower())) or
            not location or (location and location.strip() == '')):
            inventory_status = 'UNAVAILABLE'
        else:
            inventory_status = 'AVAILABLE'
        
        # Format last maintenance date properly for GraphQL - Ion expects DateTime format (ISO 8601 with time)
        # tipqa_last_maintenance_date (TipQA LAST_CAL_DATE) → lastMaintainedDate field
        last_maintenance_date_raw = tool_data.get('tipqa_last_maintenance_date') or tool_data.get('last_maintenance_date')
        last_maintenance_date = format_date_for_ion(last_maintenance_date_raw)
        
        # SAFEGUARD: If formatting failed but we have a raw date, log warning and don't include it
        if not last_maintenance_date and last_maintenance_date_raw:
            log_and_print(f"Warning: Could not format date '{last_maintenance_date_raw}' for tool {serial_number}, omitting lastMaintainedDate", 'warning')
        
        # CRITICAL: Verify serial_number is set and valid before creating inventory
        # Clean the serial number to check for invalid values like "N/A"
        cleaned_serial = clean_serial_number(serial_number)
        if not cleaned_serial or cleaned_serial.strip() == '':
            log_and_print(f"ERROR: Cannot create inventory for tool - serial_number is missing or invalid (was: {repr(serial_number)})", 'error')
            return False
        
        # Ensure serial_number is a string and use the cleaned version
        serial_number = str(cleaned_serial).strip()
        
        # CRITICAL SAFETY CHECK: Verify part_number is valid before creating inventory
        # This prevents creating inventory for parts with invalid part numbers like "NA" or whitespace-only
        cleaned_part = clean_part_number(part_number)
        if not cleaned_part or cleaned_part.strip() == '' or cleaned_part.isspace():
            log_and_print(f"ERROR: Cannot create inventory for tool {serial_number} - part_number is missing, invalid, or whitespace-only (was: {repr(part_number)}). Blocking inventory creation.", 'error')
            return False
        
        # Additional check: Ensure the part number is not "NA" or any other blocked value (case-insensitive)
        # This catches variations like "na", "Na", "nA", "N/A", "n/a", "N/a", etc.
        part_upper = cleaned_part.upper().strip()
        if part_upper in ('N/A', 'NA', 'NONE', 'NULL', '<NA>', 'NAN'):
            log_and_print(f"ERROR: Cannot create inventory for tool {serial_number} - part_number '{part_number}' is blocked (invalid value). Blocking inventory creation.", 'error')
            return False
        
        inventory_variables = {
            'input': {
                'partId': part_id,
                'serialNumber': serial_number,
                'attributes': inventory_attributes if inventory_attributes else None
            }
        }
        
        # Only add lastMaintainedDate if it's properly formatted (DateTime format)
        # CRITICAL: Never send date-only strings - Ion requires DateTime format
        if last_maintenance_date:
            inventory_variables['input']['lastMaintainedDate'] = last_maintenance_date
        
        # Debug: Log the serial number being used
        log_and_print(f"Creating inventory for serial_number: {repr(serial_number)}, partId: {part_id}", 'info')
        
        # Add location if found
        if location_id:
            inventory_variables['input']['locationId'] = location_id
        
        inventory_result = post_graphql(token, config, {'query': inventory_mutation, 'variables': inventory_variables}, environment)
        
        if 'errors' in inventory_result:
            # Check if this is a non-inventoried part error
            error_messages = [error.get('message', '') for error in inventory_result['errors']]
            if any('non-inventoried part' in msg.lower() for msg in error_messages):
                error_msg = f"Cannot create inventory for tool {serial_number} - part {part_number} is marked as non-inventoried. Skipping creation."
                log_and_print(f"{error_msg}", 'warning')
                return False, error_msg
            else:
                error_msg = f"Failed to create inventory for tool {serial_number}: {inventory_result['errors']}"
                log_and_print(f"{error_msg}", 'error')
                return False, error_msg
        
        inventory_id = inventory_result.get('data', {}).get('createPartInventory', {}).get('partInventory', {}).get('id')
        if inventory_id:
            log_and_print(f"Successfully created tool {serial_number} with all TipQA data (inventory_id: {inventory_id})", 'info')
            # When we used a pre-existing part, sync service_interval from TipQA to Ion (part-level field)
            if part_was_pre_existing and part_id:
                _sync_part_service_interval_after_create(token, config, part_id, part_number, tool_data, environment)
            log_and_print(f"create_tool returning True for {serial_number}", 'info')
            return True, ''
        else:
            error_msg = f"Unexpected response when creating inventory for tool {serial_number} - no inventory_id found. Response structure: {json.dumps(inventory_result, indent=2)}"
            log_and_print(f"Unexpected response when creating inventory for tool {serial_number}", 'error')
            log_and_print(f"Response structure: {json.dumps(inventory_result, indent=2)}", 'error')
            log_and_print(f"create_tool returning False for {serial_number} - no inventory_id found", 'error')
            return False, error_msg
            
    except Exception as e:
        error_msg = f"Exception creating tool {serial_number}: {str(e)}"
        log_and_print(f"{error_msg}", 'error')
        return False, error_msg

def update_tool(token: str, config: Dict[str, Any], tool_data: Dict, match_info: Dict, environment: str = 'v1_sandbox', dry_run: bool = True, updated_parts_cache: set = None, parts_cache_lock = None) -> tuple[bool, str]:
    """
    Update an existing tool in Ion with TipQA data following the flow document requirements.
    
    Returns:
        tuple[bool, str]: (success, error_message) - success is True if tool was updated successfully,
                         error_message contains detailed error information if success is False
    """
    
    serial_number = tool_data.get('serial_number', 'UNKNOWN')
    _trace = serial_number in ('JT00004653',)
    part_number = clean_part_number(tool_data.get('part_number', ''))
    revision = tool_data.get('revision', '')
    cleaned_revision = clean_revision(revision)
    description = tool_data.get('tipqa_description', '') or tool_data.get('description', '')
    if not description:
        description = str(tool_data.get('tipqa_description', '')).strip()

    if _trace:
        log_and_print(f"TRACE {serial_number}: update_tool ENTERED. part_number={part_number}, revision={revision}, tipqa_location={tool_data.get('tipqa_location') or tool_data.get('location')}, maintenance_status={tool_data.get('tipqa_maintenance_status') or tool_data.get('maintenance_status')}", 'info')

    # Handle both direct match data and wrapped match data
    if 'match' in match_info:
        match = match_info.get('match', {})
    else:
        match = match_info
    
    if not match:
        error_msg = f"Cannot update tool {serial_number} - no match information provided"
        log_and_print(f"{error_msg}", 'error')
        return False, error_msg
    
    tool_id = match.get('id')
    tool_etag = match.get('_etag')
    part = match.get('part', {})
    current_part_id = part.get('id')
    current_part_etag = part.get('_etag')
    current_part_number = part.get('partNumber', '')
    current_revision = part.get('revision', '')

    if _trace:
        log_and_print(f"TRACE {serial_number}: tool_id={tool_id}, current_part_id={current_part_id}, current_part_number={current_part_number}", 'info')
    
    # Convert Ion IDs from float to int if needed
    import pandas as pd
    if isinstance(tool_id, float) and not pd.isna(tool_id):
        tool_id = int(tool_id)
    if isinstance(current_part_id, float) and not pd.isna(current_part_id):
        current_part_id = int(current_part_id)
    
    # Check for missing required data - handle empty strings, None, and NaN values
    import pandas as pd
    if (not tool_id or (isinstance(tool_id, str) and tool_id.strip() == '') or 
        (isinstance(tool_id, float) and pd.isna(tool_id)) or
        not tool_etag or (isinstance(tool_etag, str) and tool_etag.strip() == '') or
        (isinstance(tool_etag, float) and pd.isna(tool_etag)) or
        not current_part_id or (isinstance(current_part_id, str) and current_part_id.strip() == '') or
        (isinstance(current_part_id, float) and pd.isna(current_part_id)) or
        not current_part_etag or (isinstance(current_part_etag, str) and current_part_etag.strip() == '') or
        (isinstance(current_part_etag, float) and pd.isna(current_part_etag))):
        error_msg = f"Cannot update tool {serial_number} - missing required data (tool_id={tool_id}, tool_etag={bool(tool_etag)}, part_id={current_part_id}, part_etag={bool(current_part_etag)})"
        log_and_print(f"{error_msg}", 'error')
        return False, error_msg
    
    if dry_run:
        log_and_print(f"DRY RUN: Would update tool {serial_number} with part {part_number}, revision {revision}", 'info')
        return True, ''
    
    try:
        # Check if we need to create a new part or use existing one
        target_part_id = current_part_id
        
        # Normalize revisions for comparison (handle empty/null revisions)
        current_revision_normalized = str(current_revision).strip().lower() if current_revision else ''
        cleaned_revision_normalized = cleaned_revision.strip().lower() if cleaned_revision else ''
        # Treat empty revision as 'A' for comparison purposes
        if not current_revision_normalized:
            current_revision_normalized = 'a'
        if not cleaned_revision_normalized:
            cleaned_revision_normalized = 'a'
        
        # Check if part number OR revision is changing
        part_number_changing = current_part_number.lower() != part_number.lower()
        revision_changing = current_revision_normalized != cleaned_revision_normalized
        
        if part_number_changing or revision_changing:
            # Part number or revision is changing - need to find or create the correct part
            if part_number_changing and revision_changing:
                log_and_print(f"Part number changing from {current_part_number} to {part_number} and revision changing from {current_revision} to {cleaned_revision} for tool {serial_number}", 'info')
            elif part_number_changing:
                log_and_print(f"Part number changing from {current_part_number} to {part_number} for tool {serial_number}", 'info')
            elif revision_changing:
                log_and_print(f"Revision changing from {current_revision} to {cleaned_revision} for part {part_number} (tool {serial_number})", 'info')
            
            # CRITICAL FIX: When part number is changing, we need to find the new part.
            # The new part might not have any inventory yet, so we should use direct part query first.
            # Only search inventory if direct query fails (for backwards compatibility).
            existing_parts = []
            target_part_id = None
            
            # Strategy 1: Direct part query (works even if part has no inventory)
            # This is critical when part number is changing - the new part might exist but have no inventory
            log_and_print(f"Strategy 1: Querying Ion directly for part {part_number} (revision {cleaned_revision})...", 'info')
            get_part_query = read_query('get_part_by_number_and_revision.graphql')
            part_variables = {
                'partNumber': part_number.lower(),
                'revision': cleaned_revision.lower() if cleaned_revision else 'a',
                'first': 10,
                'after': None
            }
            
            part_search_result = post_graphql(token, config, {'query': get_part_query, 'variables': part_variables}, environment)
            
            if 'errors' not in part_search_result:
                part_edges = part_search_result.get('data', {}).get('parts', {}).get('edges', [])
                if part_edges:
                    first_edge = part_edges[0]
                    part_info = first_edge.get('node', {})
                    if part_info:
                        existing_parts.append({'node': part_info})
                        target_part_id = part_info.get('id')
                        log_and_print(f"Strategy 1: Found existing part {part_number} (revision {cleaned_revision}) with ID {target_part_id} via direct query", 'info')
            
            # Strategy 2: Search inventory (fallback - only works if part has inventory)
            if not target_part_id:
                log_and_print(f"Strategy 2: Searching inventory for part {part_number} (revision {cleaned_revision})...", 'info')
                find_inventory_query = read_query('get_tool_inventory_by_serial_and_part.graphql')
                find_variables = {'serialNumber': serial_number, 'partNumber': part_number.lower(), 'first': 10, 'after': None}
                
                find_result = post_graphql(token, config, {'query': find_inventory_query, 'variables': find_variables}, environment)
                
                if 'errors' in find_result:
                    log_and_print(f"Strategy 2 failed: {find_result['errors']}", 'warning')
                else:
                    inventory_edges = find_result.get('data', {}).get('partInventories', {}).get('edges', [])
                    # Extract part info from inventory and filter by revision
                    target_revision_normalized = cleaned_revision_normalized
                    for edge in inventory_edges:
                        inv_node = edge.get('node', {})
                        part_info = inv_node.get('part', {})
                        if part_info:
                            part_revision = part_info.get('revision', '').strip().lower() if part_info.get('revision') else ''
                            if not part_revision:
                                part_revision = 'a'
                            if part_revision == target_revision_normalized:
                                existing_parts.append({'node': part_info})
                                target_part_id = part_info.get('id')
                                log_and_print(f"Strategy 2: Found existing part {part_number} (revision {cleaned_revision}) with ID {target_part_id} in inventory", 'info')
                                break
            
            # Strategy 2b: Query parts directly WITHOUT partType filter (catches PART-type parts with no inventory)
            if not target_part_id:
                log_and_print(f"Strategy 2b: Querying Ion directly for part {part_number} (revision {cleaned_revision}) with ANY partType...", 'info')
                get_part_any_type_query = read_query('get_part_by_number_and_revision_any_type.graphql')
                part_variables_any = {
                    'partNumber': part_number.lower(),
                    'revision': cleaned_revision.lower() if cleaned_revision else 'a',
                    'first': 10,
                    'after': None
                }
                
                part_search_any = post_graphql(token, config, {'query': get_part_any_type_query, 'variables': part_variables_any}, environment)
                
                if 'errors' in part_search_any:
                    log_and_print(f"Strategy 2b failed: GraphQL errors: {part_search_any['errors']}", 'warning')
                else:
                    part_edges_any = part_search_any.get('data', {}).get('parts', {}).get('edges', [])
                    if part_edges_any:
                        part_info_any = part_edges_any[0].get('node', {})
                        if part_info_any:
                            found_part_type = part_info_any.get('partType', '').upper()
                            found_part_id = part_info_any.get('id')
                            log_and_print(f"Strategy 2b: Found part {part_number} with ID {found_part_id}, partType={found_part_type}", 'info')
                            
                            if found_part_type == 'PART':
                                log_and_print(f"Strategy 2b: Found PART-type part {part_number} (ID {found_part_id}) - will convert to TOOL for serial {serial_number}", 'info')
                                existing_parts.append({'node': part_info_any})
                                target_part_id = found_part_id
                            else:
                                existing_parts.append({'node': part_info_any})
                                target_part_id = found_part_id
                    else:
                        log_and_print(f"Strategy 2b: No part found with partNumber='{part_number.lower()}', revision='{cleaned_revision.lower()}' (any type)", 'warning')
            
            # If it's a PART type, we need to convert it to TOOL (this is allowed)
            if existing_parts:
                part_node = existing_parts[0]['node']
                part_type = part_node.get('partType', '')
                
                if part_type == 'PART':
                    log_and_print(f"Found PART type {part_number} - will convert to TOOL for {serial_number}", 'info')
            
            # If still not found, create it
            if not target_part_id:
                log_and_print(f"Part {part_number} (revision {cleaned_revision}) doesn't exist, creating new part", 'info')
                
                create_part_mutation = read_query('create_tool.graphql')
                
                # Build attributes from TipQA data
                attributes = build_tipqa_attributes(tool_data)
                
                # CRITICAL SAFETY CHECK: Verify part number is not protected before creating as TOOL
                if is_part_number_protected(part_number, config):
                    error_msg = f"CRITICAL SAFETY CHECK FAILED: Part number '{part_number}' is protected and cannot be created as TOOL. Skipping creation."
                    log_and_print(f"{error_msg}", 'error')
                    return False, error_msg
                
                create_variables_input = {
                        'partNumber': part_number,
                        'revision': cleaned_revision,  # Use cleaned revision for Ion compatibility
                        'partType': 'TOOL',
                        'description': description,
                        'trackingType': 'SERIAL',
                        'attributes': attributes
                    }
                # Only include maintenanceIntervalSeconds if we have a valid integer value
                service_interval = safe_convert_service_interval(tool_data.get('service_interval_seconds'))
                if service_interval is not None:
                    create_variables_input['maintenanceIntervalSeconds'] = service_interval
                create_variables = {'input': create_variables_input}
                
                # Add a small random delay to prevent race conditions when creating the same part
                delay = random.uniform(0.1, 0.5)
                time.sleep(delay)
                
                create_result = post_graphql(token, config, {'query': create_part_mutation, 'variables': create_variables}, environment)
                
                if 'errors' in create_result:
                    # Check if this is a part already exists error
                    error_messages = [str(error) for error in create_result['errors']]
                    if any('already exists' in msg for msg in error_messages):
                        log_and_print(f"Part {part_number} already exists (created by another process), searching for it...", 'warning')
                        # Try to find the existing part with multiple search strategies
                        target_part_id = None
                        
                        # Strategy 3: Query Ion directly for the part (CRITICAL: works even if part has no inventory)
                        # This should be tried FIRST because the part might exist but have no inventory yet
                        if not target_part_id:
                            log_and_print(f"Strategy 3: Querying Ion directly for part {part_number} (revision {cleaned_revision}) with partType = TOOL...", 'info')
                            get_part_query = read_query('get_part_by_number_and_revision.graphql')
                            part_variables = {
                                'partNumber': part_number.lower(),
                                'revision': cleaned_revision.lower() if cleaned_revision else 'a',
                                'first': 10,
                                'after': None
                            }
                            
                            part_search_result = post_graphql(token, config, {'query': get_part_query, 'variables': part_variables}, environment)
                            
                            if 'errors' in part_search_result:
                                log_and_print(f"Strategy 3 failed: GraphQL errors: {part_search_result['errors']}", 'warning')
                            else:
                                part_edges = part_search_result.get('data', {}).get('parts', {}).get('edges', [])
                                if part_edges:
                                    first_edge = part_edges[0]
                                    part_info = first_edge.get('node', {})
                                    if part_info:
                                        target_part_id = part_info.get('id')
                                        log_and_print(f"Strategy 3: Found existing part {part_number} with ID {target_part_id} via direct parts query", 'info')
                                else:
                                    log_and_print(f"Strategy 3: No part found with partNumber='{part_number.lower()}', revision='{cleaned_revision.lower()}', and partType=TOOL", 'warning')
                        
                        # Strategy 4: Search for parts through ALL inventory items (paginated) - fallback
                        # Only used if direct part query fails (part might be in inventory but not queryable directly)
                        if not target_part_id:
                            log_and_print(f"Strategy 4: Searching for part through all inventory items (any serial) for part {part_number} (revision {cleaned_revision})...", 'info')
                            get_all_inventory_query = read_query('get_all_tool_inventory.graphql')
                            found_parts = {}
                            page_count = 0
                            max_pages = 10  # Search up to 10,000 inventory items (10 pages * 1000)
                            cursor = None
                            
                            while page_count < max_pages and not found_parts:
                                all_inventory_variables = {'first': 1000, 'after': cursor}
                                all_inventory_result = post_graphql(token, config, {'query': get_all_inventory_query, 'variables': all_inventory_variables}, environment)
                                
                                if 'errors' in all_inventory_result:
                                    log_and_print(f"Strategy 5 failed on page {page_count + 1}: GraphQL errors: {all_inventory_result['errors']}", 'warning')
                                    break
                                
                                all_inventory_edges = all_inventory_result.get('data', {}).get('partInventories', {}).get('edges', [])
                                if not all_inventory_edges:
                                    break
                                
                                # Search through inventory items to find parts matching our part number and revision
                                for edge in all_inventory_edges:
                                    inv_node = edge.get('node', {})
                                    part_info = inv_node.get('part', {})
                                    if part_info:
                                        part_num = part_info.get('partNumber', '').lower()
                                        part_rev = part_info.get('revision', '').lower()
                                        part_info_id = part_info.get('id')
                                        
                                        # Match part number and revision (case-insensitive)
                                        if (part_num == part_number.lower() and 
                                            part_rev == cleaned_revision.lower() and
                                            part_info_id not in found_parts):
                                            # Found matching part - store it
                                            found_parts[part_info_id] = part_info_id
                                
                                # Check if there are more pages
                                page_info = all_inventory_result.get('data', {}).get('partInventories', {}).get('pageInfo', {})
                                if page_info.get('hasNextPage', False):
                                    cursor = page_info.get('endCursor')
                                    page_count += 1
                                else:
                                    break
                                
                                # If we found the part, stop paginating
                                if found_parts:
                                    break
                            
                            if found_parts:
                                # Use the first matching part
                                target_part_id = list(found_parts.values())[0]
                                log_and_print(f"Strategy 4: Found existing part {part_number} with ID {target_part_id} after searching {page_count + 1} page(s) of inventory", 'info')
                            else:
                                log_and_print(f"Strategy 4: Searched {page_count + 1} page(s) of inventory but no part matches partNumber='{part_number.lower()}' and revision='{cleaned_revision.lower()}'", 'warning')
                        
                        # Strategy 5: Query parts directly WITHOUT partType filter (catches PART-type parts with no inventory)
                        if not target_part_id:
                            log_and_print(f"Strategy 5: Querying Ion directly for part {part_number} (revision {cleaned_revision}) with ANY partType...", 'info')
                            get_part_any_type_query = read_query('get_part_by_number_and_revision_any_type.graphql')
                            part_variables_any = {
                                'partNumber': part_number.lower(),
                                'revision': cleaned_revision.lower() if cleaned_revision else 'a',
                                'first': 10,
                                'after': None
                            }
                            
                            part_search_any = post_graphql(token, config, {'query': get_part_any_type_query, 'variables': part_variables_any}, environment)
                            
                            if 'errors' in part_search_any:
                                log_and_print(f"Strategy 5 failed: GraphQL errors: {part_search_any['errors']}", 'warning')
                            else:
                                part_edges_any = part_search_any.get('data', {}).get('parts', {}).get('edges', [])
                                if part_edges_any:
                                    part_info_any = part_edges_any[0].get('node', {})
                                    if part_info_any:
                                        found_part_type = part_info_any.get('partType', '').upper()
                                        found_part_id = part_info_any.get('id')
                                        log_and_print(f"Strategy 5: Found part {part_number} with ID {found_part_id}, partType={found_part_type}", 'info')
                                        
                                        if found_part_type == 'PART':
                                            log_and_print(f"Strategy 5: Found PART-type part {part_number} (ID {found_part_id}) - will convert to TOOL for serial {serial_number}", 'info')
                                            target_part_id = found_part_id
                                        else:
                                            target_part_id = found_part_id
                                else:
                                    log_and_print(f"Strategy 5: No part found with partNumber='{part_number.lower()}', revision='{cleaned_revision.lower()}' (any type)", 'warning')
                        
                        if not target_part_id:
                            error_msg = f"Failed to find existing part {part_number} (revision {cleaned_revision}) after all search strategies. Cannot proceed with update."
                            log_and_print(f"{error_msg}", 'error')
                            return False, error_msg
                    else:
                        error_msg = f"Failed to create part {part_number} (revision {cleaned_revision}): {create_result['errors']}"
                        log_and_print(f"{error_msg}", 'error')
                        return False, error_msg
                else:
                    target_part_id = create_result.get('data', {}).get('createPart', {}).get('part', {}).get('id')
                    if not target_part_id:
                        error_msg = f"Unexpected response when creating part {part_number} (revision {cleaned_revision}) - no part ID in response"
                        log_and_print(f"{error_msg}", 'error')
                        return False, error_msg
                    
                    log_and_print(f"Created new part {part_number} with ID {target_part_id}", 'info')
        
        # Update the inventory to point to the correct part
        if target_part_id != current_part_id:
            log_and_print(f"Updating inventory for tool {serial_number} to use part ID {target_part_id}", 'info')
            
            # Ion requires unit of measure to match between inventory and target part.
            # Tools should use "each" (id=1). If the target part has a different UoM, fix it first.
            try:
                uom_check_query = '''query($id: ID!) { part(id: $id) { id unitOfMeasure { id type } _etag } }'''
                source_part_query = '''query($id: ID!) { part(id: $id) { id unitOfMeasure { id type } } }'''
                target_uom_result = post_graphql(token, config, {'query': uom_check_query, 'variables': {'id': str(target_part_id)}}, environment)
                source_uom_result = post_graphql(token, config, {'query': source_part_query, 'variables': {'id': str(current_part_id)}}, environment)
                
                target_uom_id = target_uom_result.get('data', {}).get('part', {}).get('unitOfMeasure', {}).get('id')
                source_uom_id = source_uom_result.get('data', {}).get('part', {}).get('unitOfMeasure', {}).get('id')
                
                if target_uom_id and source_uom_id and target_uom_id != source_uom_id:
                    target_etag = target_uom_result['data']['part']['_etag']
                    log_and_print(f"Target part {target_part_id} has unitOfMeasure id={target_uom_id}, source has id={source_uom_id}. Fixing target part UoM before inventory move.", 'warning')
                    fix_uom_mutation = read_query('update_tool.graphql')
                    fix_uom_variables = {
                        'input': {
                            'id': target_part_id,
                            'etag': target_etag,
                            'unitOfMeasureId': source_uom_id
                        }
                    }
                    fix_uom_result = post_graphql_with_etag_refresh(token, config, {'query': fix_uom_mutation, 'variables': fix_uom_variables}, environment)
                    if 'errors' in fix_uom_result:
                        log_and_print(f"Warning: Could not fix UoM on target part {target_part_id}: {fix_uom_result['errors']}", 'warning')
                    else:
                        log_and_print(f"Fixed target part {target_part_id} unitOfMeasure to id={source_uom_id}", 'info')
            except Exception as e:
                log_and_print(f"Warning: UoM pre-check failed for target part {target_part_id}: {e}", 'warning')
            
            update_inventory_mutation = read_query('update_inventory_with_attributes.graphql')
            update_variables = {
                'input': {
                    'id': tool_id,
                    'etag': tool_etag,
                    'partId': target_part_id
                }
            }
            
            inventory_result = post_graphql_with_etag_refresh(token, config, {'query': update_inventory_mutation, 'variables': update_variables}, environment)
            
            if 'errors' in inventory_result:
                error_msg = f"Failed to update inventory for tool {serial_number} to use part {part_number} (revision {cleaned_revision}): {inventory_result['errors']}"
                log_and_print(f"{error_msg}", 'error')
                return False, error_msg
            
            log_and_print(f"Successfully updated inventory for tool {serial_number}", 'info')
        
        # Update the part information if needed (description, maintenanceIntervalSeconds, etc.)
        # NOTE: Part updates are NON-BLOCKING - if they fail, we continue with inventory updates
        # Only update description if the part doesn't already have one (to avoid concurrency errors when multiple tools share the same part)
        service_interval = safe_convert_service_interval(
            tool_data.get('service_interval_seconds') or tool_data.get('tipqa_service_interval_seconds')
        )
        should_update_part = False

        # CRITICAL: Check if service_interval has changed by comparing TipQA to Ion.
        # Use multiple possible keys for Ion (merged df may use ion_part_maintenanceIntervalSeconds or others).
        tipqa_si_raw = tool_data.get('service_interval_seconds') or tool_data.get('tipqa_service_interval_seconds', '')
        ion_si_raw = (
            tool_data.get('ion_part_maintenanceIntervalSeconds') if tool_data.get('ion_part_maintenanceIntervalSeconds') is not None and not pd.isna(tool_data.get('ion_part_maintenanceIntervalSeconds')) else
            tool_data.get('ion_maintenance_interval_seconds') if tool_data.get('ion_maintenance_interval_seconds') is not None and not pd.isna(tool_data.get('ion_maintenance_interval_seconds')) else
            tool_data.get('ion_service_interval_seconds', '')
        )
        if ion_si_raw is None or (isinstance(ion_si_raw, str) and ion_si_raw.strip() == ''):
            ion_si_raw = ''
        tipqa_si_int = normalize_service_interval_for_comparison(tipqa_si_raw)
        ion_si_int = normalize_service_interval_for_comparison(ion_si_raw)
        needs_service_interval_update = (tipqa_si_int != ion_si_int)
        
        # Prepare description variables for comparison and updates (available in broader scope)
        current_description = part.get('description', '')
        current_desc_str = str(current_description).strip() if current_description else ''
        tipqa_desc_str = str(description).strip() if description else ''
        
        # Normalize for comparison (case-insensitive, whitespace normalized)
        current_desc_normalized = re.sub(r'\s+', ' ', current_desc_str).lower() if current_desc_str else ''
        tipqa_desc_normalized = re.sub(r'\s+', ' ', tipqa_desc_str).lower() if tipqa_desc_str else ''
        
        # Determine if description needs update
        description_needs_update = (not current_desc_normalized and tipqa_desc_normalized) or (current_desc_normalized != tipqa_desc_normalized and tipqa_desc_normalized)
        
        # OPTIMIZATION: Reduce verbose logging - only log for first few tools or specific serials
        # Debug logging for description comparison (reduced verbosity)
        if (current_desc_str or tipqa_desc_str) and description_needs_update:
            # Only log when description actually needs update, and only for first occurrence or specific tools
            if not current_desc_normalized:
                log_and_print(f"Updating description for part {part_number} (part has no description, adding from TipQA)", 'info')
            else:
                log_and_print(f"Updating description for part {part_number} (description changed: Ion='{current_desc_str}' -> TipQA='{tipqa_desc_str}')", 'info')
        
        if target_part_id == current_part_id:
            # Same part, just update the description if needed
            
            # OPTIMIZATION: Check cache to avoid redundant part updates
            part_cache_key = None
            if updated_parts_cache is not None and parts_cache_lock is not None:
                # part_number and revision are already cleaned at the start of the function
                part_cache_key = (part_number.lower(), cleaned_revision.lower() if cleaned_revision else 'a')
                with parts_cache_lock:
                    if part_cache_key in updated_parts_cache:
                        # Part was already updated in batch phase - skip redundant update
                        log_and_print(f"Skipping redundant part description update for {part_number} (already updated in batch)", 'info')
                        description_needs_update = False
            
            # Update description if needed
            if description_needs_update:
                # Description needs update
                if not current_desc_normalized:
                    log_and_print(f"Updating description for part {part_number} (part has no description, adding from TipQA: '{description}')", 'info')
                else:
                    log_and_print(f"Updating description for part {part_number} (description changed: Ion='{current_description}' -> TipQA='{description}')", 'info')
                    log_and_print(f"  Normalized comparison: Ion='{current_desc_normalized}' vs TipQA='{tipqa_desc_normalized}'", 'info')
                
                # Update the part description and attributes with retry logic for concurrency errors
                part_mutation = read_query('update_tool.graphql')
                max_retries = 5  # Increased retries for concurrency errors
                part_update_success = False
                
                for attempt in range(max_retries):
                    # Refresh ETag for the part before each attempt using dedicated function
                    updated_etag = get_part_etag(token, config, target_part_id, environment)
                    
                    if not updated_etag:
                        log_and_print(f"Failed to get updated ETag for part {part_number}", 'warning')
                        if attempt < max_retries - 1:
                            wait_time = 0.5 * (2 ** attempt) + random.uniform(0, 0.5)
                            time.sleep(wait_time)
                            continue
                        # Non-blocking: Log warning but continue
                        log_and_print(f"Warning: Could not update part description for {part_number} after {max_retries} attempts - continuing with inventory update", 'warning')
                        break
                    
                    # Get existing attributes with their etags - need to query part details
                    refresh_query = read_query('get_part_etag.graphql')
                    refresh_variables = {'id': target_part_id}
                    refresh_result = post_graphql(token, config, {'query': refresh_query, 'variables': refresh_variables}, environment)
                    
                    existing_attributes = {}
                    if 'errors' not in refresh_result:
                        updated_part = refresh_result.get('data', {}).get('part', {})
                        if 'attributes' in updated_part:
                            for attr in updated_part.get('attributes', []):
                                # GraphQL returns 'Etag' (capital E)
                                attr_etag = attr.get('Etag', '')
                                existing_attributes[attr.get('key', '')] = {
                                    'value': attr.get('value', ''),
                                    'etag': attr_etag
                                }
                    
                    # Always update attributes with TipQA data
                    tipqa_attributes = build_tipqa_attributes(tool_data)
                    updated_attributes = []
                    
                    if existing_attributes:
                        # Part has existing attributes - preserve etags for existing ones
                        for tipqa_attr in tipqa_attributes:
                            key = tipqa_attr['key']
                            value = tipqa_attr['value']
                            
                            if key in existing_attributes:
                                # This attribute exists - update with its etag if we have one
                                attr_etag = existing_attributes[key].get('etag', '')
                                if attr_etag:
                                    # Update existing attribute with its etag
                                    updated_attributes.append({
                                        'key': key,
                                        'value': value,
                                        'etag': attr_etag
                                    })
                                else:
                                    # Attribute exists but no etag - omit etag field
                                    updated_attributes.append({
                                        'key': key,
                                        'value': value
                                    })
                            else:
                                # New attribute - add without etag
                                updated_attributes.append({
                                    'key': key,
                                    'value': value
                                })
                    else:
                        # Part has no existing attributes - add all TipQA attributes without etags
                        updated_attributes = tipqa_attributes
                    
                    # Always include attributes in the update
                    part_variables_input = {
                        'id': target_part_id,
                        'etag': updated_etag,
                        'description': description,
                        'attributes': updated_attributes
                    }
                    # TipQA is the source of truth for maintenanceIntervalSeconds:
                    # send the value (or null to clear) when an update is needed.
                    if needs_service_interval_update:
                        part_variables_input['maintenanceIntervalSeconds'] = service_interval
                    part_variables = {'input': part_variables_input}
                    
                    part_result = post_graphql(token, config, {'query': part_mutation, 'variables': part_variables}, environment)
                    
                    if 'errors' in part_result:
                        # Check if it's a concurrency error
                        error_messages = [str(error) for error in part_result['errors']]
                        has_concurrency_error = any(
                            'CONCURRENCY_ERROR' in msg or 'Etag does not match' in msg
                            for msg in error_messages
                        )
                        
                        if has_concurrency_error and attempt < max_retries - 1:
                            wait_time = 0.5 * (2 ** attempt) + random.uniform(0, 0.5)
                            log_and_print(f"Concurrency error updating part {part_number}, retrying in {wait_time:.1f}s (attempt {attempt + 2}/{max_retries})...", 'warning')
                            time.sleep(wait_time)
                            # ETag will be refreshed at the start of the next loop iteration
                            continue
                        else:
                            # Non-blocking: Log warning but continue with inventory update
                            log_and_print(f"Warning: Could not update part description for {part_number} after {attempt + 1} attempts: {error_messages} - continuing with inventory update", 'warning')
                            break
                    else:
                        # Success - part description updated
                        part_update_success = True
                        log_and_print(f"Successfully updated part description for {part_number}", 'info')
                        # Add to cache to avoid redundant updates
                        if updated_parts_cache is not None and parts_cache_lock is not None and part_cache_key:
                            with parts_cache_lock:
                                updated_parts_cache.add(part_cache_key)
                        break
                
                # Note: We don't return False here - part description updates are non-blocking
                if part_update_success:
                    log_and_print(f"Successfully updated part description for {part_number}", 'info')
                else:
                    log_and_print(f"Warning: Part description update failed for {part_number} - continuing with inventory update (non-blocking)", 'warning')
                
                # After updating part, get fresh inventory ETag
                fresh_inventory_etag = get_inventory_etag(token, config, tool_id, environment)
                if fresh_inventory_etag:
                    tool_etag = fresh_inventory_etag
                    log_and_print(f"Got fresh inventory ETag after part update: {fresh_inventory_etag}", 'info')
            
            # Check service interval against live Ion and update when TipQA differs
            # (including clearing when TipQA is null). TipQA is the source of truth.
            #
            # If the batch pre-update phase already updated this part (it's in
            # updated_parts_cache), skip the redundant per-tool update.
            si_already_batched = False
            if updated_parts_cache is not None and parts_cache_lock is not None and part_cache_key:
                with parts_cache_lock:
                    si_already_batched = part_cache_key in updated_parts_cache
            
            if si_already_batched:
                log_and_print(f"Skipping redundant service interval update for part {part_number} (already updated in batch)", 'info')
                service_interval_differs_from_live = False
            else:
                refresh_query = read_query('get_part_etag.graphql')
                refresh_variables = {'id': target_part_id}
                refresh_result = post_graphql(token, config, {'query': refresh_query, 'variables': refresh_variables}, environment)
                current_interval = None
                if 'errors' not in refresh_result:
                    updated_part = refresh_result.get('data', {}).get('part', {})
                    current_interval = updated_part.get('maintenanceIntervalSeconds')
                current_interval_normalized = None if (current_interval is None or pd.isna(current_interval) or str(current_interval).strip() == '') else int(float(current_interval))
                service_interval_normalized = service_interval
                service_interval_differs_from_live = (current_interval_normalized != service_interval_normalized)

            if service_interval_differs_from_live:
                log_and_print(f"Updating maintenanceIntervalSeconds for part {part_number} (ID: {target_part_id}) to {service_interval} seconds", 'info')
                part_mutation = read_query('update_tool.graphql')
                max_retries = 5
                part_update_success = False
                for attempt in range(max_retries):
                    updated_etag = get_part_etag(token, config, target_part_id, environment)
                    if not updated_etag:
                        log_and_print(f"Failed to get updated ETag for part {part_number}", 'warning')
                        if attempt < max_retries - 1:
                            wait_time = 0.5 * (2 ** attempt) + random.uniform(0, 0.5)
                            time.sleep(wait_time)
                            continue
                        log_and_print(f"Warning: Could not update maintenanceIntervalSeconds for {part_number} after {max_retries} attempts - continuing", 'warning')
                        break
                    # Re-fetch part inside loop for fresh attributes and etag
                    refresh_query_loop = read_query('get_part_etag.graphql')
                    refresh_result_loop = post_graphql(token, config, {'query': refresh_query_loop, 'variables': refresh_variables}, environment)
                    existing_attributes = {}
                    if 'errors' not in refresh_result_loop:
                        updated_part = refresh_result_loop.get('data', {}).get('part', {})
                        if 'attributes' in updated_part:
                            for attr in updated_part.get('attributes', []):
                                attr_etag = attr.get('Etag', '')
                                existing_attributes[attr.get('key', '')] = {
                                    'value': attr.get('value', ''),
                                    'etag': attr_etag
                                }
                    tipqa_attributes = build_tipqa_attributes(tool_data)
                    updated_attributes = []
                    if existing_attributes:
                        for tipqa_attr in tipqa_attributes:
                            key = tipqa_attr['key']
                            value = tipqa_attr['value']
                            if key in existing_attributes:
                                attr_etag = existing_attributes[key].get('etag', '')
                                if attr_etag:
                                    updated_attributes.append({'key': key, 'value': value, 'etag': attr_etag})
                                else:
                                    updated_attributes.append({'key': key, 'value': value})
                            else:
                                updated_attributes.append({'key': key, 'value': value})
                    else:
                        updated_attributes = tipqa_attributes
                    part_variables_input = {
                        'id': target_part_id,
                        'etag': updated_etag,
                        'maintenanceIntervalSeconds': service_interval,
                        'description': description if (description_needs_update or tipqa_desc_str) else current_desc_str,
                        'attributes': updated_attributes
                    }
                    part_variables = {'input': part_variables_input}
                    part_result = post_graphql(token, config, {'query': part_mutation, 'variables': part_variables}, environment)
                    if 'errors' in part_result:
                        error_messages = [str(error) for error in part_result['errors']]
                        has_concurrency_error = any('CONCURRENCY_ERROR' in msg or 'Etag does not match' in msg for msg in error_messages)
                        if has_concurrency_error and attempt < max_retries - 1:
                            wait_time = 0.5 * (2 ** attempt) + random.uniform(0, 0.5)
                            log_and_print(f"Concurrency error updating part {part_number}, retrying in {wait_time:.1f}s (attempt {attempt + 2}/{max_retries})...", 'warning')
                            time.sleep(wait_time)
                            continue
                        else:
                            log_and_print(f"Warning: Could not update maintenanceIntervalSeconds for {part_number} after {attempt + 1} attempts: {error_messages} - continuing", 'warning')
                            break
                    else:
                        part_update_success = True
                        log_and_print(f"Successfully updated maintenanceIntervalSeconds for part {part_number}", 'info')
                        break
        else:
            # Part ID changed - check service interval against live Ion when TipQA
            # has a positive value.  Never clear (service_interval is None) because
            # maintenanceIntervalSeconds is a part-level field shared across serials.
            service_interval_differs_from_live = False
            if service_interval is not None:
                refresh_query = read_query('get_part_etag.graphql')
                refresh_variables = {'id': target_part_id}
                refresh_result = post_graphql(token, config, {'query': refresh_query, 'variables': refresh_variables}, environment)
                current_interval = None
                if 'errors' not in refresh_result:
                    updated_part = refresh_result.get('data', {}).get('part', {})
                    current_interval = updated_part.get('maintenanceIntervalSeconds')
                current_interval_normalized = None if (current_interval is None or pd.isna(current_interval) or str(current_interval).strip() == '') else int(float(current_interval))
                service_interval_normalized = service_interval
                service_interval_differs_from_live = (current_interval_normalized != service_interval_normalized)
                if not service_interval_differs_from_live:
                    log_and_print(f"Service interval for part {part_number} (ID: {target_part_id}) already matches TipQA (no update needed)", 'info')

            if service_interval_differs_from_live:
                log_and_print(f"Updating maintenanceIntervalSeconds for part {part_number} (ID: {target_part_id}) to {service_interval} seconds", 'info')

            part_mutation = read_query('update_tool.graphql')
            max_retries = 5
            part_update_success = False

            if service_interval_differs_from_live:
                for attempt in range(max_retries):
                    updated_etag = get_part_etag(token, config, target_part_id, environment)

                    if not updated_etag:
                        log_and_print(f"Failed to get updated ETag for part {part_number}", 'warning')
                        if attempt < max_retries - 1:
                            wait_time = 0.5 * (2 ** attempt) + random.uniform(0, 0.5)
                            time.sleep(wait_time)
                            continue
                        log_and_print(f"Warning: Could not update maintenanceIntervalSeconds for {part_number} after {max_retries} attempts - continuing", 'warning')
                        break

                    # Get existing part info to preserve description and attributes
                    refresh_result = post_graphql(token, config, {'query': refresh_query, 'variables': refresh_variables}, environment)

                    existing_attributes = {}
                    existing_description = ''
                    if 'errors' not in refresh_result:
                        updated_part = refresh_result.get('data', {}).get('part', {})
                        existing_description = updated_part.get('description', '')
                        if 'attributes' in updated_part:
                            for attr in updated_part.get('attributes', []):
                                attr_etag = attr.get('Etag', '')
                                existing_attributes[attr.get('key', '')] = {
                                    'value': attr.get('value', ''),
                                    'etag': attr_etag
                                }

                    # Update attributes with TipQA data
                    tipqa_attributes = build_tipqa_attributes(tool_data)
                    updated_attributes = []

                    if existing_attributes:
                        for tipqa_attr in tipqa_attributes:
                            key = tipqa_attr['key']
                            value = tipqa_attr['value']
                            if key in existing_attributes:
                                attr_etag = existing_attributes[key].get('etag', '')
                                if attr_etag:
                                    updated_attributes.append({'key': key, 'value': value, 'etag': attr_etag})
                                else:
                                    updated_attributes.append({'key': key, 'value': value})
                            else:
                                updated_attributes.append({'key': key, 'value': value})
                    else:
                        updated_attributes = tipqa_attributes

                    # Include description in update (use TipQA description if it differs or if Ion has none)
                    part_variables_input = {
                        'id': target_part_id,
                        'etag': updated_etag,
                        'maintenanceIntervalSeconds': service_interval,
                        'description': description if (description_needs_update or tipqa_desc_str) else existing_description,
                        'attributes': updated_attributes
                    }

                    part_variables = {'input': part_variables_input}
                    part_result = post_graphql(token, config, {'query': part_mutation, 'variables': part_variables}, environment)

                    if 'errors' in part_result:
                        error_messages = [str(error) for error in part_result['errors']]
                        has_concurrency_error = any('CONCURRENCY_ERROR' in msg or 'Etag does not match' in msg for msg in error_messages)

                        if has_concurrency_error and attempt < max_retries - 1:
                            wait_time = 0.5 * (2 ** attempt) + random.uniform(0, 0.5)
                            log_and_print(f"Concurrency error updating part {part_number}, retrying in {wait_time:.1f}s (attempt {attempt + 2}/{max_retries})...", 'warning')
                            time.sleep(wait_time)
                            continue
                        else:
                            log_and_print(f"Warning: Could not update maintenanceIntervalSeconds for {part_number} after {attempt + 1} attempts: {error_messages} - continuing", 'warning')
                            break
                    else:
                        part_update_success = True
                        log_and_print(f"Successfully updated maintenanceIntervalSeconds for part {part_number}", 'info')
                        break

        # Update inventory-level attributes (Manufacturer, Asset Serial Number, Location, etc.)
        log_and_print(f"Updating inventory-level attributes for tool {serial_number}", 'info')
        if _trace:
            log_and_print(f"TRACE {serial_number}: REACHED inventory-level update section", 'info')

        # Get current inventory data to get existing inventory attributes with etags
        inventory_query = read_query('get_inventory_etag.graphql')
        inventory_variables = {'id': tool_id}
        
        inventory_result = post_graphql(token, config, {'query': inventory_query, 'variables': inventory_variables}, environment)
        
        if 'errors' in inventory_result:
            log_and_print(f"Failed to get inventory data for tool {serial_number}: {inventory_result['errors']}", 'error')
            return False
        
        current_inventory = inventory_result.get('data', {}).get('partInventory', {})
        if not current_inventory:
            log_and_print(f"No inventory data found for tool {serial_number}", 'error')
            return False
        
        # Get existing inventory attributes with their etags from the query
        existing_inventory_attributes = {}
        if 'attributes' in current_inventory:
            for attr in current_inventory.get('attributes', []):
                # GraphQL returns 'Etag' (capital E)
                attr_etag = attr.get('Etag', '')
                existing_inventory_attributes[attr.get('key', '')] = {
                    'value': attr.get('value', ''),
                    'etag': attr_etag
                }
        
        # Build inventory-level attributes from TipQA data
        tipqa_inventory_attributes = build_tipqa_inventory_attributes(tool_data)
        updated_inventory_attributes = []
        
        if existing_inventory_attributes:
            # Inventory has existing attributes - preserve etags for existing ones
            for tipqa_attr in tipqa_inventory_attributes:
                key = tipqa_attr['key']
                value = tipqa_attr['value']
                
                if key in existing_inventory_attributes:
                    # This attribute exists - update with its etag if we have one
                    attr_etag = existing_inventory_attributes[key].get('etag', '')
                    if attr_etag:
                        # Update existing attribute with its etag
                        updated_inventory_attributes.append({
                            'key': key,
                            'value': value,
                            'etag': attr_etag
                        })
                    else:
                        # Attribute exists but no etag - omit etag field
                        updated_inventory_attributes.append({
                            'key': key,
                            'value': value
                        })
                else:
                    # New attribute - add without etag
                    updated_inventory_attributes.append({
                        'key': key,
                        'value': value
                    })
        else:
            # Inventory has no existing attributes - add all TipQA attributes without etags
            updated_inventory_attributes = tipqa_inventory_attributes
        
        # Update inventory with attributes
        inventory_mutation = read_query('update_inventory_with_attributes.graphql')
        
        # Get location ID for TipQA location (name or code); used for inventory update and when marking available
        tipqa_location = tool_data.get('tipqa_location') or tool_data.get('location') or tool_data.get('tipqa_location_name')
        if _trace:
            log_and_print(f"TRACE {serial_number}: Resolving tipqa_location='{tipqa_location}'", 'info')
        location_id, location_source = get_ion_location_id_for_tipqa(token, config, environment, tipqa_location)
        if not (location_id and location_source) and tipqa_location and str(tipqa_location).strip() and str(tipqa_location).strip().lower() != 'nan':
            log_and_print(f"Warning: Could not resolve TipQA location '{tipqa_location}' to an Ion location for tool {serial_number}. Add sync_exceptions.location_code_to_ion_name or location_code_to_ion_id in config if TipQA uses location codes.", 'warning')
        
        # Format last maintenance date properly
        last_maintenance_date_raw = tool_data.get('tipqa_last_maintenance_date') or tool_data.get('last_maintenance_date')
        last_maintenance_date = format_date_for_ion(last_maintenance_date_raw)
        
        # Determine if tool should be AVAILABLE based on TipQA data
        maintenance_status = tool_data.get('tipqa_maintenance_status') or tool_data.get('maintenance_status', '')
        revision_status = tool_data.get('tipqa_revision_status') or tool_data.get('revision_status', '')
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
        
        # Get current Ion status to check if we need to change availability
        ion_status = tool_data.get('ion_status', '')
        # Handle ion_unavailable - it might be a boolean, string, or missing
        import pandas as pd
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
        needs_availability_update = False
        
        if _trace:
            log_and_print(f"TRACE {serial_number}: location_id={location_id}, location_source={location_source}, should_be_available={should_be_available}, is_currently_unavailable={is_currently_unavailable}, ion_status={ion_status}, ion_unavailable={ion_unavailable}", 'info')

        if should_be_available and is_currently_unavailable:
            # Tool should be available but is currently unavailable - mark as available and update location
            needs_availability_update = True
            log_and_print(f"Tool {serial_number} should be AVAILABLE (TipQA: maintenance_status={maintenance_status}, location={tipqa_location}) but Ion shows UNAVAILABLE - will set unavailable=False and update location to TipQA location", 'info')
            if not location_id and tipqa_location and str(tipqa_location).strip():
                log_and_print(f"Tool {serial_number}: TipQA location could not be resolved to Ion; availability will be cleared but location will not change. Add sync_exceptions.location_code_to_ion_name or location_code_to_ion_id in config to update location.", 'warning')
            # Debug logging for JT00004887
            if serial_number == 'JT00004887':
                log_and_print(f"DEBUG: update_tool for JT00004887 - needs_availability_update=True, will set unavailable=False", 'info')
        
        # Use robust etag handling with retry logic for inventory updates
        max_retries = 3
        update_success = False
        
        for attempt in range(max_retries):
            # Refresh etag before each attempt (CRITICAL for avoiding concurrency errors)
            fresh_etag = get_inventory_etag(token, config, tool_id, environment)
            if not fresh_etag:
                error_msg = f"Failed to get fresh etag for tool {serial_number}"
                log_and_print(f"{error_msg}", 'error')
                if attempt < max_retries - 1:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                return False, error_msg
            
            inventory_update_variables = {
                'input': {
                    'id': tool_id,
                    'etag': fresh_etag,
                    'attributes': updated_inventory_attributes if updated_inventory_attributes else None
                }
            }
            
            # Add location if found
            if location_id:
                inventory_update_variables['input']['locationId'] = location_id
            
            # Add lastMaintainedDate if available
            if last_maintenance_date:
                inventory_update_variables['input']['lastMaintainedDate'] = last_maintenance_date
            
            # IMPORTANT: Do NOT include unavailable in this mutation.
            # Location + attributes are updated first. Availability is handled in a separate step
            # so that a rejected unavailable=False never blocks the location/attribute update.

            # Debug logging for availability-debug serials
            if serial_number in ('JT00004887', 'JT00004653'):
                log_and_print(f"DEBUG: {serial_number} GraphQL inventory update (location+attrs) BEFORE mutation: {inventory_update_variables}", 'info')

            inventory_update_result = post_graphql(token, config, {'query': inventory_mutation, 'variables': inventory_update_variables}, environment)

            if serial_number in ('JT00004887', 'JT00004653'):
                log_and_print(f"DEBUG: {serial_number} GraphQL inventory update (location+attrs) result: {inventory_update_result}", 'info')

            if 'errors' in inventory_update_result:
                error_messages = [str(error) for error in inventory_update_result['errors']]
                has_concurrency_error = any(
                    'CONCURRENCY_ERROR' in msg or 'Etag does not match' in msg
                    for msg in error_messages
                )
                
                if has_concurrency_error and attempt < max_retries - 1:
                    wait_time = 0.5 * (2 ** attempt) + random.uniform(0, 0.5)
                    log_and_print(f"Concurrency error updating inventory for {serial_number}, retrying in {wait_time:.1f}s (attempt {attempt + 2}/{max_retries})...", 'warning')
                    time.sleep(wait_time)
                    continue
                else:
                    error_msg = f"Failed to update inventory attributes for tool {serial_number}: {inventory_update_result['errors']}"
                    log_and_print(f"{error_msg}", 'error')
                    return False, error_msg
            else:
                update_success = True
                break
        
        if not update_success:
            error_msg = f"Failed to update inventory attributes for tool {serial_number} after {max_retries} attempts"
            log_and_print(f"{error_msg}", 'error')
            return False, error_msg
        
        log_and_print(f"Successfully updated inventory attributes for tool {serial_number}", 'info')

        # STEP 2: If tool needs to become AVAILABLE, send a separate mutation for unavailable=False.
        # This is split from Step 1 so that a rejected unavailable field never blocks location/attribute updates.
        if needs_availability_update:
            log_and_print(f"Tool {serial_number} - sending separate mutation to set unavailable=False (marking available)", 'info')
            avail_fresh_etag = get_inventory_etag(token, config, tool_id, environment)
            if avail_fresh_etag:
                avail_variables = {
                    'input': {
                        'id': tool_id,
                        'etag': avail_fresh_etag,
                        'unavailable': False
                    }
                }
                if serial_number in ('JT00004887', 'JT00004653'):
                    log_and_print(f"DEBUG: {serial_number} availability mutation BEFORE: {avail_variables}", 'info')
                avail_result = post_graphql(token, config, {'query': inventory_mutation, 'variables': avail_variables}, environment)
                if serial_number in ('JT00004887', 'JT00004653'):
                    log_and_print(f"DEBUG: {serial_number} availability mutation result: {avail_result}", 'info')
                if 'errors' in avail_result:
                    log_and_print(f"Warning: Could not set unavailable=False for {serial_number}: {avail_result['errors']} - location and attributes were already updated successfully", 'warning')
                else:
                    log_and_print(f"Successfully marked tool {serial_number} as available", 'info')
            else:
                log_and_print(f"Warning: Could not get fresh etag to mark {serial_number} as available - location and attributes were already updated", 'warning')

        log_and_print(f"Successfully updated tool {serial_number}", 'info')
        return True, ''
        
    except Exception as e:
        error_msg = f"Exception updating tool {serial_number}: {str(e)}"
        log_and_print(f"{error_msg}", 'error')
        return False, error_msg

def convert_part_to_tool(token: str, config: Dict[str, Any], tool_data: Dict, match_info: Dict, environment: str = 'v1_sandbox', dry_run: bool = True) -> bool:
    """
    Convert a PART to TOOL following the flow document requirements.
    CRITICAL: Only for exact matches (same serial AND same part number).
    """
    
    serial_number = tool_data.get('serial_number', 'UNKNOWN')
    part_number = clean_part_number(tool_data.get('part_number', ''))
    
    if dry_run:
        log_and_print(f"DRY RUN: Would convert PART to TOOL for {serial_number}", 'info')
        return True
    
    try:
        # CRITICAL SAFETY CHECK: Verify exact serial + part number match
        if 'match' in match_info:
            match = match_info.get('match', {})
        else:
            match = match_info
            
        # Get the current part information from Ion
        current_part_id = match.get('part', {}).get('id')
        current_part_etag = match.get('part', {}).get('_etag')
        
        if not current_part_id:
            log_and_print(f"Cannot convert PART to TOOL for {serial_number} - no part ID", 'error')
            return False
        
        # Verify the part exists and get its current data
        get_part_query = read_query('get_schema.graphql')  # We need a query to get part details
        # For now, we'll use the match data we already have
        
        # CRITICAL SAFETY CHECK: Verify this is actually a PART type
        current_part_type = match.get('part', {}).get('partType', '')
        if current_part_type != 'PART':
            log_and_print(f"SAFETY CHECK FAILED: Cannot convert {serial_number} - part type is '{current_part_type}', not 'PART'", 'error')
            return False
        
        # CRITICAL SAFETY CHECK: Verify exact serial + part number match with TipQA
        ion_serial = match.get('serialNumber', '')
        ion_part_number = match.get('part', {}).get('partNumber', '')
        
        if ion_serial != serial_number or ion_part_number != part_number:
            log_and_print(f"SAFETY CHECK FAILED: Serial/part mismatch for {serial_number} - Ion: {ion_serial}/{ion_part_number}, TipQA: {serial_number}/{part_number}", 'error')
            return False
        
        log_and_print(f"SAFETY CHECKS PASSED: Converting PART to TOOL for {serial_number} (exact match)", 'info')
        
        # Step 1: Change trackingType to SERIAL first
        log_and_print(f"Step 1: Changing trackingType to SERIAL for part {part_number}", 'info')
        
        update_tracking_mutation = read_query('update_tool.graphql')
        update_tracking_variables = {
            'input': {
                'id': current_part_id,
                'etag': current_part_etag,
                'trackingType': 'SERIAL'
            }
        }
        
        tracking_result = post_graphql_with_etag_refresh(token, config, {'query': update_tracking_mutation, 'variables': update_tracking_variables}, environment)
        
        if 'errors' in tracking_result:
            log_and_print(f"Failed to update trackingType for part {part_number}: {tracking_result['errors']}", 'error')
            return False
        
        # Verify the trackingType was actually updated
        updated_tracking_type = tracking_result.get('data', {}).get('updatePart', {}).get('part', {}).get('trackingType')
        log_and_print(f"Successfully updated trackingType to {updated_tracking_type} for part {part_number}", 'info')
        
        # Step 2: Get fresh etag after trackingType change
        fresh_part_etag = tracking_result.get('data', {}).get('updatePart', {}).get('part', {}).get('_etag')
        if not fresh_part_etag:
            log_and_print(f"Cannot get fresh etag after trackingType update for {serial_number}", 'error')
            return False
        
        # Step 3: Update any existing inventory items to be serial-tracked
        log_and_print(f"Step 3: Ensuring all inventory items are serial-tracked for part {part_number}", 'info')
        
        # Get all inventory items for this part using approved query
        # Use get_all_tool_inventory and filter by part number in code
        get_inventory_query = read_query('get_all_tool_inventory.graphql')
        inventory_variables = {'first': 1000, 'after': None}
        inventory_result = post_graphql(token, config, {'query': get_inventory_query, 'variables': inventory_variables}, environment)
        
        if 'errors' in inventory_result:
            log_and_print(f"Error querying inventory for part {part_number}: {inventory_result['errors']}", 'error')
            inventory_items = []
        else:
            # Filter inventory items by part number and revision
            all_inventory_edges = inventory_result.get('data', {}).get('partInventories', {}).get('edges', [])
            inventory_items = []
            for edge in all_inventory_edges:
                inv_node = edge.get('node', {})
                part_info = inv_node.get('part', {})
                if part_info:
                    part_num = part_info.get('partNumber', '').lower()
                    part_rev = part_info.get('revision', '').lower()
                    # Match by part number and revision, and also check if part ID matches
                    if (part_num == part_number.lower() and 
                        part_rev == revision.lower() and
                        part_info.get('id') == current_part_id):
                        inventory_items.append(edge)
            log_and_print(f"Found {len(inventory_items)} inventory items for part {part_number}", 'info')
            
            if len(inventory_items) == 0:
                log_and_print(f"No inventory items found for part {part_number} - this might be why conversion is failing", 'warning')
            
            for edge in inventory_items:
                inventory_item = edge.get('node', {})
                inventory_id = inventory_item.get('id')
                inventory_etag = inventory_item.get('_etag')
                current_serial = inventory_item.get('serialNumber', '')
                
                log_and_print(f"Updating inventory item {inventory_id} (current serial: '{current_serial}') to be serial-tracked", 'info')
                
                # Update inventory item to ensure it's serial-tracked
                update_inventory_mutation = read_query('update_inventory_with_attributes.graphql')
                update_inventory_variables = {
                    'input': {
                        'id': inventory_id,
                        'etag': inventory_etag,
                        'serialNumber': inventory_item.get('serialNumber', '')  # Ensure serial number is set
                    }
                }
                
                inventory_update_result = post_graphql_with_etag_refresh(token, config, {'query': update_inventory_mutation, 'variables': update_inventory_variables}, environment)
                
                if 'errors' in inventory_update_result:
                    log_and_print(f"Warning: Failed to update inventory item {inventory_id} for part {part_number}: {inventory_update_result['errors']}", 'warning')
                else:
                    log_and_print(f"Updated inventory item {inventory_id} to be serial-tracked", 'info')
        
        # CRITICAL SAFETY CHECK: Verify part number is not protected before conversion
        if is_part_number_protected(part_number, config):
            log_and_print(f"CRITICAL SAFETY CHECK FAILED: Part number '{part_number}' is protected and cannot be converted to TOOL. Skipping conversion.", 'error')
            return False
        
        # Step 4: Change partType to TOOL using fresh etag
        log_and_print(f"Step 4: Changing partType to TOOL for part {part_number}", 'info')
        
        update_parttype_mutation = read_query('update_tool.graphql')
        update_parttype_variables_input = {
                'id': current_part_id,
                'etag': fresh_part_etag,
                'partType': 'TOOL',
            'description': tool_data.get('description', '')
        }
        # Only include maintenanceIntervalSeconds if we have a valid integer value
        service_interval = safe_convert_service_interval(tool_data.get('service_interval_seconds'))
        if service_interval is not None:
            update_parttype_variables_input['maintenanceIntervalSeconds'] = service_interval
        update_parttype_variables = {'input': update_parttype_variables_input}
        
        parttype_result = post_graphql_with_etag_refresh(token, config, {'query': update_parttype_mutation, 'variables': update_parttype_variables}, environment)
        
        if 'errors' in parttype_result:
            log_and_print(f"Failed to update partType to TOOL for part {part_number}: {parttype_result['errors']}", 'error')
            return False
        
        log_and_print(f"Successfully converted PART to TOOL for {serial_number}", 'info')
        return True
        
    except Exception as e:
        log_and_print(f"Exception converting PART to TOOL for {serial_number}: {e}", 'error')
        return False

def update_then_mark_unavailable(token: str, config: Dict[str, Any], tool_data: Dict, match_info: Dict, lost_location_id: str, environment: str = 'v1_sandbox', dry_run: bool = True, updated_parts_cache: set = None, parts_cache_lock = None) -> bool:
    """
    Update tool data then mark as unavailable following the flow document requirements.
    OPTIMIZATION: If only inventory-level updates are needed, combine into 1 mutation.
    """
    
    serial_number = tool_data.get('serial_number', 'UNKNOWN')
    
    if dry_run:
        log_and_print(f"DRY RUN: Would update then mark unavailable {serial_number}", 'info')
        return True
    
    try:
        # Check if we need part-level updates (2 mutations) or just inventory-level updates (1 mutation)
        from utilities.shared_sync_utils import determine_update_mutation_complexity
        update_complexity = determine_update_mutation_complexity(tool_data)
        
        if update_complexity == 'update_inventory':
            # OPTIMIZATION: Only inventory-level updates needed - combine into 1 mutation
            log_and_print(f"OPTIMIZATION: Combining inventory update + mark unavailable into 1 mutation for {serial_number}", 'info')
            
            # Handle both direct match data and wrapped match data
            if 'match' in match_info:
                match = match_info.get('match', {})
            else:
                match = match_info
            tool_id = match.get('id')
            if not tool_id:
                log_and_print(f"Cannot get tool ID for {serial_number}", 'error')
                return False
            
            # Convert Ion ID from float to int if needed
            import pandas as pd
            if isinstance(tool_id, float) and not pd.isna(tool_id):
                tool_id = int(tool_id)
            
            # Get current inventory data to get existing inventory attributes with etags
            # CRITICAL: Ion requires etags for each existing attribute when updating
            inventory_query = read_query('get_inventory_etag.graphql')
            inventory_variables = {'id': tool_id}
            
            inventory_result = post_graphql(token, config, {'query': inventory_query, 'variables': inventory_variables}, environment)
            
            if 'errors' in inventory_result:
                log_and_print(f"Failed to get inventory data for tool {serial_number}: {inventory_result['errors']}", 'error')
                return False
            
            current_inventory = inventory_result.get('data', {}).get('partInventory', {})
            if not current_inventory:
                log_and_print(f"No inventory data found for tool {serial_number}", 'error')
                return False
            
            # Get fresh etag from the inventory query result
            fresh_etag = current_inventory.get('_etag')
            if not fresh_etag:
                log_and_print(f"Cannot get fresh etag for tool {serial_number}", 'error')
                return False
            
            # Get existing inventory attributes with their etags from the query
            existing_inventory_attributes = {}
            if 'attributes' in current_inventory:
                for attr in current_inventory.get('attributes', []):
                    # GraphQL returns 'Etag' (capital E)
                    attr_etag = attr.get('Etag', '')
                    existing_inventory_attributes[attr.get('key', '')] = {
                        'value': attr.get('value', ''),
                        'etag': attr_etag
                    }
            
            # Build inventory-level attributes from TipQA data
            from utilities.tool_processing_utils import build_tipqa_inventory_attributes
            tipqa_inventory_attributes = build_tipqa_inventory_attributes(tool_data)
            updated_inventory_attributes = []
            
            if existing_inventory_attributes:
                # Inventory has existing attributes - preserve etags for existing ones
                for tipqa_attr in tipqa_inventory_attributes:
                    key = tipqa_attr['key']
                    value = tipqa_attr['value']
                    
                    if key in existing_inventory_attributes:
                        # This attribute exists - update with its etag if we have one
                        attr_etag = existing_inventory_attributes[key].get('etag', '')
                        if attr_etag:
                            # Update existing attribute with its etag
                            updated_inventory_attributes.append({
                                'key': key,
                                'value': value,
                                'etag': attr_etag
                            })
                        else:
                            # Attribute exists but no etag - omit etag field
                            updated_inventory_attributes.append({
                                'key': key,
                                'value': value
                            })
                    else:
                        # New attribute - add without etag
                        updated_inventory_attributes.append({
                            'key': key,
                            'value': value
                        })
            else:
                # Inventory has no existing attributes - add all TipQA attributes without etags
                updated_inventory_attributes = tipqa_inventory_attributes
            
            # Format last maintenance date properly
            last_maintenance_date_raw = tool_data.get('tipqa_last_maintenance_date') or tool_data.get('last_maintenance_date')
            last_maintenance_date = format_date_for_ion(last_maintenance_date_raw)
            
            # Combined mutation: update inventory attributes + mark unavailable + move location
            mutation = read_query('update_inventory_with_attributes.graphql')
            variables = {
                'input': {
                    'id': tool_id,
                    'etag': fresh_etag,
                    'unavailable': True,
                    'locationId': lost_location_id,
                    'attributes': updated_inventory_attributes
                }
            }
            
            # Only add lastMaintainedDate if it's properly formatted (DateTime format)
            if last_maintenance_date:
                variables['input']['lastMaintainedDate'] = last_maintenance_date
            
            result = post_graphql_with_etag_refresh(token, config, {'query': mutation, 'variables': variables}, environment)
            
            if 'errors' in result:
                log_and_print(f"Failed to update and mark unavailable tool {serial_number}: {result['errors']}", 'error')
                return False
            
            log_and_print(f"Successfully updated and marked unavailable tool {serial_number} (1 mutation)", 'info')
            return True
            
        else:
            # Part-level updates needed - use 2 mutations (existing logic)
            log_and_print(f"Part-level updates needed - using 2 mutations for {serial_number}", 'info')
            
            # First update the tool (including part updates)
            update_success = update_tool(token, config, tool_data, match_info, environment, dry_run=False)
            if not update_success:
                log_and_print(f"Failed to update tool {serial_number} before marking unavailable", 'error')
                return False
            
            # Get fresh etag after update
            # Handle both direct match data and wrapped match data
            if 'match' in match_info:
                match = match_info.get('match', {})
            else:
                match = match_info
            tool_id = match.get('id')
            if not tool_id:
                log_and_print(f"Cannot get fresh etag for tool {serial_number} - no tool ID", 'error')
                return False
            
            fresh_etag = get_inventory_etag(token, config, tool_id, environment)
            if not fresh_etag:
                log_and_print(f"Cannot get fresh etag for tool {serial_number}", 'error')
                return False
            
            # Update match_info with fresh etag
            updated_match_info = match_info.copy()
            if 'match' in match_info:
                updated_match_info['match'] = match_info['match'].copy()
                updated_match_info['match']['_etag'] = fresh_etag
            else:
                # If match_info doesn't have 'match' key, create it
                updated_match_info['match'] = match.copy()
                updated_match_info['match']['_etag'] = fresh_etag
            
            # Then mark as unavailable with fresh etag
            mark_success = mark_tool_unavailable(token, config, tool_data, updated_match_info, lost_location_id, environment, dry_run=False)
            if not mark_success:
                log_and_print(f"Failed to mark tool {serial_number} as unavailable after update", 'error')
                return False
            
            log_and_print(f"Successfully updated then marked unavailable tool {serial_number} (2 mutations)", 'info')
            return True
        
    except Exception as e:
        log_and_print(f"Exception updating then marking unavailable {serial_number}: {e}", 'error')
        return False

def mark_tool_unavailable(token: str, config: Dict[str, Any], tool_data: Dict, match_info: Dict, lost_location_id: str, environment: str = 'v1_sandbox', dry_run: bool = True) -> bool:
    """
    Mark a tool as unavailable following the flow document requirements.
    """
    import pandas as pd
    
    serial_number = tool_data.get('serial_number', 'UNKNOWN')
    
    # Handle both direct match data and wrapped match data
    if 'match' in match_info:
        match = match_info.get('match', {})
    else:
        match = match_info
    
    if not match or not match.get('id'):
        log_and_print(f"Cannot mark tool {serial_number} as unavailable - no match information", 'error')
        return False
    
    tool_id = match.get('id')
    tool_etag = match.get('_etag')
    
    # Convert Ion ID from float to int if needed
    if isinstance(tool_id, float) and not pd.isna(tool_id):
        tool_id = int(tool_id)
    
    # Get fresh etag before marking unavailable
    fresh_etag = get_inventory_etag(token, config, tool_id, environment)
    if not fresh_etag:
        log_and_print(f"Cannot get fresh etag for tool {serial_number}", 'error')
        return False
    
    # Update tool_etag with fresh etag
    tool_etag = fresh_etag
    
    if dry_run:
        log_and_print(f"DRY RUN: Would mark tool {serial_number} as unavailable", 'info')
        return True
    
    try:
        # Format last maintenance date properly
        last_maintenance_date_raw = tool_data.get('tipqa_last_maintenance_date') or tool_data.get('last_maintenance_date')
        last_maintenance_date = format_date_for_ion(last_maintenance_date_raw)
        
        # Update the inventory to mark as unavailable and move to lost location
        mutation = read_query('update_inventory_with_attributes.graphql')
        variables = {
            'input': {
                'id': tool_id,
                'etag': tool_etag,
                'unavailable': True,
                'locationId': lost_location_id
            }
        }
        
        # Only add lastMaintainedDate if it's properly formatted (DateTime format)
        if last_maintenance_date:
            variables['input']['lastMaintainedDate'] = last_maintenance_date
        
        result = post_graphql(token, config, {'query': mutation, 'variables': variables}, environment)
        
        if 'errors' in result:
            log_and_print(f"Failed to mark tool {serial_number} as unavailable: {result['errors']}", 'error')
            log_and_print(f"Variables sent: {variables}", 'error')
            log_and_print(f"Mutation used: {mutation}", 'error')
            return False
        
        log_and_print(f"Successfully marked tool {serial_number} as unavailable", 'info')
        return True
        
    except Exception as e:
        log_and_print(f"Exception marking tool {serial_number} as unavailable: {e}", 'error')
        return False

def mark_tool_available(token: str, config: Dict[str, Any], tool_data: Dict, match_info: Dict, environment: str = 'v1_sandbox', dry_run: bool = True) -> bool:
    """
    Mark a tool as available following the flow document requirements.
    This is the counterpart to mark_tool_unavailable - used when a tool should be available
    based on TipQA status but is currently marked unavailable in Ion.
    """
    import pandas as pd
    
    serial_number = tool_data.get('serial_number') or tool_data.get('tipqa_serial_number', 'UNKNOWN')
    
    # Handle both direct match data and wrapped match data
    if 'match' in match_info:
        match = match_info.get('match', {})
    else:
        match = match_info
    
    if not match or not match.get('id'):
        log_and_print(f"Cannot mark tool {serial_number} as available - no match information", 'error')
        return False
    
    tool_id = match.get('id')
    tool_etag = match.get('_etag')
    
    # Convert Ion ID from float to int if needed
    if isinstance(tool_id, float) and not pd.isna(tool_id):
        tool_id = int(tool_id)
    
    # Get fresh etag before marking available
    fresh_etag = get_inventory_etag(token, config, tool_id, environment)
    if not fresh_etag:
        log_and_print(f"Cannot get fresh etag for tool {serial_number}", 'error')
        return False
    
    # Update tool_etag with fresh etag
    tool_etag = fresh_etag
    
    if dry_run:
        log_and_print(f"DRY RUN: Would mark tool {serial_number} as available", 'info')
        return True
    
    try:
        # Format last maintenance date properly
        last_maintenance_date_raw = tool_data.get('tipqa_last_maintenance_date') or tool_data.get('last_maintenance_date')
        last_maintenance_date = format_date_for_ion(last_maintenance_date_raw)
        
        # Resolve TipQA location to Ion location ID (so we move tool off Lost to correct location)
        tipqa_location = tool_data.get('tipqa_location') or tool_data.get('location', '') or tool_data.get('tipqa_location_name', '')
        location_id, location_source = get_ion_location_id_for_tipqa(token, config, environment, tipqa_location)
        if not location_id and tipqa_location and str(tipqa_location).strip():
            log_and_print(f"Warning: Could not resolve TipQA location '{tipqa_location}' for {serial_number}; marking available but location will not change. Add sync_exceptions.location_code_to_ion_name or location_code_to_ion_id in config.", 'warning')

        # STEP 1: Update location + lastMaintainedDate first (WITHOUT unavailable).
        # This ensures location is always updated even if the unavailable field is rejected.
        mutation = read_query('update_inventory_with_attributes.graphql')
        variables = {
            'input': {
                'id': tool_id,
                'etag': tool_etag,
            }
        }
        if location_id:
            variables['input']['locationId'] = location_id
        if last_maintenance_date:
            variables['input']['lastMaintainedDate'] = last_maintenance_date

        if serial_number in ('JT00004887', 'JT00004653'):
            log_and_print(f"DEBUG: mark_tool_available STEP 1 (location) for {serial_number} - variables={variables}", 'info')

        result = post_graphql(token, config, {'query': mutation, 'variables': variables}, environment)

        if serial_number in ('JT00004887', 'JT00004653'):
            log_and_print(f"DEBUG: mark_tool_available STEP 1 result for {serial_number}: {result}", 'info')

        if 'errors' in result:
            log_and_print(f"Failed to update location for {serial_number}: {result['errors']}", 'error')
            return False
        log_and_print(f"Successfully updated location for {serial_number}", 'info')

        # STEP 2: Set unavailable=False in a separate mutation (so location update is never blocked).
        avail_etag = get_inventory_etag(token, config, tool_id, environment)
        if not avail_etag:
            log_and_print(f"Warning: Could not get fresh etag to mark {serial_number} as available - location was already updated", 'warning')
            return True

        avail_variables = {
            'input': {
                'id': tool_id,
                'etag': avail_etag,
                'unavailable': False
            }
        }
        if serial_number in ('JT00004887', 'JT00004653'):
            log_and_print(f"DEBUG: mark_tool_available STEP 2 (unavailable=False) for {serial_number} - variables={avail_variables}", 'info')

        avail_result = post_graphql(token, config, {'query': mutation, 'variables': avail_variables}, environment)

        if serial_number in ('JT00004887', 'JT00004653'):
            log_and_print(f"DEBUG: mark_tool_available STEP 2 result for {serial_number}: {avail_result}", 'info')

        if 'errors' in avail_result:
            log_and_print(f"Warning: Could not set unavailable=False for {serial_number}: {avail_result['errors']} - location was already updated successfully", 'warning')
        else:
            log_and_print(f"Successfully marked tool {serial_number} as available", 'info')

        return True
        
    except Exception as e:
        log_and_print(f"Exception marking tool {serial_number} as available: {e}", 'error')
        return False

def log_and_print(message: str, level: str = 'info'):
    """Simple logging function for tool processing utilities."""
    print(f"[{level.upper()}] {message}")
