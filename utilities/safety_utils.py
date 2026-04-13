#!/usr/bin/env python3
'''
Safety and Validation Utilities Module
=====================================

Handles safety checks and validation logic for tool operations.
Separated from main sync logic for better maintainability.

Created: 2025-01-28
Author: Jae Osenbach
Purpose: Safety checks and validation utilities
'''

import os
import time
from typing import Dict, Any, Optional, List
from utilities.logging_config import get_logger

def check_if_inventory_installed(token: str, config: Dict[str, Any], inventory_id: str, environment: str) -> bool:
    """Check if an inventory item is currently installed."""
    from utilities.graphql_utils import post_graphql, read_query
    
    query = read_query('get_installation_details.graphql')
    result = post_graphql(token, config, {"query": query, "variables": {"id": inventory_id}}, environment)
    
    if 'errors' in result or not result.get('data', {}).get('partInventory'):
        return False
    
    installations = result['data']['partInventory']['abomInstallations']
    return len(installations) > 0

def uninstall_tool_if_installed(token: str, config: Dict[str, Any], inventory_id: str, serial_number: str, environment: str) -> bool:
    """Uninstall a tool if it's currently installed with comprehensive safety checks."""
    
    # Check if the tool is installed
    if not check_if_inventory_installed(token, config, inventory_id, environment):
        log_and_print(f"Tool {serial_number} is not installed")
        return True  # Not installed, so nothing to do
    
    from utilities.graphql_utils import post_graphql, read_query
    
    # Get current etag and part type for the inventory
    etag_query = read_query('get_inventory_etag.graphql')
    etag_variables = {"id": inventory_id}
    
    try:
        etag_result = post_graphql(token, config, {"query": etag_query, "variables": etag_variables}, environment)
        
        if 'errors' in etag_result or not etag_result.get('data', {}).get('partInventory'):
            log_and_print(f"Could not get etag for tool {serial_number}", 'error')
            return False
        
        inventory_data = etag_result['data']['partInventory']
        current_etag = inventory_data['_etag']
        part_type = inventory_data.get('part', {}).get('partType', '')
        
        # CRITICAL SAFETY CHECK #1: Only uninstall tools, NEVER parts
        if part_type != 'TOOL':
            log_and_print(f"SAFETY CHECK FAILED: Cannot uninstall {serial_number} - it's a {part_type}, not a TOOL. Skipping uninstall.", 'error')
            return False
        
        # CRITICAL SAFETY CHECK #2: Double-check partType is exactly 'TOOL'
        if part_type.upper() != 'TOOL':
            log_and_print(f"SAFETY CHECK FAILED: partType '{part_type}' is not exactly 'TOOL'. Skipping uninstall.", 'error')
            return False
        
        # CRITICAL SAFETY CHECK #3: Verify this is not a part by checking the part object
        part_info = inventory_data.get('part', {})
        if not part_info:
            log_and_print(f"SAFETY CHECK FAILED: No part information found for {serial_number}. Skipping uninstall.", 'error')
            return False
        
        part_type_from_part = part_info.get('partType', '')
        if part_type_from_part != 'TOOL':
            log_and_print(f"SAFETY CHECK FAILED: Part object shows partType '{part_type_from_part}' for {serial_number}. Skipping uninstall.", 'error')
            return False
        
        log_and_print(f"Tool {serial_number} is installed - attempting to uninstall", 'warning')
        
        # Get the installation details to find the installation ID
        installation_query = read_query('get_installation_details.graphql')
        installation_result = post_graphql(token, config, {"query": installation_query, "variables": {"id": inventory_id}}, environment)
        
        if 'errors' in installation_result or not installation_result.get('data', {}).get('partInventory'):
            log_and_print(f"Could not get installation details for tool {serial_number}", 'error')
            return False
        
        installations = installation_result['data']['partInventory']['abomInstallations']
        if not installations:
            log_and_print(f"Tool {serial_number} shows as installed but has no installation records", 'warning')
            return True  # Consider this uninstalled
        
        # Remove each installation
        for installation in installations:
            installation_id = installation['id']
            
            # CRITICAL SAFETY CHECK #4: Final verification before uninstall
            # Re-query the inventory to double-check partType before proceeding
            final_check_query = read_query('final_safety_check.graphql')
            final_check_result = post_graphql(token, config, {"query": final_check_query, "variables": {"id": inventory_id}}, environment)
            
            if 'errors' in final_check_result or not final_check_result.get('data', {}).get('partInventory'):
                log_and_print(f"SAFETY CHECK FAILED: Could not verify partType for {serial_number} before uninstall. Skipping.", 'error')
                continue
            
            final_part_type = final_check_result['data']['partInventory']['part']['partType']
            if final_part_type != 'TOOL':
                log_and_print(f"SAFETY CHECK FAILED: Final check shows partType '{final_part_type}' for {serial_number}. ABORTING UNINSTALL.", 'error')
                continue
            
            log_and_print(f"SAFETY CHECK PASSED: Confirmed {serial_number} is a TOOL. Proceeding with uninstall of installation {installation_id}", 'info')
            
            # Uninstall the tool
            uninstall_mutation = read_query('uninstall_tool.graphql')
            uninstall_variables = {
                "id": installation_id
            }
            
            uninstall_result = post_graphql(token, config, {"query": uninstall_mutation, "variables": uninstall_variables}, environment)
            
            if 'errors' in uninstall_result:
                log_and_print(f"Failed to uninstall tool {serial_number}: {uninstall_result['errors']}", 'error')
                return False
            else:
                log_and_print(f"Successfully uninstalled tool {serial_number}", 'info')
                time.sleep(0.2)  # Brief delay after uninstall
        
        return True
        
    except Exception as e:
        log_and_print(f"Error uninstalling tool {serial_number}: {e}", 'error')
        return False

def validate_tool_data(tool_data: Dict) -> List[str]:
    """Validate tool data and return list of validation errors."""
    errors = []
    
    # Check required fields
    if not tool_data.get('serial_number'):
        errors.append("Missing serial_number")
    
    if not tool_data.get('part_number'):
        errors.append("Missing part_number")
    
    if not tool_data.get('location'):
        errors.append("Missing location")
    
    # Check serial number format
    serial_number = tool_data.get('serial_number', '')
    if serial_number and len(serial_number.strip()) < 3:
        errors.append("Serial number too short")
    
    # Check part number format
    part_number = tool_data.get('part_number', '')
    if part_number and len(part_number.strip()) < 3:
        errors.append("Part number too short")
    
    return errors

def validate_environment_config(config: Dict[str, Any], environment: str) -> List[str]:
    """Validate environment configuration."""
    errors = []
    
    if environment == 'v1_production':
        if not config.get('one_ion_api'):
            errors.append("Missing one_ion_api configuration for production")
        elif not config['one_ion_api'].get('url'):
            errors.append("Missing url in one_ion_api configuration")
    else:  # v1_sandbox
        if not config.get('sandbox_api'):
            errors.append("Missing sandbox_api configuration for sandbox")
        elif not config['sandbox_api'].get('url'):
            errors.append("Missing url in sandbox_api configuration")
    
    if not config.get('tipqa_databricks'):
        errors.append("Missing tipqa_databricks configuration")
    elif not config['tipqa_databricks'].get('host'):
        errors.append("Missing host in tipqa_databricks configuration")
    
    return errors

def log_and_print(message: str, level: str = 'info'):
    """Simple logging function for safety utilities."""
    print(f"[{level.upper()}] {message}")
