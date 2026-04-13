#!/usr/bin/env python3
'''
Simple Data Processing Module
============================

Simplified approach to data processing:
1. Get TipQA data - prefix all fields with "tipqa_"
2. Get Ion data - prefix all fields with "ion_" 
3. Merge them together
4. Run analysis

No complex field mapping, no duplicate field assignments, no confusion.

Created: 2025-01-17
Author: Jae Osenbach
Purpose: Simple, clean data processing
'''

import pandas as pd
from typing import Dict, Any, List
from utilities.logging_config import get_logger

logger = get_logger(__name__)

def process_tipqa_data(tipqa_df: pd.DataFrame) -> pd.DataFrame:
    """Process TipQA data by prefixing all fields with 'tipqa_'."""
    logger.info(f"Processing {len(tipqa_df)} TipQA records")
    
    # Create a copy to avoid modifying original
    processed_df = tipqa_df.copy()
    
    # Prefix all columns with 'tipqa_'
    processed_df.columns = [f'tipqa_{col}' if not col.startswith('tipqa_') else col for col in processed_df.columns]
    
    logger.info(f"TipQA processing complete: {len(processed_df)} records with {len(processed_df.columns)} fields")
    return processed_df

def process_ion_data(ion_data: Dict) -> pd.DataFrame:
    """Process Ion data by prefixing all fields with 'ion_'."""
    logger.info(f"Processing Ion data with {len(ion_data.get('by_serial', {}))} serial numbers")
    
    records = []
    
    for serial_number, ion_records in ion_data.get('by_serial', {}).items():
        for ion_record in ion_records:
            # Flatten the Ion data structure and prefix with 'ion_'
            flattened_record = {}
            
            # Process top-level fields
            for key, value in ion_record.items():
                if key == 'location' and isinstance(value, dict):
                    # Handle location separately
                    flattened_record['ion_location_id'] = str(value.get('id', '')) if value.get('id') else ''
                    flattened_record['ion_location_name'] = str(value.get('name', '')) if value.get('name') else ''
                elif key == 'part' and isinstance(value, dict):
                    # Handle part separately
                    for part_key, part_value in value.items():
                        if part_key == 'attributes' and isinstance(part_value, list):
                            # Handle part attributes
                            for attr in part_value:
                                attr_key = attr.get('key', '')
                                attr_value = attr.get('value', '')
                                if attr_key:
                                    flattened_record[f'ion_part_attr_{attr_key.replace(" ", "_").replace("-", "_").lower()}'] = str(attr_value) if attr_value else ''
                        else:
                            flattened_record[f'ion_part_{part_key}'] = str(part_value) if part_value is not None else ''
                elif key == 'attributes' and isinstance(value, list):
                    # Handle inventory attributes separately
                    for attr in value:
                        attr_key = attr.get('key', '')
                        attr_value = attr.get('value', '')
                        if attr_key:
                            flattened_record[f'ion_attr_{attr_key.replace(" ", "_").replace("-", "_").lower()}'] = str(attr_value) if attr_value else ''
                elif key == 'abomInstallations':
                    flattened_record['ion_installations_count'] = len(value) if isinstance(value, list) else 0
                else:
                    # Regular field
                    flattened_record[f'ion_{key}'] = str(value) if value is not None else ''
            
            # Add serial number for merging
            flattened_record['ion_serial_number'] = serial_number
            records.append(flattened_record)
    
    ion_df = pd.DataFrame(records)
    logger.info(f"Ion processing complete: {len(ion_df)} records with {len(ion_df.columns)} fields")
    return ion_df

def create_simple_master_dataframe(tipqa_df: pd.DataFrame, ion_df: pd.DataFrame) -> pd.DataFrame:
    """Create master dataframe by merging TipQA and Ion data on serial number."""
    logger.info("Creating simple master dataframe")
    
    # Ensure we have serial number columns for merging
    tipqa_serial_col = 'tipqa_serial_number' if 'tipqa_serial_number' in tipqa_df.columns else 'serial_number'
    ion_serial_col = 'ion_serial_number'
    
    # Merge on serial number
    master_df = pd.merge(
        tipqa_df, 
        ion_df, 
        left_on=tipqa_serial_col, 
        right_on=ion_serial_col, 
        how='outer'
    )
    
    logger.info(f"Master dataframe created: {len(master_df)} records")
    logger.info(f"  - TipQA fields: {len([col for col in master_df.columns if col.startswith('tipqa_')])}")
    logger.info(f"  - Ion fields: {len([col for col in master_df.columns if col.startswith('ion_')])}")
    
    return master_df

def get_analysis_fields(tool_data: Dict) -> Dict:
    """Extract fields needed for analysis from the merged data."""
    # Map the prefixed fields to the analysis function's expected field names
    analysis_data = {
        'serial_number': tool_data.get('tipqa_serial_number', ''),
        'part_number': tool_data.get('tipqa_part_number', ''),
        'revision': tool_data.get('tipqa_revision', ''),
        'maintenance_status': tool_data.get('tipqa_maintenance_status', ''),
        'revision_status': tool_data.get('tipqa_revision_status', ''),
        'location': tool_data.get('tipqa_location', ''),
        'manufacturer': tool_data.get('tipqa_manufacturer', ''),
        'asset_serial_number': tool_data.get('tipqa_asset_serial_number', ''),
        'model_number': tool_data.get('tipqa_model_number', ''),
        'condition': tool_data.get('tipqa_condition', ''),
        'asset_type': tool_data.get('tipqa_asset_type', ''),
        'service_interval_seconds': tool_data.get('tipqa_service_interval_seconds', ''),
        'last_maintenance_date': tool_data.get('tipqa_last_maintenance_date', ''),
        'notes': tool_data.get('tipqa_notes', ''),
        'date_added': tool_data.get('tipqa_date_added', ''),
        'last_updated': tool_data.get('tipqa_last_updated', ''),
        'stock_room': tool_data.get('tipqa_stock_room', ''),
        'location_name': tool_data.get('tipqa_location_name', ''),
        
        # Ion fields for analysis
        'ion_id': tool_data.get('ion_id', ''),
        'ion_status': tool_data.get('ion_status', ''),
        'ion_unavailable': tool_data.get('ion_unavailable', False),
        'ion_location_id': tool_data.get('ion_location_id', ''),
        'ion_location_name': tool_data.get('ion_location_name', ''),
        'ion_asset_serial_number': tool_data.get('ion_attr_asset_serial_number', ''),
        'ion_manufacturer': tool_data.get('ion_attr_manufacturer', ''),
        'ion_model_number': tool_data.get('ion_attr_model_number', ''),
        'ion_condition': tool_data.get('ion_attr_condition', ''),
        'ion_asset_type': tool_data.get('ion_attr_asset_type', ''),
        'ion_service_interval_seconds': tool_data.get('ion_part_maintenanceIntervalSeconds', ''),
        'ion_last_maintenance_date': tool_data.get('ion_lastMaintainedDate', ''),
        'ion_notes': tool_data.get('ion_attr_notes', ''),
        'ion_date_added': tool_data.get('ion_attr_date_added', ''),
        'ion_last_updated': tool_data.get('ion_attr_last_updated', ''),
    }
    
    return analysis_data

