#!/usr/bin/env python3
'''
GraphQL API Utilities Module
===========================

Handles all GraphQL API operations for Ion.
Separated from main sync logic for better maintainability.

Created: 2025-01-28
Author: Jae Osenbach
Purpose: GraphQL API utilities and operations
'''

import os
import time
import random
import requests
import json
from typing import Dict, Any, Optional, List
from urllib.parse import urljoin
from utilities.logging_config import get_logger, log_api_request

class AuthenticationError(Exception):
    """Custom exception for authentication errors."""
    pass

def refresh_token_if_needed(token: str, config: Dict[str, Any], environment: str = 'v1_sandbox') -> str:
    """Refresh token if it's expired or about to expire."""
    try:
        # Try a simple API call to test if token is still valid
        test_query = read_query('get_schema.graphql')
        result = post_graphql(token, config, {'query': test_query}, environment)
        
        if 'errors' in result and any('UNAUTHORIZED' in str(error) for error in result['errors']):
            log_and_print("Token expired, refreshing...", 'warning')
            return get_token(config, environment, force_refresh=True)
        
        return token
        
    except Exception as e:
        if '401' in str(e) or 'UNAUTHORIZED' in str(e):
            log_and_print("Token expired, refreshing...", 'warning')
            return get_token(config, environment, force_refresh=True)
        else:
            # Other error, return original token
            return token

def get_token(config: Dict[str, Any], environment: str = 'v1_sandbox', force_refresh: bool = False) -> str:
    """Get authentication token for Ion API."""
    api_logger = get_logger('api')
    
    # Determine API configuration based on environment
    if environment == 'v1_production':
        api_config = config['one_ion_api']
        client_id = os.getenv('V1CLIENT')
        client_secret = os.getenv('V1SECRET')
    elif environment == 'v2_production':
        api_config = config['reloaded_ion_api']
        client_id = os.getenv('V2CLIENT')
        client_secret = os.getenv('V2SECRET')
    elif environment == 'v2_staging':
        api_config = config['sandbox_api']
        client_id = os.getenv('V2STAGINGCLIENT')
        client_secret = os.getenv('V2STAGINGSECRET')
    elif environment == 'v2_sandbox':
        api_config = config['sandbox_api']  # Same URL as V1 Sandbox
        client_id = os.getenv('V2SANDBOX_CLIENT')
        client_secret = os.getenv('V2SANDBOX_SECRET')
    else:  # v1_sandbox
        api_config = config['sandbox_api']
        client_id = os.getenv('V1SANDBOX_CLIENT')
        client_secret = os.getenv('V1SANDBOX_SECRET')
    
    if not client_id or not client_secret:
        raise Exception(f"Missing authentication credentials for {environment}. Required: V1CLIENT/V1SECRET for v1_production, V2CLIENT/V2SECRET for v2_production, V2STAGINGCLIENT/V2STAGINGSECRET for v2_staging, V1SANDBOX_CLIENT/V1SANDBOX_SECRET for v1_sandbox, V2SANDBOX_CLIENT/V2SANDBOX_SECRET for v2_sandbox")
    
    token_url = api_config['url_auth']
    
    try:
        response = requests.post(token_url, data={
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret
        }, timeout=30)
        
        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get('access_token')
            if access_token:
                log_and_print(f"Successfully obtained token for {environment}")
                return access_token
            else:
                raise Exception(f"No access token in response for {environment}")
        else:
            raise Exception(f"Token request failed for {environment}: {response.status_code} - {response.text}")
            
    except Exception as e:
        api_logger.error(f"Token retrieval failed for {environment}: {str(e)}")
        raise

def post_graphql(token: str, config: Dict[str, Any], payload: Dict[str, Any], environment: str = 'v1_sandbox') -> Dict[str, Any]:
    """Post GraphQL request with retry logic and comprehensive error handling."""
    api_logger = get_logger('api')
    start_time = time.time()
    
    # Determine API configuration based on environment
    if environment == 'v1_production':
        api_config = config['one_ion_api']
    elif environment == 'v2_production':
        api_config = config['reloaded_ion_api']
    elif environment == 'v2_staging':
        api_config = config['sandbox_api']
    elif environment == 'v2_sandbox':
        api_config = config['sandbox_api']  # Same URL as V1 Sandbox
    else:  # v1_sandbox
        api_config = config['sandbox_api']
    
    graphql_url = urljoin(api_config['url'], '/graphql')
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    max_retries = 3
    base_delay = 1
    
    for attempt in range(max_retries):
        try:
            log_api_request(api_logger, 'graphql', graphql_url, payload)
            
            response = requests.post(
                graphql_url,
                headers=headers,
                json=payload,
                timeout=60
            )
            
            duration = time.time() - start_time
            
            if response.status_code == 200:
                result = response.json()
                api_logger.info(f"GraphQL request successful in {duration:.2f}s", extra={"extra_fields": {
                    "url": graphql_url,
                    "duration": duration,
                    "attempt": attempt + 1
                }})
                return result
            elif response.status_code == 429:  # Rate limited
                if attempt < max_retries - 1:
                    wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    log_and_print(f"Rate limited, waiting {wait_time:.1f}s before retry {attempt + 2}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    raise requests.exceptions.HTTPError(f"Rate limited after {max_retries} attempts")
            else:
                # Try to parse GraphQL errors from response body before raising
                try:
                    result = response.json()
                    if 'errors' in result:
                        # Check for authentication errors that should trigger token refresh
                        has_auth_error = any(
                            'Unable to validate authentication token' in str(error) or 
                            'authentication' in str(error).lower() or
                            'UNAUTHORIZED' in str(error).upper()
                            for error in result['errors']
                        )
                        
                        if has_auth_error:
                            raise AuthenticationError(f"Authentication error in GraphQL response: {result['errors']}")
                        
                        # Check for concurrency errors that should trigger retry
                        has_concurrency_error = any(
                            'CONCURRENCY_ERROR' in str(error) or 
                            'Etag does not match' in str(error) 
                            for error in result['errors']
                        )
                        
                        if has_concurrency_error and attempt < max_retries - 1:
                            wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                            log_and_print(f"Concurrency error detected, waiting {wait_time:.1f}s before retry {attempt + 2}/{max_retries}")
                            time.sleep(wait_time)
                            continue
                        
                        # Return the GraphQL errors instead of raising HTTP exception
                        api_logger.error(f"GraphQL errors in response: {result['errors']}", extra={"extra_fields": {
                            "url": graphql_url,
                            "duration": duration,
                            "attempt": attempt + 1,
                            "status_code": response.status_code,
                            "errors": result['errors']
                        }})
                        return result
                except (ValueError, json.JSONDecodeError):
                    # Response body is not valid JSON, fall back to HTTP error
                    pass
                
                response.raise_for_status()
                
        except requests.exceptions.RequestException as e:
            duration = time.time() - start_time
            if attempt < max_retries - 1:
                wait_time = base_delay * (2 ** attempt)
                log_and_print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}, retrying in {wait_time}s")
                time.sleep(wait_time)
                continue
            else:
                raise
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Unexpected error in GraphQL request for {environment}: {str(e)}"
            api_logger.error(error_msg, extra={"extra_fields": {
                "url": graphql_url,
                "duration": duration,
                "attempt": attempt + 1,
                "error": str(e)
            }})
            raise
    
    raise Exception(f"GraphQL request failed after {max_retries} attempts")

def read_query(filename: str) -> str:
    """Read GraphQL query from file."""
    try:
        with open(f'queries/{filename}', 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        raise Exception(f"GraphQL query file not found: queries/{filename}")

def get_locations(token: str, config: Dict[str, Any], environment: str = 'v1_sandbox') -> Dict[str, Any]:
    """
    Get all locations from Ion, paginating through all results.
    """
    query = read_query('get_locations.graphql')
    all_edges = []
    after = None
    page_size = 5000

    while True:
        variables = {'first': page_size}
        if after:
            variables['after'] = after
        result = post_graphql(token, config, {'query': query, 'variables': variables}, environment)
        if 'errors' in result:
            return result
        edges = result.get('data', {}).get('locations', {}).get('edges', [])
        all_edges.extend(edges)
        page_info = result.get('data', {}).get('locations', {}).get('pageInfo', {})
        if not page_info.get('hasNextPage', False):
            break
        after = page_info.get('endCursor')

    return {'data': {'locations': {'edges': all_edges}}}

def get_lost_location_id(token: str, config: Dict[str, Any], environment: str = 'v1_sandbox') -> Optional[str]:
    """Get the lost location ID from Ion."""
    # Known lost location IDs per environment
    known_lost_location_ids = {
        'v2_production': '10043'
    }
    
    # Check if we have a known location ID for this environment
    if environment in known_lost_location_ids:
        known_id = known_lost_location_ids[environment]
        log_and_print(f"Using known lost location ID for {environment}: {known_id}")
        return known_id
    
    try:
        result = get_locations(token, config, environment)
        
        if result.get('data', {}).get('locations', {}).get('edges'):
            locations = result['data']['locations']['edges']
            
            for edge in locations:
                location = edge['node']
                if 'lost' in location['name'].lower():
                    log_and_print(f"Found lost location: {location['name']} (ID: {location['id']})")
                    return location['id']
            
            log_and_print("Warning: No lost location found")
            return None
        else:
            log_and_print("No locations found in Ion")
            return None
            
    except Exception as e:
        log_and_print(f"Error finding lost location: {e}", 'error')
        return None


def get_ion_location_id_for_tipqa(
    token: str, config: Dict[str, Any], environment: str, tipqa_location: Any
) -> tuple[Optional[str], str]:
    """
    Resolve TipQA location (code or name) to an Ion location ID.
    Used when updating a tool's location (e.g. when marking available so we move it off Lost).
    - First tries exact name match against Ion location names.
    - Then tries config sync_exceptions.location_code_to_ion_name (TipQA code -> Ion name).
    - Then tries config sync_exceptions.location_code_to_ion_id (TipQA code -> Ion ID).
    Returns (location_id or None, source) where source is for logging (e.g. 'exact_name', 'config_name', 'config_id').
    """
    if not tipqa_location or str(tipqa_location).strip() == '' or str(tipqa_location).strip().lower() == 'nan':
        return None, ''
    tipqa_str = str(tipqa_location).strip()
    result = get_locations(token, config, environment)
    if 'errors' in result or not result.get('data', {}).get('locations', {}).get('edges'):
        return None, ''
    edges = result['data']['locations']['edges']
    # 1) Exact name match (case-insensitive)
    for edge in edges:
        node = edge.get('node', {})
        ion_name = (node.get('name') or '').strip()
        if ion_name.lower() == tipqa_str.lower():
            return node.get('id'), 'exact_name'
    # 2) Config: location_code_to_ion_name (TipQA code -> Ion location name)
    code_to_name = (config.get('sync_exceptions') or {}).get('location_code_to_ion_name') or {}
    if isinstance(code_to_name, dict) and tipqa_str in code_to_name:
        target_name = str(code_to_name[tipqa_str]).strip()
        for edge in edges:
            node = edge.get('node', {})
            if (node.get('name') or '').strip().lower() == target_name.lower():
                return node.get('id'), 'config_name'
    # 3) Config: location_code_to_ion_id (TipQA code -> Ion location ID)
    code_to_id = (config.get('sync_exceptions') or {}).get('location_code_to_ion_id') or {}
    if isinstance(code_to_id, dict) and tipqa_str in code_to_id:
        loc_id = code_to_id[tipqa_str]
        if loc_id:
            return str(loc_id), 'config_id'
    return None, ''


def get_all_ion_tool_inventory(token: str, config: Dict[str, Any], environment: str = 'v1_sandbox') -> List[Dict]:
    """Get ALL inventory in Ion where partType = TOOL."""
    
    log_and_print('Getting all TOOL type inventory from Ion...')
    start_time = time.time()
    
    query = read_query('get_all_tool_inventory.graphql')
    
    all_tools = []
    after = None
    page = 1
    
    while True:
        variables = {
            'first': 200,
            'after': after
        }
        
        result = post_graphql(token, config, {'query': query, 'variables': variables}, environment)
        
        if 'errors' in result:
            log_and_print(f"Error getting Ion tools: {result['errors']}", 'error')
            break
            
        data = result.get('data', {}).get('partInventories', {})
        edges = data.get('edges', [])
        
        for edge in edges:
            tool = edge['node']
            all_tools.append(tool)
        
        page_info = data.get('pageInfo', {})
        if not page_info.get('hasNextPage', False):
            break
            
        after = page_info.get('endCursor')
        page += 1
        
        if page % 10 == 0:
            log_and_print(f"  Fetched {len(all_tools)} tools so far...")
    
    elapsed = time.time() - start_time
    log_and_print(f"Retrieved {len(all_tools)} TOOL type inventory items in {elapsed:.1f}s")
    
    return all_tools

def get_inventory_etag(token: str, config: Dict[str, Any], inventory_id: str, environment: str = 'v1_sandbox') -> Optional[str]:
    """Get the current etag for an inventory item."""
    try:
        query = read_query('get_inventory_etag.graphql')
        variables = {"id": inventory_id}
        
        result = post_graphql(token, config, {"query": query, "variables": variables}, environment)
        
        if 'errors' in result or not result.get('data', {}).get('partInventory'):
            log_and_print(f"Error getting etag for inventory {inventory_id}: {result.get('errors', 'No data')}", 'error')
            return None
        
        return result['data']['partInventory']['_etag']
        
    except Exception as e:
        log_and_print(f"Exception getting etag for inventory {inventory_id}: {e}", 'error')
        return None

def get_part_etag(token: str, config: Dict[str, Any], part_id: str, environment: str = 'v1_sandbox') -> Optional[str]:
    """Get the current etag for a part item."""
    try:
        query = read_query('get_part_etag.graphql')
        variables = {"id": part_id}
        
        result = post_graphql(token, config, {"query": query, "variables": variables}, environment)
        
        if 'errors' in result or not result.get('data', {}).get('part'):
            log_and_print(f"Error getting etag for part {part_id}: {result.get('errors', 'No data')}", 'error')
            return None
        
        return result['data']['part']['_etag']
        
    except Exception as e:
        log_and_print(f"Exception getting etag for part {part_id}: {e}", 'error')
        return None

def _refresh_payload_etags(token: str, config: Dict[str, Any], payload: Dict[str, Any], environment: str) -> None:
    """
    Re-fetch the part etag (and any attribute etags) from the server and update
    the payload in-place so the next retry uses current values.
    """
    try:
        input_data = payload.get('variables', {}).get('input', {})
        part_id = input_data.get('id')
        if not part_id or 'etag' not in input_data:
            return

        fresh_etag = get_part_etag(token, config, part_id, environment)
        if fresh_etag:
            payload['variables']['input']['etag'] = fresh_etag
            log_and_print(f"Refreshed part etag for {part_id} before retry")

        if input_data.get('attributes'):
            part_query = read_query('get_part_etag.graphql')
            part_result = post_graphql(token, config, {'query': part_query, 'variables': {'id': part_id}}, environment)
            if 'errors' not in part_result:
                existing_attrs = part_result.get('data', {}).get('part', {}).get('attributes', [])
                attr_etags = {
                    a.get('key'): (a.get('Etag') or a.get('etag'))
                    for a in existing_attrs if a.get('key')
                }
                for attr in input_data['attributes']:
                    new_etag = attr_etags.get(attr.get('key'))
                    if new_etag:
                        attr['etag'] = new_etag
    except Exception as e:
        log_and_print(f"Warning: Failed to refresh etag before retry: {e}", 'warning')


def post_graphql_with_etag_refresh(token: str, config: Dict[str, Any], payload: Dict[str, Any], 
                                 environment: str = 'v1_sandbox', max_retries: int = 3) -> Dict[str, Any]:
    """
    Post GraphQL request with automatic etag refresh for concurrency errors.
    This function specifically handles mutations that may fail due to stale etags.
    """
    api_logger = get_logger('api')
    start_time = time.time()
    
    # Determine API configuration based on environment
    if environment == 'v1_production':
        api_config = config['one_ion_api']
    elif environment == 'v2_production':
        api_config = config['reloaded_ion_api']
    elif environment == 'v2_sandbox':
        api_config = config['sandbox_api']  # Same URL as V1 Sandbox
    else:  # v1_sandbox
        api_config = config['sandbox_api']
    
    graphql_url = urljoin(api_config['url'], '/graphql')
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    base_delay = 1
    
    for attempt in range(max_retries):
        try:
            log_api_request(api_logger, 'graphql', graphql_url, payload)
            
            response = requests.post(
                graphql_url,
                headers=headers,
                json=payload,
                timeout=60
            )
            
            duration = time.time() - start_time
            
            if response.status_code == 200:
                result = response.json()
                
                # Check for concurrency errors in successful responses
                if 'errors' in result:
                    has_concurrency_error = any(
                        'CONCURRENCY_ERROR' in str(error) or 
                        'Etag does not match' in str(error) 
                        for error in result['errors']
                    )
                    
                    if has_concurrency_error and attempt < max_retries - 1:
                        wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                        log_and_print(f"Concurrency error in successful response, waiting {wait_time:.1f}s before retry {attempt + 2}/{max_retries}")
                        time.sleep(wait_time)
                        _refresh_payload_etags(token, config, payload, environment)
                        continue
                
                api_logger.info(f"GraphQL request successful in {duration:.2f}s", extra={"extra_fields": {
                    "url": graphql_url,
                    "duration": duration,
                    "attempt": attempt + 1
                }})
                return result
            elif response.status_code == 429:  # Rate limited
                if attempt < max_retries - 1:
                    wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    log_and_print(f"Rate limited, waiting {wait_time:.1f}s before retry {attempt + 2}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    raise requests.exceptions.HTTPError(f"Rate limited after {max_retries} attempts")
            else:
                # Try to parse GraphQL errors from response body before raising
                try:
                    result = response.json()
                    if 'errors' in result:
                        # Check for authentication errors that should trigger token refresh
                        has_auth_error = any(
                            'Unable to validate authentication token' in str(error) or 
                            'authentication' in str(error).lower() or
                            'UNAUTHORIZED' in str(error).upper()
                            for error in result['errors']
                        )
                        
                        if has_auth_error:
                            raise AuthenticationError(f"Authentication error in GraphQL response: {result['errors']}")
                        
                        # Check for concurrency errors that should trigger retry
                        has_concurrency_error = any(
                            'CONCURRENCY_ERROR' in str(error) or 
                            'Etag does not match' in str(error) 
                            for error in result['errors']
                        )
                        
                        if has_concurrency_error and attempt < max_retries - 1:
                            wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                            log_and_print(f"Concurrency error detected, waiting {wait_time:.1f}s before retry {attempt + 2}/{max_retries}")
                            time.sleep(wait_time)
                            _refresh_payload_etags(token, config, payload, environment)
                            continue
                        
                        # Return the GraphQL errors instead of raising HTTP exception
                        api_logger.error(f"GraphQL errors in response: {result['errors']}", extra={"extra_fields": {
                            "url": graphql_url,
                            "duration": duration,
                            "attempt": attempt + 1,
                            "status_code": response.status_code,
                            "errors": result['errors']
                        }})
                        return result
                except (ValueError, json.JSONDecodeError):
                    # Response body is not valid JSON, fall back to HTTP error
                    pass
                
                response.raise_for_status()
                
        except requests.exceptions.RequestException as e:
            duration = time.time() - start_time
            if attempt < max_retries - 1:
                wait_time = base_delay * (2 ** attempt)
                log_and_print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}, retrying in {wait_time}s")
                time.sleep(wait_time)
                continue
            else:
                raise
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Unexpected error in GraphQL request for {environment}: {str(e)}"
            api_logger.error(error_msg, extra={"extra_fields": {
                "url": graphql_url,
                "duration": duration,
                "attempt": attempt + 1,
                "error": str(e)
            }})
            raise
    
    raise Exception(f"GraphQL request failed after {max_retries} attempts")

def organize_ion_data_by_serial(ion_tools: List[Dict]) -> Dict[str, List[Dict]]:
    """Organize Ion tools by serial number for efficient lookup."""
    
    by_serial = {}
    for tool in ion_tools:
        serial = tool.get('serialNumber') or ''
        serial = serial.strip() if serial else ''
        if serial:
            if serial not in by_serial:
                by_serial[serial] = []
            by_serial[serial].append(tool)
    
    return {'by_serial': by_serial}

def log_and_print(message: str, level: str = 'info'):
    """Simple logging function for API utilities."""
    print(f"[{level.upper()}] {message}")
