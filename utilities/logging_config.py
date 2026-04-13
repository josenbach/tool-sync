#!/usr/bin/env python3
'''
Prefect-Compatible Logging Configuration for Ion Tool Population
Provides structured logging that works with Prefect's logging system
'''

import logging
import json
import traceback
from datetime import datetime
from typing import Dict, Any, Optional
from contextlib import contextmanager

# Try to import Prefect logging utilities, fallback to standard logging
try:
    from prefect.utilities.logging import get_logger as prefect_get_logger
    PREFECT_AVAILABLE = True
except ImportError:
    PREFECT_AVAILABLE = False


class StructuredFormatter(logging.Formatter):
    '''Custom formatter that outputs structured JSON logs for better parsing'''
    
    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + 'Z',
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "thread": record.thread,
            "process": record.process
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": traceback.format_exception(*record.exc_info)
            }
        
        # Add extra fields if present
        if hasattr(record, 'extra_fields'):
            log_entry.update(record.extra_fields)
        
        return json.dumps(log_entry, default=str)


class PerformanceLogger:
    '''Context manager for timing operations'''
    
    def __init__(self, logger, operation_name: str, level: int = logging.INFO):
        self.logger = logger
        self.operation_name = operation_name
        self.level = level
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.utcnow()
        self.logger.log(self.level, f"Starting {self.operation_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration = (datetime.utcnow() - self.start_time).total_seconds()
            status = 'completed' if exc_type is None else 'failed'
            self.logger.log(
                self.level, 
                f"{self.operation_name} {status} in {duration:.2f} seconds",
                extra={"extra_fields": {"operation": self.operation_name, "duration_seconds": duration, "status": status}}
            )


def setup_logging(
    log_level: str = 'INFO',
    enable_structured_logging: bool = True,
    use_prefect_logging: bool = True
) -> logging.Logger:
    '''
    Set up logging configuration compatible with Prefect
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        enable_structured_logging: Whether to use structured JSON logging
        use_prefect_logging: Whether to use Prefect's logging system if available
    
    Returns:
        Configured logger instance
    '''
    
    if use_prefect_logging and PREFECT_AVAILABLE:
        # Use Prefect's logging system
        logger = prefect_get_logger()
        logger.setLevel(getattr(logging, log_level.upper()))
        
        # Configure structured logging if enabled
        if enable_structured_logging:
            # Add a custom handler with structured formatter
            handler = logging.StreamHandler()
            handler.setFormatter(StructuredFormatter())
            logger.addHandler(handler)
        
        return logger
    
    else:
        # Fallback to standard logging
        logger = logging.getLogger()
        logger.setLevel(getattr(logging, log_level.upper()))
        
        # Clear existing handlers
        logger.handlers.clear()
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, log_level.upper()))
        
        if enable_structured_logging:
            console_handler.setFormatter(StructuredFormatter())
        else:
            console_handler.setFormatter(
                logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                )
            )
        
        logger.addHandler(console_handler)
        
        # Configure specific loggers
        configure_module_loggers()
        
        return logger


def configure_module_loggers():
    '''Configure specific module loggers with appropriate levels'''
    
    # Database operations
    db_logger = logging.getLogger('database')
    db_logger.setLevel(logging.DEBUG)
    
    # API operations
    api_logger = logging.getLogger('api')
    api_logger.setLevel(logging.DEBUG)
    
    # Tool processing
    tool_logger = logging.getLogger('tool_processing')
    tool_logger.setLevel(logging.DEBUG)
    
    # Prefect operations
    prefect_logger = logging.getLogger('prefect')
    prefect_logger.setLevel(logging.INFO)
    
    # External libraries (reduce noise)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('databricks').setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    '''Get a logger instance with the given name, using Prefect logging if available'''
    if PREFECT_AVAILABLE:
        return prefect_get_logger(name)
    else:
        return logging.getLogger(name)


def log_function_call(logger: logging.Logger, func_name: str, args: tuple = None, kwargs: dict = None):
    '''Log function call with parameters'''
    call_info = {
        "function": func_name,
        "args": args if args else [],
        "kwargs": kwargs if kwargs else {}
    }
    logger.debug(f"Calling {func_name}", extra={'extra_fields': call_info})


def log_api_request(logger: logging.Logger, method: str, url: str, status_code: int = None, 
                   response_time: float = None, error: str = None):
    '''Log API request details'''
    request_info = {
        "api_request": {
            "method": method,
            "url": url,
            "status_code": status_code,
            "response_time_ms": response_time * 1000 if response_time else None,
            "error": error
        }
    }
    
    if error:
        logger.error(f"API request failed: {method} {url} - {error}", extra={'extra_fields': request_info})
    else:
        logger.info(f"API request: {method} {url} - {status_code}", extra={'extra_fields': request_info})


def log_database_operation(logger: logging.Logger, operation: str, table: str = None, 
                          record_count: int = None, duration: float = None, error: str = None):
    '''Log database operation details'''
    db_info = {
        "database_operation": {
            "operation": operation,
            "table": table,
            "record_count": record_count,
            "duration_seconds": duration,
            "error": error
        }
    }
    
    if error:
        logger.error(f"Database operation failed: {operation}", extra={'extra_fields': db_info})
    else:
        logger.info(f"Database operation: {operation}", extra={'extra_fields': db_info})


def log_tool_processing(logger: logging.Logger, tool_serial: str, action: str, 
                       success: bool = True, error: str = None, details: dict = None):
    '''Log tool processing details'''
    tool_info = {
        "tool_processing": {
            "serial_number": tool_serial,
            "action": action,
            "success": success,
            "error": error,
            "details": details or {}
        }
    }
    
    if not success:
        logger.error(f"Tool processing failed: {tool_serial} - {action} - {error}", 
                    extra={"extra_fields": tool_info})
    else:
        logger.info(f"Tool processed: {tool_serial} - {action}", extra={'extra_fields': tool_info})


def log_tool_analysis_decision(logger: logging.Logger, analysis_result: dict):
    '''Log detailed tool analysis decision with all relevant data (similar to CSV content)'''
    
    # Extract key information from analysis result
    action = analysis_result.get('action', 'unknown')
    reason = analysis_result.get('reason', 'unknown')
    tipqa_data = analysis_result.get('tipqa_data', {})
    ion_data = analysis_result.get('ion_data', {})
    
    # Create comprehensive tool decision log entry
    tool_decision = {
        "tool_analysis_decision": {
            "action_in_ion": action,
            "reason": reason,
            "has_serial_match_no_part": analysis_result.get('has_serial_match_no_part', False),
            "exact_match_found": analysis_result.get('exact_match_found', False),
            "ion_inventory_id": analysis_result.get('ion_inventory_id', ''),
            "ion_part_id": analysis_result.get('ion_part_id', ''),
            
            # TipQA data (source)
            "tipqa_serial_number": tipqa_data.get('serial_number', ''),
            "tipqa_part_number": tipqa_data.get('part_number', ''),
            "tipqa_revision": tipqa_data.get('revision', ''),
            "tipqa_maintenance_status": tipqa_data.get('maintenance_status', ''),
            "tipqa_revision_status": tipqa_data.get('revision_status', ''),
            "tipqa_location": tipqa_data.get('location', ''),
            "tipqa_location_name": tipqa_data.get('location_name', ''),
            "tipqa_stock_room": tipqa_data.get('stock_room', ''),
            "tipqa_asset_type": tipqa_data.get('asset_type', ''),
            "tipqa_asset_serial_number": tipqa_data.get('asset_serial_number', ''),
            "tipqa_manufacturer": tipqa_data.get('manufacturer', ''),
            "tipqa_description": tipqa_data.get('description', ''),
            "tipqa_last_maintenance_date": tipqa_data.get('last_maintenance_date', ''),
            "tipqa_service_interval_seconds": tipqa_data.get('service_interval_seconds', ''),
            "tipqa_last_updated": tipqa_data.get('last_updated', ''),
            
            # Ion data (target)
            "ion_serial_number": ion_data.get('serial_number', ''),
            "ion_part_number": ion_data.get('part_number', ''),
            "ion_revision": ion_data.get('revision', ''),
            "ion_maintenance_status": ion_data.get('maintenance_status', ''),
            "ion_location": ion_data.get('location', ''),
            "ion_manufacturer": ion_data.get('manufacturer', ''),
            "ion_description": ion_data.get('description', ''),
            "ion_last_updated": ion_data.get('last_updated', '')
        }
    }
    
    # Log at INFO level for daily tracking
    logger.info(f"Tool decision: {tipqa_data.get('serial_number', 'unknown')} -> {action}: {reason}", 
                extra={"extra_fields": tool_decision})


def log_analysis_summary(logger: logging.Logger, summary_stats: dict):
    '''Log analysis summary statistics'''
    summary_info = {
        "analysis_summary": {
            "total_tipqa_tools": summary_stats.get('total_tipqa_tools', 0),
            "active_tools_from_tipqa": summary_stats.get('active_tools_from_tipqa', 0),
            "inactive_tools_from_tipqa": summary_stats.get('inactive_tools_from_tipqa', 0),
            "ion_inventories_loaded": summary_stats.get('ion_inventories_loaded', 0),
            "total_tools_in_analysis": summary_stats.get('total_tools_in_analysis', 0),
            "actions": {
                "create_new_tools": summary_stats.get('create', 0),
                "create_and_move_to_lost": summary_stats.get('create_move_to_lost', 0),
                "update_existing_tools": summary_stats.get('update', 0),
                "update_and_move_to_lost": summary_stats.get('update_move_to_lost', 0),
                "mark_unavailable": summary_stats.get('mark_unavailable', 0)
            },
            "skipped_tools": {
                "inactive_in_tipqa": summary_stats.get('skip_inactive_tipqa', 0),
                "location_doesnt_exist_in_ion": summary_stats.get('skip_location_not_in_ion', 0),
                "invalid_location": summary_stats.get('skip_invalid_location', 0),
                "missing_part_number": summary_stats.get('skip_no_part_number', 0),
                "other_reasons": summary_stats.get('skip', 0)
            },
            "errors": summary_stats.get('error', 0)
        }
    }
    
    logger.info('Analysis summary completed', extra={"extra_fields": summary_info})


@contextmanager
def log_exceptions(logger: logging.Logger, operation: str, reraise: bool = True):
    '''Context manager to log exceptions with full traceback'''
    try:
        yield
    except Exception as e:
        logger.error(
            f"Exception in {operation}: {str(e)}",
            exc_info=True,
            extra={"extra_fields": {"operation": operation, "exception_type": type(e).__name__}}
        )
        if reraise:
            raise


def log_flow_start(logger: logging.Logger, flow_name: str, parameters: dict = None):
    '''Log flow start with parameters'''
    flow_info = {
        "flow_start": {
            "flow_name": flow_name,
            "parameters": parameters or {},
            "start_time": datetime.utcnow().isoformat()
        }
    }
    logger.info(f"Starting flow: {flow_name}", extra={'extra_fields': flow_info})


def log_flow_end(logger: logging.Logger, flow_name: str, success: bool = True, 
                duration: float = None, error: str = None, stats: dict = None):
    '''Log flow completion with results'''
    flow_info = {
        "flow_end": {
            "flow_name": flow_name,
            "success": success,
            "duration_seconds": duration,
            "error": error,
            "stats": stats or {},
            "end_time": datetime.utcnow().isoformat()
        }
    }
    
    if success:
        logger.info(f"Flow completed: {flow_name}", extra={'extra_fields': flow_info})
    else:
        logger.error(f"Flow failed: {flow_name} - {error}", extra={'extra_fields': flow_info})


# Convenience function for quick setup
def quick_setup_logging(log_level: str = 'INFO') -> logging.Logger:
    '''Quick setup with Prefect-compatible configuration'''
    return setup_logging(
        log_level=log_level,
        enable_structured_logging=True,
        use_prefect_logging=True
    )


def log_tool_decision_summary(logger: logging.Logger, tool_decisions: list, summary_stats: dict):
    '''
    Log a comprehensive summary of tool decisions that can be used to generate reports
    This replaces the need for CSV/MD files in Prefect by logging all the data
    '''
    
    # Log summary statistics
    summary_info = {
        "tool_sync_summary": {
            "total_tools_processed": len(tool_decisions),
            "success_rate": summary_stats.get('success_rate', 0),
            "total_time_seconds": summary_stats.get('total_time', 0),
            "actions": {
                "create": summary_stats.get('create', 0),
                "update": summary_stats.get('update', 0),
                "mark_unavailable": summary_stats.get('mark_unavailable', 0),
                "skip": summary_stats.get('skip', 0)
            },
            "reasons": summary_stats.get('reason_breakdown', {})
        }
    }
    
    logger.info("Tool sync summary completed", extra={"extra_fields": summary_info})
    
    # Log individual tool decisions (similar to CSV content)
    for i, decision in enumerate(tool_decisions):
        tool_info = {
            "tool_decision": {
                "index": i + 1,
                "serial_number": decision.get('serial_number', ''),
                "part_number": decision.get('part_number', ''),
                "action": decision.get('action', ''),
                "reason": decision.get('reason', ''),
                "success": decision.get('success', False),
                "duration_seconds": decision.get('duration_seconds', 0),
                "message": decision.get('message', ''),
                "timestamp": decision.get('timestamp', ''),
                "exists_in_ion": decision.get('exists_in_ion', False)
            }
        }
        
        logger.info(f"Tool decision {i+1}: {decision.get('serial_number', 'unknown')} -> {decision.get('action', 'unknown')}", 
                   extra={"extra_fields": tool_info})
