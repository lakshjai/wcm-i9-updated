#!/usr/bin/env python3
"""
Catalog-specific logging and monitoring functionality.

This module provides structured logging, performance metrics collection,
and monitoring capabilities specifically for catalog operations.
"""

import logging
import time
import uuid
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from threading import Lock
from contextlib import contextmanager
import json
from datetime import datetime

from ..utils.logging_config import logger as base_logger


@dataclass
class CatalogMetrics:
    """Metrics for catalog operations."""
    api_calls: int = 0
    total_processing_time: float = 0.0
    pages_processed: int = 0
    documents_processed: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    errors: int = 0
    token_usage: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            'api_calls': self.api_calls,
            'total_processing_time': self.total_processing_time,
            'pages_processed': self.pages_processed,
            'documents_processed': self.documents_processed,
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'errors': self.errors,
            'token_usage': self.token_usage,
            'avg_processing_time_per_page': (
                self.total_processing_time / self.pages_processed 
                if self.pages_processed > 0 else 0.0
            ),
            'cache_hit_ratio': (
                self.cache_hits / (self.cache_hits + self.cache_misses)
                if (self.cache_hits + self.cache_misses) > 0 else 0.0
            )
        }


@dataclass
class OperationContext:
    """Context for catalog operations with correlation ID."""
    correlation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    operation_type: str = ""
    document_id: Optional[str] = None
    page_number: Optional[int] = None
    start_time: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for logging."""
        return {
            'correlation_id': self.correlation_id,
            'operation_type': self.operation_type,
            'document_id': self.document_id,
            'page_number': self.page_number,
            'start_time': self.start_time,
            'metadata': self.metadata
        }


class CatalogLogger:
    """Enhanced logger for catalog operations with structured logging and metrics."""
    
    def __init__(self):
        self.logger = logging.getLogger("hri9.catalog")
        self.logger.setLevel(base_logger.level)
        
        # Add handlers from base logger if not already present
        if not self.logger.handlers:
            for handler in base_logger.handlers:
                self.logger.addHandler(handler)
        
        self.metrics = CatalogMetrics()
        self._metrics_lock = Lock()
        self._operation_contexts: Dict[str, OperationContext] = {}
        self._contexts_lock = Lock()
    
    def start_operation(self, operation_type: str, document_id: Optional[str] = None, 
                       page_number: Optional[int] = None, **metadata) -> str:
        """
        Start a new catalog operation and return correlation ID.
        
        Args:
            operation_type: Type of operation (e.g., 'page_analysis', 'document_catalog')
            document_id: Optional document identifier
            page_number: Optional page number
            **metadata: Additional metadata for the operation
            
        Returns:
            Correlation ID for the operation
        """
        context = OperationContext(
            operation_type=operation_type,
            document_id=document_id,
            page_number=page_number,
            metadata=metadata
        )
        
        with self._contexts_lock:
            self._operation_contexts[context.correlation_id] = context
        
        self.logger.info(
            "Starting catalog operation",
            extra={
                'catalog_operation': 'start',
                **context.to_dict()
            }
        )
        
        return context.correlation_id
    
    def end_operation(self, correlation_id: str, success: bool = True, 
                     result_metadata: Optional[Dict[str, Any]] = None):
        """
        End a catalog operation and log results.
        
        Args:
            correlation_id: Correlation ID from start_operation
            success: Whether the operation was successful
            result_metadata: Additional metadata about the result
        """
        with self._contexts_lock:
            context = self._operation_contexts.pop(correlation_id, None)
        
        if not context:
            self.logger.warning(f"No context found for correlation ID: {correlation_id}")
            return
        
        end_time = time.time()
        duration = end_time - context.start_time
        
        log_data = {
            'catalog_operation': 'end',
            'success': success,
            'duration': duration,
            **context.to_dict()
        }
        
        if result_metadata:
            log_data['result'] = result_metadata
        
        # Update metrics
        with self._metrics_lock:
            self.metrics.total_processing_time += duration
            if context.operation_type == 'page_analysis':
                self.metrics.pages_processed += 1
            elif context.operation_type == 'document_catalog':
                self.metrics.documents_processed += 1
            
            if not success:
                self.metrics.errors += 1
        
        level = logging.INFO if success else logging.ERROR
        message = f"Completed catalog operation: {context.operation_type}"
        if not success:
            message = f"Failed catalog operation: {context.operation_type}"
        
        self.logger.log(level, message, extra=log_data)
    
    @contextmanager
    def operation_context(self, operation_type: str, document_id: Optional[str] = None,
                         page_number: Optional[int] = None, **metadata):
        """
        Context manager for catalog operations.
        
        Usage:
            with catalog_logger.operation_context('page_analysis', doc_id='123'):
                # perform operation
                pass
        """
        correlation_id = self.start_operation(
            operation_type, document_id, page_number, **metadata
        )
        
        try:
            yield correlation_id
            self.end_operation(correlation_id, success=True)
        except Exception as e:
            self.end_operation(
                correlation_id, 
                success=False, 
                result_metadata={'error': str(e), 'error_type': type(e).__name__}
            )
            raise
    
    def log_api_call(self, correlation_id: str, model: str, tokens_used: int = 0,
                    response_time: float = 0.0):
        """
        Log an API call with metrics.
        
        Args:
            correlation_id: Correlation ID for the operation
            model: Model used for the API call
            tokens_used: Number of tokens consumed
            response_time: API response time in seconds
        """
        with self._metrics_lock:
            self.metrics.api_calls += 1
            self.metrics.token_usage += tokens_used
        
        self.logger.info(
            "API call completed",
            extra={
                'catalog_operation': 'api_call',
                'correlation_id': correlation_id,
                'model': model,
                'tokens_used': tokens_used,
                'response_time': response_time
            }
        )
    
    def log_cache_operation(self, correlation_id: str, operation: str, hit: bool,
                           cache_size: Optional[int] = None):
        """
        Log cache operations.
        
        Args:
            correlation_id: Correlation ID for the operation
            operation: Cache operation type ('get', 'set', 'clear')
            hit: Whether it was a cache hit (for 'get' operations)
            cache_size: Current cache size
        """
        with self._metrics_lock:
            if operation == 'get':
                if hit:
                    self.metrics.cache_hits += 1
                else:
                    self.metrics.cache_misses += 1
        
        self.logger.debug(
            f"Cache {operation}",
            extra={
                'catalog_operation': 'cache',
                'correlation_id': correlation_id,
                'cache_operation': operation,
                'cache_hit': hit if operation == 'get' else None,
                'cache_size': cache_size
            }
        )
    
    def log_performance_metrics(self, correlation_id: str, metrics: Dict[str, Any]):
        """
        Log performance metrics for an operation.
        
        Args:
            correlation_id: Correlation ID for the operation
            metrics: Performance metrics dictionary
        """
        self.logger.info(
            "Performance metrics",
            extra={
                'catalog_operation': 'performance',
                'correlation_id': correlation_id,
                'metrics': metrics
            }
        )
    
    def log_validation_result(self, correlation_id: str, validation_type: str,
                            passed: bool, details: Optional[Dict[str, Any]] = None):
        """
        Log validation results.
        
        Args:
            correlation_id: Correlation ID for the operation
            validation_type: Type of validation performed
            passed: Whether validation passed
            details: Additional validation details
        """
        level = logging.INFO if passed else logging.WARNING
        
        log_data = {
            'catalog_operation': 'validation',
            'correlation_id': correlation_id,
            'validation_type': validation_type,
            'passed': passed
        }
        
        if details:
            log_data['details'] = details
        
        self.logger.log(
            level,
            f"Validation {validation_type}: {'PASSED' if passed else 'FAILED'}",
            extra=log_data
        )
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics as dictionary."""
        with self._metrics_lock:
            return self.metrics.to_dict()
    
    def reset_metrics(self):
        """Reset all metrics to zero."""
        with self._metrics_lock:
            self.metrics = CatalogMetrics()
        
        self.logger.info("Catalog metrics reset")
    
    def export_metrics(self, filepath: str):
        """
        Export current metrics to a JSON file.
        
        Args:
            filepath: Path to export metrics file
        """
        metrics_data = {
            'timestamp': datetime.now().isoformat(),
            'metrics': self.get_metrics()
        }
        
        try:
            with open(filepath, 'w') as f:
                json.dump(metrics_data, f, indent=2)
            
            self.logger.info(f"Metrics exported to {filepath}")
        except Exception as e:
            self.logger.error(f"Failed to export metrics: {e}")
    
    def log_summary(self):
        """Log a summary of current metrics."""
        metrics = self.get_metrics()
        
        self.logger.info(
            "Catalog processing summary",
            extra={
                'catalog_operation': 'summary',
                'summary': metrics
            }
        )


# Global catalog logger instance
catalog_logger = CatalogLogger()


def get_catalog_logger() -> CatalogLogger:
    """Get the global catalog logger instance."""
    return catalog_logger