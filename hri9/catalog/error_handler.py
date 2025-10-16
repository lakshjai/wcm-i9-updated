#!/usr/bin/env python3
"""
Error handling and fallback mechanisms for the document catalog system.

This module provides comprehensive error handling, recovery mechanisms, and
fallback strategies to ensure robust operation of the catalog system.
"""

import os
import time
import random
import threading
from pathlib import Path
from typing import Optional, Dict, Any, List, Callable, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field

from ..utils.logging_config import logger
from .models import PageAnalysis, PageMetadata, DocumentCatalogEntry, ProcessingSummary


@dataclass
class ErrorStatistics:
    """Statistics for error tracking and monitoring."""
    api_errors: int = 0
    memory_errors: int = 0
    parsing_errors: int = 0
    cache_errors: int = 0
    fallback_activations: int = 0
    recovery_attempts: int = 0
    successful_recoveries: int = 0
    last_error_time: Optional[datetime] = None
    error_history: List[Dict[str, Any]] = field(default_factory=list)
    
    def record_error(self, error_type: str, error_msg: str, context: Dict[str, Any] = None):
        """Record an error occurrence."""
        self.last_error_time = datetime.now()
        
        # Increment specific error counter
        if error_type == "api":
            self.api_errors += 1
        elif error_type == "memory":
            self.memory_errors += 1
        elif error_type == "parsing":
            self.parsing_errors += 1
        elif error_type == "cache":
            self.cache_errors += 1
        
        # Add to error history (keep last 100 errors)
        error_record = {
            "timestamp": self.last_error_time.isoformat(),
            "type": error_type,
            "message": error_msg,
            "context": context or {}
        }
        
        self.error_history.append(error_record)
        if len(self.error_history) > 100:
            self.error_history.pop(0)
    
    def record_fallback(self):
        """Record a fallback activation."""
        self.fallback_activations += 1
    
    def record_recovery_attempt(self, successful: bool = False):
        """Record a recovery attempt."""
        self.recovery_attempts += 1
        if successful:
            self.successful_recoveries += 1
    
    def get_error_rate(self, window_minutes: int = 60) -> float:
        """Calculate error rate within a time window."""
        if not self.error_history:
            return 0.0
        
        cutoff_time = datetime.now() - timedelta(minutes=window_minutes)
        recent_errors = [
            e for e in self.error_history 
            if datetime.fromisoformat(e["timestamp"]) > cutoff_time
        ]
        
        return len(recent_errors) / window_minutes  # errors per minute
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert statistics to dictionary for reporting."""
        return {
            "api_errors": self.api_errors,
            "memory_errors": self.memory_errors,
            "parsing_errors": self.parsing_errors,
            "cache_errors": self.cache_errors,
            "fallback_activations": self.fallback_activations,
            "recovery_attempts": self.recovery_attempts,
            "successful_recoveries": self.successful_recoveries,
            "recovery_success_rate": (
                self.successful_recoveries / self.recovery_attempts 
                if self.recovery_attempts > 0 else 0.0
            ),
            "last_error_time": self.last_error_time.isoformat() if self.last_error_time else None,
            "recent_error_rate": self.get_error_rate(),
            "total_errors": len(self.error_history)
        }


class CatalogErrorHandler:
    """
    Comprehensive error handler for the document catalog system.
    
    Provides graceful error handling, fallback mechanisms, and recovery
    strategies for various failure scenarios in the catalog system.
    """
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, 
                 max_delay: float = 60.0, memory_threshold_mb: float = 400.0):
        """
        Initialize the error handler.
        
        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Base delay for exponential backoff (seconds)
            max_delay: Maximum delay between retries (seconds)
            memory_threshold_mb: Memory threshold for pressure detection (MB)
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.memory_threshold_mb = memory_threshold_mb
        
        # Error tracking
        self.stats = ErrorStatistics()
        self._lock = threading.RLock()
        
        # Circuit breaker state
        self._circuit_breaker_open = False
        self._circuit_breaker_open_time = None
        self._circuit_breaker_timeout = 300  # 5 minutes
        
        # Fallback handlers
        self._fallback_handlers = {}
        self._register_default_fallbacks()
    
    def handle_api_error(self, pdf_path: str, page_num: int, error: Exception, 
                        text: str = "", retry_count: int = 0, llm_response: str = None) -> Optional[PageAnalysis]:
        """
        Handle API errors with exponential backoff and fallback.
        
        Args:
            pdf_path: Path to the PDF file
            page_num: Page number (1-indexed)
            error: The API error that occurred
            text: Text content of the page
            retry_count: Current retry attempt
            llm_response: Raw LLM response if available
            
        Returns:
            PageAnalysis object or None if all recovery attempts fail
        """
        with self._lock:
            self.stats.record_error("api", str(error), {
                "pdf_path": pdf_path,
                "page_num": page_num,
                "retry_count": retry_count
            })
        
        logger.warning(f"API error on page {page_num} of {pdf_path}: {error}")
        
        # Log LLM response if available
        if llm_response:
            self._log_llm_response_error(pdf_path, page_num, error, llm_response)
        
        # Check circuit breaker
        if self._is_circuit_breaker_open():
            logger.warning("Circuit breaker is open, using fallback immediately")
            return self._fallback_page_analysis(pdf_path, page_num, text, "Circuit breaker open")
        
        # Attempt retry with exponential backoff
        if retry_count < self.max_retries:
            delay = min(self.base_delay * (2 ** retry_count) + random.uniform(0, 1), self.max_delay)
            logger.info(f"Retrying API call in {delay:.2f} seconds (attempt {retry_count + 1}/{self.max_retries})")
            
            time.sleep(delay)
            
            try:
                # This would be called by the PageAnalyzer with retry logic
                self.stats.record_recovery_attempt(successful=True)
                return None  # Signal to retry
            except Exception as retry_error:
                logger.error(f"Retry attempt {retry_count + 1} failed: {retry_error}")
                return self.handle_api_error(pdf_path, page_num, retry_error, text, retry_count + 1, llm_response)
        
        # All retries exhausted, activate circuit breaker and use fallback
        self._activate_circuit_breaker()
        return self._fallback_page_analysis(pdf_path, page_num, text, f"API error after {self.max_retries} retries: {error}")
    
    def handle_memory_pressure(self, catalog_cache, current_usage_mb: float) -> bool:
        """
        Handle memory pressure by cleaning up cache and optimizing memory usage.
        
        Args:
            catalog_cache: The CatalogCache instance
            current_usage_mb: Current memory usage in MB
            
        Returns:
            True if memory pressure was successfully handled
        """
        with self._lock:
            self.stats.record_error("memory", f"Memory pressure: {current_usage_mb:.1f}MB", {
                "current_usage_mb": current_usage_mb,
                "threshold_mb": self.memory_threshold_mb
            })
        
        logger.warning(f"Memory pressure detected: {current_usage_mb:.1f}MB (threshold: {self.memory_threshold_mb}MB)")
        
        try:
            # Step 1: Clean up old entries (older than 1 hour)
            removed_count = catalog_cache.cleanup_old_entries(max_age_seconds=3600)
            if removed_count > 0:
                logger.info(f"Cleaned up {removed_count} old cache entries")
                new_usage = catalog_cache.get_memory_usage_mb()
                if new_usage < self.memory_threshold_mb * 0.8:  # 80% of threshold
                    logger.info(f"Memory pressure resolved: {new_usage:.1f}MB")
                    return True
            
            # Step 2: More aggressive cleanup (older than 30 minutes)
            removed_count = catalog_cache.cleanup_old_entries(max_age_seconds=1800)
            if removed_count > 0:
                logger.info(f"Aggressive cleanup removed {removed_count} entries")
                new_usage = catalog_cache.get_memory_usage_mb()
                if new_usage < self.memory_threshold_mb * 0.8:
                    logger.info(f"Memory pressure resolved: {new_usage:.1f}MB")
                    return True
            
            # Step 3: Force eviction of least recently used entries
            initial_count = len(catalog_cache)
            target_count = max(1, int(initial_count * 0.5))  # Keep 50% of entries
            
            while len(catalog_cache) > target_count:
                # Remove oldest document (first in OrderedDict)
                if catalog_cache._documents:
                    oldest_doc_id = next(iter(catalog_cache._documents))
                    catalog_cache.remove_document(oldest_doc_id)
                else:
                    break
            
            evicted_count = initial_count - len(catalog_cache)
            logger.info(f"Force evicted {evicted_count} cache entries")
            
            new_usage = catalog_cache.get_memory_usage_mb()
            logger.info(f"Memory usage after cleanup: {new_usage:.1f}MB")
            
            self.stats.record_recovery_attempt(successful=new_usage < self.memory_threshold_mb)
            return new_usage < self.memory_threshold_mb
            
        except Exception as e:
            logger.error(f"Error handling memory pressure: {e}")
            self.stats.record_recovery_attempt(successful=False)
            return False
    
    def handle_parsing_error(self, raw_response: str, page_num: int, error: Exception, 
                           pdf_path: str = None) -> PageAnalysis:
        """
        Handle parsing errors by attempting alternative parsing strategies.
        
        Args:
            raw_response: Raw response from API
            page_num: Page number (1-indexed)
            error: The parsing error that occurred
            pdf_path: Path to the PDF file (for logging)
            
        Returns:
            PageAnalysis object with available data
        """
        with self._lock:
            self.stats.record_error("parsing", str(error), {
                "page_num": page_num,
                "response_length": len(raw_response) if raw_response else 0,
                "pdf_path": pdf_path
            })
        
        logger.warning(f"Parsing error on page {page_num}: {error}")
        
        # Log the problematic LLM response
        if raw_response and pdf_path:
            self._log_llm_response_error(pdf_path, page_num, error, raw_response)
        
        try:
            # Attempt alternative parsing strategies
            page_analysis = self._attempt_alternative_parsing(raw_response, page_num)
            if page_analysis:
                logger.info(f"Successfully recovered from parsing error using alternative method")
                self.stats.record_recovery_attempt(successful=True)
                return page_analysis
        except Exception as alt_error:
            logger.error(f"Alternative parsing also failed: {alt_error}")
        
        # Create fallback analysis with error information
        self.stats.record_recovery_attempt(successful=False)
        return self._create_error_page_analysis(page_num, f"Parsing error: {error}")
    
    def handle_cache_error(self, operation: str, document_id: str, error: Exception) -> bool:
        """
        Handle cache operation errors.
        
        Args:
            operation: The cache operation that failed
            document_id: Document ID involved in the operation
            error: The cache error that occurred
            
        Returns:
            True if error was handled successfully
        """
        with self._lock:
            self.stats.record_error("cache", str(error), {
                "operation": operation,
                "document_id": document_id
            })
        
        logger.error(f"Cache error during {operation} for document {document_id}: {error}")
        
        # For now, cache errors are logged but don't prevent processing
        # The system can continue without caching
        return True
    
    def should_use_fallback(self, error_type: str) -> bool:
        """
        Determine if fallback should be used based on error patterns.
        
        Args:
            error_type: Type of error encountered
            
        Returns:
            True if fallback should be used
        """
        # Use fallback if circuit breaker is open
        if self._is_circuit_breaker_open():
            return True
        
        # Use fallback if error rate is too high
        error_rate = self.stats.get_error_rate(window_minutes=10)
        if error_rate > 0.5:  # More than 0.5 errors per minute (30 errors per hour)
            logger.warning(f"High error rate detected: {error_rate:.2f} errors/minute, using fallback")
            return True
        
        return False
    
    def get_fallback_i9_detector(self):
        """
        Get a fallback I-9 detector that uses original keyword-based detection.
        
        Returns:
            Function that performs keyword-based I-9 detection
        """
        def fallback_detector(pdf_path: str, document_id: str = None) -> Tuple[List[int], List[int]]:
            """Fallback I-9 detection using original keyword-based method."""
            try:
                logger.info(f"Using fallback I-9 detection for {pdf_path}")
                self.stats.record_fallback()
                
                # Import here to avoid circular imports
                from ..core.i9_detector import I9Detector
                
                # Create a detector without catalog cache (forces original method)
                fallback_detector_instance = I9Detector(catalog_cache=None)
                return fallback_detector_instance._detect_all_i9_pages_original(pdf_path)
                
            except Exception as e:
                logger.error(f"Fallback I-9 detection failed: {e}")
                return [], []
        
        return fallback_detector
    
    def register_fallback_handler(self, error_type: str, handler: Callable):
        """
        Register a custom fallback handler for specific error types.
        
        Args:
            error_type: Type of error to handle
            handler: Callable that handles the error
        """
        self._fallback_handlers[error_type] = handler
        logger.info(f"Registered fallback handler for {error_type}")
    
    def get_error_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive error statistics.
        
        Returns:
            Dictionary containing error statistics and metrics
        """
        with self._lock:
            return self.stats.to_dict()
    
    def reset_circuit_breaker(self):
        """Manually reset the circuit breaker."""
        with self._lock:
            self._circuit_breaker_open = False
            self._circuit_breaker_open_time = None
            logger.info("Circuit breaker manually reset")
    
    def _is_circuit_breaker_open(self) -> bool:
        """Check if circuit breaker is currently open."""
        if not self._circuit_breaker_open:
            return False
        
        # Check if timeout has passed
        if (self._circuit_breaker_open_time and 
            datetime.now() - self._circuit_breaker_open_time > timedelta(seconds=self._circuit_breaker_timeout)):
            logger.info("Circuit breaker timeout expired, attempting to close")
            self._circuit_breaker_open = False
            self._circuit_breaker_open_time = None
            return False
        
        return True
    
    def _activate_circuit_breaker(self):
        """Activate the circuit breaker to prevent further API calls."""
        with self._lock:
            self._circuit_breaker_open = True
            self._circuit_breaker_open_time = datetime.now()
            logger.warning(f"Circuit breaker activated for {self._circuit_breaker_timeout} seconds")
    
    def _fallback_page_analysis(self, pdf_path: str, page_num: int, text: str, reason: str) -> PageAnalysis:
        """
        Create a fallback page analysis using keyword-based detection.
        
        Args:
            pdf_path: Path to the PDF file
            page_num: Page number (1-indexed)
            text: Text content of the page
            reason: Reason for fallback
            
        Returns:
            PageAnalysis object with basic classification
        """
        logger.info(f"Creating fallback analysis for page {page_num}: {reason}")
        self.stats.record_fallback()
        
        try:
            # Use keyword-based classification
            page_type, page_subtype, confidence = self._classify_page_by_keywords(text)
            
            return PageAnalysis(
                page_number=page_num,
                page_title=f"Page {page_num} (Fallback Analysis)",
                page_type=page_type,
                page_subtype=page_subtype,
                confidence_score=confidence,
                extracted_values={"fallback_reason": reason},
                text_regions=[],
                page_metadata=PageMetadata(
                    has_handwritten_text=False,
                    has_signatures=False,
                    image_quality="low",
                    language="en",
                    text_extraction_method="text"
                )
            )
            
        except Exception as e:
            logger.error(f"Error creating fallback analysis: {e}")
            return self._create_error_page_analysis(page_num, f"Fallback failed: {e}")
    
    def _classify_page_by_keywords(self, text: str) -> Tuple[str, str, float]:
        """
        Classify page type using keyword-based detection.
        
        Args:
            text: Text content of the page
            
        Returns:
            Tuple of (page_type, page_subtype, confidence_score)
        """
        if not text:
            return "other", "other", 0.1
        
        text_lower = text.lower()
        
        # I-9 form detection
        if any(keyword in text_lower for keyword in [
            "employment eligibility verification", "form i-9", "i-9", "uscis", 
            "department of homeland security"
        ]):
            return "government_form", "i9_form", 0.8
        
        # Other government forms
        if any(keyword in text_lower for keyword in [
            "internal revenue service", "irs", "social security", "w-2", "w-4", 
            "1099", "tax form"
        ]):
            return "government_form", "tax_document", 0.7
        
        # Identity documents
        if any(keyword in text_lower for keyword in [
            "passport", "driver", "license", "identification", "birth certificate",
            "social security card"
        ]):
            return "identity_document", "other", 0.7
        
        # Employment records
        if any(keyword in text_lower for keyword in [
            "pay stub", "payroll", "salary", "wage", "employment", "hire", 
            "position", "job title"
        ]):
            return "employment_record", "other", 0.6
        
        return "other", "other", 0.3
    
    def _attempt_alternative_parsing(self, raw_response: str, page_num: int) -> Optional[PageAnalysis]:
        """
        Attempt alternative parsing strategies for malformed responses.
        
        Args:
            raw_response: Raw response from API
            page_num: Page number (1-indexed)
            
        Returns:
            PageAnalysis object if parsing succeeds, None otherwise
        """
        if not raw_response:
            return None
        
        try:
            import json
            import re
            
            # Strategy 1: Extract JSON from markdown code blocks
            json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', raw_response, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group(1))
                return self._create_page_analysis_from_partial_data(data, page_num)
            
            # Strategy 2: Find JSON object in response
            json_match = re.search(r'(\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\})', raw_response, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group(1))
                return self._create_page_analysis_from_partial_data(data, page_num)
            
            # Strategy 3: Extract key information using regex
            return self._extract_info_with_regex(raw_response, page_num)
            
        except Exception as e:
            logger.debug(f"Alternative parsing strategy failed: {e}")
            return None
    
    def _create_page_analysis_from_partial_data(self, data: Dict[str, Any], page_num: int) -> PageAnalysis:
        """
        Create PageAnalysis from partial data dictionary.
        
        Args:
            data: Partial data dictionary
            page_num: Page number (1-indexed)
            
        Returns:
            PageAnalysis object
        """
        return PageAnalysis(
            page_number=page_num,
            page_title=data.get("page_title", f"Page {page_num}"),
            page_type=data.get("page_type", "other"),
            page_subtype=data.get("page_subtype", "other"),
            confidence_score=float(data.get("confidence_score", 0.5)),
            extracted_values=data.get("extracted_values", {}),
            text_regions=[],  # Skip complex parsing for fallback
            page_metadata=PageMetadata(
                has_handwritten_text=False,
                has_signatures=False,
                image_quality="low",
                language="en",
                text_extraction_method="text"
            )
        )
    
    def _extract_info_with_regex(self, response: str, page_num: int) -> PageAnalysis:
        """
        Extract basic information using regex patterns.
        
        Args:
            response: Raw response text
            page_num: Page number (1-indexed)
            
        Returns:
            PageAnalysis object with extracted information
        """
        # Extract page type
        page_type = "other"
        page_subtype = "other"
        confidence = 0.3
        
        response_lower = response.lower()
        
        if "i-9" in response_lower or "i9" in response_lower:
            page_type = "government_form"
            page_subtype = "i9_form"
            confidence = 0.7
        elif "government" in response_lower or "form" in response_lower:
            page_type = "government_form"
            confidence = 0.5
        elif "identity" in response_lower or "passport" in response_lower:
            page_type = "identity_document"
            confidence = 0.5
        
        return PageAnalysis(
            page_number=page_num,
            page_title=f"Page {page_num} (Regex Recovery)",
            page_type=page_type,
            page_subtype=page_subtype,
            confidence_score=confidence,
            extracted_values={"recovery_method": "regex"},
            text_regions=[],
            page_metadata=PageMetadata(
                has_handwritten_text=False,
                has_signatures=False,
                image_quality="low",
                language="en",
                text_extraction_method="text"
            )
        )
    
    def _create_error_page_analysis(self, page_num: int, error_msg: str) -> PageAnalysis:
        """
        Create a PageAnalysis object for error cases.
        
        Args:
            page_num: Page number (1-indexed)
            error_msg: Error message
            
        Returns:
            PageAnalysis object with error information
        """
        return PageAnalysis(
            page_number=page_num,
            page_title=f"Page {page_num} (Error)",
            page_type="other",
            page_subtype="error",
            confidence_score=0.0,
            extracted_values={"error": error_msg},
            text_regions=[],
            page_metadata=PageMetadata(
                has_handwritten_text=False,
                has_signatures=False,
                image_quality="low",
                language="en",
                text_extraction_method="text"
            )
        )
    
    def _log_llm_response_error(self, pdf_path: str, page_num: int, error: Exception, llm_response: str):
        """
        Log LLM response when processing errors occur.
        
        Args:
            pdf_path: Path to the PDF file
            page_num: Page number (1-indexed)
            error: The error that occurred
            llm_response: Raw LLM response
        """
        try:
            # Create error log entry
            error_log_entry = {
                "timestamp": datetime.now().isoformat(),
                "pdf_path": pdf_path,
                "page_number": page_num,
                "error_type": type(error).__name__,
                "error_message": str(error),
                "llm_response": llm_response,
                "response_length": len(llm_response) if llm_response else 0
            }
            
            # Log to main logger with structured format
            logger.error(f"LLM Response Error - PDF: {Path(pdf_path).name}, Page: {page_num}, "
                        f"Error: {error}, Response Length: {len(llm_response) if llm_response else 0}")
            logger.debug(f"Full LLM Response: {llm_response}")
            
            # Also log to a separate error response file if configured
            self._write_error_response_log(error_log_entry)
            
        except Exception as log_error:
            logger.error(f"Error logging LLM response: {log_error}")
    
    def _write_error_response_log(self, error_log_entry: Dict[str, Any]):
        """
        Write error response to a separate log file for detailed analysis.
        
        Args:
            error_log_entry: Dictionary containing error details and LLM response
        """
        try:
            import json
            from ..config import settings
            
            # Create error log directory if it doesn't exist
            error_log_dir = os.path.join(settings.WORK_DIR, "logs", "llm_errors")
            os.makedirs(error_log_dir, exist_ok=True)
            
            # Generate error log filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            pdf_name = Path(error_log_entry["pdf_path"]).stem
            error_log_filename = f"llm_error_{pdf_name}_page{error_log_entry['page_number']}_{timestamp}.json"
            error_log_path = os.path.join(error_log_dir, error_log_filename)
            
            # Write detailed error log
            with open(error_log_path, 'w', encoding='utf-8') as f:
                json.dump(error_log_entry, f, indent=2, ensure_ascii=False)
            
            logger.info(f"LLM error response logged to: {error_log_path}")
            
        except Exception as e:
            logger.error(f"Failed to write error response log: {e}")
    
    def _register_default_fallbacks(self):
        """Register default fallback handlers."""
        self._fallback_handlers["api"] = self._fallback_page_analysis
        self._fallback_handlers["parsing"] = self.handle_parsing_error
        self._fallback_handlers["memory"] = self.handle_memory_pressure