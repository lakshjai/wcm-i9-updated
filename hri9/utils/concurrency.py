#!/usr/bin/env python3
"""
Concurrency utilities for the I-9 detection system.

This module provides thread-safe resources for concurrent processing.
"""

import os
import csv
import threading
import time
from typing import Optional, Dict, Any, List
from datetime import datetime
from ..utils.logging_config import logger
from ..config import settings
from ..catalog.cache import CatalogCache, CacheStatistics

class SharedResources:
    """Thread-safe shared resources for concurrent I9 detection processing."""
    
    def __init__(self, csv_file_path=None, catalog_cache=None, use_enhanced_csv=True):
        """
        Initialize shared resources for concurrent processing.
        
        Args:
            csv_file_path (str, optional): Path to CSV output file. Defaults to settings.OUTPUT_CSV.
            catalog_cache (CatalogCache, optional): Shared catalog cache instance.
            use_enhanced_csv (bool): Whether to use enhanced CSV format with catalog data.
        """
        self.csv_lock = threading.Lock()
        self.progress_lock = threading.Lock()
        self.pdf_lock = threading.Lock()
        self.catalog_lock = threading.RLock()
        
        # Original counters
        self.processed_count = 0
        self.found_i9_count = 0
        self.removed_i9_count = 0
        self.extracted_i9_count = 0
        
        # Catalog-related counters
        self.cataloged_documents = 0
        self.cataloged_pages = 0
        self.catalog_api_calls = 0
        self.catalog_cache_hits = 0
        self.catalog_cache_misses = 0
        self.catalog_processing_time = 0.0
        self.catalog_start_time = None
        
        # File handling
        self.csv_file_path = csv_file_path or settings.OUTPUT_CSV
        self.csv_file = None
        self.csv_writer = None
        self.processed_pdfs = set()  # Track processed PDFs to avoid duplicates
        
        # Catalog cache management
        self.catalog_cache = catalog_cache or CatalogCache()
        self.catalog_enabled = True
        self.use_enhanced_csv = use_enhanced_csv
        
    def initialize_csv(self, headers=None, include_catalog_metrics=True):
        """
        Initialize CSV file and writer with specified headers.
        
        Args:
            headers (list, optional): List of column headers. Defaults to standard headers.
            include_catalog_metrics (bool): Whether to include catalog-related columns.
        """
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.csv_file_path), exist_ok=True)
            
            if self.use_enhanced_csv:
                # Use enhanced CSV format
                from ..utils.enhanced_reporting import EnhancedReporter
                self.csv_file, self.csv_writer = EnhancedReporter.initialize_enhanced_csv(
                    self.csv_file_path, include_catalog_data=self.catalog_enabled
                )
            else:
                # Use original CSV format
                if headers is None:
                    headers = ['Employee ID', 'PDF File Name', 'I-9 Forms Found', 
                              'Pages Removed', 'Success', 'Extracted I-9 Path']
                    
                    # Add catalog metrics columns if enabled
                    if include_catalog_metrics and self.catalog_enabled:
                        headers.extend([
                            'Catalog Generated', 'Pages Cataloged', 'Catalog API Calls',
                            'Catalog Processing Time (s)', 'Catalog Cache Hits', 'Catalog Cache Misses'
                        ])
                
                # Open file and initialize writer
                self.csv_file = open(self.csv_file_path, 'w', newline='', encoding='utf-8')
                self.csv_writer = csv.writer(self.csv_file)
                self.csv_writer.writerow(headers)
                self.csv_file.flush()
                
            logger.info(f"Initialized {'enhanced' if self.use_enhanced_csv else 'standard'} CSV output at {self.csv_file_path}")
        except Exception as e:
            logger.error(f"Error initializing CSV file: {e}")
    
    def write_csv_row(self, row_data):
        """
        Thread-safe CSV writing.
        
        Args:
            row_data (list): Row data to write to CSV.
        """
        with self.csv_lock:
            if self.csv_writer:
                try:
                    self.csv_writer.writerow(row_data)
                    self.csv_file.flush()
                except Exception as e:
                    logger.error(f"Error writing to CSV: {e}")
    
    def update_progress(self, found_i9=False, removed_i9=False, extracted_i9=False):
        """
        Thread-safe progress tracking.
        
        Args:
            found_i9 (bool): Whether an I-9 form was found.
            removed_i9 (bool): Whether an I-9 form was removed.
            extracted_i9 (bool): Whether an I-9 form was extracted.
            
        Returns:
            tuple: Current progress counts (processed, found, removed, extracted).
        """
        with self.progress_lock:
            self.processed_count += 1
            if found_i9:
                self.found_i9_count += 1
            if removed_i9:
                self.removed_i9_count += 1
            if extracted_i9:
                self.extracted_i9_count += 1
            return self.processed_count, self.found_i9_count, self.removed_i9_count, self.extracted_i9_count
    
    def get_progress(self):
        """
        Get current progress counts.
        
        Returns:
            tuple: Current progress counts (processed, found, removed, extracted).
        """
        with self.progress_lock:
            return self.processed_count, self.found_i9_count, self.removed_i9_count, self.extracted_i9_count
    
    def is_pdf_processed(self, pdf_path):
        """
        Check if a PDF has already been processed (thread-safe).
        
        Args:
            pdf_path (str): Path to PDF file.
            
        Returns:
            bool: True if PDF has been processed, False otherwise.
        """
        with self.pdf_lock:
            return pdf_path in self.processed_pdfs
    
    def mark_pdf_processed(self, pdf_path):
        """
        Mark a PDF as processed (thread-safe).
        
        Args:
            pdf_path (str): Path to PDF file.
        """
        with self.pdf_lock:
            self.processed_pdfs.add(pdf_path)
    
    def start_catalog_processing(self):
        """Mark the start of catalog processing for timing."""
        with self.catalog_lock:
            self.catalog_start_time = time.time()
            logger.info("Started catalog processing phase")
    
    def update_catalog_progress(self, document_cataloged=False, pages_cataloged=0, 
                               api_calls_made=0, processing_time=0.0):
        """
        Thread-safe catalog progress tracking.
        
        Args:
            document_cataloged (bool): Whether a document was successfully cataloged.
            pages_cataloged (int): Number of pages cataloged.
            api_calls_made (int): Number of API calls made.
            processing_time (float): Processing time in seconds.
            
        Returns:
            dict: Current catalog progress statistics.
        """
        with self.catalog_lock:
            if document_cataloged:
                self.cataloged_documents += 1
            self.cataloged_pages += pages_cataloged
            self.catalog_api_calls += api_calls_made
            self.catalog_processing_time += processing_time
            
            # Update cache statistics
            if self.catalog_cache:
                cache_stats = self.catalog_cache.get_statistics()
                self.catalog_cache_hits = cache_stats.cache_hits
                self.catalog_cache_misses = cache_stats.cache_misses
            
            return self.get_catalog_statistics()
    
    def get_catalog_statistics(self):
        """
        Get current catalog statistics.
        
        Returns:
            dict: Current catalog statistics.
        """
        with self.catalog_lock:
            stats = {
                'cataloged_documents': self.cataloged_documents,
                'cataloged_pages': self.cataloged_pages,
                'catalog_api_calls': self.catalog_api_calls,
                'catalog_processing_time': self.catalog_processing_time,
                'catalog_cache_hits': self.catalog_cache_hits,
                'catalog_cache_misses': self.catalog_cache_misses,
                'avg_pages_per_document': (self.cataloged_pages / self.cataloged_documents 
                                         if self.cataloged_documents > 0 else 0),
                'avg_processing_time_per_document': (self.catalog_processing_time / self.cataloged_documents 
                                                   if self.cataloged_documents > 0 else 0),
                'cache_hit_rate': (self.catalog_cache_hits / (self.catalog_cache_hits + self.catalog_cache_misses) * 100
                                 if (self.catalog_cache_hits + self.catalog_cache_misses) > 0 else 0)
            }
            
            if self.catalog_cache:
                cache_stats = self.catalog_cache.get_statistics()
                stats.update({
                    'cache_memory_usage_mb': cache_stats.memory_usage_bytes / (1024 * 1024),
                    'cache_evictions': cache_stats.evictions,
                    'cached_documents_count': cache_stats.total_documents
                })
            
            return stats
    
    def store_document_catalog(self, document_id: str, catalog_entry) -> bool:
        """
        Thread-safe storage of document catalog entry.
        
        Args:
            document_id (str): Unique document identifier.
            catalog_entry: Document catalog entry to store.
            
        Returns:
            bool: True if stored successfully, False otherwise.
        """
        if self.catalog_cache is None or not self.catalog_enabled:
            return False
            
        with self.catalog_lock:
            try:
                success = self.catalog_cache.store_document_catalog(document_id, catalog_entry)
                if success:
                    logger.debug(f"Stored catalog for document: {document_id}")
                else:
                    logger.warning(f"Failed to store catalog for document: {document_id}")
                return success
            except Exception as e:
                logger.error(f"Error storing catalog for document {document_id}: {e}")
                return False
    
    def get_document_catalog(self, document_id: str):
        """
        Thread-safe retrieval of document catalog entry.
        
        Args:
            document_id (str): Unique document identifier.
            
        Returns:
            Document catalog entry if found, None otherwise.
        """
        if self.catalog_cache is None or not self.catalog_enabled:
            return None
            
        with self.catalog_lock:
            try:
                return self.catalog_cache.get_document_catalog(document_id)
            except Exception as e:
                logger.error(f"Error retrieving catalog for document {document_id}: {e}")
                return None
    
    def get_page_analysis(self, document_id: str, page_number: int):
        """
        Thread-safe retrieval of page analysis.
        
        Args:
            document_id (str): Unique document identifier.
            page_number (int): Page number to retrieve.
            
        Returns:
            Page analysis if found, None otherwise.
        """
        if self.catalog_cache is None or not self.catalog_enabled:
            return None
            
        with self.catalog_lock:
            try:
                return self.catalog_cache.get_page_analysis(document_id, page_number)
            except Exception as e:
                logger.error(f"Error retrieving page analysis for document {document_id}, page {page_number}: {e}")
                return None
    
    def cleanup_catalog_cache(self, max_age_seconds: int = 3600) -> int:
        """
        Clean up old catalog entries to manage memory.
        
        Args:
            max_age_seconds (int): Maximum age of entries to keep.
            
        Returns:
            int: Number of entries removed.
        """
        if self.catalog_cache is None or not self.catalog_enabled:
            return 0
            
        with self.catalog_lock:
            try:
                removed_count = self.catalog_cache.cleanup_old_entries(max_age_seconds)
                if removed_count > 0:
                    logger.info(f"Cleaned up {removed_count} old catalog entries")
                return removed_count
            except Exception as e:
                logger.error(f"Error cleaning up catalog cache: {e}")
                return 0
    
    def is_catalog_memory_pressure(self) -> bool:
        """
        Check if catalog cache is under memory pressure.
        
        Returns:
            bool: True if under memory pressure, False otherwise.
        """
        if self.catalog_cache is None or not self.catalog_enabled:
            return False
            
        with self.catalog_lock:
            try:
                return self.catalog_cache.is_memory_pressure()
            except Exception as e:
                logger.error(f"Error checking catalog memory pressure: {e}")
                return False
    
    def get_catalog_cache_info(self) -> Dict[str, Any]:
        """
        Get detailed catalog cache information.
        
        Returns:
            dict: Cache information including statistics and configuration.
        """
        if self.catalog_cache is None or not self.catalog_enabled:
            return {'enabled': False}
            
        with self.catalog_lock:
            try:
                stats = self.catalog_cache.get_statistics()
                return {
                    'enabled': True,
                    'statistics': stats.to_dict(),
                    'memory_usage_mb': stats.memory_usage_bytes / (1024 * 1024),
                    'cached_documents': len(self.catalog_cache),
                    'memory_pressure': self.catalog_cache.is_memory_pressure()
                }
            except Exception as e:
                logger.error(f"Error getting catalog cache info: {e}")
                return {'enabled': True, 'error': str(e)}
    
    def write_enhanced_csv_row(self, base_data, catalog_entry=None, catalog_files=None):
        """
        Write an enhanced CSV row with catalog data.
        
        Args:
            base_data (dict): Base processing data.
            catalog_entry: Document catalog entry.
            catalog_files (dict): Paths to catalog files.
        """
        if self.use_enhanced_csv:
            from ..utils.enhanced_reporting import EnhancedReporter
            # Log successful extraction
            logger.debug(f"Writing enhanced CSV row: catalog_entry={type(catalog_entry)}, has_pages={hasattr(catalog_entry, 'pages') if catalog_entry else False}")
            with self.csv_lock:
                if self.csv_writer and self.csv_file:
                    EnhancedReporter.write_enhanced_csv_row(
                        self.csv_writer, self.csv_file, base_data, 
                        catalog_entry, catalog_files
                    )
        else:
            # Fallback to original format
            row_data = [
                base_data.get('employee_id', ''),
                base_data.get('pdf_file_name', ''),
                base_data.get('i9_forms_found', 'No'),
                base_data.get('pages_removed', 0),
                base_data.get('success', 'No'),
                base_data.get('extracted_i9_path', '')
            ]
            self.write_csv_row(row_data)
    
    def write_csv_row_with_catalog_metrics(self, base_row_data: List[Any], 
                                          catalog_metrics: Optional[Dict[str, Any]] = None):
        """
        Write a CSV row including catalog metrics.
        
        Args:
            base_row_data (list): Base row data (original columns).
            catalog_metrics (dict, optional): Catalog metrics for this document.
        """
        row_data = list(base_row_data)
        
        # Add catalog metrics if enabled
        if self.catalog_enabled:
            if catalog_metrics:
                row_data.extend([
                    catalog_metrics.get('catalog_generated', False),
                    catalog_metrics.get('pages_cataloged', 0),
                    catalog_metrics.get('api_calls_made', 0),
                    round(catalog_metrics.get('processing_time', 0.0), 3),
                    catalog_metrics.get('cache_hits', 0),
                    catalog_metrics.get('cache_misses', 0)
                ])
            else:
                # Default values when no catalog metrics available
                row_data.extend([False, 0, 0, 0.0, 0, 0])
        
        self.write_csv_row(row_data)
    
    def generate_catalog_summary(self) -> str:
        """
        Generate a summary of catalog processing statistics.
        
        Returns:
            str: Formatted catalog summary.
        """
        if not self.catalog_enabled:
            return "Catalog processing: Disabled"
            
        stats = self.get_catalog_statistics()
        cache_info = self.get_catalog_cache_info()
        
        total_time = time.time() - self.catalog_start_time if self.catalog_start_time else 0
        
        summary_lines = [
            "Catalog Processing Summary",
            "-------------------------",
            f"Documents cataloged: {stats['cataloged_documents']}",
            f"Pages cataloged: {stats['cataloged_pages']}",
            f"API calls made: {stats['catalog_api_calls']}",
            f"Total processing time: {stats['catalog_processing_time']:.2f} seconds",
            f"Average pages per document: {stats['avg_pages_per_document']:.1f}",
            f"Average processing time per document: {stats['avg_processing_time_per_document']:.2f} seconds",
            "",
            "Cache Statistics",
            "----------------",
            f"Cache hits: {stats['catalog_cache_hits']}",
            f"Cache misses: {stats['catalog_cache_misses']}",
            f"Cache hit rate: {stats['cache_hit_rate']:.1f}%",
            f"Memory usage: {stats.get('cache_memory_usage_mb', 0):.1f} MB",
            f"Cache evictions: {stats.get('cache_evictions', 0)}",
            f"Cached documents: {stats.get('cached_documents_count', 0)}"
        ]
        
        if total_time > 0:
            summary_lines.extend([
                "",
                "Performance Metrics",
                "------------------",
                f"Documents per second: {stats['cataloged_documents'] / total_time:.2f}",
                f"Pages per second: {stats['cataloged_pages'] / total_time:.2f}",
                f"API calls per second: {stats['catalog_api_calls'] / total_time:.2f}"
            ])
        
        return "\n".join(summary_lines)
    
    def close(self):
        """Close CSV file and clean up resources."""
        if self.csv_file:
            try:
                self.csv_file.close()
                logger.info(f"Closed CSV output file: {self.csv_file_path}")
            except Exception as e:
                logger.error(f"Error closing CSV file: {e}")
        
        # Clean up catalog cache if needed
        if self.catalog_cache and self.catalog_enabled:
            try:
                # Log final statistics
                final_stats = self.generate_catalog_summary()
                logger.info(f"Final catalog statistics:\n{final_stats}")
                
                # Optional: Clear cache to free memory
                # self.catalog_cache.clear_cache()
            except Exception as e:
                logger.error(f"Error during catalog cleanup: {e}")
