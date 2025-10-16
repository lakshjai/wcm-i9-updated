"""
Thread-safe in-memory cache for document catalog data.

This module provides the CatalogCache class that manages document catalog
entries with LRU eviction, thread-safe operations, and monitoring capabilities.
"""

import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime

from .models import DocumentCatalogEntry, PageAnalysis
from .logging import get_catalog_logger


@dataclass
class CacheStatistics:
    """Statistics for cache operations and performance monitoring."""
    total_documents: int = 0
    total_pages: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    evictions: int = 0
    memory_usage_bytes: int = 0
    last_access_time: Optional[datetime] = None
    creation_time: datetime = field(default_factory=datetime.now)
    
    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate as a percentage."""
        total_requests = self.cache_hits + self.cache_misses
        if total_requests == 0:
            return 0.0
        return (self.cache_hits / total_requests) * 100.0
    
    def to_dict(self) -> Dict[str, any]:
        """Convert statistics to dictionary for reporting."""
        return {
            'total_documents': self.total_documents,
            'total_pages': self.total_pages,
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'evictions': self.evictions,
            'hit_rate_percent': round(self.hit_rate, 2),
            'memory_usage_bytes': self.memory_usage_bytes,
            'last_access_time': self.last_access_time.isoformat() if self.last_access_time else None,
            'creation_time': self.creation_time.isoformat(),
            'uptime_seconds': (datetime.now() - self.creation_time).total_seconds()
        }


class CatalogCache:
    """
    Thread-safe LRU cache for document catalog entries.
    
    Provides efficient storage and retrieval of document catalog data with
    configurable size limits, LRU eviction, and comprehensive monitoring.
    """
    
    def __init__(self, max_documents: int = 1000, max_memory_mb: int = 512):
        """
        Initialize the catalog cache.
        
        Args:
            max_documents: Maximum number of documents to cache
            max_memory_mb: Maximum memory usage in megabytes (approximate)
        """
        self._max_documents = max_documents
        self._max_memory_bytes = max_memory_mb * 1024 * 1024
        
        # Thread-safe storage using OrderedDict for LRU behavior
        self._documents: OrderedDict[str, DocumentCatalogEntry] = OrderedDict()
        self._document_access_times: Dict[str, float] = {}
        
        # Thread synchronization
        self._lock = threading.RLock()
        
        # Statistics tracking
        self._stats = CacheStatistics()
        
        # Page lookup optimization
        self._page_index: Dict[str, Dict[int, PageAnalysis]] = {}
        
        # Logging
        self.catalog_logger = get_catalog_logger()
    
    def store_document_catalog(self, document_id: str, catalog: DocumentCatalogEntry) -> bool:
        """
        Store a document catalog entry in the cache.
        
        Args:
            document_id: Unique identifier for the document
            catalog: Complete catalog entry to store
            
        Returns:
            True if stored successfully, False if rejected due to size limits
        """
        with self._lock:
            # Check if we need to make room
            if not self._ensure_capacity_for_document(catalog):
                return False
            
            # Remove existing entry if present (for updates)
            if document_id in self._documents:
                self._remove_document_internal(document_id)
            
            # Store the document
            self._documents[document_id] = catalog
            self._document_access_times[document_id] = time.time()
            
            # Build page index for fast page lookups
            self._page_index[document_id] = {
                page.page_number: page for page in catalog.pages
            }
            
            # Update statistics
            self._stats.total_documents += 1
            self._stats.total_pages += len(catalog.pages)
            self._stats.memory_usage_bytes += self._estimate_document_size(catalog)
            self._stats.last_access_time = datetime.now()
            
            return True
    
    def get_document_catalog(self, document_id: str) -> Optional[DocumentCatalogEntry]:
        """
        Retrieve a document catalog entry from the cache.
        
        Args:
            document_id: Unique identifier for the document
            
        Returns:
            Document catalog entry if found, None otherwise
        """
        with self._lock:
            if document_id in self._documents:
                # Update access time for LRU
                self._document_access_times[document_id] = time.time()
                
                # Move to end of OrderedDict (most recently used)
                catalog = self._documents.pop(document_id)
                self._documents[document_id] = catalog
                
                # Update statistics
                self._stats.cache_hits += 1
                self._stats.last_access_time = datetime.now()
                
                return catalog
            else:
                self._stats.cache_misses += 1
                return None
    
    def get_page_analysis(self, document_id: str, page_number: int) -> Optional[PageAnalysis]:
        """
        Retrieve analysis for a specific page from the cache.
        
        Args:
            document_id: Unique identifier for the document
            page_number: Page number to retrieve
            
        Returns:
            Page analysis if found, None otherwise
        """
        with self._lock:
            if document_id in self._page_index:
                # Update document access time
                self._document_access_times[document_id] = time.time()
                
                # Move document to end of OrderedDict
                if document_id in self._documents:
                    catalog = self._documents.pop(document_id)
                    self._documents[document_id] = catalog
                
                # Get page from index
                page_analysis = self._page_index[document_id].get(page_number)
                
                if page_analysis:
                    self._stats.cache_hits += 1
                    self._stats.last_access_time = datetime.now()
                    return page_analysis
            
            self._stats.cache_misses += 1
            return None
    
    def get_pages_by_type(self, document_id: str, page_type: str) -> List[PageAnalysis]:
        """
        Get all pages of a specific type from a document.
        
        Args:
            document_id: Unique identifier for the document
            page_type: Type of pages to retrieve
            
        Returns:
            List of matching page analyses
        """
        catalog = self.get_document_catalog(document_id)
        if catalog:
            return catalog.get_pages_by_type(page_type)
        return []
    
    def get_pages_by_subtype(self, document_id: str, page_subtype: str) -> List[PageAnalysis]:
        """
        Get all pages of a specific subtype from a document.
        
        Args:
            document_id: Unique identifier for the document
            page_subtype: Subtype of pages to retrieve
            
        Returns:
            List of matching page analyses
        """
        catalog = self.get_document_catalog(document_id)
        if catalog:
            return catalog.get_pages_by_subtype(page_subtype)
        return []
    
    def contains_document(self, document_id: str) -> bool:
        """
        Check if a document is cached.
        
        Args:
            document_id: Unique identifier for the document
            
        Returns:
            True if document is cached, False otherwise
        """
        with self._lock:
            return document_id in self._documents
    
    def get_cached_document_ids(self) -> Set[str]:
        """
        Get all cached document IDs.
        
        Returns:
            Set of document IDs currently in cache
        """
        with self._lock:
            return set(self._documents.keys())
    
    def get_cache_size(self) -> int:
        """
        Get the current number of documents in cache.
        
        Returns:
            Number of documents currently cached
        """
        with self._lock:
            return len(self._documents)
    
    def remove_document(self, document_id: str) -> bool:
        """
        Remove a document from the cache.
        
        Args:
            document_id: Unique identifier for the document
            
        Returns:
            True if document was removed, False if not found
        """
        with self._lock:
            if document_id in self._documents:
                self._remove_document_internal(document_id)
                return True
            return False
    
    def clear_cache(self) -> None:
        """Clear all cached data and reset statistics."""
        with self._lock:
            self._documents.clear()
            self._document_access_times.clear()
            self._page_index.clear()
            
            # Reset statistics but preserve creation time
            creation_time = self._stats.creation_time
            self._stats = CacheStatistics()
            self._stats.creation_time = creation_time
    
    def get_statistics(self) -> CacheStatistics:
        """
        Get current cache statistics.
        
        Returns:
            Current cache statistics
        """
        with self._lock:
            # Update memory usage estimate
            self._stats.memory_usage_bytes = sum(
                self._estimate_document_size(doc) for doc in self._documents.values()
            )
            return self._stats
    
    def get_memory_usage_mb(self) -> float:
        """
        Get current memory usage in megabytes.
        
        Returns:
            Estimated memory usage in MB
        """
        return self.get_statistics().memory_usage_bytes / (1024 * 1024)
    
    def is_memory_pressure(self, threshold_percent: float = 0.8) -> bool:
        """
        Check if cache is under memory pressure.
        
        Args:
            threshold_percent: Memory usage threshold as percentage of limit
        
        Returns:
            True if memory usage exceeds threshold
        """
        current_usage = self.get_statistics().memory_usage_bytes
        return current_usage > (self._max_memory_bytes * threshold_percent)
    
    def cleanup_old_entries(self, max_age_seconds: int = 3600) -> int:
        """
        Remove entries older than specified age.
        
        Args:
            max_age_seconds: Maximum age in seconds
            
        Returns:
            Number of entries removed
        """
        with self._lock:
            current_time = time.time()
            old_documents = []
            
            for doc_id, access_time in self._document_access_times.items():
                if current_time - access_time > max_age_seconds:
                    old_documents.append(doc_id)
            
            removed_count = 0
            for doc_id in old_documents:
                if doc_id in self._documents:  # Double-check existence
                    self._remove_document_internal(doc_id)
                    removed_count += 1
            
            return removed_count
    
    def _ensure_capacity_for_document(self, catalog: DocumentCatalogEntry) -> bool:
        """
        Ensure there's capacity for a new document, evicting if necessary.
        
        Args:
            catalog: Document catalog to be stored
            
        Returns:
            True if capacity is available, False if document is too large
        """
        document_size = self._estimate_document_size(catalog)
        
        # Check if document is too large for cache
        if document_size > self._max_memory_bytes:
            return False
        
        # Evict documents if necessary
        while (len(self._documents) >= self._max_documents or 
               self._stats.memory_usage_bytes + document_size > self._max_memory_bytes):
            
            if not self._documents:
                break
                
            # Remove least recently used document
            oldest_doc_id = next(iter(self._documents))
            self._remove_document_internal(oldest_doc_id)
            self._stats.evictions += 1
        
        return True
    
    def _remove_document_internal(self, document_id: str) -> None:
        """
        Internal method to remove a document and update statistics.
        
        Args:
            document_id: Document ID to remove
        """
        if document_id in self._documents:
            catalog = self._documents.pop(document_id)
            self._document_access_times.pop(document_id, None)
            self._page_index.pop(document_id, None)
            
            # Update statistics
            self._stats.total_documents -= 1
            self._stats.total_pages -= len(catalog.pages)
            self._stats.memory_usage_bytes -= self._estimate_document_size(catalog)
    
    def _estimate_document_size(self, catalog: DocumentCatalogEntry) -> int:
        """
        Estimate memory usage of a document catalog entry.
        
        Args:
            catalog: Document catalog to estimate
            
        Returns:
            Estimated size in bytes
        """
        # Rough estimation based on string lengths and object overhead
        base_size = 1000  # Base object overhead
        
        # Document metadata
        base_size += len(catalog.document_id) * 2
        base_size += len(catalog.document_name) * 2
        base_size += len(catalog.processing_timestamp) * 2
        
        # Pages
        for page in catalog.pages:
            base_size += 500  # Page object overhead
            base_size += len(page.page_title) * 2
            base_size += len(page.page_type) * 2
            base_size += len(page.page_subtype) * 2
            
            # Text regions
            for region in page.text_regions:
                base_size += 200  # Region overhead
                base_size += len(region.text) * 2
                base_size += len(region.region_id) * 2
            
            # Extracted values (rough estimate)
            base_size += len(str(page.extracted_values)) * 2
        
        return base_size
    
    def __len__(self) -> int:
        """Return number of cached documents."""
        with self._lock:
            return len(self._documents)
    
    def __contains__(self, document_id: str) -> bool:
        """Check if document is in cache."""
        return self.contains_document(document_id)
    
    def __repr__(self) -> str:
        """String representation of cache."""
        with self._lock:
            return (f"CatalogCache(documents={len(self._documents)}, "
                   f"memory_mb={self.get_memory_usage_mb():.1f}, "
                   f"hit_rate={self._stats.hit_rate:.1f}%)")