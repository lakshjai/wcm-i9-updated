#!/usr/bin/env python3
"""
Document catalog orchestration layer for comprehensive document analysis.

This module provides the DocumentCatalog class that coordinates end-to-end
document processing, including page analysis, metadata collection, and
processing summary generation.
"""

import os
import time
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..api.gemini_client import GeminiClient
from ..core.pdf_processor import PDFProcessor
from ..utils.logging_config import logger
from .models import (
    DocumentCatalogEntry, DocumentMetadata, DocumentClassification,
    ProcessingSummary, PageAnalysis
)
from .page_analyzer import PageAnalyzer
from .cache import CatalogCache
from .error_handler import CatalogErrorHandler
from .logging import get_catalog_logger


class DocumentCatalog:
    """
    Orchestration layer for comprehensive document analysis and cataloging.
    
    This class coordinates the analysis of entire PDF documents, managing
    page-by-page analysis, metadata collection, and processing summaries.
    """
    
    def __init__(self, gemini_client: GeminiClient, catalog_cache: Optional[CatalogCache] = None,
                 error_handler: Optional[CatalogErrorHandler] = None, 
                 catalog_output_dir: Optional[str] = None):
        """
        Initialize the DocumentCatalog.
        
        Args:
            gemini_client (GeminiClient): Configured Gemini API client
            catalog_cache (CatalogCache, optional): Cache for storing catalog entries
            error_handler (CatalogErrorHandler, optional): Error handler for recovery
            catalog_output_dir (str, optional): Directory to write individual catalog files
        """
        self.gemini_client = gemini_client
        self.error_handler = error_handler or CatalogErrorHandler()
        self.page_analyzer = PageAnalyzer(gemini_client, self.error_handler)
        self.catalog_cache = catalog_cache or CatalogCache()
        self.catalog_output_dir = catalog_output_dir
        self.catalog_logger = get_catalog_logger()
        
        # Processing statistics
        self.total_documents_processed = 0
        self.total_pages_analyzed = 0
        self.total_api_calls = 0
        self.total_processing_time = 0.0
        
    def analyze_document(self, pdf_path: str, document_name: Optional[str] = None) -> DocumentCatalogEntry:
        """
        Perform comprehensive analysis of a PDF document.
        
        Args:
            pdf_path (str): Path to the PDF file
            document_name (str, optional): Custom name for the document
            
        Returns:
            DocumentCatalogEntry: Complete catalog entry for the document
        """
        # Generate document ID and name
        document_id = self._generate_document_id(pdf_path)
        if document_name is None:
            document_name = Path(pdf_path).name
        
        with self.catalog_logger.operation_context(
            "document_catalog",
            document_id=document_id,
            pdf_path=pdf_path,
            document_name=document_name
        ) as correlation_id:
            
            try:
                # Check cache first
                if self.catalog_cache.contains_document(document_id):
                    self.catalog_logger.log_cache_operation(
                        correlation_id, "get", hit=True, 
                        cache_size=self.catalog_cache.get_cache_size()
                    )
                    logger.info(f"Using cached catalog for document: {document_name}")
                    cached_entry = self.catalog_cache.get_document_catalog(document_id)
                    if cached_entry:
                        return cached_entry
                
                # Log cache miss
                self.catalog_logger.log_cache_operation(
                    correlation_id, "get", hit=False,
                    cache_size=self.catalog_cache.get_cache_size()
                )
                
                logger.info(f"Starting comprehensive analysis of document: {document_name}")
                
                # Collect document metadata
                document_metadata = self._collect_document_metadata(pdf_path)
                
                # Get page count
                total_pages = PDFProcessor.get_pdf_page_count(pdf_path)
                if total_pages == 0:
                    raise ValueError(f"Could not determine page count for PDF: {pdf_path}")
                
                # Log document metadata
                self.catalog_logger.log_performance_metrics(
                    correlation_id,
                    {
                        "total_pages": total_pages,
                        "file_size_mb": document_metadata.file_size / (1024 * 1024),
                        "document_name": document_name
                    }
                )
                
                # Initialize processing summary
                processing_summary = ProcessingSummary(
                    total_pages_analyzed=0,
                    api_calls_made=0,
                    processing_time_seconds=0.0
                )
                
                # Analyze all pages using batch processing
                pages = []
                api_calls_before = self.gemini_client.call_counter
                
                # Process pages in batches for improved performance
                from ..config import settings
                batch_size = settings.CATALOG_BATCH_SIZE
                
                for batch_start in range(1, total_pages + 1, batch_size):
                    batch_end = min(batch_start + batch_size - 1, total_pages)
                    batch_pages = list(range(batch_start, batch_end + 1))
                    
                    logger.debug(f"Analyzing pages {batch_start}-{batch_end}/{total_pages} (batch of {len(batch_pages)})")
                    
                    try:
                        # Check for memory pressure before processing each batch
                        current_memory = self.catalog_cache.get_memory_usage_mb()
                        if current_memory > self.error_handler.memory_threshold_mb:
                            logger.warning(f"Memory pressure detected: {current_memory:.1f}MB")
                            if not self.error_handler.handle_memory_pressure(self.catalog_cache, current_memory):
                                logger.error("Failed to resolve memory pressure, continuing with reduced caching")
                        
                        # Prepare batch data (page_num, text, image_base64)
                        batch_data = []
                        for page_num in batch_pages:
                            batch_data.append((page_num, None, None))  # Let analyzer extract text/image
                        
                        # Process batch
                        batch_analyses = self.page_analyzer.analyze_pages_batch(pdf_path, batch_data)
                        
                        # Process results
                        for page_num, page_analysis in zip(batch_pages, batch_analyses):
                            if page_analysis is None:
                                logger.warning(f"Page {page_num} analysis returned None, skipping")
                                processing_summary.error_count += 1
                                processing_summary.errors.append(f"Page {page_num}: Analysis returned None")
                                continue
                            
                            pages.append(page_analysis)
                            processing_summary.total_pages_analyzed += 1
                            
                            # Update confidence statistics
                            if page_analysis.confidence_score >= 0.8:
                                processing_summary.high_confidence_pages += 1
                            elif page_analysis.confidence_score < 0.7:
                                processing_summary.low_confidence_pages += 1
                        
                    except Exception as e:
                        logger.error(f"Error analyzing batch {batch_start}-{batch_end}: {e}")
                        
                        # Fallback to individual page analysis for this batch
                        logger.info(f"Falling back to individual analysis for pages {batch_start}-{batch_end}")
                        for page_num in batch_pages:
                            try:
                                page_analysis = self.page_analyzer.analyze_page(pdf_path, page_num)
                                
                                if page_analysis is None:
                                    logger.warning(f"Page {page_num} analysis returned None, skipping")
                                    processing_summary.error_count += 1
                                    processing_summary.errors.append(f"Page {page_num}: Analysis returned None")
                                    continue
                                
                                pages.append(page_analysis)
                                processing_summary.total_pages_analyzed += 1
                                
                                # Update confidence statistics
                                if page_analysis.confidence_score >= 0.8:
                                    processing_summary.high_confidence_pages += 1
                                elif page_analysis.confidence_score < 0.7:
                                    processing_summary.low_confidence_pages += 1
                                    
                            except Exception as page_error:
                                logger.error(f"Error analyzing page {page_num}: {page_error}")
                                processing_summary.error_count += 1
                                processing_summary.errors.append(f"Page {page_num}: {str(page_error)}")
                                continue
                
                # Calculate API calls made for this document
                api_calls_after = self.gemini_client.call_counter
                processing_summary.api_calls_made = api_calls_after - api_calls_before
                
                # Generate document classification
                document_classification = self._classify_document(pages)
                
                # Determine if manual review is required
                processing_summary.manual_review_required = (
                    processing_summary.low_confidence_pages > 0 or
                    processing_summary.error_count > 0 or
                    document_classification.i9_form_count > 1
                )
                
                # Log validation results
                self.catalog_logger.log_validation_result(
                    correlation_id,
                    "document_quality_check",
                    passed=not processing_summary.manual_review_required,
                    details={
                        "high_confidence_pages": processing_summary.high_confidence_pages,
                        "low_confidence_pages": processing_summary.low_confidence_pages,
                        "error_count": processing_summary.error_count,
                        "i9_forms_found": document_classification.i9_form_count
                    }
                )
                
                # Create catalog entry
                catalog_entry = DocumentCatalogEntry(
                    document_id=document_id,
                    document_name=document_name,
                    total_pages=total_pages,
                    processing_timestamp=datetime.now().isoformat(),
                    document_metadata=document_metadata,
                    pages=pages,
                    document_classification=document_classification,
                    processing_summary=processing_summary
                )
                
                # Store in cache with error handling
                try:
                    self.catalog_cache.store_document_catalog(document_id, catalog_entry)
                    self.catalog_logger.log_cache_operation(
                        correlation_id, "set", hit=True,
                        cache_size=self.catalog_cache.get_cache_size()
                    )
                except Exception as cache_error:
                    self.error_handler.handle_cache_error("store_document", document_id, cache_error)
                
                # Update global statistics
                self.total_documents_processed += 1
                self.total_pages_analyzed += len(pages)
                self.total_api_calls += processing_summary.api_calls_made
                self.total_processing_time += processing_summary.processing_time_seconds
                
                # Log final performance metrics
                self.catalog_logger.log_performance_metrics(
                    correlation_id,
                    {
                        "pages_analyzed": len(pages),
                        "api_calls_made": processing_summary.api_calls_made,
                        "processing_time_seconds": processing_summary.processing_time_seconds,
                        "avg_confidence": sum(p.confidence_score for p in pages) / len(pages) if pages else 0.0,
                        "cache_size_after": self.catalog_cache.get_cache_size()
                    }
                )
                
                logger.info(f"Completed analysis of {document_name}: "
                           f"{len(pages)} pages, {processing_summary.api_calls_made} API calls, "
                           f"{processing_summary.processing_time_seconds:.2f}s")
                
                # Write individual catalog file if output directory is specified
                if self.catalog_output_dir:
                    self._write_individual_catalog_file(catalog_entry, pdf_path)
                
                return catalog_entry
                
            except Exception as e:
                logger.error(f"Error analyzing document {pdf_path}: {e}")
                # Return minimal catalog entry with error information
                return self._create_error_catalog_entry(pdf_path, document_name, str(e))
    
    def analyze_documents_batch(self, pdf_paths: List[str], max_workers: int = 2) -> Dict[str, DocumentCatalogEntry]:
        """
        Analyze multiple documents concurrently.
        
        Args:
            pdf_paths (List[str]): List of PDF file paths to analyze
            max_workers (int): Maximum number of concurrent workers
            
        Returns:
            Dict[str, DocumentCatalogEntry]: Mapping of document IDs to catalog entries
        """
        logger.info(f"Starting batch analysis of {len(pdf_paths)} documents with {max_workers} workers")
        
        results = {}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_path = {
                executor.submit(self.analyze_document, pdf_path): pdf_path
                for pdf_path in pdf_paths
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_path):
                pdf_path = future_to_path[future]
                try:
                    catalog_entry = future.result()
                    results[catalog_entry.document_id] = catalog_entry
                    logger.info(f"Completed analysis of {Path(pdf_path).name}")
                except Exception as e:
                    logger.error(f"Error in batch analysis of {pdf_path}: {e}")
                    # Create error entry
                    error_entry = self._create_error_catalog_entry(pdf_path, None, str(e))
                    results[error_entry.document_id] = error_entry
        
        logger.info(f"Batch analysis completed: {len(results)} documents processed")
        return results
    
    def get_document_catalog(self, document_id: str) -> Optional[DocumentCatalogEntry]:
        """
        Retrieve a document catalog from cache.
        
        Args:
            document_id (str): Unique document identifier
            
        Returns:
            DocumentCatalogEntry: Catalog entry if found, None otherwise
        """
        return self.catalog_cache.get_document_catalog(document_id)
    
    def get_page_analysis(self, document_id: str, page_number: int) -> Optional[PageAnalysis]:
        """
        Retrieve analysis for a specific page.
        
        Args:
            document_id (str): Unique document identifier
            page_number (int): Page number (1-indexed)
            
        Returns:
            PageAnalysis: Page analysis if found, None otherwise
        """
        return self.catalog_cache.get_page_analysis(document_id, page_number)
    
    def get_pages_by_type(self, document_id: str, page_type: str) -> List[PageAnalysis]:
        """
        Get all pages of a specific type from a document.
        
        Args:
            document_id (str): Unique document identifier
            page_type (str): Type of pages to retrieve
            
        Returns:
            List[PageAnalysis]: List of matching page analyses
        """
        return self.catalog_cache.get_pages_by_type(document_id, page_type)
    
    def export_catalog(self, output_path: str, document_ids: Optional[List[str]] = None, 
                      include_pii: bool = False, export_format: str = 'json',
                      compression: bool = False) -> bool:
        """
        Export catalog data to file with enhanced functionality.
        
        Args:
            output_path (str): Path for the output file
            document_ids (List[str], optional): Specific document IDs to export
            include_pii (bool): Whether to include PII in export
            export_format (str): Export format ('json', 'jsonl')
            compression (bool): Whether to compress the output
            
        Returns:
            bool: True if export successful, False otherwise
        """
        try:
            from .export import CatalogExporter
            
            exporter = CatalogExporter()
            return exporter.export_from_cache(
                self.catalog_cache, output_path, document_ids, 
                include_pii, export_format, compression
            )
            
        except Exception as e:
            import traceback
            logger.error(f"Error exporting catalog to {output_path}: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    def import_catalog(self, input_path: str) -> Dict[str, Any]:
        """
        Import catalog data from file with enhanced functionality.
        
        Args:
            input_path (str): Path to the input file
            
        Returns:
            Dict[str, Any]: Import results with statistics and error information
        """
        try:
            from .export import CatalogImporter
            
            importer = CatalogImporter()
            result = importer.import_catalog_file(input_path, self.catalog_cache)
            
            if result['success']:
                logger.info(f"Imported catalog data from {input_path}: {result['imported_count']} documents")
            else:
                logger.error(f"Import failed for {input_path}: {result.get('error', 'Unknown error')}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error importing catalog from {input_path}: {e}")
            return {
                'success': False,
                'error': str(e),
                'imported_count': 0,
                'failed_count': 0,
                'import_metadata': {}
            }
    
    def get_processing_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive processing statistics.
        
        Returns:
            Dict[str, Any]: Processing statistics and metrics
        """
        cache_stats = self.catalog_cache.get_statistics()
        
        return {
            "documents_processed": self.total_documents_processed,
            "pages_analyzed": self.total_pages_analyzed,
            "api_calls_made": self.total_api_calls,
            "total_processing_time_seconds": self.total_processing_time,
            "average_time_per_document": (
                self.total_processing_time / self.total_documents_processed
                if self.total_documents_processed > 0 else 0.0
            ),
            "average_time_per_page": (
                self.total_processing_time / self.total_pages_analyzed
                if self.total_pages_analyzed > 0 else 0.0
            ),
            "cache_statistics": cache_stats.to_dict(),
            "token_usage": self.gemini_client.get_token_usage()
        }
    
    def clear_cache(self) -> None:
        """Clear all cached catalog data."""
        self.catalog_cache.clear_cache()
        self.page_analyzer.clear_cache()
        logger.info("Cleared all catalog caches")
    
    def _generate_document_id(self, pdf_path: str) -> str:
        """
        Generate a unique document ID based on file path and metadata.
        
        Args:
            pdf_path (str): Path to the PDF file
            
        Returns:
            str: Unique document identifier
        """
        # Use file path and modification time for uniqueness
        try:
            stat = os.stat(pdf_path)
            content = f"{pdf_path}_{stat.st_size}_{stat.st_mtime}"
            return hashlib.md5(content.encode()).hexdigest()
        except Exception:
            # Fallback to just the file path
            return hashlib.md5(pdf_path.encode()).hexdigest()
    
    def _collect_document_metadata(self, pdf_path: str) -> DocumentMetadata:
        """
        Collect metadata about the document file.
        
        Args:
            pdf_path (str): Path to the PDF file
            
        Returns:
            DocumentMetadata: Document metadata
        """
        try:
            stat = os.stat(pdf_path)
            
            return DocumentMetadata(
                file_size=stat.st_size,
                creation_date=datetime.fromtimestamp(stat.st_ctime),
                modification_date=datetime.fromtimestamp(stat.st_mtime),
                file_path=pdf_path
            )
        except Exception as e:
            logger.warning(f"Error collecting document metadata for {pdf_path}: {e}")
            return DocumentMetadata(
                file_size=0,
                file_path=pdf_path
            )
    
    def _classify_document(self, pages: List[PageAnalysis]) -> DocumentClassification:
        """
        Generate document-level classification based on page analyses.
        
        Args:
            pages (List[PageAnalysis]): List of page analyses
            
        Returns:
            DocumentClassification: Document classification
        """
        # Filter out None pages
        valid_pages = [p for p in pages if p is not None]
        
        # Count page types
        government_forms = sum(1 for p in valid_pages if p.page_type == "government_form")
        identity_documents = sum(1 for p in valid_pages if p.page_type == "identity_document")
        employment_records = sum(1 for p in valid_pages if p.page_type == "employment_record")
        
        # Count I-9 forms and find latest
        i9_pages = [p for p in valid_pages if p.page_subtype == "i9_form"]
        i9_form_count = len(i9_pages)
        
        # Find latest I-9 form (highest page number)
        latest_i9_page = None
        if i9_pages:
            latest_i9_page = max(i9_pages, key=lambda p: p.page_number).page_number
        
        # CRITICAL FIX: Check for I-9 forms in page subtypes even if page_type is not "government_form"
        # This handles cases where I-9 forms might be classified with different page_type but correct subtype
        has_i9_forms = i9_form_count > 0
        
        # Also check for other government form subtypes that should count as government forms
        government_form_subtypes = {"i9_form", "i9_document_list", "i9_instructions", "tax_form", "visa", "passport"}
        has_government_subtypes = any(p.page_subtype in government_form_subtypes for p in valid_pages)
        
        # Update government forms count to include I-9 forms found by subtype
        contains_government_forms = government_forms > 0 or has_i9_forms or has_government_subtypes
        
        # Determine primary document type with corrected logic
        if contains_government_forms:
            primary_type = "government_forms"
        elif employment_records > 0:
            primary_type = "employment_records"
        elif identity_documents > 0:
            primary_type = "identity_documents"
        else:
            primary_type = "mixed_documents"
        
        # Log the classification decision for debugging
        logger.debug(f"Document classification: government_forms={government_forms}, "
                    f"i9_forms={i9_form_count}, has_government_subtypes={has_government_subtypes}, "
                    f"contains_government_forms={contains_government_forms}, primary_type={primary_type}")
        
        return DocumentClassification(
            primary_document_type=primary_type,
            contains_government_forms=contains_government_forms,
            contains_identity_documents=identity_documents > 0,
            contains_employment_records=employment_records > 0,
            i9_form_count=i9_form_count,
            latest_i9_page=latest_i9_page
        )
    
    def _create_error_catalog_entry(self, pdf_path: str, document_name: Optional[str], 
                                   error_msg: str) -> DocumentCatalogEntry:
        """
        Create a minimal catalog entry for failed document analysis.
        
        Args:
            pdf_path (str): Path to the PDF file
            document_name (str, optional): Document name
            error_msg (str): Error message
            
        Returns:
            DocumentCatalogEntry: Error catalog entry
        """
        document_id = self._generate_document_id(pdf_path)
        if document_name is None:
            document_name = Path(pdf_path).name
        
        document_metadata = DocumentMetadata(
            file_size=0,
            file_path=pdf_path
        )
        
        processing_summary = ProcessingSummary(
            total_pages_analyzed=0,
            api_calls_made=0,
            processing_time_seconds=0.0,
            error_count=1,
            errors=[error_msg],
            manual_review_required=True
        )
        
        document_classification = DocumentClassification(
            primary_document_type="error"
        )
        
        return DocumentCatalogEntry(
            document_id=document_id,
            document_name=document_name,
            total_pages=0,
            processing_timestamp=datetime.now().isoformat(),
            document_metadata=document_metadata,
            pages=[],
            document_classification=document_classification,
            processing_summary=processing_summary
        )
    
    def _write_individual_catalog_file(self, catalog_entry: DocumentCatalogEntry, original_pdf_path: str) -> bool:
        """
        Write an individual catalog file for a processed document.
        
        Args:
            catalog_entry (DocumentCatalogEntry): The catalog entry to write
            original_pdf_path (str): Path to the original PDF file
            
        Returns:
            bool: True if file written successfully, False otherwise
        """
        try:
            import json
            
            # Generate catalog filename: original_filename.catalog.txt
            original_filename = Path(original_pdf_path).stem  # filename without extension
            catalog_filename = f"{original_filename}.catalog.txt"
            catalog_file_path = os.path.join(self.catalog_output_dir, catalog_filename)
            
            # Also create JSON version
            json_filename = f"{original_filename}.catalog.json"
            json_file_path = os.path.join(self.catalog_output_dir, json_filename)
            
            # Ensure output directory exists
            os.makedirs(self.catalog_output_dir, exist_ok=True)
            
            # Write human-readable text version
            with open(catalog_file_path, 'w', encoding='utf-8') as f:
                f.write(f"DOCUMENT CATALOG\n")
                f.write(f"=" * 50 + "\n\n")
                f.write(f"Original File: {Path(original_pdf_path).name}\n")
                f.write(f"Document ID: {catalog_entry.document_id}\n")
                f.write(f"Total Pages: {catalog_entry.total_pages}\n")
                f.write(f"Processing Date: {catalog_entry.processing_timestamp}\n")
                f.write(f"Primary Type: {catalog_entry.document_classification.primary_document_type}\n")
                f.write(f"I-9 Forms Found: {catalog_entry.document_classification.i9_form_count}\n")
                f.write(f"Manual Review Required: {catalog_entry.processing_summary.manual_review_required}\n\n")
                
                f.write(f"PROCESSING SUMMARY\n")
                f.write(f"-" * 30 + "\n")
                f.write(f"Pages Analyzed: {catalog_entry.processing_summary.total_pages_analyzed}\n")
                f.write(f"API Calls Made: {catalog_entry.processing_summary.api_calls_made}\n")
                f.write(f"Processing Time: {catalog_entry.processing_summary.processing_time_seconds:.2f}s\n")
                f.write(f"High Confidence Pages: {catalog_entry.processing_summary.high_confidence_pages}\n")
                f.write(f"Low Confidence Pages: {catalog_entry.processing_summary.low_confidence_pages}\n")
                f.write(f"Errors: {catalog_entry.processing_summary.error_count}\n\n")
                
                f.write(f"PAGE ANALYSIS\n")
                f.write(f"-" * 30 + "\n")
                for page in catalog_entry.pages:
                    if page is not None:
                        f.write(f"Page {page.page_number}: {page.page_type}/{page.page_subtype} ")
                        f.write(f"(confidence: {page.confidence_score:.2f})\n")
                        f.write(f"  Title: {page.page_title}\n")
                        if page.extracted_values:
                            f.write(f"  Key Data: {len(page.extracted_values)} fields extracted\n")
                        f.write(f"\n")
                
                if catalog_entry.processing_summary.errors:
                    f.write(f"ERRORS\n")
                    f.write(f"-" * 30 + "\n")
                    for error in catalog_entry.processing_summary.errors:
                        f.write(f"  {error}\n")
            
            # Prepare catalog data for JSON version
            catalog_data = {
                "catalog_version": "1.0",
                "original_file": original_pdf_path,
                "original_filename": Path(original_pdf_path).name,
                "catalog_generated_timestamp": datetime.now().isoformat(),
                "document_catalog": catalog_entry.to_dict()
            }
            
            # Write JSON version
            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(catalog_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Written individual catalog files: {catalog_file_path} and {json_file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error writing individual catalog file for {original_pdf_path}: {e}")
            return False