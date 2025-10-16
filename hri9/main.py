#!/usr/bin/env python3
"""
Main entry point for the I-9 processing system.

This module provides data extraction and cataloging functionality for I-9 documents
without PDF manipulation. Results are categorized into SUCCESS, PARTIAL_SUCCESS, and ERROR files.
"""

import os
import sys
import time
import concurrent.futures
import shutil
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Add the parent directory to the path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from .config import settings
    from .utils.logging_config import logger
    from .utils.categorized_reporter import CategorizedReporter
    from .utils.enhanced_csv_reporter import EnhancedCSVReporter
    from .core.enhanced_processor import EnhancedI9Processor
    from .api.gemini_client import GeminiClient
    from .catalog.cache import CatalogCache
    from .data.file_manager import FileManager
    from .cli.arguments import parse_arguments
except ImportError:
    # Fallback to absolute imports when run directly
    from hri9.config import settings
    from hri9.utils.logging_config import logger
    from hri9.utils.categorized_reporter import CategorizedReporter
    from hri9.utils.enhanced_csv_reporter import EnhancedCSVReporter
    from hri9.utils.concurrency import SharedResources
    from hri9.utils.reporting import Reporter
    from hri9.utils.enhanced_reporting import EnhancedReporter
    from hri9.utils.file_filter import FileFilter
    from hri9.core.i9_detector import I9Detector
    from hri9.core.pdf_manipulator import PDFManipulator
    from hri9.core.enhanced_processor import EnhancedI9Processor
    from hri9.data.excel_reader import ExcelReader
    from hri9.data.file_manager import FileManager
    from hri9.cli.arguments import parse_arguments
    from hri9.api.gemini_client import GeminiClient
    from hri9.utils.reporting import Reporter
    from hri9.utils.enhanced_reporting import EnhancedReporter
    from hri9.utils.file_filter import FileFilter
    from hri9.core.i9_detector import I9Detector
    from hri9.core.pdf_manipulator import PDFManipulator
    from hri9.core.enhanced_processor import EnhancedI9Processor, ProcessingPipeline
    from hri9.data.excel_reader import ExcelReader
    from hri9.data.file_manager import FileManager
    from hri9.cli.arguments import parse_arguments
    from hri9.api.gemini_client import GeminiClient
    from hri9.catalog.document_catalog import DocumentCatalog
    from hri9.catalog.cache import CatalogCache

def process_employee_document_enhanced(employee_id, shared_resources, use_local=False, mode='all', 
                                      enhanced_processor=None, extract_all_data=False, 
                                      skip_existing_catalog=True, catalog_output_dir=None):
    """
    Process a single employee document using the enhanced processor with business rules.
    
    Args:
        employee_id: Employee ID
        shared_resources: SharedResources instance for thread-safe operations
        use_local: Whether to use local sample data instead of network drive
        mode: Processing mode ('detect', 'remove', 'extract', or 'all')
        enhanced_processor: EnhancedI9Processor instance for advanced processing
        extract_all_data: Whether to extract data from all pages, not just I-9 forms
        skip_existing_catalog: Whether to skip catalog generation if files already exist
        catalog_output_dir: Directory for individual catalog files
    """
    try:
        # Get PDF path
        pdf_path = None
        if use_local:
            pdf_path = FileManager.get_pdf_from_local_sample(employee_id)
        else:
            pdf_path = FileManager.get_pdf_from_network_drive(employee_id)
        
        if not pdf_path:
            logger.warning(f"No PDF found for employee {employee_id}")
            shared_resources.write_csv_row([employee_id, "No PDF found", "No", 0, "N/A", ""])
            shared_resources.update_progress()
            return
        
        # Check if this PDF has already been processed
        if shared_resources.is_pdf_processed(pdf_path):
            logger.info(f"PDF already processed: {pdf_path}")
            return
        
        # Mark this PDF as processed
        shared_resources.mark_pdf_processed(pdf_path)
        
        # Extract employee name from folder path
        employee_folder = os.path.dirname(pdf_path)
        employee_name = os.path.basename(employee_folder)
        
        # Use enhanced processor if available
        if enhanced_processor:
            logger.info(f"Processing {employee_name} with enhanced business rules processor")
            
            from pathlib import Path
            processing_result = enhanced_processor.process_pdf(Path(pdf_path), employee_name)
            
            # Convert processing result to legacy format for compatibility
            success = "Yes" if processing_result.status.value == "COMPLETE_SUCCESS" else "Partial" if "PARTIAL" in processing_result.status.value else "No"
            
            # Extract I-9 forms if requested and forms were found
            extracted_i9_path = ""
            pages_removed = 0
            
            if processing_result.primary_i9_data and mode in ['extract', 'all']:
                # Create filenames for output
                pdf_filename = os.path.basename(pdf_path)
                pdf_name_without_ext = os.path.splitext(pdf_filename)[0]
                
                # Path for extracted I-9 forms
                extracted_i9_filename = f"{pdf_name_without_ext}_i9_forms.pdf"
                extracted_i9_path = os.path.join(settings.I9_EXTRACT_DIR, extracted_i9_filename)
                
                # Skip PDF manipulation - focus on data extraction only
                # Note: PDF extraction and page removal disabled for this project
                extracted_i9_path = "N/A - PDF manipulation disabled"
            
            # Prepare enhanced CSV data with business rules results
            base_data = {
                'employee_id': employee_id,
                'pdf_file_name': os.path.basename(pdf_path),
                'i9_forms_found': "Yes" if processing_result.primary_i9_data else "No",
                'pages_removed': pages_removed,
                'success': success,
                'extracted_i9_path': extracted_i9_path,
                'input_file_path': pdf_path,
                'processed_file_path': "",
                'business_rules_status': processing_result.status.value,
                'validation_success_rate': f"{processing_result.validation_success_rate:.1f}%",
                'critical_issues': processing_result.critical_issues,
                'form_type': processing_result.form_type_selected,
                'selection_reason': processing_result.selection_reason,
                'notes': processing_result.notes
            }
            
            # Get catalog data from enhanced processor
            catalog_entry = None
            catalog_files = {}
            
            if hasattr(processing_result, 'catalog_entry') and processing_result.catalog_entry:
                catalog_entry = processing_result.catalog_entry
            elif hasattr(enhanced_processor, 'catalog'):
                # Try to get catalog entry from the processor's cache
                doc_cache = enhanced_processor.catalog.catalog_cache
                cached_ids = list(doc_cache.get_cached_document_ids())
                if cached_ids:
                    catalog_entry = doc_cache.get_document_catalog(cached_ids[-1])  # Get latest
            
            # Add business rules data to base_data
            base_data.update({
                'total_validations': processing_result.total_validations,
                'passed_validations': processing_result.passed_validations,
                'failed_validations': processing_result.failed_validations,
                'scenario_count': len(processing_result.scenario_results),
                'primary_scenario': processing_result.scenario_results[0].scenario_name if processing_result.scenario_results else "None"
            })
            
            # Generate and save business rules report
            try:
                from hri9.utils.business_rules_reporter import BusinessRulesReporter
                
                # Generate comprehensive business rules report
                business_report = BusinessRulesReporter.generate_business_rules_report(
                    processing_result, employee_id, os.path.basename(pdf_path)
                )
                
                # Save report in both JSON and text formats
                json_path = BusinessRulesReporter.save_business_rules_report(
                    business_report, employee_id, "json"
                )
                text_path = BusinessRulesReporter.save_business_rules_report(
                    business_report, employee_id, "txt"
                )
                
                if json_path:
                    logger.info(f"Business rules report saved: {json_path}")
                if text_path:
                    logger.info(f"Business rules text report saved: {text_path}")
                    
            except Exception as e:
                logger.error(f"Failed to generate business rules report: {e}")
            
            # Write enhanced CSV row
            if hasattr(shared_resources, 'write_enhanced_csv_row'):
                shared_resources.write_enhanced_csv_row(base_data, catalog_entry, catalog_files)
            else:
                # Fallback to original CSV format
                shared_resources.write_csv_row([
                    employee_id, os.path.basename(pdf_path), 
                    "Yes" if processing_result.primary_i9_data else "No", 
                    pages_removed, success, extracted_i9_path
                ])
            
            # Update progress
            i9_found = processing_result.primary_i9_data is not None
            shared_resources.update_progress(
                found_i9=i9_found, 
                removed_i9=(pages_removed > 0),
                extracted_i9=(extracted_i9_path != "")
            )
            
        else:
            # Fall back to original processing logic
            logger.info(f"Processing {employee_name} with original logic (enhanced processor not available)")
            # Call original function logic here...
            pass
            
    except Exception as e:
        logger.error(f"Error processing employee {employee_id}: {e}")
        shared_resources.write_csv_row([employee_id, "Error", "Error", 0, f"Error: {str(e)}", ""])
        shared_resources.update_progress()


def process_employee_document(employee_id, shared_resources, use_local=False, mode='all', 
                            document_catalog=None, extract_all_data=False, 
                            skip_existing_catalog=True, catalog_output_dir=None):
    """
    Process a single employee document for I9 detection, extraction, and/or removal.
    
    Args:
        employee_id: Employee ID
        shared_resources: SharedResources instance for thread-safe operations
        use_local: Whether to use local sample data instead of network drive
        mode: Processing mode ('detect', 'remove', 'extract', or 'all')
        document_catalog: DocumentCatalog instance for catalog generation
    """
    try:
        # Get PDF path
        pdf_path = None
        if use_local:
            pdf_path = FileManager.get_pdf_from_local_sample(employee_id)
        else:
            pdf_path = FileManager.get_pdf_from_network_drive(employee_id)
        
        if not pdf_path:
            logger.warning(f"No PDF found for employee {employee_id}")
            shared_resources.write_csv_row([employee_id, "No PDF found", "No", 0, "N/A", ""])
            shared_resources.update_progress()
            return
        
        # Check if this PDF has already been processed
        if shared_resources.is_pdf_processed(pdf_path):
            logger.info(f"PDF already processed: {pdf_path}")
            return
        
        # Mark this PDF as processed
        shared_resources.mark_pdf_processed(pdf_path)
        
        # Extract employee name from folder path
        employee_folder = os.path.dirname(pdf_path)
        employee_name = os.path.basename(employee_folder)
        
        # Check for existing catalog files if skip_existing_catalog is enabled
        catalog_files = {'text_path': None, 'json_path': None}
        should_generate_catalog = document_catalog is not None
        
        if should_generate_catalog and skip_existing_catalog and catalog_output_dir:
            existing_catalog = FileFilter.check_existing_catalog_files(
                employee_id, catalog_output_dir, use_local
            )
            if existing_catalog['exists']:
                logger.info(f"Existing catalog found for {employee_name}, skipping catalog generation")
                should_generate_catalog = False
                catalog_files = {
                    'text_path': existing_catalog.get('text_path'),
                    'json_path': existing_catalog.get('json_path')
                }
        
        # Generate document catalog if needed
        document_id = None
        catalog_entry = None
        if should_generate_catalog:
            try:
                logger.info(f"Generating catalog for {pdf_path}")
                catalog_entry = document_catalog.analyze_document(pdf_path, employee_name)
                document_id = catalog_entry.document_id
                logger.info(f"Catalog generated for {employee_name} (ID: {document_id})")
                
                # Get catalog file paths
                if catalog_output_dir:
                    catalog_file_paths = FileFilter.get_catalog_file_paths(
                        employee_id, catalog_output_dir, use_local
                    )
                    catalog_files = {
                        'text_path': catalog_file_paths.get('text_path'),
                        'json_path': catalog_file_paths.get('json_path')
                    }
                    
            except Exception as e:
                logger.warning(f"Failed to generate catalog for {pdf_path}: {e}")

        
        # Initialize I9 detector and use catalog data directly (no cache needed)
        i9_detector = I9Detector()
        
        # Detect pages based on extract_all_data setting
        if extract_all_data:
            # Extract all pages when extract_all_data is True
            logger.info(f"Extract all data mode: will extract all pages from document")
            doc = fitz.open(pdf_path)
            total_pages = len(doc)
            doc.close()
            all_i9_pages = list(range(1, total_pages + 1))  # All pages
            latest_i9_pages = all_i9_pages  # All pages are considered "latest"
            i9_found = total_pages > 0
        else:
            # Normal I-9 detection
            if catalog_entry:
                logger.info(f"Using catalog data directly for I-9 detection")
                all_i9_pages, latest_i9_pages = i9_detector.detect_i9_pages_from_catalog_entry(catalog_entry)
            else:
                logger.info(f"No catalog data available, using original detection method")
                all_i9_pages, latest_i9_pages = i9_detector._detect_all_i9_pages_original(pdf_path)
            i9_found = len(all_i9_pages) > 0
        
        # Log detection results
        if extract_all_data:
            if i9_found:
                logger.info(f"Extract all data mode: will process all {len(all_i9_pages)} pages from {pdf_path}")
        else:
            if i9_found:
                logger.info(f"Found {len(all_i9_pages)} total I-9 pages in {pdf_path}")
                logger.info(f"Identified {len(latest_i9_pages)} pages as the latest I-9 form")
                
                if len(latest_i9_pages) < len(all_i9_pages):
                    logger.info(f"Will extract only the latest I-9 form (pages {latest_i9_pages})")
                    logger.info(f"Will remove all I-9 pages (pages {all_i9_pages})")
        
        
        # Process PDF if I-9 forms found
        success = "No"
        pages_removed = 0
        extracted_i9_path = ""
        cleaned_pdf_path = ""  # Initialize cleaned_pdf_path to avoid UnboundLocalError
        
        if i9_found:
            # Create filenames for output
            pdf_filename = os.path.basename(pdf_path)
            pdf_name_without_ext = os.path.splitext(pdf_filename)[0]
            
            # Path for extracted I-9 forms
            extracted_i9_filename = f"{pdf_name_without_ext}_i9_forms.pdf"
            extracted_i9_path = os.path.join(settings.I9_EXTRACT_DIR, extracted_i9_filename)
            
            # Path for cleaned PDF (with I-9 forms removed)
            cleaned_pdf_path = os.path.join(settings.CLEANED_PDF_DIR, pdf_filename)
            
            extraction_success = False
            removal_success = False
            
            # Extract only the latest I-9 pages if requested
            if mode in ['extract', 'all']:
                # Check if there's an existing extracted file and remove it
                if os.path.exists(extracted_i9_path):
                    try:
                        os.remove(extracted_i9_path)
                        logger.info(f"Removed existing extracted file: {extracted_i9_path}")
                    except Exception as e:
                        logger.error(f"Failed to remove existing extracted file: {e}")
                
                # Extract only the latest I-9 form
                extraction_success = PDFManipulator.extract_pages(pdf_path, latest_i9_pages, extracted_i9_path)
                if extraction_success:
                    logger.info(f"Successfully extracted latest I-9 form to {extracted_i9_path}")
                else:
                    logger.error(f"Failed to extract latest I-9 form to {extracted_i9_path}")
            
            # Remove ALL I-9 pages if requested
            if mode in ['remove', 'all']:
                # Remove all I-9 pages (including old forms and document lists)
                removal_success = PDFManipulator.remove_pages(pdf_path, all_i9_pages, cleaned_pdf_path)
                
                # If removal was successful, write to the deletion CSV file
                if removal_success:
                    from .utils.reporting import Reporter
                    Reporter.write_deletion_record(
                        settings.DELETE_FILE_LIST_CSV,
                        employee_id,
                        employee_name,
                        os.path.abspath(pdf_path)
                    )
                    logger.info(f"Recorded file for deletion in {settings.DELETE_FILE_LIST_CSV}")
                else:
                    logger.error(f"Failed to remove I-9 pages from {pdf_path}")

            
            # Determine overall success
            if extract_all_data:
                # Special handling for extract_all_data mode
                if mode == 'all' and extraction_success and removal_success:
                    success = "Yes - All Data Extracted"
                    pages_removed = len(all_i9_pages)
                elif mode == 'extract' and extraction_success:
                    success = "Yes - All Data Extracted"
                    pages_removed = 0
                elif mode == 'remove' and removal_success:
                    success = "Yes - All Pages Removed"
                    pages_removed = len(all_i9_pages)
                    extracted_i9_path = "N/A"
                elif mode == 'detect':
                    success = "Yes - All Pages Detected"
                    pages_removed = 0
                    extracted_i9_path = "N/A"
                else:
                    success = "Failed - Extract All Data Mode"
            else:
                # Normal I-9 processing
                if mode == 'all' and extraction_success and removal_success:
                    success = "Yes"
                    pages_removed = len(all_i9_pages)
                elif mode == 'extract' and extraction_success:
                    success = "Yes - Extraction Only"
                    pages_removed = 0
                elif mode == 'remove' and removal_success:
                    success = "Yes - Removal Only"
                    pages_removed = len(all_i9_pages)
                    extracted_i9_path = "N/A"
                elif mode == 'detect':
                    success = "Yes - Detection Only"
                    pages_removed = 0
                    extracted_i9_path = "N/A"
                else:
                    if extraction_success:
                        success = "Partial - Extraction Only"
                        pages_removed = 0
                    elif removal_success:
                        success = "Partial - Removal Only"
                        pages_removed = len(all_i9_pages)
                        extracted_i9_path = "Extraction failed"
                    else:
                        success = "Failed"
        
        # Prepare enhanced CSV data
        base_data = {
            'employee_id': employee_id,
            'pdf_file_name': os.path.basename(pdf_path),
            'i9_forms_found': "Yes" if i9_found else "No",
            'pages_removed': pages_removed,
            'success': success,
            'extracted_i9_path': extracted_i9_path,
            'input_file_path': pdf_path,
            'processed_file_path': cleaned_pdf_path if mode in ['remove', 'all'] else ''
        }
        
        # Write enhanced CSV row with catalog data
        if hasattr(shared_resources, 'write_enhanced_csv_row'):
            shared_resources.write_enhanced_csv_row(base_data, catalog_entry, catalog_files)
        else:
            # Fallback to original CSV format
            shared_resources.write_csv_row([employee_id, os.path.basename(pdf_path), 
                                          "Yes" if i9_found else "No", pages_removed, success, extracted_i9_path])
        
        # Update progress
        shared_resources.update_progress(
            found_i9=i9_found, 
            removed_i9=(success == "Yes" or success == "Yes - Removal Only" or success == "Partial - Removal Only"),
            extracted_i9=(success == "Yes" or success == "Yes - Extraction Only" or success == "Partial - Extraction Only")
        )
        
    except Exception as e:
        logger.error(f"Error processing employee {employee_id}: {e}")
        shared_resources.write_csv_row([employee_id, "Error", "Error", 0, f"Error: {str(e)}", ""])
        shared_resources.update_progress()

def generate_catalog_only(employee_id, document_catalog, use_local=False):
    """
    Generate catalog for a single employee document without business logic processing.
    
    Args:
        employee_id: Employee ID
        document_catalog: DocumentCatalog instance for catalog generation
        use_local: Whether to use local sample data instead of network drive
        
    Returns:
        str: Document ID if successful, None if failed
    """
    try:
        # Get PDF path
        pdf_path = None
        if use_local:
            pdf_path = FileManager.get_pdf_from_local_sample(employee_id)
        else:
            pdf_path = FileManager.get_pdf_from_network_drive(employee_id)
        
        if not pdf_path:
            logger.warning(f"No PDF found for employee {employee_id}")
            return None
        
        # Extract employee name from folder path
        employee_folder = os.path.dirname(pdf_path)
        employee_name = os.path.basename(employee_folder)
        
        # Generate catalog
        logger.info(f"Generating catalog for {employee_name}")
        catalog_entry = document_catalog.analyze_document(pdf_path, employee_name)
        
        logger.info(f"Catalog generated for {employee_name}: {len(catalog_entry.pages)} pages analyzed")
        return catalog_entry.document_id
        
    except Exception as e:
        logger.error(f"Error generating catalog for employee {employee_id}: {e}")
        return None

def process_all_employees_concurrent(employee_ids, use_local=False, workers=settings.CONCURRENT_WORKERS, 
                                   limit=settings.MAX_DOCUMENTS, batch_size=settings.BATCH_SIZE, mode='all',
                                   enable_catalog=True, catalog_only=False, catalog_export_path=None,
                                   catalog_export_format='json', catalog_include_pii=False,
                                   catalog_confidence_threshold=0.7, catalog_text_regions=True,
                                   catalog_structured_extraction=True, extract_all_data=False,
                                   skip_existing_catalog=True, debug_files_pattern=None,
                                   use_enhanced_processor=True):
    """
    Process all employees concurrently using a thread pool.
    
    Args:
        employee_ids: List of employee IDs to process
        use_local: Whether to use local sample data instead of network drive
        workers: Number of concurrent workers
        limit: Maximum number of documents to process (0 = all)
        batch_size: Progress reporting interval
        mode: Processing mode ('detect', 'remove', 'extract', or 'all')
        enable_catalog: Whether to enable document catalog system
        catalog_only: Whether to generate catalog only, skip business logic
        catalog_export_path: Path for catalog export files
        catalog_export_format: Export format ('json', 'csv', or 'both')
        catalog_include_pii: Whether to include PII in catalog export
        catalog_confidence_threshold: Minimum confidence score for catalog entries
        catalog_text_regions: Whether to enable text region extraction
        catalog_structured_extraction: Whether to enable structured data extraction
        extract_all_data: Whether to extract data from all pages, not just I-9 forms
        skip_existing_catalog: Whether to skip catalog generation if files already exist
        debug_files_pattern: Pattern to filter files for debug mode
        
    Returns:
        Tuple of (processed_count, found_i9_count, removed_i9_count, extracted_i9_count)
    """
    # Apply debug file pattern filter if specified
    if debug_files_pattern:
        logger.info(f"Debug mode: filtering files by pattern '{debug_files_pattern}'")
        original_count = len(employee_ids)
        employee_ids = FileFilter.filter_employees_by_pattern(
            employee_ids, debug_files_pattern, use_local
        )
        logger.info(f"Debug filter reduced employee list from {original_count} to {len(employee_ids)}")
        
        if not employee_ids:
            logger.error(f"No employees match debug pattern '{debug_files_pattern}'. Exiting.")
            return 0, 0, 0, 0
    
    # Limit the number of employees to process if specified
    if limit > 0:
        employee_ids = employee_ids[:limit]
    
    total_employees = len(employee_ids)
    logger.info(f"Processing {total_employees} employees with {workers} concurrent workers")
    
    # Initialize shared resources with enhanced CSV support
    shared_resources = SharedResources(
        os.path.join(settings.OUTPUT_DIR, settings.OUTPUT_CSV),
        use_enhanced_csv=True
    )
    shared_resources.initialize_csv()
    
    # Initialize processing systems
    document_catalog = None
    catalog_output_dir = None
    enhanced_processor = None
    
    if enable_catalog or use_enhanced_processor:
        logger.info("Initializing processing systems")
        try:
            # Initialize Gemini client
            gemini_client = GeminiClient()
            
            # Initialize catalog cache
            catalog_cache = CatalogCache(max_documents=settings.CATALOG_CACHE_SIZE)
            
            # Set up catalog output directory
            catalog_output_dir = os.path.join(settings.WORK_DIR, "output", "individual_catalogs")
            
            if enable_catalog:
                # Initialize document catalog
                document_catalog = DocumentCatalog(gemini_client, catalog_cache, catalog_output_dir=catalog_output_dir)
                logger.info(f"Catalog system initialized with cache size: {settings.CATALOG_CACHE_SIZE}")
            
            if use_enhanced_processor:
                # Initialize enhanced processor with business rules
                enhanced_processor = EnhancedI9Processor(gemini_client, catalog_cache)
                logger.info("Enhanced I-9 processor with business rules initialized")
                
                # Log processor statistics
                stats = enhanced_processor.get_processing_statistics()
                logger.info(f"Enhanced processor stats: {stats['rule_engine_stats']['total_rules_registered']} rules registered")
            
        except Exception as e:
            logger.error(f"Failed to initialize processing systems: {e}")
            logger.info("Falling back to basic processing")
            enable_catalog = False
            use_enhanced_processor = False
    
    start_time = time.time()
    last_report_time = start_time
    last_report_count = 0
    
    try:
        # Process employees concurrently using ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit all tasks
            if catalog_only:
                # For catalog-only mode, use a specialized function
                futures = [executor.submit(generate_catalog_only, employee_id, document_catalog, use_local) 
                          for employee_id in employee_ids]
            elif use_enhanced_processor and enhanced_processor:
                # Use enhanced processor with business rules
                futures = [executor.submit(process_employee_document_enhanced, employee_id, shared_resources, use_local, mode,
                                         enhanced_processor, extract_all_data, skip_existing_catalog, catalog_output_dir) 
                          for employee_id in employee_ids]
            else:
                # Normal processing with optional catalog integration
                futures = [executor.submit(process_employee_document, employee_id, shared_resources, use_local, mode,
                                         document_catalog, extract_all_data, skip_existing_catalog, catalog_output_dir) 
                          for employee_id in employee_ids]
            
            # Wait for tasks to complete and report progress
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                try:
                    # Get result (to catch any exceptions)
                    future.result()
                    
                    # Report progress at intervals
                    processed, found_i9, removed_i9, extracted_i9 = shared_resources.get_progress()
                    
                    last_report_time, last_report_count = Reporter.log_progress(
                        processed, total_employees, found_i9, removed_i9, extracted_i9,
                        start_time, last_report_time, last_report_count, batch_size
                    )
                    
                except Exception as e:
                    logger.error(f"Error in worker task: {e}")
    
    finally:
        # Close shared resources
        shared_resources.close()
    
    # Get final counts
    if not catalog_only:
        processed, found_i9, removed_i9, extracted_i9 = shared_resources.get_progress()
    else:
        # For catalog-only mode, count processed documents
        processed = len([f for f in futures if f.result() is not None])
        found_i9 = removed_i9 = extracted_i9 = 0
    
    elapsed = time.time() - start_time
    
    # Export catalog data if catalog system was used
    catalog_source = None
    if use_enhanced_processor and enhanced_processor:
        # Use enhanced processor's catalog
        catalog_source = enhanced_processor.catalog
    elif document_catalog:
        # Use regular document catalog
        catalog_source = document_catalog
        
    if enable_catalog and catalog_source and catalog_export_path:
        try:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            export_success = False
            
            if catalog_export_format in ['json', 'both']:
                json_filename = f"document_catalog_{timestamp}.json"
                json_path = os.path.join(catalog_export_path, json_filename)
                
                logger.info(f"Exporting catalog data to JSON: {json_path}")
                json_success = catalog_source.export_catalog(json_path, include_pii=catalog_include_pii)
                
                if json_success:
                    logger.info("JSON catalog export completed successfully")
                    export_success = True
                else:
                    logger.error("JSON catalog export failed")
            
            if catalog_export_format in ['csv', 'both']:
                csv_filename = f"document_catalog_{timestamp}.csv"
                csv_path = os.path.join(catalog_export_path, csv_filename)
                
                logger.info(f"Exporting catalog data to CSV: {csv_path}")
                csv_success = catalog_source.export_catalog_csv(csv_path, include_pii=catalog_include_pii)
                
                if csv_success:
                    logger.info("CSV catalog export completed successfully")
                    export_success = True
                else:
                    logger.error("CSV catalog export failed")
            
            if export_success:
                # Log catalog statistics
                stats = catalog_source.get_processing_statistics()
                logger.info(f"Catalog Statistics:")
                logger.info(f"  Documents processed: {stats['documents_processed']}")
                logger.info(f"  Pages analyzed: {stats['pages_analyzed']}")
                logger.info(f"  API calls made: {stats['api_calls_made']}")
                logger.info(f"  Total processing time: {stats['total_processing_time_seconds']:.2f}s")
                logger.info(f"  Cache hit rate: {stats['cache_statistics']['hit_rate_percent']:.1f}%")
                
                # Apply confidence threshold filtering if specified
                if catalog_confidence_threshold > 0.0:
                    low_confidence_count = stats.get('low_confidence_entries', 0)
                    if low_confidence_count > 0:
                        logger.warning(f"  {low_confidence_count} entries below confidence threshold ({catalog_confidence_threshold})")
            
        except Exception as e:
            logger.error(f"Error during catalog export: {e}")
    
    # Generate and log summary
    if not catalog_only:
        summary = Reporter.generate_summary(processed, found_i9, removed_i9, extracted_i9, elapsed)
        logger.info("\n" + summary)
    else:
        logger.info(f"Catalog generation completed: {processed} documents processed in {elapsed:.2f}s")
    
    return processed, found_i9, removed_i9, extracted_i9

def handle_catalog_validation(args):
    """
    Handle catalog validation mode.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    logger.info("Starting catalog validation mode")
    
    try:
        from .catalog.validation import CatalogValidator
        
        validator = CatalogValidator()
        catalog_files = validator.find_catalog_files(args.catalog_export_path)
        
        if not catalog_files:
            logger.warning(f"No catalog files found in {args.catalog_export_path}")
            return 1
        
        logger.info(f"Found {len(catalog_files)} catalog files to validate")
        
        validation_results = []
        for catalog_file in catalog_files:
            logger.info(f"Validating {catalog_file}")
            result = validator.validate_catalog_file(catalog_file)
            validation_results.append(result)
            
            if result['valid']:
                logger.info(f"✓ {catalog_file}: Valid")
            else:
                logger.error(f"✗ {catalog_file}: {result['error']}")
        
        # Generate validation report
        valid_count = sum(1 for r in validation_results if r['valid'])
        total_count = len(validation_results)
        
        logger.info(f"Validation complete: {valid_count}/{total_count} files valid")
        
        if args.catalog_stats:
            # Generate detailed statistics
            stats_report = validator.generate_validation_stats(validation_results)
            logger.info("Validation Statistics:")
            for key, value in stats_report.items():
                logger.info(f"  {key}: {value}")
        
        return 0 if valid_count == total_count else 1
        
    except Exception as e:
        logger.error(f"Error during catalog validation: {e}")
        return 1

def handle_catalog_import(args):
    """
    Handle catalog import mode.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    logger.info(f"Starting catalog import from {args.catalog_import}")
    
    try:
        from .catalog.validation import CatalogValidator
        
        validator = CatalogValidator()
        
        # Validate the import file
        logger.info("Validating import file...")
        validation_result = validator.validate_catalog_file(args.catalog_import)
        
        if not validation_result['valid']:
            logger.error(f"Import file validation failed: {validation_result['error']}")
            return 1
        
        logger.info("✓ Import file is valid")
        
        # Load and analyze the catalog
        catalog_data = validator.load_catalog_file(args.catalog_import)
        
        logger.info(f"Catalog contains {len(catalog_data.get('documents', []))} documents")
        
        if args.catalog_stats:
            # Generate statistics for imported catalog
            stats = validator.generate_catalog_statistics(catalog_data)
            logger.info("Imported Catalog Statistics:")
            for key, value in stats.items():
                logger.info(f"  {key}: {value}")
        
        logger.info("Catalog import completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"Error during catalog import: {e}")
        return 1

def check_network_drive_health(network_path, max_retries=3):
    """
    Check network drive health and connectivity.
    
    Args:
        network_path (str): Path to network drive
        max_retries (int): Maximum retry attempts
        
    Returns:
        bool: True if network drive is healthy, False otherwise
    """
    import time
    
    for attempt in range(max_retries):
        try:
            network_dir = Path(network_path)
            if network_dir.exists() and network_dir.is_dir():
                # Try to list a few directories to test responsiveness
                list(network_dir.iterdir())
                logger.info(f"Network drive health check passed: {network_path}")
                return True
        except Exception as e:
            logger.warning(f"Network drive health check failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(1.0)
    
    logger.error(f"Network drive health check failed after {max_retries} attempts: {network_path}")
    return False


def process_single_document_data_only(employee_id: str, enhanced_processor, 
                                     categorized_reporter, use_local: bool = False):
    """
    Process a single document for data extraction only (no PDF manipulation)
    
    Args:
        employee_id: Employee ID to process
        enhanced_processor: Enhanced I-9 processor instance
        categorized_reporter: Reporter for categorized output
        use_local: Whether to use local sample data
    """
    
    try:
        # Find PDF file
        if use_local:
            pdf_path = FileManager.get_pdf_from_local_sample(employee_id)
        else:
            pdf_path = FileManager.get_pdf_from_network_drive(employee_id)
        
        if not pdf_path or not os.path.exists(pdf_path):
            logger.warning(f"No PDF found for employee {employee_id}")
            categorized_reporter._add_error_result(employee_id, f"employee_{employee_id}.pdf", "PDF file not found")
            return False
        
        logger.info(f"Processing employee {employee_id}: {os.path.basename(pdf_path)}")
        
        # Process with enhanced processor (data extraction only)
        processing_result = enhanced_processor.process_pdf(Path(pdf_path))
        
        # Add result to categorized reporter
        categorized_reporter.add_result(
            employee_id=employee_id,
            pdf_path=pdf_path,
            processing_result=processing_result,
            validation_result=None,  # Will be extracted from processing_result if available
            catalog_data={'catalog_file_path': '', 'processing_time': 0}
        )
        
        # Determine success
        if hasattr(processing_result, 'status'):
            success = processing_result.status.value in ['COMPLETE_SUCCESS', 'PARTIAL_SUCCESS']
        else:
            success = False
        
        logger.info(f"Completed employee {employee_id}: {processing_result.status.value if hasattr(processing_result, 'status') else 'ERROR'}")
        return success
        
    except Exception as e:
        logger.error(f"Error processing employee {employee_id}: {e}")
        categorized_reporter._add_error_result(employee_id, pdf_path if 'pdf_path' in locals() else f"employee_{employee_id}.pdf", str(e))
        return False


def process_all_employees_data_only(employee_ids: list, workers: int = 4, use_local: bool = False):
    """
    Process all employees for data extraction only
    
    Args:
        employee_ids: List of employee IDs to process
        workers: Number of concurrent workers
        use_local: Whether to use local sample data
    """
    
    start_time = time.time()
    
    # Initialize components
    logger.info("Initializing processing components for data-only mode...")
    
    try:
        # Import here to avoid circular imports
        try:
            from .api.gemini_client import GeminiClient
            from .catalog.cache import CatalogCache
            from .core.enhanced_processor import EnhancedI9Processor
        except ImportError:
            from hri9.api.gemini_client import GeminiClient
            from hri9.catalog.cache import CatalogCache
            from hri9.core.enhanced_processor import EnhancedI9Processor
        
        # Initialize Gemini client and enhanced processor
        gemini_client = GeminiClient()
        catalog_cache = CatalogCache()
        enhanced_processor = EnhancedI9Processor(gemini_client, catalog_cache)
        
        logger.info("Enhanced I-9 processor initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize processing components: {e}")
        return 0, 0, 0
    
    # Initialize categorized reporter
    output_dir = os.path.join(settings.OUTPUT_DIR, "categorized_results")
    categorized_reporter = CategorizedReporter(output_dir)
    
    # Process documents concurrently
    logger.info(f"Processing {len(employee_ids)} employees with {workers} workers (data extraction only)")
    
    processed_count = 0
    success_count = 0
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit all tasks
        futures = [
            executor.submit(process_single_document_data_only, employee_id, enhanced_processor, categorized_reporter, use_local)
            for employee_id in employee_ids
        ]
        
        # Process results as they complete
        for i, future in enumerate(as_completed(futures)):
            try:
                success = future.result()
                processed_count += 1
                if success:
                    success_count += 1
                
                # Progress reporting
                progress = (i + 1) / len(employee_ids) * 100
                elapsed = time.time() - start_time
                rate = processed_count / elapsed if elapsed > 0 else 0
                eta = (len(employee_ids) - processed_count) / rate if rate > 0 else 0
                
                logger.info(f"Progress: {i+1}/{len(employee_ids)} ({progress:.1f}%) | "
                           f"Success: {success_count} | Rate: {rate:.2f} docs/sec | ETA: {eta:.1f}s")
                
            except Exception as e:
                logger.error(f"Future execution error: {e}")
                processed_count += 1
    
    # Finalize reporting
    processing_time = time.time() - start_time
    summary = categorized_reporter.finalize()
    
    # Generate enhanced comprehensive CSV report
    try:
        enhanced_reporter = EnhancedCSVReporter()
        
        # Collect all processing results for comprehensive report
        all_results = []
        for category in ['success', 'partial', 'error']:
            category_results = getattr(categorized_reporter, f'{category}_results', [])
            all_results.extend(category_results)
        
        if all_results:
            # Generate comprehensive CSV with all requested fields
            comprehensive_report_path = enhanced_reporter.generate_comprehensive_report(all_results)
            logger.info(f"Generated comprehensive CSV report: {comprehensive_report_path}")
            
            # Generate summary statistics report
            summary_report_path = enhanced_reporter.generate_summary_report(all_results)
            logger.info(f"Generated summary statistics report: {summary_report_path}")
        else:
            logger.warning("No results available for comprehensive reporting")
            
    except Exception as e:
        logger.error(f"Error generating enhanced CSV reports: {e}")
    
    logger.info(f"\n=== Final Data-Only Processing Summary ===")
    logger.info(f"Total processed: {processed_count}")
    logger.info(f"Processing time: {processing_time:.1f} seconds")
    logger.info(f"Processing rate: {processed_count/processing_time:.2f} docs/sec")
    logger.info(f"Success rate: {success_count/processed_count*100:.1f}%")
    logger.info(f"=========================================")
    
    return summary['success'], summary['partial'], summary['error']


def handle_existing_catalog_processing(args):
    """
    Handle processing using existing catalog files without AI re-extraction
    
    Args:
        args: Command line arguments
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    logger.info("Starting I-9 processing using existing catalog data (no AI re-extraction)")
    
    try:
        # Initialize enhanced processor
        processor = EnhancedI9Processor()
        
        if args.catalog_file:
            # Process specific catalog file
            catalog_path = Path(args.catalog_file)
            if not catalog_path.exists():
                logger.error(f"Catalog file not found: {catalog_path}")
                return 1
            
            # Determine corresponding PDF file
            pdf_name = catalog_path.stem.replace('.catalog', '') + '.pdf'
            pdf_path = Path('input') / pdf_name
            
            if not pdf_path.exists():
                logger.warning(f"PDF file not found: {pdf_path}, using catalog file name")
                pdf_path = catalog_path.with_suffix('.pdf')
            
            logger.info(f"Processing catalog: {catalog_path}")
            logger.info(f"Corresponding PDF: {pdf_path}")
            
            # Process using existing catalog
            result = processor.process_from_existing_catalog(pdf_path, str(catalog_path))
            
            # Log results
            logger.info(f"Processing completed with status: {result.status}")
            logger.info(f"Selection reason: {result.selection_reason}")
            logger.info(f"Form type: {result.form_type_selected}")
            
            if result.primary_i9_data:
                logger.info(f"Employee: {result.primary_i9_data.first_name} {result.primary_i9_data.last_name}")
                logger.info(f"Work authorization until: {result.primary_i9_data.authorized_to_work_until}")
                
                if result.primary_i9_data.section_3_documents:
                    logger.info(f"Section 3 documents: {len(result.primary_i9_data.section_3_documents)}")
                    for doc in result.primary_i9_data.section_3_documents:
                        logger.info(f"  - {doc.document_type} (Expiry: {doc.expiration_date})")
            
            return 0 if result.status.name in ['COMPLETE_SUCCESS', 'PARTIAL_SUCCESS'] else 1
            
        else:
            # Process all catalog files in the catalog directory
            from pathlib import Path
            catalog_dir = Path(args.catalog_export_path)
            if not catalog_dir.exists():
                logger.error(f"Catalog directory not found: {catalog_dir}")
                return 1
            
            catalog_files = list(catalog_dir.glob("*.catalog.json"))
            if not catalog_files:
                logger.error(f"No catalog files found in: {catalog_dir}")
                return 1
            
            logger.info(f"Found {len(catalog_files)} catalog files to process")
            
            success_count = 0
            error_count = 0
            all_results = []
            
            for catalog_file in catalog_files:
                try:
                    # Determine corresponding PDF file
                    pdf_name = catalog_file.stem.replace('.catalog', '') + '.pdf'
                    pdf_path = Path('input') / pdf_name
                    
                    logger.info(f"Processing catalog: {catalog_file.name}")
                    
                    # Process using existing catalog
                    result = processor.process_from_existing_catalog(pdf_path, str(catalog_file))
                    
                    if result.status.name in ['COMPLETE_SUCCESS', 'PARTIAL_SUCCESS']:
                        success_count += 1
                        logger.info(f"✅ Success: {catalog_file.name}")
                    else:
                        error_count += 1
                        logger.error(f"❌ Error: {catalog_file.name} - {result.notes}")
                    
                    # Collect results for categorized output generation
                    all_results.append(result)
                        
                except Exception as e:
                    error_count += 1
                    logger.error(f"❌ Error processing {catalog_file.name}: {e}")
            
            logger.info(f"Catalog processing complete: {success_count} success, {error_count} errors")
            
            # Generate categorized results files with updated validation logic
            if all_results:
                logger.info("🔄 Generating categorized results with updated validation logic...")
                try:
                    # Use the existing run_processing.py script to generate categorized results
                    import subprocess
                    import os
                    
                    # Save results to temporary processing results for categorization
                    logger.info("📊 Saving processing results for categorization...")
                    
                    # Create a simple CSV writer to save results in the expected format
                    from datetime import datetime
                    import csv
                    from pathlib import Path
                    
                    # Ensure output directory exists
                    output_dir = Path("workdir/categorized_results")
                    output_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Generate timestamp for file naming
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    
                    # Separate results by status
                    success_results = []
                    partial_results = []
                    error_results = []
                    
                    for result in all_results:
                        if result.status.name == 'COMPLETE_SUCCESS':
                            success_results.append(result)
                        elif result.status.name == 'PARTIAL_SUCCESS':
                            partial_results.append(result)
                        else:
                            error_results.append(result)
                    
                    # Write categorized results files
                    def write_results_file(results, category, filename):
                        if not results:
                            return None
                            
                        filepath = output_dir / filename
                        with open(filepath, 'w', newline='', encoding='utf-8') as f:
                            writer = csv.writer(f)
                            
                            # Write header
                            header = [
                                'employee_id', 'pdf_file_name', 'processing_status', 'validation_score',
                                'first_name', 'last_name', 'citizenship_status', 'employee_signature_date',
                                'total_forms_detected', 'form_type_selected', 'selection_reason',
                                'critical_issues', 'total_validations', 'passed_validations', 'failed_validations',
                                'document_matches_found', 'supporting_documents_count', 'expiration_matches',
                                'documents_mentioned_count', 'documents_attached_count', 'documents_missing_count',
                                'document_attachment_status', 'business_rules_applied', 'scenario_results',
                                'validation_details', 'input_file_path', 'catalog_file_path', 'processing_time', 'notes'
                            ]
                            writer.writerow(header)
                            
                            # Write data rows
                            for result in results:
                                # Extract basic info
                                pdf_name = "unknown.pdf"
                                if result.primary_i9_data and hasattr(result.primary_i9_data, 'last_name'):
                                    last_name = result.primary_i9_data.last_name or "Unknown"
                                    pdf_name = f"{last_name}.pdf"
                                
                                row = [
                                    getattr(result.primary_i9_data, 'employee_id', '') if result.primary_i9_data else '',
                                    pdf_name,
                                    result.status.name,
                                    'N/A',  # validation_score - not available in ProcessingResult
                                    getattr(result.primary_i9_data, 'first_name', '') if result.primary_i9_data else '',
                                    getattr(result.primary_i9_data, 'last_name', '') if result.primary_i9_data else '',
                                    getattr(result.primary_i9_data, 'citizenship_status', '') if result.primary_i9_data else '',
                                    getattr(result.primary_i9_data, 'employee_signature_date', '') if result.primary_i9_data else '',
                                    result.total_forms_detected or 0,
                                    result.form_type_selected or '',
                                    result.selection_reason or '',
                                    result.critical_issues or 0,
                                    result.total_validations or 0,
                                    result.passed_validations or 0,
                                    result.failed_validations or 0,
                                    result.document_matches_found or 0,
                                    result.supporting_documents_count or 0,
                                    result.expiration_matches or 0,
                                    result.documents_mentioned_count or 0,
                                    result.documents_attached_count or 0,
                                    result.documents_missing_count or 0,
                                    result.document_attachment_status or '',
                                    'Applied',
                                    '',  # scenario_results
                                    '',  # validation_details
                                    f"input/{pdf_name}",  # input_file_path
                                    '',  # catalog_file_path
                                    '0',  # processing_time
                                    result.notes or ''
                                ]
                                writer.writerow(row)
                        
                        return str(filepath)
                    
                    # Generate categorized files
                    result_files = {}
                    
                    if success_results:
                        success_file = write_results_file(success_results, 'success', f'i9_success_{timestamp}.csv')
                        if success_file:
                            result_files['Success'] = success_file
                    
                    if partial_results:
                        partial_file = write_results_file(partial_results, 'partial', f'i9_partial_{timestamp}.csv')
                        if partial_file:
                            result_files['Partial'] = partial_file
                    
                    if error_results:
                        error_file = write_results_file(error_results, 'errors', f'i9_errors_{timestamp}.csv')
                        if error_file:
                            result_files['Errors'] = error_file
                    
                    logger.info("✅ Categorized results generated:")
                    for category, file_path in result_files.items():
                        logger.info(f"   - {category}: {file_path}")
                    
                    # Auto-generate comprehensive CSV report using processed results
                    logger.info("🔄 Auto-generating comprehensive CSV report using processed I-9 set data...")
                    import subprocess
                    import os
                    
                    # Run the fixed comprehensive CSV generation
                    result = subprocess.run([
                        "python", "generate_comprehensive_csv.py"
                    ], cwd=os.getcwd(), capture_output=True, text=True)
                    
                    if result.returncode == 0:
                        logger.info("✅ Comprehensive CSV report generated successfully using processed I-9 set data")
                        logger.info("📋 Check workdir/enhanced_reports/ for comprehensive CSV files")
                        logger.info("🎯 Report now uses correct Section 3 expiry dates and document attachment verification")
                    else:
                        logger.warning(f"⚠️ Comprehensive CSV generation had issues: {result.stderr}")
                        if result.stdout:
                            logger.info(f"Output: {result.stdout}")
                        
                except Exception as e:
                    logger.error(f"Error generating categorized results: {e}")
                    import traceback
                    logger.error(f"Traceback: {traceback.format_exc()}")
            
            return 0 if error_count == 0 else 1
            
    except Exception as e:
        logger.error(f"Error in catalog processing: {e}")
        return 1


def print_configuration_info(args, enable_catalog):
    """Print comprehensive configuration information at startup"""
    logger.info("=" * 80)
    logger.info("🚀 HRI9 SYSTEM CONFIGURATION")
    logger.info("=" * 80)
    
    # Core Directories
    logger.info("📁 CORE DIRECTORIES:")
    logger.info(f"   🏠 Project Root: {settings.BASE_DIR.parent}")
    logger.info(f"   📂 Data Directory: {settings.DATA_DIR}")
    logger.info(f"   📂 Work Directory: {settings.WORK_DIR}")
    logger.info(f"   📂 Input Directory: {settings.LOCAL_SAMPLE_PATH}")
    logger.info(f"   📂 Output Directory: {args.output_dir}")
    
    # Processing Configuration
    logger.info("\n⚙️  PROCESSING CONFIGURATION:")
    logger.info(f"   👥 Workers: {args.workers}")
    logger.info(f"   📊 Batch Size: {args.batch_size}")
    logger.info(f"   🔢 Document Limit: {args.limit if args.limit else 'ALL'}")
    logger.info(f"   🏠 Use Local Data: {args.use_local}")
    logger.info(f"   📝 Verbose Logging: {args.verbose}")
    
    # Processing Mode
    logger.info("\n🔄 PROCESSING MODE:")
    if args.data_only:
        logger.info("   📊 Data-Only Mode (No PDF manipulation)")
    elif args.catalog_only:
        logger.info("   📋 Catalog-Only Mode")
    elif args.use_existing_catalog:
        logger.info("   ⚡ Existing Catalog Processing (Ultra-fast)")
    else:
        logger.info(f"   🔧 Traditional Mode: {args.mode}")
    
    # Catalog System
    logger.info("\n📋 CATALOG SYSTEM:")
    logger.info(f"   🔧 Catalog Enabled: {enable_catalog}")
    logger.info(f"   📂 Catalog Export Path: {args.catalog_export_path}")
    logger.info(f"   📄 Export Format: {args.catalog_export_format}")
    logger.info(f"   🔒 Include PII: {args.catalog_include_pii}")
    logger.info(f"   ⏭️  Skip Existing: {args.skip_existing_catalog}")
    
    # Output Files
    logger.info("\n📤 OUTPUT FILES:")
    if args.data_only:
        logger.info(f"   ✅ Success CSV: workdir/categorized_results/i9_success_[timestamp].csv")
        logger.info(f"   ⚠️  Partial CSV: workdir/categorized_results/i9_partial_[timestamp].csv")
        logger.info(f"   ❌ Error CSV: workdir/categorized_results/i9_errors_[timestamp].csv")
    else:
        logger.info(f"   📊 Main CSV: {args.output_csv}")
        logger.info(f"   📂 Extracted I-9s: {args.extract_dir}")
        logger.info(f"   🧹 Cleaned PDFs: {args.cleaned_dir}")
    
    # Input File Discovery
    if args.use_local and os.path.exists(settings.LOCAL_SAMPLE_PATH):
        employee_dirs = [d for d in os.listdir(settings.LOCAL_SAMPLE_PATH) 
                        if os.path.isdir(os.path.join(settings.LOCAL_SAMPLE_PATH, d)) and d.isdigit()]
        logger.info(f"\n📁 INPUT FILE DISCOVERY:")
        logger.info(f"   📂 Scanning Directory: {settings.LOCAL_SAMPLE_PATH}")
        logger.info(f"   👥 Employee Directories Found: {len(employee_dirs)}")
        for emp_id in sorted(employee_dirs):
            emp_dir = os.path.join(settings.LOCAL_SAMPLE_PATH, emp_id)
            pdf_files = [f for f in os.listdir(emp_dir) if f.endswith('.pdf')]
            logger.info(f"      📋 {emp_id}: {len(pdf_files)} PDF file(s)")
    
    logger.info("=" * 80)


def prepare_input_directory(input_folder: str) -> int:
    """
    Prepare PDF files in user-specified directory
    Returns count of PDF files found
    """
    input_path = Path(input_folder)
    
    # Count PDFs in main directory (case-insensitive)
    main_pdfs = []
    for pattern in ["*.pdf", "*.PDF"]:
        main_pdfs.extend(input_path.glob(pattern))
    
    # If no PDFs in main directory, look for subdirectories
    if not main_pdfs:
        logger.info("No PDFs found in main directory, checking subdirectories...")
        
        # Find PDFs in subdirectories
        subdirectory_pdfs = []
        for subdir in input_path.iterdir():
            if subdir.is_dir():
                for pattern in ["*.pdf", "*.PDF"]:
                    subdirectory_pdfs.extend(subdir.glob(pattern))
        
        if subdirectory_pdfs:
            logger.info(f"Found {len(subdirectory_pdfs)} PDFs in subdirectories")
            logger.info("Copying PDFs to main directory for batch processing...")
            
            for pdf_file in subdirectory_pdfs:
                target_file = input_path / pdf_file.name
                if not target_file.exists():
                    shutil.copy2(pdf_file, target_file)
                    logger.info(f"✅ Copied: {pdf_file.name}")
                else:
                    logger.info(f"⏭️  Already exists: {pdf_file.name}")
            
            return len(subdirectory_pdfs)
        else:
            logger.warning("No PDF files found in directory or subdirectories")
            return 0
    else:
        logger.info(f"Found {len(main_pdfs)} PDFs in main directory")
        return len(main_pdfs)


def generate_catalogs_for_directory(input_folder: str, catalog_export_path: str):
    """Generate catalog files for all PDFs in the specified directory"""
    input_path = Path(input_folder)
    # Find PDF files (case-insensitive)
    pdf_files = []
    for pattern in ["*.pdf", "*.PDF"]:
        pdf_files.extend(input_path.glob(pattern))
    
    logger.info(f"Generating catalogs for {len(pdf_files)} PDF files...")
    
    # Import required modules
    try:
        from .catalog.document_catalog import DocumentCatalog
        from .api.gemini_client import GeminiClient
        from .catalog.cache import CatalogCache
    except ImportError:
        from hri9.catalog.document_catalog import DocumentCatalog
        from hri9.api.gemini_client import GeminiClient
        from hri9.catalog.cache import CatalogCache
    
    # Initialize required components
    catalog_output_dir = Path(catalog_export_path)
    catalog_output_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize Gemini client and catalog cache
    gemini_client = GeminiClient()
    catalog_cache = CatalogCache(max_documents=settings.CATALOG_CACHE_SIZE)
    
    # Initialize document catalog
    document_catalog = DocumentCatalog(
        gemini_client=gemini_client,
        catalog_cache=catalog_cache,
        catalog_output_dir=str(catalog_output_dir)
    )
    
    # Generate catalog for each PDF
    successful_catalogs = 0
    for pdf_file in pdf_files:
        try:
            # Check if catalog already exists
            catalog_filename = f"{pdf_file.stem}.catalog.json"
            catalog_filepath = catalog_output_dir / catalog_filename
            
            if catalog_filepath.exists():
                logger.info(f"⏭️  Catalog already exists: {catalog_filename} (skipping)")
                successful_catalogs += 1
                continue
            
            logger.info(f"Generating catalog for: {pdf_file.name}")
            
            # Extract employee name from filename (remove .pdf extension)
            employee_name = pdf_file.stem
            
            # Generate catalog (this also writes the catalog file with proper format)
            catalog_entry = document_catalog.analyze_document(str(pdf_file), employee_name)
            
            logger.info(f"✅ Catalog saved: {catalog_filename}")
            successful_catalogs += 1
            
        except Exception as e:
            logger.error(f"❌ Failed to generate catalog for {pdf_file.name}: {e}")
    
    logger.info(f"✅ Catalog generation complete: {successful_catalogs}/{len(pdf_files)} successful")
    return successful_catalogs


def process_batch_files(args):
    """Process all PDF files using batch processor"""
    try:
        from .batch_processor import BatchProcessor
    except ImportError:
        from hri9.batch_processor import BatchProcessor
    
    batch_processor = BatchProcessor(args.catalog_export_path)
    return batch_processor.process_all_files(args.input_folder)


def generate_consolidated_reports(batch_results, args):
    """Generate all consolidated reports"""
    try:
        # Generate comprehensive CSV using existing catalogs
        logger.info("Generating comprehensive CSV report...")
        
        def _calculate_expiry_date_matches(result, catalog_path):
            """Calculate expiry date matches following business rules hierarchy"""
            try:
                # Get work authorization expiry date from result
                work_auth_expiry = None
                if hasattr(result, 'primary_i9_data') and result.primary_i9_data:
                    work_auth_expiry = getattr(result.primary_i9_data, 'authorized_to_work_until', None)
                
                if not work_auth_expiry or work_auth_expiry in ['N/A', '', None]:
                    return "No work authorization expiry date found"
                
                # Load catalog data to check document expiry dates
                with open(catalog_path, 'r', encoding='utf-8') as f:
                    catalog_data = json.load(f)
                
                # Get pages from correct location in catalog structure
                pages = catalog_data.get('document_catalog', {}).get('pages', catalog_data.get('pages', []))
                
                # Business Rules Hierarchy: Supplement B > Section 3 > Section 2
                matches = []
                
                # PRIORITY 1: Check Supplement B documents (highest priority)
                supplement_b_expiry = _find_supplement_b_expiry(catalog_data)
                if supplement_b_expiry and supplement_b_expiry == work_auth_expiry:
                    matches.append(f"Supplement B: {supplement_b_expiry}")
                
                # PRIORITY 2: Check Section 3 documents (second priority)
                section_3_expiry = _find_section_3_expiry(catalog_data, result)
                if section_3_expiry and section_3_expiry == work_auth_expiry:
                    matches.append(f"Section 3: {section_3_expiry}")
                
                # PRIORITY 3: Check Section 2 documents (lowest priority)
                section_2_expiry = _find_section_2_expiry(catalog_data)
                if section_2_expiry and section_2_expiry == work_auth_expiry:
                    matches.append(f"Section 2: {section_2_expiry}")
                
                if matches:
                    return f"Work Auth: {work_auth_expiry} matches " + " | ".join(matches)
                else:
                    return f"Work Auth: {work_auth_expiry} - No matching document expiry dates found"
                    
            except Exception as e:
                logger.warning(f"Error calculating expiry matches: {e}")
                return "Error calculating expiry matches"
        
        def _find_supplement_b_expiry(catalog_data):
            """Find latest Supplement B document expiry date"""
            latest_expiry = None
            latest_signature_date = None
            
            # Get pages from correct location in catalog structure
            pages = catalog_data.get('document_catalog', {}).get('pages', catalog_data.get('pages', []))
            
            for page in pages:
                page_title = page.get('page_title', '').lower()
                if 'supplement' in page_title:
                    extracted = page.get('extracted_values', {})
                    
                    # Get employer signature date
                    employer_sig_date = (
                        extracted.get('supplement_b_employer_signature_date') or
                        extracted.get('employer_signature_date') or
                        extracted.get('reverification_signature_date')
                    )
                    
                    # Get expiry date
                    expiry_date = (
                        extracted.get('reverification_1_expiration_date') or
                        extracted.get('supplement_b_expiration_date') or
                        extracted.get('document_expiration_date') or
                        extracted.get('document_expiration_date_1') or  # ← ADD this field (found in Balder)
                        extracted.get('reverification_document_expiration_date') or
                        extracted.get('reverification_expiration_date')
                    )
                    
                    # For Supplement B, prioritize expiry date even if no signature date
                    if expiry_date:
                        if employer_sig_date:
                            # If we have both signature and expiry, use signature date for selection
                            if not latest_signature_date or employer_sig_date > latest_signature_date:
                                latest_signature_date = employer_sig_date
                                latest_expiry = expiry_date
                        else:
                            # If we only have expiry date, use it (common in Supplement B)
                            if not latest_expiry:
                                latest_expiry = expiry_date
                                logger.info(f"Found Supplement B expiry date without signature: {expiry_date}")
            
            return latest_expiry
        
        def _find_section_3_expiry(catalog_data, result):
            """Find Section 3 document expiry date using enhanced detection logic (same as document extraction)"""
            try:
                # Get pages from correct location in catalog structure
                pages = catalog_data.get('document_catalog', {}).get('pages', catalog_data.get('pages', []))
                
                # Use the same enhanced Section 3 detection logic as document extraction
                section_3_pages = []
                for page in pages:
                    page_title = page.get('page_title', '').lower()
                    extracted = page.get('extracted_values', {})
                    
                    # Enhanced Section 3 detection: check both title patterns AND presence of section_3 fields
                    is_section_3_by_title = any(pattern in page_title for pattern in ['section 3', 'reverification', 'section 2 and 3', 'section 2 & 3'])
                    is_section_3_by_fields = any(field in extracted for field in [
                        'section_3_document_title', 'section_3_signature_date', 'section_3_document_number', 'section_3_expiration_date',
                        # Fresh catalog variations
                        'reverification_document_title', 'reverification_document_number', 'reverification_expiration_date',
                        'reverification_date_signed', 'reverification_employer_name', 'employer_signature_date_reverification'
                    ])
                    
                    if is_section_3_by_title or is_section_3_by_fields:
                        # Check all possible signature date fields (including fresh catalog variations)
                        sig_dates = [
                            extracted.get('employer_signature_date'),
                            extracted.get('reverification_signature_date'),
                            extracted.get('section_3_signature_date'),
                            extracted.get('section_3_employer_signature_date'),
                            # Fresh catalog variations
                            extracted.get('reverification_date_signed'),  # ← Wu page 19 has this
                            extracted.get('employer_signature_date_reverification')  # ← Wu page 9 has this
                        ]
                        
                        # Get the latest signature date from this page
                        latest_sig = None
                        for sig_date in sig_dates:
                            if sig_date and sig_date not in ['N/A', '', None]:
                                if not latest_sig or sig_date > latest_sig:
                                    latest_sig = sig_date
                        
                        if latest_sig:
                            section_3_pages.append({
                                'page_number': page.get('page_number'),
                                'signature_date': latest_sig,
                                'extracted': extracted
                            })
                
                # Find the page with the latest signature date overall (using proper date comparison)
                if section_3_pages:
                    def parse_date_for_comparison(date_str):
                        """Parse date string for proper comparison"""
                        try:
                            from datetime import datetime
                            return datetime.strptime(date_str, '%m/%d/%Y')
                        except:
                            return datetime.min  # Return minimum date if parsing fails
                    
                    latest_page = max(section_3_pages, key=lambda x: parse_date_for_comparison(x['signature_date']))
                    extracted = latest_page['extracted']
                    
                    logger.info(f"Section 3 expiry matching: Using page {latest_page['page_number']} with signature date {latest_page['signature_date']}")
                    
                    # Check Section 3 specific expiry fields (including fresh catalog variations)
                    expiry_date = (
                        extracted.get('section_3_expiration_date') or
                        extracted.get('reverification_expiration_date') or  # ← Wu fresh catalog has this
                        extracted.get('reverification_document_expiration_date') or
                        extracted.get('rehire_expiration_date') or
                        extracted.get('list_a_expiration_date') or
                        extracted.get('document_expiration_date')
                    )
                    
                    if expiry_date and expiry_date not in ['N/A', '', None]:
                        return expiry_date
                
                return None
                
            except Exception as e:
                logger.warning(f"Error finding Section 3 expiry: {e}")
                return None
        
        def _find_latest_section_3_pages(catalog_data):
            """Find Section 3 pages with latest employer signature date, preferring pages with expiry data"""
            latest_signature_date = None
            latest_pages = []
            
            # Get pages from correct location in catalog structure
            pages = catalog_data.get('document_catalog', {}).get('pages', catalog_data.get('pages', []))
            
            for page in pages:
                page_title = page.get('page_title', '').lower()
                # Match various Section 3 patterns
                is_section_3 = any([
                    'section 3' in page_title,
                    'section3' in page_title,
                    'reverification' in page_title,
                    'section 2 and 3' in page_title,
                    'section 2 & 3' in page_title
                ])
                if is_section_3:
                    extracted = page.get('extracted_values', {})
                    
                    # Get employer signature date using same priority as selection logic
                    employer_sig_date = (
                        extracted.get('reverification_signature_date') or
                        extracted.get('employer_signature_date') or
                        extracted.get('section_3_employer_signature_date')
                    )
                    
                    # Check if this page has expiry date
                    has_expiry_date = any([
                        extracted.get('section_3_expiration_date'),
                        extracted.get('reverification_expiration_date'),
                        extracted.get('reverification_document_expiration_date'),
                        extracted.get('rehire_expiration_date'),
                        extracted.get('list_a_expiration_date'),
                        extracted.get('document_expiration_date')
                    ])
                    
                    if employer_sig_date:
                        should_select = False
                        if not latest_signature_date or employer_sig_date > latest_signature_date:
                            should_select = True
                            latest_pages = [page.get('page_number')]
                        elif employer_sig_date == latest_signature_date:
                            # If same signature date, prefer page with expiry data
                            if has_expiry_date:
                                # Remove pages without expiry data and add this one
                                latest_pages = [p for p in latest_pages if _page_has_expiry_data(pages, p)]
                                if page.get('page_number') not in latest_pages:
                                    latest_pages.append(page.get('page_number'))
                            elif not any(_page_has_expiry_data(pages, p) for p in latest_pages):
                                # Only add if no existing pages have expiry data
                                latest_pages.append(page.get('page_number'))
                        
                        if should_select:
                            latest_signature_date = employer_sig_date
            
            return latest_pages
        
        def _page_has_expiry_data(pages, page_number):
            """Check if a page has expiry data"""
            for page in pages:
                if page.get('page_number') == page_number:
                    extracted = page.get('extracted_values', {})
                    return any([
                        extracted.get('section_3_expiration_date'),
                        extracted.get('reverification_expiration_date'),
                        extracted.get('reverification_document_expiration_date'),
                        extracted.get('rehire_expiration_date'),
                        extracted.get('list_a_expiration_date'),
                        extracted.get('document_expiration_date')
                    ])
            return False
        
        def _find_section_2_expiry(catalog_data):
            """Find Section 2 document expiry date from latest Section 2 page"""
            # Get pages from correct location in catalog structure
            pages = catalog_data.get('document_catalog', {}).get('pages', catalog_data.get('pages', []))
            
            # Find Section 2 pages and their signature dates to get the latest one
            section_2_pages = []
            for page in pages:
                page_title = page.get('page_title', '').lower()
                extracted = page.get('extracted_values', {})
                
                # Enhanced Section 2 detection: check both title patterns AND presence of section_2/list fields
                is_section_2_by_title = ('section 2' in page_title or 'section 1 and 2' in page_title)
                is_section_2_by_fields = any(field in extracted for field in [
                    'list_a_document_title', 'list_b_document_title', 'list_c_document_title',
                    'section_2_list_a_document_title', 'section_2_list_b_document_title', 'section_2_list_c_document_title',
                    # Additional variations found in catalogs (for expiry matching)
                    'list_a_document_title_1', 'list_a_document_1_title',  # ← Debek format
                    'additional_information_document_title_2', 'additional_information_document_title_3'  # ← Debek format
                ]) and extracted.get('employer_signature_date')  # Must have employer signature to be Section 2
                
                if is_section_2_by_title or is_section_2_by_fields:
                    # Get employer signature date for this page
                    employer_sig_date = extracted.get('employer_signature_date')
                    
                    section_2_pages.append({
                        'page_number': page.get('page_number'),
                        'signature_date': employer_sig_date or '',
                        'extracted': extracted
                    })
            
            # Sort by signature date to get the latest Section 2 page (using proper date comparison)
            if section_2_pages:
                def parse_date_for_comparison(date_str):
                    """Parse date string for proper comparison"""
                    try:
                        from datetime import datetime
                        return datetime.strptime(date_str, '%m/%d/%Y')
                    except:
                        return datetime.min  # Return minimum date if parsing fails
                
                latest_page = max(section_2_pages, key=lambda x: parse_date_for_comparison(x['signature_date']))
                extracted = latest_page['extracted']
                
                logger.info(f"Section 2 expiry matching: Using page {latest_page['page_number']} with signature date {latest_page['signature_date']}")
                
                # Check Section 2 expiry fields (including additional variations found in catalogs)
                expiry_dates = []
                expiry_fields = [
                    'section_2_list_a_expiration_date',
                    'section_2_list_a_expiration_date_1',
                    'section_2_list_b_expiration_date',
                    'section_2_list_c_expiration_date',
                    'list_a_expiration_date',
                    'list_b_expiration_date',
                    'list_c_expiration_date',
                    # Additional variations found in catalogs
                    'list_a_document_1_expiration_date',  # ← De Lima format
                    'list_a_document_2_expiration_date',
                    'list_a_document_3_expiration_date',  # ← De Lima has this matching 03/31/2025
                    'list_a_expiration_date_1',
                    'additional_information_expiration_date_3',  # ← Debek format
                    'document_expiration_date'
                ]
                
                # Collect all expiry dates and find one that matches work auth
                for field in expiry_fields:
                    expiry_date = extracted.get(field)
                    if expiry_date and expiry_date not in ['N/A', '', None]:
                        expiry_dates.append(expiry_date)
                
                # Return the first expiry date that matches work auth, or the first one found
                work_auth_expiry = None
                if hasattr(result, 'primary_i9_data') and result.primary_i9_data:
                    work_auth_expiry = getattr(result.primary_i9_data, 'authorized_to_work_until', None)
                
                if work_auth_expiry and expiry_dates:
                    for expiry_date in expiry_dates:
                        if expiry_date == work_auth_expiry:
                            return expiry_date
                
                # If no match found, return first expiry date
                if expiry_dates:
                    return expiry_dates[0]
            
            return None
        
        def _calculate_document_tracking(result, catalog_path):
            """Calculate document tracking following business rules hierarchy (Supplement B > Section 3 > Section 2)"""
            try:
                # Load catalog data to check document information
                with open(catalog_path, 'r', encoding='utf-8') as f:
                    catalog_data = json.load(f)
                
                # Get pages from correct location in catalog structure
                pages = catalog_data.get('document_catalog', {}).get('pages', catalog_data.get('pages', []))
                
                # Business Rules Hierarchy: Supplement B > Section 3 > Section 2
                documents_listed = []
                documents_found = []
                documents_not_found = []
                
                # PRIORITY 1: Check Supplement B documents (highest priority)
                supplement_b_docs = _extract_supplement_b_documents(pages)
                if supplement_b_docs:
                    documents_listed.extend(supplement_b_docs)
                    # Check which Supplement B documents are found in supporting pages
                    found, not_found = _check_document_attachments(supplement_b_docs, pages)
                    documents_found.extend(found)
                    documents_not_found.extend(not_found)
                
                # PRIORITY 2: Check Section 3 documents (second priority) - only if no Supplement B
                elif hasattr(result, 'form_type_selected') and ('section_3' in str(result.form_type_selected) or 'reverification' in str(result.form_type_selected)):
                    section_3_docs = _extract_section_3_documents(pages, result)
                    if section_3_docs:
                        documents_listed.extend(section_3_docs)
                        found, not_found = _check_document_attachments(section_3_docs, pages)
                        documents_found.extend(found)
                        documents_not_found.extend(not_found)
                
                # PRIORITY 3: Check Section 2 documents (lowest priority) - only if no Supplement B or Section 3
                else:
                    section_2_docs = _extract_section_2_documents(pages)
                    if section_2_docs:
                        documents_listed.extend(section_2_docs)
                        found, not_found = _check_document_attachments(section_2_docs, pages)
                        documents_found.extend(found)
                        documents_not_found.extend(not_found)
                
                return {
                    'documents_listed': ' | '.join(documents_listed) if documents_listed else 'No documents listed',
                    'documents_found': ' | '.join(documents_found) if documents_found else 'No documents found',
                    'documents_not_found': ' | '.join(documents_not_found) if documents_not_found else 'All documents found'
                }
                    
            except Exception as e:
                logger.warning(f"Error calculating document tracking: {e}")
                return {
                    'documents_listed': 'Error extracting documents',
                    'documents_found': 'Error checking attachments', 
                    'documents_not_found': 'Error checking attachments'
                }
        
        def _extract_supplement_b_documents(pages):
            """Extract document information from Supplement B pages"""
            documents = []
            
            for page in pages:
                page_title = page.get('page_title', '').lower()
                if 'supplement b' in page_title:  # More specific check
                    extracted = page.get('extracted_values', {})
                    
                    # Check various Supplement B document fields (including the actual field found)
                    doc_fields = [
                        'document_title',  # ← This is the actual field in Balder
                        'document_title_1', 'document_title_2', 'document_title_3',
                        'reverification_1_document_title', 'reverification_2_document_title',
                        'reverification_document_title', 'supplement_b_document_title'
                    ]
                    
                    for field in doc_fields:
                        doc_title = extracted.get(field)
                        if doc_title and doc_title not in ['N/A', '', None]:
                            # Get document number if available
                            doc_number = extracted.get(field.replace('title', 'number'), '')
                            if not doc_number:
                                doc_number = extracted.get('document_number')  # ← Check generic document_number field
                            
                            if doc_number and doc_number not in ['N/A', '', None]:
                                documents.append(f"{doc_title} (#{doc_number})")
                            else:
                                documents.append(doc_title)
            
            return documents
        
        def _extract_section_3_documents(pages, result):
            """Extract document information from Section 3 pages with latest employer signature date"""
            documents = []
            
            # Find Section 3 pages and their signature dates
            section_3_pages = []
            for page in pages:
                page_title = page.get('page_title', '').lower()
                extracted = page.get('extracted_values', {})
                
                # Enhanced Section 3 detection: check both title patterns AND presence of section_3 fields
                is_section_3_by_title = any(pattern in page_title for pattern in ['section 3', 'reverification', 'section 2 and 3', 'section 2 & 3'])
                is_section_3_by_fields = any(field in extracted for field in [
                    'section_3_document_title', 'section_3_signature_date', 'section_3_document_number', 'section_3_expiration_date',
                    # Fresh catalog variations
                    'reverification_document_title', 'reverification_document_number', 'reverification_expiration_date',
                    'reverification_date_signed', 'reverification_employer_name', 'employer_signature_date_reverification'
                ])
                
                if is_section_3_by_title or is_section_3_by_fields:
                    # Check all possible signature date fields (including fresh catalog variations)
                    sig_dates = [
                        extracted.get('employer_signature_date'),
                        extracted.get('reverification_signature_date'),
                        extracted.get('section_3_signature_date'),
                        extracted.get('section_3_employer_signature_date'),
                        # Fresh catalog variations
                        extracted.get('reverification_date_signed'),  # ← Wu page 19 has this
                        extracted.get('employer_signature_date_reverification')  # ← Wu page 9 has this
                    ]
                    
                    # Get the latest signature date from this page
                    latest_sig = None
                    for sig_date in sig_dates:
                        if sig_date and sig_date not in ['N/A', '', None]:
                            if not latest_sig or sig_date > latest_sig:
                                latest_sig = sig_date
                    
                    if latest_sig:
                        section_3_pages.append({
                            'page_number': page.get('page_number'),
                            'signature_date': latest_sig,
                            'extracted': extracted
                        })
            
            # Find the page with the latest signature date overall (using proper date comparison)
            if section_3_pages:
                def parse_date_for_comparison(date_str):
                    """Parse date string for proper comparison"""
                    try:
                        from datetime import datetime
                        return datetime.strptime(date_str, '%m/%d/%Y')
                    except:
                        return datetime.min  # Return minimum date if parsing fails
                
                latest_page = max(section_3_pages, key=lambda x: parse_date_for_comparison(x['signature_date']))
                extracted = latest_page['extracted']
                
                logger.info(f"Section 3 document extraction: Using page {latest_page['page_number']} with signature date {latest_page['signature_date']}")
                
                # Check Section 3 document fields (including the actual field found in Wu)
                doc_fields = [
                    'section_3_document_title',  # ← This is the actual field in Wu page 19
                    'reverification_document_title',
                    'rehire_document_title', 
                    'list_a_document_title',
                    'section_2_list_a_document_title'  # ← Also check Section 2 fields in combined pages
                ]
                
                for field in doc_fields:
                    doc_title = extracted.get(field)
                    if doc_title and doc_title not in ['N/A', '', None]:
                        doc_number = extracted.get(field.replace('title', 'number'), '')
                        if doc_number and doc_number not in ['N/A', '', None]:
                            documents.append(f"{doc_title} (#{doc_number})")
                        else:
                            documents.append(doc_title)
            
            return documents
        
        def _extract_section_2_documents(pages):
            """Extract document information from Section 2 pages"""
            documents = []
            
            # Find Section 2 pages and their signature dates to get the latest one
            section_2_pages = []
            for page in pages:
                page_title = page.get('page_title', '').lower()
                extracted = page.get('extracted_values', {})
                
                # Enhanced Section 2 detection: check both title patterns AND presence of section_2/list fields
                is_section_2_by_title = ('section 2' in page_title or 'section 1 and 2' in page_title)
                is_section_2_by_fields = any(field in extracted for field in [
                    'list_a_document_title', 'list_b_document_title', 'list_c_document_title',
                    'section_2_list_a_document_title', 'section_2_list_b_document_title', 'section_2_list_c_document_title',
                    # Additional variations found in catalogs (for document extraction)
                    'list_a_document_title_1', 'list_a_document_1_title',  # ← Debek format
                    'additional_information_document_title_2', 'additional_information_document_title_3'  # ← Debek format
                ]) and extracted.get('employer_signature_date')  # Must have employer signature to be Section 2
                
                if is_section_2_by_title or is_section_2_by_fields:
                    # Get employer signature date for this page
                    employer_sig_date = extracted.get('employer_signature_date')
                    
                    section_2_pages.append({
                        'page_number': page.get('page_number'),
                        'signature_date': employer_sig_date or '',
                        'extracted': extracted
                    })
            
            # Sort by signature date to get the latest Section 2 page (using proper date comparison)
            if section_2_pages:
                def parse_date_for_comparison(date_str):
                    """Parse date string for proper comparison"""
                    try:
                        from datetime import datetime
                        return datetime.strptime(date_str, '%m/%d/%Y')
                    except:
                        return datetime.min  # Return minimum date if parsing fails
                
                latest_page = max(section_2_pages, key=lambda x: parse_date_for_comparison(x['signature_date']))
                extracted = latest_page['extracted']
                
                logger.info(f"Section 2 document extraction: Using page {latest_page['page_number']} with signature date {latest_page['signature_date']}")
                
                # Check Section 2 document fields (List A, B, C) - including actual fields found in catalogs
                doc_fields = [
                    'list_a_document_title', 'list_a_document_title_1', 'list_a_document_title_2',
                    'list_b_document_title', 'list_b_document_title_1', 'list_b_document_title_2',  # ← Drivers License
                    'list_c_document_title', 'list_c_document_title_1', 'list_c_document_title_2',  # ← SSN Account Card
                    'section_2_list_a_document_title', 'section_2_list_b_document_title', 'section_2_list_c_document_title',
                    # Additional variations found in catalogs
                    'list_a_document_1_title', 'list_a_document_2_title', 'list_a_document_3_title',  # ← De Lima format
                    'list_b_document_1_title', 'list_b_document_2_title', 'list_b_document_3_title',
                    'list_c_document_1_title', 'list_c_document_2_title', 'list_c_document_3_title',
                    # Additional information fields (Debek format)
                    'additional_information_document_title_2', 'additional_information_document_title_3'  # ← Debek format
                ]
                
                for field in doc_fields:
                    doc_title = extracted.get(field)
                    if doc_title and doc_title not in ['N/A', '', None]:
                        doc_number = extracted.get(field.replace('title', 'number'), '')
                        if doc_number and doc_number not in ['N/A', '', None]:
                            documents.append(f"{doc_title} (#{doc_number})")
                        else:
                            documents.append(doc_title)
            
            return documents
        
        def _find_latest_section_3_pages_for_docs(pages):
            """Find Section 3 pages with latest employer signature date for document extraction"""
            latest_signature_date = None
            latest_pages = []
            
            for page in pages:
                page_title = page.get('page_title', '').lower()
                is_section_3 = any([
                    'section 3' in page_title,
                    'section3' in page_title,
                    'reverification' in page_title,
                    'section 2 and 3' in page_title,
                    'section 2 & 3' in page_title
                ])
                
                if is_section_3:
                    extracted = page.get('extracted_values', {})
                    employer_sig_date = (
                        extracted.get('reverification_signature_date') or
                        extracted.get('employer_signature_date') or
                        extracted.get('section_3_employer_signature_date')
                    )
                    
                    if employer_sig_date:
                        if not latest_signature_date or employer_sig_date > latest_signature_date:
                            latest_signature_date = employer_sig_date
                            latest_pages = [page.get('page_number')]
                        elif employer_sig_date == latest_signature_date:
                            latest_pages.append(page.get('page_number'))
            
            return latest_pages
        
        def _check_document_attachments(listed_documents, pages):
            """Check which documents are found as attachments in the PDF"""
            found_documents = []
            not_found_documents = []
            
            # Get supporting document pages
            supporting_pages = [p for p in pages if 'supporting' in p.get('page_title', '').lower() or 
                              any(doc_type in p.get('page_title', '').lower() for doc_type in 
                                  ['passport', 'driver', 'license', 'card', 'certificate', 'i-94', 'ds-2019', 'ead'])]
            
            for listed_doc in listed_documents:
                # Extract document type and number from listed document
                doc_found = False
                
                # Simple matching - check if any supporting page title contains key terms from the listed document
                doc_lower = listed_doc.lower()
                
                for support_page in supporting_pages:
                    support_title = support_page.get('page_title', '').lower()
                    
                    # Check for common document type matches
                    if any(term in doc_lower and term in support_title for term in 
                           ['passport', 'driver', 'license', 'card', 'i-94', 'ds-2019', 'ead', 'certificate']):
                        doc_found = True
                        break
                
                if doc_found:
                    found_documents.append(listed_doc)
                else:
                    not_found_documents.append(listed_doc)
            
            return found_documents, not_found_documents
        
        def _extract_citizenship_from_catalog(catalog_path):
            """Extract citizenship status directly from catalog data"""
            try:
                with open(catalog_path, 'r', encoding='utf-8') as f:
                    catalog_data = json.load(f)
                
                pages = catalog_data.get('document_catalog', {}).get('pages', catalog_data.get('pages', []))
                
                # Look for citizenship status in Section 1 pages
                for page in pages:
                    page_title = page.get('page_title', '').lower()
                    if 'section 1' in page_title or 'employment eligibility' in page_title:
                        extracted = page.get('extracted_values', {})
                        
                        # Check various citizenship fields
                        citizenship_fields = [
                            extracted.get('citizenship_status'),
                            extracted.get('employee_citizenship_status'),
                            extracted.get('section_1_citizenship_status')
                        ]
                        
                        for citizenship in citizenship_fields:
                            if citizenship and citizenship not in ['N/A', '', None]:
                                citizenship_lower = str(citizenship).lower()
                                
                                # Map actual catalog values to our standard format
                                if any(term in citizenship_lower for term in ['citizen of the united states', 'us citizen', 'citizen']) and 'non' not in citizenship_lower:
                                    return 'US Citizen'
                                elif any(term in citizenship_lower for term in ['alien_authorized_to_work', 'noncitizen_authorized_to_work', 'an_alien_authorized_to_work', 'authorized', 'noncitizen']):
                                    return 'NON Citizen'
                                elif citizenship == '4':  # Some forms use numeric codes
                                    return 'NON Citizen'
                
                return None
                    
            except Exception as e:
                logger.warning(f"Error extracting citizenship from catalog: {e}")
                return None
        
        def _extract_ssn_from_catalog(catalog_path):
            """Extract SSN directly from catalog data"""
            try:
                with open(catalog_path, 'r', encoding='utf-8') as f:
                    catalog_data = json.load(f)
                
                pages = catalog_data.get('document_catalog', {}).get('pages', catalog_data.get('pages', []))
                
                # Look for SSN in Section 1 pages
                for page in pages:
                    page_title = page.get('page_title', '').lower()
                    if 'section 1' in page_title or 'employment eligibility' in page_title:
                        extracted = page.get('extracted_values', {})
                        
                        # Check various SSN fields
                        ssn_fields = [
                            extracted.get('ssn'),
                            extracted.get('social_security_number'),
                            extracted.get('employee_ssn'),
                            extracted.get('section_1_ssn'),
                            extracted.get('employee_social_security_number')
                        ]
                        
                        for ssn in ssn_fields:
                            if ssn and ssn not in ['N/A', '', None, 'None']:
                                return str(ssn)
                
                return None
                    
            except Exception as e:
                logger.warning(f"Error extracting SSN from catalog: {e}")
                return None
        
        def _extract_dob_from_catalog(catalog_path):
            """Extract date of birth directly from catalog data"""
            try:
                with open(catalog_path, 'r', encoding='utf-8') as f:
                    catalog_data = json.load(f)
                
                pages = catalog_data.get('document_catalog', {}).get('pages', catalog_data.get('pages', []))
                
                # Look for date of birth in Section 1 pages
                for page in pages:
                    page_title = page.get('page_title', '').lower()
                    if 'section 1' in page_title or 'employment eligibility' in page_title:
                        extracted = page.get('extracted_values', {})
                        
                        # Check various date of birth fields
                        dob_fields = [
                            extracted.get('date_of_birth'),
                            extracted.get('employee_date_of_birth'),
                            extracted.get('employee_dob'),  # ← This is the field in De Lima
                            extracted.get('birth_date'),
                            extracted.get('dob'),
                            extracted.get('section_1_date_of_birth'),
                            extracted.get('employee_birth_date')
                        ]
                        
                        for dob in dob_fields:
                            if dob and dob not in ['N/A', '', None, 'None']:
                                return str(dob)
                
                return None
                    
            except Exception as e:
                logger.warning(f"Error extracting date of birth from catalog: {e}")
                return None
        
        def _extract_names_from_catalog(catalog_path):
            """Extract names directly from catalog data"""
            try:
                with open(catalog_path, 'r', encoding='utf-8') as f:
                    catalog_data = json.load(f)
                
                pages = catalog_data.get('document_catalog', {}).get('pages', catalog_data.get('pages', []))
                
                # Look for names in Section 1 pages
                for page in pages:
                    page_title = page.get('page_title', '').lower()
                    if 'section 1' in page_title or 'employment eligibility' in page_title:
                        extracted = page.get('extracted_values', {})
                        
                        names = {}
                        
                        # Check various first name fields
                        first_name_fields = [
                            extracted.get('first_name'),
                            extracted.get('employee_first_name'),
                            extracted.get('section_1_first_name'),
                            extracted.get('employee_name_first')
                        ]
                        
                        for first_name in first_name_fields:
                            if first_name and first_name not in ['N/A', '', None, 'None']:
                                names['first_name'] = str(first_name)
                                break
                        
                        # Check various middle name fields
                        middle_name_fields = [
                            extracted.get('middle_name'),
                            extracted.get('employee_middle_name'),
                            extracted.get('employee_middle_initial'),
                            extracted.get('section_1_middle_name'),
                            extracted.get('middle_initial')
                        ]
                        
                        for middle_name in middle_name_fields:
                            if middle_name and middle_name not in ['N/A', '', None, 'None']:
                                names['middle_name'] = str(middle_name)
                                break
                        
                        # Check various last name fields
                        last_name_fields = [
                            extracted.get('last_name'),
                            extracted.get('employee_last_name'),
                            extracted.get('section_1_last_name'),
                            extracted.get('employee_name_last')
                        ]
                        
                        for last_name in last_name_fields:
                            if last_name and last_name not in ['N/A', '', None, 'None']:
                                names['last_name'] = str(last_name)
                                break
                        
                        if names:
                            return names
                
                return None
                    
            except Exception as e:
                logger.warning(f"Error extracting names from catalog: {e}")
                return None
        
        def _extract_work_auth_expiry_from_catalog(catalog_path):
            """Extract work authorization expiry date directly from catalog data"""
            try:
                with open(catalog_path, 'r', encoding='utf-8') as f:
                    catalog_data = json.load(f)
                
                pages = catalog_data.get('document_catalog', {}).get('pages', catalog_data.get('pages', []))
                
                # Look for work authorization expiry in Section 1 pages
                for page in pages:
                    page_title = page.get('page_title', '').lower()
                    if 'section 1' in page_title or 'employment eligibility' in page_title:
                        extracted = page.get('extracted_values', {})
                        
                        # Check various work authorization expiry fields (matching enhanced_processor.py logic)
                        work_auth_fields = [
                            extracted.get('work_auth_expiration_date'),
                            extracted.get('work_until_date'),  # ← This is the field in Balder
                            extracted.get('work_authorization_expiration_date'),
                            extracted.get('section_1_alien_authorized_to_work_until'),
                            extracted.get('alien_authorized_to_work_until'),
                            extracted.get('alien_authorized_to_work_until_date'),
                            extracted.get('alien_expiration_date')
                        ]
                        
                        for work_auth in work_auth_fields:
                            if work_auth and work_auth not in ['N/A', '', None, 'None']:
                                return str(work_auth)
                
                return None
                    
            except Exception as e:
                logger.warning(f"Error extracting work authorization expiry from catalog: {e}")
                return None

        try:
            from hri9.core.enhanced_processor import EnhancedI9Processor
            from pathlib import Path
            import json
            import csv
            
            logger.info("🔧 Generating comprehensive CSV from existing catalogs...")
            
            # Find all catalog files
            catalog_dir = Path("workdir/catalogs")
            catalog_files = list(catalog_dir.glob("*.catalog.json"))
            
            if catalog_files:
                # Initialize processor
                processor = EnhancedI9Processor()
                comprehensive_data = []
                
                # Process each catalog
                for catalog_path in catalog_files:
                    try:
                        pdf_name = catalog_path.stem.replace('.catalog', '.pdf')
                        pdf_path = Path("data/input") / pdf_name
                        
                        if pdf_path.exists():
                            # Process using existing catalog
                            result = processor.process_from_existing_catalog(pdf_path, str(catalog_path))
                            
                            # Clean up status
                            status = str(result.status).replace('ProcessingStatus.', '')
                            form_type = str(result.form_type_selected) if hasattr(result, 'form_type_selected') else ''
                            
                            # Initialize row data
                            row_data = {
                                'pdf_file': pdf_name,
                                'status': status,
                                'form_type_selected': form_type,
                                'first_name': '',
                                'middle_name': '',
                                'last_name': '',
                                'ssn': '',
                                'date_of_birth': '',
                                'citizenship_status': 'Unknown',
                                'authorized_to_work_until': '',
                                'document_matches_found': getattr(result, 'document_matches_found', 0),
                                'supporting_documents_count': getattr(result, 'supporting_documents_count', 0)
                            }
                            
                            # Extract I-9 data if available
                            if hasattr(result, 'primary_i9_data') and result.primary_i9_data:
                                i9_data = result.primary_i9_data
                                first = getattr(i9_data, 'first_name', '') or ''
                                middle = getattr(i9_data, 'middle_name', '') or getattr(i9_data, 'middle_initial', '') or ''
                                last = getattr(i9_data, 'last_name', '') or ''
                                ssn = getattr(i9_data, 'ssn', '') or getattr(i9_data, 'social_security_number', '') or ''
                                
                                row_data.update({
                                    'first_name': first,
                                    'middle_name': middle,
                                    'last_name': last,
                                    'ssn': ssn,
                                    'date_of_birth': getattr(i9_data, 'date_of_birth', '') or '',
                                    'authorized_to_work_until': getattr(i9_data, 'authorized_to_work_until', '') or '',
                                })
                                
                                # Enhanced citizenship status logic - check catalog data directly
                                citizenship_from_catalog = _extract_citizenship_from_catalog(catalog_path)
                                if citizenship_from_catalog:
                                    row_data['citizenship_status'] = citizenship_from_catalog
                                else:
                                    # Fallback to i9_data
                                    citizenship = str(getattr(i9_data, 'citizenship_status', 'Unknown')).upper()
                                    if any(term in citizenship for term in ['CITIZEN', 'US CITIZEN', 'U.S. CITIZEN', 'AMERICAN']):
                                        row_data['citizenship_status'] = 'US Citizen'
                                    elif any(term in citizenship for term in ['ALIEN', 'NON-CITIZEN', 'NONCITIZEN', 'PERMANENT RESIDENT', 'AUTHORIZED']):
                                        row_data['citizenship_status'] = 'NON Citizen'
                                    else:
                                        # If citizenship status is unclear, default based on work authorization
                                        auth_until = row_data.get('authorized_to_work_until', '')
                                        if auth_until and auth_until.strip():
                                            row_data['citizenship_status'] = 'NON Citizen'  # Has expiry date, likely non-citizen
                                        else:
                                            row_data['citizenship_status'] = 'US Citizen'  # No expiry date, likely US citizen
                                
                                # Enhanced SSN extraction from catalog data directly
                                ssn_from_catalog = _extract_ssn_from_catalog(catalog_path)
                                if ssn_from_catalog:
                                    row_data['ssn'] = ssn_from_catalog
                                
                                # Enhanced date of birth extraction from catalog data directly
                                dob_from_catalog = _extract_dob_from_catalog(catalog_path)
                                if dob_from_catalog:
                                    row_data['date_of_birth'] = dob_from_catalog
                                
                                # Enhanced name extraction from catalog data directly
                                names_from_catalog = _extract_names_from_catalog(catalog_path)
                                if names_from_catalog:
                                    if names_from_catalog.get('first_name'):
                                        row_data['first_name'] = names_from_catalog['first_name']
                                    if names_from_catalog.get('middle_name'):
                                        row_data['middle_name'] = names_from_catalog['middle_name']
                                    if names_from_catalog.get('last_name'):
                                        row_data['last_name'] = names_from_catalog['last_name']
                                
                                # Enhanced work authorization expiry extraction from catalog data directly
                                work_auth_from_catalog = _extract_work_auth_expiry_from_catalog(catalog_path)
                                if work_auth_from_catalog:
                                    row_data['authorized_to_work_until'] = work_auth_from_catalog
                            
                            # Add expiry date matching logic following business rules hierarchy
                            row_data['expiry_date_matches'] = _calculate_expiry_date_matches(result, catalog_path)
                            
                            # Add document tracking logic following business rules hierarchy
                            document_info = _calculate_document_tracking(result, catalog_path)
                            row_data['documents_listed'] = document_info['documents_listed']
                            row_data['documents_found'] = document_info['documents_found'] 
                            row_data['documents_not_found'] = document_info['documents_not_found']
                            
                            comprehensive_data.append(row_data)
                            logger.info(f"✅ Processed {pdf_name}: {status}")
                            
                    except Exception as e:
                        logger.error(f"❌ Failed to process {catalog_path.name}: {str(e)}")
                
                # Write comprehensive CSV
                if comprehensive_data:
                    csv_path = Path("workdir/comprehensive_i9_results_clean.csv")
                    
                    fieldnames = ['pdf_file', 'status', 'form_type_selected', 'first_name', 'middle_name', 'last_name', 
                                 'ssn', 'date_of_birth', 'citizenship_status', 'authorized_to_work_until',
                                 'expiry_date_matches', 'documents_listed', 'documents_found', 'documents_not_found',
                                 'document_matches_found', 'supporting_documents_count']
                    
                    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(comprehensive_data)
                    
                    logger.info(f"✅ Clean comprehensive CSV generated: {csv_path} ({len(comprehensive_data)} records)")
                else:
                    logger.warning("No data processed for comprehensive CSV")
            else:
                logger.warning("No catalog files found for comprehensive CSV generation")
                
        except Exception as e:
            logger.error(f"❌ Comprehensive CSV generation failed: {str(e)}")
            logger.warning(f"⚠️ Comprehensive CSV generation had issues: {str(e)}")
        
    except Exception as e:
        logger.warning(f"Could not generate comprehensive CSV: {e}")


def finalize_batch_processing(batch_results, input_folder: str):
    """Print final summary with input directory info"""
    stats = batch_results['statistics']
    
    logger.info("🎯 Automated Batch Processing Workflow Complete!")
    logger.info("=" * 60)
    logger.info(f"📁 Input Directory: {Path(input_folder).absolute()}")
    logger.info(f"📊 Processing Results:")
    logger.info(f"   - Total files: {stats['total_files']}")
    logger.info(f"   - Successful: {stats['success']}")
    logger.info(f"   - Partial: {stats['partial']}")
    logger.info(f"   - Errors: {stats['errors']}")
    logger.info(f"   - Success Rate: {(stats['success'] / max(stats['total_files'], 1)) * 100:.1f}%")
    logger.info("=" * 60)
    logger.info("📋 Generated Reports:")
    logger.info("   - Comprehensive CSV: workdir/enhanced_reports/")
    logger.info("   - Processing Summary: workdir/enhanced_reports/")
    logger.info("   - Categorized Results: workdir/categorized_results/")
    logger.info("=" * 60)


def handle_batch_processing(args):
    """
    Complete automated batch processing workflow for user-specified input directory
    
    Args:
        args: Command line arguments containing batch processing options
        
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    try:
        # STEP 1: Validate user-specified input folder
        if not args.input_folder:
            logger.error("--input-folder is required when using --batch-mode")
            logger.error("Example: --input-folder /path/to/your/pdf/files")
            return 1
        
        input_path = Path(args.input_folder)
        if not input_path.exists():
            logger.error(f"Input folder does not exist: {args.input_folder}")
            return 1
        
        logger.info("🚀 Starting Complete Automated Batch Processing Workflow")
        logger.info(f"📁 Input Directory: {input_path.absolute()}")
        logger.info("=" * 60)
        
        # STEP 2: Prepare PDFs in the specified directory
        logger.info("📁 STEP 1: Preparing PDF files...")
        pdf_count = prepare_input_directory(args.input_folder)
        
        if pdf_count == 0:
            logger.error("No PDF files found in the specified directory")
            return 1
        
        logger.info(f"✅ Found {pdf_count} PDF files ready for processing")
        
        # STEP 3: Check for existing catalogs or generate new ones
        logger.info("📋 STEP 2: Checking for catalog files...")
        
        # Check if we should use existing catalogs
        catalog_dir = Path(args.catalog_export_path)
        existing_catalogs = list(catalog_dir.glob("*.catalog.json")) if catalog_dir.exists() else []
        
        if existing_catalogs:
            logger.info(f"✅ Found {len(existing_catalogs)} existing catalog files - using existing catalogs")
            catalog_count = len(existing_catalogs)
        else:
            logger.info("No existing catalogs found - generating new catalog files...")
            catalog_count = generate_catalogs_for_directory(args.input_folder, args.catalog_export_path)
            
            if catalog_count == 0:
                logger.error("No catalogs were generated successfully")
                return 1
            
            logger.info(f"✅ Generated {catalog_count} catalog files")
        
        # STEP 4: Process all PDFs using existing catalogs
        logger.info("🔄 STEP 3: Processing I-9 forms...")
        batch_results = process_batch_files(args)
        
        # STEP 5: Generate consolidated comprehensive report
        logger.info("📊 STEP 4: Generating comprehensive reports...")
        generate_consolidated_reports(batch_results, args)
        
        # Generate batch processing summary
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path("workdir/enhanced_reports")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        statistics = batch_results['statistics']
        
        summary_file = output_dir / f"i9_batch_processing_summary_{timestamp}.csv"
        with open(summary_file, 'w', newline='', encoding='utf-8') as f:
            import csv
            writer = csv.writer(f)
            writer.writerow(['Metric', 'Count'])
            writer.writerow(['Total Files Found', statistics['total_files']])
            writer.writerow(['Successfully Processed', statistics['success']])
            writer.writerow(['Partially Processed', statistics['partial']])
            writer.writerow(['Processing Errors', statistics['errors']])
            writer.writerow(['Overall Success Rate', f"{(statistics['success'] / max(statistics['total_files'], 1)) * 100:.1f}%"])
        
        logger.info(f"📋 Batch processing summary: {summary_file}")
        
        # Generate error log if there were errors
        if batch_results['error_results']:
            error_file = output_dir / f"i9_batch_error_log_{timestamp}.csv"
            
            with open(error_file, 'w', newline='', encoding='utf-8') as f:
                import csv
                writer = csv.DictWriter(f, fieldnames=['pdf_file', 'error', 'catalog_path'])
                writer.writeheader()
                writer.writerows(batch_results['error_results'])
            
            logger.warning(f"⚠️ Error log generated: {error_file}")
        
        # STEP 6: Generate summary and cleanup
        logger.info("🎯 STEP 5: Finalizing batch processing...")
        finalize_batch_processing(batch_results, args.input_folder)
        
        return 0 if statistics['errors'] == 0 else 1
        
    except Exception as e:
        logger.error(f"Error in automated batch processing workflow: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return 1


def main():
    """
    Main function to run the I-9 detection and processing system.
    """
    # Parse command line arguments
    args = parse_arguments()
    
    # Handle catalog system enable/disable logic
    enable_catalog = args.enable_catalog and not args.disable_catalog
    
    # Configure logging level
    if args.verbose:
        logger.setLevel('DEBUG')
    
    # Print comprehensive configuration information
    print_configuration_info(args, enable_catalog)
    
    # Handle special catalog-only modes
    if args.catalog_validate:
        return handle_catalog_validation(args)
    
    if args.catalog_import:
        return handle_catalog_import(args)
    
    # Handle batch processing mode
    if args.batch_mode:
        return handle_batch_processing(args)
    
    # Handle existing catalog processing mode
    if args.use_existing_catalog:
        return handle_existing_catalog_processing(args)
    
    # Handle data-only mode
    if args.data_only:
        logger.info("Starting I-9 system in data-only mode (no PDF manipulation)")
        
        # Get employee IDs
        if args.use_local:
            # Get sample employee IDs from local data directory
            sample_dir = settings.LOCAL_SAMPLE_PATH
            if os.path.exists(sample_dir):
                employee_ids = [d for d in os.listdir(sample_dir) 
                               if os.path.isdir(os.path.join(sample_dir, d)) and d.isdigit()]
            else:
                employee_ids = ['0540']  # Default test employee
        else:
            # For network drive, would need to implement employee ID discovery
            logger.error("Network drive employee ID discovery not implemented for data-only mode")
            return 1
        
        if args.limit:
            employee_ids = employee_ids[:args.limit]
        
        if not employee_ids:
            logger.error("No employee IDs found to process")
            return 1
        
        logger.info(f"Processing {len(employee_ids)} employees in data-only mode")
        
        # Process employees with data-only mode
        success_count, partial_count, error_count = process_all_employees_data_only(
            employee_ids, 
            workers=args.workers, 
            use_local=args.use_local
        )
        
        total_count = success_count + partial_count + error_count
        
        if total_count > 0:
            logger.info(f"Data-only processing completed: {success_count} success, {partial_count} partial, {error_count} errors")
            
            # Auto-generate comprehensive CSV report after processing completion
            try:
                logger.info("🔄 Auto-generating comprehensive CSV report...")
                
                # Try direct import first (more efficient)
                try:
                    # Change to the parent directory to import generate_comprehensive_csv
                    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                    sys.path.insert(0, parent_dir)
                    
                    import generate_comprehensive_csv
                    
                    # Call the main function directly
                    generate_comprehensive_csv.main()
                    logger.info("✅ Comprehensive CSV report generated successfully via direct import")
                    logger.info("📋 Check workdir/enhanced_reports/ for comprehensive CSV files")
                    
                except ImportError:
                    # Fallback to subprocess if direct import fails
                    logger.info("Direct import failed, using subprocess...")
                    import subprocess
                    
                    result = subprocess.run([
                        sys.executable, 
                        "generate_comprehensive_csv.py"
                    ], 
                    cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    capture_output=True, 
                    text=True, 
                    timeout=30
                    )
                    
                    if result.returncode == 0:
                        logger.info("✅ Comprehensive CSV report generated successfully via subprocess")
                        logger.info("📋 Check workdir/enhanced_reports/ for comprehensive CSV files")
                        # Log key output lines
                        for line in result.stdout.strip().split('\n'):
                            if line.strip() and ('Generated' in line or 'Report contains' in line):
                                logger.info(f"CSV Generator: {line}")
                    else:
                        logger.warning(f"⚠️ Comprehensive CSV generation completed with warnings: {result.stderr}")
                        
            except subprocess.TimeoutExpired:
                logger.warning("⚠️ Comprehensive CSV generation timed out after 30 seconds")
            except Exception as e:
                logger.error(f"❌ Failed to auto-generate comprehensive CSV: {str(e)}")
            
            return 0
        else:
            logger.error("No documents were processed successfully")
            return 1
    
    if args.catalog_only:
        logger.info("Starting I-9 system in catalog-only mode")
    else:
        logger.info(f"Starting I-9 detection system in {args.mode} mode")
    
    # Determine skip_existing_catalog setting
    skip_existing_catalog = args.skip_existing_catalog and not args.force_catalog_regeneration
    
    logger.info(f"Configuration: workers={args.workers}, limit={args.limit}, batch_size={args.batch_size}, use_local={args.use_local}")
    logger.info(f"Catalog system: enabled={enable_catalog}, catalog_only={args.catalog_only}")
    logger.info(f"Data extraction: extract_all_data={args.extract_all_data}")
    logger.info(f"Catalog files: skip_existing={skip_existing_catalog}")
    
    if args.debug_files:
        logger.info(f"Debug mode: filtering files by pattern '{args.debug_files}'")
    
    if enable_catalog:
        logger.info(f"Catalog settings: cache_size={args.catalog_cache_size}, confidence_threshold={args.catalog_confidence_threshold}")
        logger.info(f"Catalog export: format={args.catalog_export_format}, path={args.catalog_export_path}, include_pii={args.catalog_include_pii}")
    
    # Determine data source with enhanced network drive validation
    use_local = args.use_local
    if not use_local:
        logger.info("Performing network drive health check...")
        if not check_network_drive_health(settings.NETWORK_DRIVE_PATH):
            logger.error(f"Network drive health check failed: {settings.NETWORK_DRIVE_PATH}")
            logger.info("Falling back to local sample data")
            use_local = True
        else:
            logger.info("Network drive is healthy and accessible")
    
    # Read employee IDs from Excel
    employee_ids = ExcelReader.read_employee_ids(args.excel_file)
    
    if not employee_ids:
        logger.error("No employee IDs found. Exiting.")
        return 1
    
    # Validate debug pattern if provided
    if args.debug_files:
        logger.info(f"Validating debug pattern: '{args.debug_files}'")
        validation = FileFilter.validate_debug_pattern(args.debug_files, employee_ids, use_local)
        
        if not validation['valid']:
            logger.error(f"Debug pattern validation failed: {validation['error']}")
            if validation.get('suggestions'):
                logger.info(f"Suggested employee IDs that might match: {validation['suggestions']}")
            return 1
        else:
            logger.info(f"Debug pattern '{args.debug_files}' will match {validation['matches']} out of {validation['total_employees']} employees")
            if validation['matches'] > 10:
                logger.warning(f"Debug pattern matches many files ({validation['matches']}). Consider using a more specific pattern.")
            logger.info(f"First few matches: {validation['matched_ids']}")
            
            # Ask for confirmation if not in batch mode
            if validation['matches'] > 5:
                logger.info(f"Debug mode will process {validation['matches']} files. Continuing in 3 seconds...")
                time.sleep(3)
    
    # Process all employees concurrently
    processed, found_i9, removed_i9, extracted_i9 = process_all_employees_concurrent(
        employee_ids,
        use_local=use_local,
        workers=args.workers,
        limit=args.limit,
        batch_size=args.batch_size,
        mode=args.mode,
        enable_catalog=enable_catalog,
        catalog_only=args.catalog_only,
        catalog_export_path=args.catalog_export_path,
        catalog_export_format=args.catalog_export_format,
        catalog_include_pii=args.catalog_include_pii,
        catalog_confidence_threshold=args.catalog_confidence_threshold,
        catalog_text_regions=args.catalog_text_regions,
        catalog_structured_extraction=args.catalog_structured_extraction,
        extract_all_data=args.extract_all_data,
        skip_existing_catalog=skip_existing_catalog,
        debug_files_pattern=args.debug_files,
        use_enhanced_processor=True  # Enable enhanced processor with business rules
    )
    
    # Final summary
    if not args.catalog_only:
        logger.info(f"Results saved to {os.path.join(args.output_dir, os.path.basename(args.output_csv))}")
        
        if args.mode in ['extract', 'all']:
            logger.info(f"Extracted I-9 forms saved to {args.extract_dir}")
        
        if args.mode in ['remove', 'all']:
            logger.info(f"Cleaned PDFs saved to {args.cleaned_dir}")
    
    if args.enable_catalog:
        logger.info(f"Catalog data exported to {args.catalog_export_path}")
    
    return 0

if __name__ == "__main__":
    exit(main())
