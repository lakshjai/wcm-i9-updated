#!/usr/bin/env python3
"""
Main entry point for the I-9 processing system.

This module provides data extraction and cataloging functionality for I-9 documents
without PDF manipulation. Results are categorized into SUCCESS, PARTIAL_SUCCESS, and ERROR files.
"""

import os
import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add the parent directory to the path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from .config import settings
    from .utils.logging_config import logger
    from .utils.categorized_reporter import CategorizedReporter
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
    from hri9.core.enhanced_processor import EnhancedI9Processor
    from hri9.api.gemini_client import GeminiClient
    from hri9.catalog.cache import CatalogCache
    from hri9.data.file_manager import FileManager
    from hri9.cli.arguments import parse_arguments
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
            elif hasattr(enhanced_processor, 'document_catalog'):
                # Try to get catalog entry from the processor's cache
                doc_cache = enhanced_processor.document_catalog.catalog_cache
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
        catalog_source = enhanced_processor.document_catalog
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
    
    # Handle special catalog-only modes
    if args.catalog_validate:
        return handle_catalog_validation(args)
    
    if args.catalog_import:
        return handle_catalog_import(args)
    
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
