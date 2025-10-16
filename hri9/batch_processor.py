#!/usr/bin/env python3
"""
Batch Processor for HRI9 System

This module handles batch processing of multiple PDF files in a directory,
generating consolidated comprehensive reports for all employees.
"""

import os
import glob
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional
from datetime import datetime

from .utils.logging_config import logger
from .core.models import I9FormData
from .review_queue import ReviewQueueManager
from .core.models import ProcessingResult, ProcessingStatus


class BatchProcessor:
    """Handles batch processing of I-9 forms from input directories"""
    
    def __init__(self, catalog_export_path=None):
        """Initialize the batch processor"""
        self.enhanced_processor = None
        self.enhanced_results = {}  # Store enhanced processor results by filename
        self.review_queue = ReviewQueueManager()  # Initialize review queue manager
        self.catalog_export_path = Path(catalog_export_path) if catalog_export_path else Path("workdir/catalogs")
        # Import EnhancedI9Processor dynamically to avoid circular imports
        try:
            from .core.enhanced_processor import EnhancedI9Processor
            self.processor = EnhancedI9Processor()
        except ImportError:
            logger.warning("EnhancedI9Processor not available, using basic processing")
            self.processor = None
        
    def discover_pdf_files(self, input_folder: str) -> List[Tuple[str, str]]:
        """
        Discover all PDF files in input folder and map to corresponding catalog files
        
        Args:
            input_folder: Path to directory containing PDF files
            
        Returns:
            List of (pdf_path, catalog_path) tuples
        """
        input_path = Path(input_folder)
        
        if not input_path.exists():
            logger.error(f"Input folder does not exist: {input_folder}")
            return []
        
        # Find all PDF files (case-insensitive)
        pdf_files = []
        for pattern in ["*.pdf", "*.PDF"]:
            pdf_files.extend(input_path.glob(pattern))
        logger.info(f"Found {len(pdf_files)} PDF files in {input_folder}")
        
        # Map PDF files to catalog files
        file_mappings = []
        
        for pdf_file in pdf_files:
            catalog_name = pdf_file.stem + ".catalog.json"
            catalog_path = self.catalog_export_path / catalog_name
            
            file_mappings.append((str(pdf_file), str(catalog_path)))
            
            if catalog_path.exists():
                logger.debug(f"Mapped: {pdf_file.name} → {catalog_name}")
            else:
                logger.warning(f"Missing catalog for: {pdf_file.name} (expected: {catalog_name})")
        
        return file_mappings
    
    def find_corresponding_catalog(self, pdf_file: str) -> Optional[str]:
        """
        Find corresponding catalog file for a PDF file
        
        Args:
            pdf_file: Path to PDF file
            
        Returns:
            Path to catalog file if found, None otherwise
        """
        pdf_path = Path(pdf_file)
        
        # Try exact name match first
        catalog_name = pdf_path.stem + ".catalog.json"
        catalog_path = self.catalog_export_path / catalog_name
        
        if catalog_path.exists():
            return str(catalog_path)
        
        # Try fuzzy matching for different naming conventions
        possible_patterns = [
            f"{pdf_path.stem}.catalog.json",
            f"*{pdf_path.stem}*.catalog.json",
            f"{pdf_path.stem.replace(' ', '_')}.catalog.json",
            f"{pdf_path.stem.replace('_', ' ')}.catalog.json"
        ]
        
        for pattern in possible_patterns:
            matches = list(self.catalog_export_path.glob(pattern))
            if matches:
                logger.info(f"Found catalog via fuzzy match: {matches[0].name} for {pdf_path.name}")
                return str(matches[0])
        
        logger.warning(f"No catalog found for PDF: {pdf_path.name}")
        return None
    
    def process_single_catalog(self, catalog_path: str, pdf_path: str) -> Optional[ProcessingResult]:
        """
        Process a single catalog file using existing HRI9 logic
        
        Args:
            catalog_path: Path to catalog file
            pdf_path: Path to original PDF file
            
        Returns:
            ProcessingResult if successful, None if failed
        """
        try:
            logger.info(f"🔧 [BATCH] Processing catalog: {Path(catalog_path).name}")
            
            # Use EnhancedI9Processor's built-in catalog processing instead of manual reconstruction
            from .core.enhanced_processor import EnhancedI9Processor
            from pathlib import Path as PathLib
            
            # Initialize processor if not already done
            if not hasattr(self, 'enhanced_processor') or self.enhanced_processor is None:
                self.enhanced_processor = EnhancedI9Processor()
            
            # Load catalog and manually process it
            from .core.catalog_adapters import CatalogProcessor
            import json
            
            # Load catalog data
            with open(catalog_path, 'r', encoding='utf-8') as f:
                catalog_data = json.load(f)
            
            # Create PDFAnalysis using CatalogProcessor (same way EnhancedI9Processor does it)
            pdf_analysis = CatalogProcessor.create_pdf_analysis_from_catalog(catalog_data, str(pdf_path))
            
            # Group I-9 sets using the set grouper
            from .core.set_grouping import I9SetGrouper
            grouper = I9SetGrouper()
            i9_sets = grouper.group_pages_into_sets(pdf_analysis)
            
            logger.info(f"✅ [BATCH] Grouped {len(i9_sets)} I-9 sets from catalog data")
            
            if not i9_sets:
                logger.warning(f"No I-9 sets found in catalog: {Path(catalog_path).name}")
                # Still return a result for catalog processing, just with no sets
                from .core.models import ProcessingStatus
                
                class CatalogProcessingResult:
                    def __init__(self, filename, status=ProcessingStatus.ERROR):
                        self.filename = filename
                        self.status = status
                        self.scenario_name = filename
                        self.primary_i9_data = None
                        self.document_matches_found = 0
                        self.supporting_documents_count = 0
                        self.form_type_selected = 'unknown'
                
                return [CatalogProcessingResult(Path(pdf_path).name, ProcessingStatus.ERROR)]
            
            # Select best I-9 set
            from .core.form_classifier import I9FormSelector
            form_selector = I9FormSelector()
            selected_set = form_selector.select_best_i9_set(i9_sets)
            
            if not selected_set:
                logger.error(f"❌ No I-9 set could be selected for {Path(catalog_path).name}")
                from .core.models import ProcessingStatus
                class CatalogProcessingResult:
                    def __init__(self, filename, status=ProcessingStatus.ERROR):
                        self.filename = filename
                        self.status = status
                        self.scenario_name = filename
                        self.primary_i9_data = None
                        self.document_matches_found = 0
                        self.supporting_documents_count = 0
                        self.form_type_selected = 'unknown'
                return [CatalogProcessingResult(Path(pdf_path).name, ProcessingStatus.ERROR)]
            
            logger.info(f"Selected I-9 set: '{selected_set.set_id}' using catalog data")
            
            # Validate selected set
            from .validation.comprehensive_validator import ComprehensiveValidator
            validator = ComprehensiveValidator()
            validation_result = validator.validate_i9_set(selected_set)
            
            # Store enhanced processor results for CSV generation
            filename = Path(pdf_path).name
            catalog_name = Path(catalog_path).stem  # Get catalog name without extension
            
            # Store with multiple keys for better matching
            # Extract the inner document_catalog data so pages are directly accessible
            inner_catalog_data = catalog_data.get('document_catalog', catalog_data)
            
            enhanced_result_data = {
                'selected_set': selected_set,
                'validation_result': validation_result,
                'pdf_analysis': pdf_analysis,
                'i9_sets': i9_sets,
                'catalog_data': inner_catalog_data,  # Store inner catalog data with pages at top level
                'filename': filename,
                'catalog_name': catalog_name
            }
            
            self.enhanced_results[filename] = enhanced_result_data
            self.enhanced_results[catalog_name] = enhanced_result_data
            
            logger.info(f"📊 Stored enhanced results for: {filename} (catalog: {catalog_name})")
            
            # Create a ProcessingResult-like object from catalog data
            # Don't call scenario_processor as it expects fresh AI extraction
            from .core.models import ProcessingStatus
            
            class CatalogProcessingResult:
                def __init__(self, filename, status=ProcessingStatus.COMPLETE_SUCCESS, primary_i9_data=None, document_matches=0, supporting_docs=0, form_type='new_hire'):
                    self.filename = filename
                    self.status = status
                    self.scenario_name = filename
                    self.primary_i9_data = primary_i9_data
                    self.document_matches_found = document_matches
                    self.supporting_documents_count = supporting_docs
                    self.form_type_selected = form_type
            
            # Determine status based on validation
            status = ProcessingStatus.COMPLETE_SUCCESS
            if validation_result and hasattr(validation_result, 'overall_score'):
                if validation_result.overall_score < 50:
                    status = ProcessingStatus.ERROR
                elif validation_result.overall_score < 80:
                    status = ProcessingStatus.PARTIAL_SUCCESS
            
            # Extract employee data from selected set for CSV export
            primary_i9_data = None
            if selected_set:
                # Get Section 1 data from catalog
                latest_section_1_data = self._get_fallback_section_1_data(inner_catalog_data)
                if latest_section_1_data:
                    # Create a simple object with the employee data
                    class I9Data:
                        def __init__(self, data):
                            self.first_name = data.get('employee_first_name', '')
                            self.last_name = data.get('employee_last_name', '')
                            self.ssn = data.get('employee_social_security_number', '')
                            self.date_of_birth = data.get('employee_date_of_birth', '')
                            self.citizenship_status = data.get('citizenship_status', '')
                            self.authorized_to_work_until = data.get('alien_work_until_date', '')
                    
                    primary_i9_data = I9Data(latest_section_1_data)
                    
                    # DEBUG: Log created I9Data object
                    if 'FUTNANI' in filename:
                        logger.info(f"🔍 [BATCH] FUTNANI I9Data created: first='{primary_i9_data.first_name}', last='{primary_i9_data.last_name}'")
            
            # Get document matches and supporting docs count
            doc_matches = getattr(validation_result, 'document_matches_found', 0) if validation_result else 0
            supporting_docs = len(getattr(selected_set, 'supplement_b_pages', []) or [])
            form_type = getattr(selected_set, 'set_type', 'new_hire') if selected_set else 'new_hire'
            
            processing_result = CatalogProcessingResult(filename, status, primary_i9_data, doc_matches, supporting_docs, form_type)
            
            # DEBUG: Verify processing_result has the data
            if 'FUTNANI' in filename and processing_result.primary_i9_data:
                logger.info(f"🔍 [BATCH] FUTNANI CatalogProcessingResult: first='{processing_result.primary_i9_data.first_name}', last='{processing_result.primary_i9_data.last_name}'")
            
            logger.info(f"✅ [BATCH] Successfully processed: {Path(catalog_path).name} with status: {status}")
            return [processing_result]  # Return as list to match expected format
            
        except Exception as e:
            logger.error(f"❌ Error processing {Path(catalog_path).name}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None
    
    def _create_pdf_analysis_from_catalog(self, catalog_data: Dict[str, Any], pdf_path: str) -> 'PDFAnalysis':
        """Create PDFAnalysis object from catalog data"""
        from .core.models import PDFAnalysis
        from .catalog.models import DocumentCatalogEntry, PageAnalysis
        
        filename = Path(pdf_path).name
        
        # Extract page information from catalog
        pages_data = catalog_data.get('pages', [])
        
        # Convert pages to PageAnalysis objects
        page_objects = []
        i9_pages = []
        
        for page_data in pages_data:
            # Create PageAnalysis object from page data
            page_analysis = PageAnalysis(
                page_number=page_data.get('page_number', 0),
                page_title=page_data.get('page_title', ''),
                page_type=page_data.get('page_type', ''),
                page_subtype=page_data.get('page_subtype', ''),
                confidence_score=page_data.get('confidence_score', 0.0),
                extracted_values=page_data.get('extracted_values', {}),
                text_regions=page_data.get('text_regions', [])
            )
            page_objects.append(page_analysis)
            
            # Track I-9 pages
            if page_data.get('page_subtype') == 'i9_form':
                i9_pages.append(page_data.get('page_number', 0))
        
        # Import required model classes
        from .catalog.models import DocumentMetadata, DocumentClassification, ProcessingSummary
        import os
        
        # Get file size from actual PDF file
        file_size = 0
        if os.path.exists(pdf_path):
            file_size = os.path.getsize(pdf_path)
        
        # Prepare metadata with file_size
        metadata_dict = catalog_data.get('document_metadata', {})
        if 'file_size' not in metadata_dict:
            metadata_dict['file_size'] = file_size
        
        # Create DocumentCatalogEntry object
        catalog_entry = DocumentCatalogEntry(
            document_id=catalog_data.get('document_id', ''),
            document_name=catalog_data.get('document_name', filename),
            total_pages=catalog_data.get('total_pages', len(pages_data)),
            processing_timestamp=catalog_data.get('processing_timestamp', ''),
            document_metadata=DocumentMetadata(**metadata_dict),
            pages=page_objects,
            document_classification=DocumentClassification(catalog_data.get('document_classification', {}).get('primary_type', 'unknown')),
            processing_summary=ProcessingSummary(
                total_pages_analyzed=catalog_data.get('processing_summary', {}).get('total_pages_analyzed', len(pages_data)),
                api_calls_made=catalog_data.get('processing_summary', {}).get('api_calls_made', 0),
                processing_time_seconds=catalog_data.get('processing_summary', {}).get('processing_time_seconds', 0.0)
            )
        )
        
        # Create catalog data structure expected by grouper
        structured_catalog_data = {
            'catalog_entry': catalog_entry
        }
        
        pdf_analysis = PDFAnalysis(
            filename=filename,
            total_pages=len(pages_data),
            i9_pages=i9_pages,
            catalog_data=structured_catalog_data
        )
        
        return pdf_analysis
    
    def _generate_main_csv(self, success_results: List, partial_results: List, error_results: List):
        """Generate the comprehensive i9_results.csv file"""
        import csv
        from datetime import datetime
        
        # Create comprehensive CSV file path
        main_csv_path = Path("workdir/comprehensive_i9_results.csv")
        main_csv_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Define all CSV fieldnames in the exact order specified
        fieldnames = [
            'filename', 'employee_last_name', 'employee_first_name', 'employee_middle_initial',
            'employee_other_names', 'employee_date_of_birth', 'employee_ssn', 'citizenship_status',
            'alien_registration_number', 'alien_work_until_date', 'total_i9_sets_detected',
            'i9_sets_summary', 'selected_set_pages', 'selected_set_type', 'selection_reasoning',
            'priority_hierarchy_applied', 'has_section_1', 'has_section_2', 'has_section_3',
            'has_supplement_b', 'validation_status', 'section_2_documents', 'section_3_documents',
            'supplement_b_documents', 'expiry_date_matches', 'total_pages', 'employee_signature_date',
            'employer_signature_date', 'first_day_employment', 'section_3_signature_date',
            'latest_signature_date', 'document_expiry_dates', 'supporting_documents_count',
            'validation_score', 'validation_rules_applied', 'validation_failures',
            'validation_warnings', 'critical_issues', 'minor_issues', 'processing_status',
            'error_messages', 'warnings', 'recommendations', 'manual_review_required',
            'quality_score', 'document_references_found', 'document_matches_found',
            'unmatched_documents', 'attachment_validation_results'
        ]
        
        # Prepare CSV data
        csv_data = []
        
        # Process all enhanced results directly (avoid duplicates)
        processed_files = set()
        
        # Process enhanced results directly
        for filename, enhanced_data in self.enhanced_results.items():
            if '.pdf' in filename and filename not in processed_files:
                # Create a mock scenario result for consistency
                class MockScenarioResult:
                    def __init__(self, name):
                        self.scenario_name = name
                
                mock_result = MockScenarioResult(filename)
                csv_row = self._extract_from_enhanced_processor_results(mock_result, 'COMPLETE_SUCCESS')
                csv_data.append(csv_row)
                processed_files.add(filename)
        
        # If no enhanced results were processed, fall back to scenario results
        if not csv_data:
            for result_list in success_results:
                if isinstance(result_list, list) and len(result_list) > 0:
                    primary_result = result_list[0]
                    csv_row = self._extract_from_enhanced_processor_results(primary_result, 'COMPLETE_SUCCESS')
                    actual_filename = csv_row.get('filename', 'Unknown')
                    if actual_filename not in processed_files:
                        csv_data.append(csv_row)
                        processed_files.add(actual_filename)
        
        # Process partial results
        for result_list in partial_results:
            if isinstance(result_list, list) and len(result_list) > 0:
                primary_result = result_list[0]
                csv_row = self._extract_comprehensive_data(primary_result, 'PARTIAL_SUCCESS')
                csv_data.append(csv_row)
        
        # Process error results
        for error_info in error_results:
            csv_row = self._create_error_row(error_info)
            csv_data.append(csv_row)
        
        # Evaluate all files for review queue
        logger.info("🔍 Evaluating files for manual review queue...")
        for csv_row in csv_data:
            filename = csv_row.get('filename', csv_row.get('pdf_file_name', 'Unknown'))
            self.review_queue.evaluate_for_review(filename, csv_row)
        
        # Generate review queue report
        review_summary = self.review_queue.get_review_summary()
        if review_summary['needs_review']:
            review_report_path = self.review_queue.generate_review_queue_report()
            logger.warning(f"📋 {review_summary['total_files']} files need manual review!")
            logger.warning(f"   🚨 {review_summary['high_priority']} HIGH priority")
            logger.warning(f"   ⚠️ {review_summary['medium_priority']} MEDIUM priority") 
            logger.warning(f"   ℹ️ {review_summary['low_priority']} LOW priority")
        else:
            logger.info("✅ All files processed successfully - no manual review needed!")
        
        # Write CSV file
        if csv_data:
            with open(main_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(csv_data)
            
            logger.info(f"✅ Comprehensive CSV generated: {main_csv_path} ({len(csv_data)} records)")
        else:
            logger.warning("No data to write to comprehensive CSV file")
    
    def _extract_comprehensive_data(self, scenario_result, processing_status: str) -> dict:
        """Extract comprehensive data from a scenario result for CSV output"""
        
        # Helper function to format dates
        def format_date(date_str):
            if not date_str or date_str in ['N/A', '', None]:
                return 'N/A'
            try:
                # Try to parse and reformat the date
                from datetime import datetime
                if '/' in str(date_str):
                    # Already in MM/DD/YYYY format
                    return str(date_str)
                elif '-' in str(date_str):
                    # Convert from YYYY-MM-DD or MM-DD-YYYY
                    parts = str(date_str).split('-')
                    if len(parts) == 3:
                        if len(parts[0]) == 4:  # YYYY-MM-DD
                            return f"{parts[1]}/{parts[2]}/{parts[0]}"
                        else:  # MM-DD-YYYY
                            return f"{parts[0]}/{parts[1]}/{parts[2]}"
                return str(date_str)
            except:
                return 'N/A'
        
        # Helper function to format lists with pipe separator
        def format_list(items):
            if not items:
                return 'N/A'
            if isinstance(items, list):
                return ' | '.join(str(item) for item in items if item)
            return str(items) if items else 'N/A'
        
        # Helper function to get safe attribute
        def safe_get(obj, attr, default='N/A'):
            try:
                if isinstance(obj, dict):
                    value = obj.get(attr, default)
                else:
                    value = getattr(obj, attr, default)
                # Handle null values and empty strings
                if value is None or value == '' or value == 'null':
                    return default
                return str(value)
            except:
                return default
        
        # Extract basic information
        filename = safe_get(scenario_result, 'scenario_name', 'Unknown')
        
        # Try to get the actual filename from the catalog if available
        if hasattr(scenario_result, 'pdf_analysis') and scenario_result.pdf_analysis:
            pdf_analysis = scenario_result.pdf_analysis
            filename = safe_get(pdf_analysis, 'filename', filename)
            
        # Get I-9 set and form data
        if hasattr(scenario_result, 'selected_i9_set') and scenario_result.selected_i9_set:
            i9_set = scenario_result.selected_i9_set
            form_data = getattr(i9_set, 'form_data', None)
        else:
            i9_set = None
            form_data = None
            
        # If no form_data, try to extract from catalog data directly
        if not form_data and hasattr(scenario_result, 'pdf_analysis') and scenario_result.pdf_analysis:
            catalog_data = getattr(scenario_result.pdf_analysis, 'catalog_data', None)
            if catalog_data and hasattr(catalog_data, 'pages'):
                # Extract employee information from first Section 1 page found
                for page in catalog_data.pages:
                    if (hasattr(page, 'page_subtype') and page.page_subtype == 'i9_form' and 
                        hasattr(page, 'extracted_values') and page.extracted_values):
                        # Create a mock form_data object from catalog
                        class MockFormData:
                            def __init__(self, extracted_values):
                                for key, value in extracted_values.items():
                                    setattr(self, key, value)
                        
                        form_data = MockFormData(page.extracted_values)
                        break
        
        # Initialize row with all required fields
        row = {
            'filename': filename,
            'employee_last_name': 'N/A',
            'employee_first_name': 'N/A', 
            'employee_middle_initial': 'N/A',
            'employee_other_names': 'N/A',
            'employee_date_of_birth': 'N/A',
            'employee_ssn': 'N/A',
            'citizenship_status': 'N/A',
            'alien_registration_number': 'N/A',
            'alien_work_until_date': 'N/A',
            'total_i9_sets_detected': 'N/A',
            'i9_sets_summary': 'N/A',
            'selected_set_pages': 'N/A',
            'selected_set_type': 'N/A',
            'selection_reasoning': 'N/A',
            'priority_hierarchy_applied': 'N/A',
            'has_section_1': 'False',
            'has_section_2': 'False',
            'has_section_3': 'False',
            'has_supplement_b': 'False',
            'validation_status': 'N/A',
            'section_2_documents': 'N/A',
            'section_3_documents': 'N/A',
            'supplement_b_documents': 'N/A',
            'expiry_date_matches': 'N/A',
            'total_pages': 'N/A',
            'employee_signature_date': 'N/A',
            'employer_signature_date': 'N/A',
            'first_day_employment': 'N/A',
            'section_3_signature_date': 'N/A',
            'latest_signature_date': 'N/A',
            'document_expiry_dates': 'N/A',
            'supporting_documents_count': 'N/A',
            'validation_score': 'N/A',
            'validation_rules_applied': 'N/A',
            'validation_failures': 'N/A',
            'validation_warnings': 'N/A',
            'critical_issues': 'N/A',
            'minor_issues': 'N/A',
            'processing_status': processing_status,
            'error_messages': 'N/A',
            'warnings': 'N/A',
            'recommendations': 'N/A',
            'manual_review_required': 'False',
            'quality_score': 'N/A',
            'document_references_found': '0',
            'document_matches_found': '0',
            'unmatched_documents': 'N/A',
            'attachment_validation_results': 'N/A'
        }
        
        # Extract data from form_data if available
        if form_data:
            # Try multiple field name variations for employee info
            row['employee_last_name'] = (safe_get(form_data, 'employee_last_name') or 
                                       safe_get(form_data, 'last_name') or 
                                       safe_get(form_data, 'employee_last_name'))
            row['employee_first_name'] = (safe_get(form_data, 'employee_first_name') or 
                                        safe_get(form_data, 'first_name') or 
                                        safe_get(form_data, 'employee_first_name'))
            row['employee_middle_initial'] = (safe_get(form_data, 'employee_middle_initial') or 
                                            safe_get(form_data, 'middle_initial'))
            row['employee_other_names'] = (safe_get(form_data, 'employee_other_names') or 
                                         safe_get(form_data, 'other_last_names_used'))
            row['employee_date_of_birth'] = format_date(safe_get(form_data, 'employee_date_of_birth') or 
                                                      safe_get(form_data, 'date_of_birth'))
            row['employee_ssn'] = (safe_get(form_data, 'employee_ssn') or 
                                 safe_get(form_data, 'social_security_number') or
                                 safe_get(form_data, 'us_social_security_number'))
            row['citizenship_status'] = safe_get(form_data, 'citizenship_status')
            row['alien_registration_number'] = (safe_get(form_data, 'alien_registration_number') or
                                              safe_get(form_data, 'alien_registration_number_uscis_number'))
            row['alien_work_until_date'] = format_date(safe_get(form_data, 'alien_work_until_date') or
                                                     safe_get(form_data, 'alien_work_until_date'))
            row['employee_signature_date'] = format_date(safe_get(form_data, 'employee_signature_date'))
            row['employer_signature_date'] = format_date(safe_get(form_data, 'employer_signature_date'))
            row['first_day_employment'] = format_date(safe_get(form_data, 'first_day_employment') or
                                                    safe_get(form_data, 'employee_first_day_of_employment'))
        
        # Extract data from i9_set if available
        if i9_set:
            row['selected_set_type'] = safe_get(i9_set, 'set_type', 'NEW_HIRE')
            row['has_section_1'] = str(bool(getattr(i9_set, 'section_1_pages', [])))
            row['has_section_2'] = str(bool(getattr(i9_set, 'section_2_pages', [])))
            row['has_section_3'] = str(bool(getattr(i9_set, 'section_3_pages', [])))
            row['has_supplement_b'] = str(bool(getattr(i9_set, 'supplement_b_pages', [])))
            
            # Format page numbers
            all_pages = []
            if hasattr(i9_set, 'section_1_pages'):
                all_pages.extend(getattr(i9_set, 'section_1_pages', []))
            if hasattr(i9_set, 'section_2_pages'):
                all_pages.extend(getattr(i9_set, 'section_2_pages', []))
            if hasattr(i9_set, 'section_3_pages'):
                all_pages.extend(getattr(i9_set, 'section_3_pages', []))
            if hasattr(i9_set, 'supplement_b_pages'):
                all_pages.extend(getattr(i9_set, 'supplement_b_pages', []))
            
            if all_pages:
                row['selected_set_pages'] = format_list(sorted(set(all_pages)))
        
        # Extract validation results if available
        if hasattr(scenario_result, 'validation_result'):
            validation = scenario_result.validation_result
            row['validation_status'] = safe_get(validation, 'status')
            row['validation_score'] = safe_get(validation, 'score')
            row['manual_review_required'] = str(bool(safe_get(validation, 'requires_manual_review', False)))
        
        # Extract scenario-specific information
        row['selection_reasoning'] = safe_get(scenario_result, 'notes')
        
        # Extract total pages from PDF analysis
        if hasattr(scenario_result, 'pdf_analysis') and scenario_result.pdf_analysis:
            row['total_pages'] = safe_get(scenario_result.pdf_analysis, 'total_pages')
            
        # Extract I-9 sets information if available
        if hasattr(scenario_result, 'pdf_analysis') and hasattr(scenario_result.pdf_analysis, 'i9_pages'):
            i9_pages = scenario_result.pdf_analysis.i9_pages
            if i9_pages:
                row['total_i9_sets_detected'] = str(len(set(getattr(page, 'set_id', 'unknown') for page in i9_pages)))
        
        return row
    
    def _get_latest_section_1_data(self, selected_set, catalog_data):
        """Extract data from the latest Section 1 form in the selected set"""
        pages = catalog_data.get('pages', [])
        
        # First, try to get Section 1 pages from the selected set
        section_1_page_numbers = []
        if hasattr(selected_set, 'section_1_pages') and selected_set.section_1_pages:
            for page in selected_set.section_1_pages:
                page_num = page.page_number if hasattr(page, 'page_number') else page
                section_1_page_numbers.append(page_num)
        
        # If no Section 1 pages in selected set, find all Section 1 pages in catalog
        if not section_1_page_numbers:
            for page in pages:
                if (page.get('page_subtype') == 'i9_form' and 
                    page.get('extracted_values')):
                    
                    extracted = page['extracted_values']
                    page_title = page.get('page_title', '').lower()
                    
                    # Check if this is a Section 1 page
                    if ('section 1' in page_title or 
                        any(key in extracted for key in ['employee_last_name', 'employee_first_name', 'date_of_birth', 'citizenship_status'])):
                        section_1_page_numbers.append(page.get('page_number'))
        
        if not section_1_page_numbers:
            return None
        
        # Find the latest Section 1 page (highest page number = most recent)
        latest_page_num = max(section_1_page_numbers)
        
        # Extract data from the latest Section 1 page
        for page in pages:
            if (page.get('page_number') == latest_page_num and 
                page.get('page_subtype') == 'i9_form' and 
                page.get('extracted_values')):
                
                extracted = page['extracted_values']
                page_title = page.get('page_title', '').lower()
                
                # Verify this is a Section 1 page
                if ('section 1' in page_title or 
                    any(key in extracted for key in ['employee_last_name', 'employee_first_name', 'date_of_birth', 'citizenship_status'])):
                    return extracted
        
        return None
    
    def _get_fallback_section_1_data(self, catalog_data):
        """Fallback method to get Section 1 data from any available Section 1 page"""
        pages = catalog_data.get('pages', [])
        logger.info(f"🔍 [EXTRACTION] Looking for Section 1 data in {len(pages)} pages")
        
        # Find all Section 1 pages and prioritize those with actual employee data
        section_1_pages = []
        for page in pages:
            if (page.get('page_subtype') == 'i9_form' and 
                page.get('extracted_values')):
                
                extracted = page['extracted_values']
                page_title = page.get('page_title', '').lower()
                
                # Check if this is a Section 1 page
                if ('section 1' in page_title or 
                    any(key in extracted for key in ['employee_last_name', 'employee_first_name', 'date_of_birth', 'citizenship_status'])):
                    
                    # Calculate data completeness score with priority for alien_work_until_date
                    data_score = 0
                    key_fields = ['employee_last_name', 'employee_first_name', 'employee_date_of_birth', 'citizenship_status']
                    for field in key_fields:
                        value = extracted.get(field)
                        if value and value not in [None, '', 'N/A', 'null']:
                            data_score += 1
                    
                    # Give extra priority to pages with alien_work_until_date
                    if extracted.get('alien_work_until_date') and extracted.get('alien_work_until_date') not in [None, '', 'N/A', 'null']:
                        data_score += 2  # Extra weight for alien work authorization date
                    
                    section_1_pages.append((page.get('page_number'), extracted, data_score))
        
        if not section_1_pages:
            logger.warning(f"🔍 [EXTRACTION] No Section 1 pages found with employee data")
            return None
        
        # Sort by data completeness score (descending), then by page number (descending)
        section_1_pages.sort(key=lambda x: (x[2], x[0]), reverse=True)
        best_page = section_1_pages[0]
        logger.info(f"✅ [EXTRACTION] Found Section 1 data on page {best_page[0]} (score: {best_page[2]})")
        logger.info(f"✅ [EXTRACTION] Employee: {best_page[1].get('employee_first_name')} {best_page[1].get('employee_last_name')}")
        return best_page[1]  # Return the extracted data from the best page
    
    def _extract_selected_set_documents(self, row, selected_set, catalog_data, selected_set_page_numbers, format_date, safe_get):
        """Extract documents ONLY from pages in the selected I-9 set"""
        
        # Initialize document collections
        section_2_docs = []
        section_3_docs = []
        supplement_b_docs = []
        section_3_docs_with_dates = []  # Store Section 3 docs with signature dates for sorting
        
        pages = catalog_data.get('pages', [])
        
        # If selected set page numbers is empty, extract from all I-9 pages but prioritize latest
        if not selected_set_page_numbers:
            # Fallback: extract from all I-9 pages, but get latest forms
            all_i9_pages = []
            for page in pages:
                if page.get('page_subtype') == 'i9_form' and page.get('extracted_values'):
                    all_i9_pages.append(page)
            
            # Sort by page number (latest first)
            all_i9_pages.sort(key=lambda x: x.get('page_number', 0), reverse=True)
            
            # Process pages to extract documents
            for page in all_i9_pages:
                extracted = page['extracted_values']
                page_title = page.get('page_title', '').lower()
                
                # Extract Section 2 documents (only from first/latest Section 2 found)
                if not section_2_docs and ('section 2' in page_title or 'sections 2' in page_title or
                    any(key in extracted for key in ['employer_signature_date', 'first_day_employment', 'list_a_document', 'section_2_'])):
                    section_2_docs.extend(self._extract_section_2_documents(extracted, format_date))
                
                # Extract Section 3 documents (only from first/latest Section 3 found)
                elif not section_3_docs and ('section 3' in page_title or 'sections 2 & 3' in page_title or 'sections 2 and 3' in page_title or
                      any(key in extracted for key in ['reverification', 'rehire', 'section_3_'])):
                    section_3_docs.extend(self._extract_section_3_documents(extracted, format_date))
                
                # Extract Supplement B documents (only from first/latest Supplement B found)
                elif not supplement_b_docs and 'supplement' in page_title:
                    supplement_b_docs.extend(self._extract_supplement_b_documents(extracted, format_date))
        else:
            # BUSINESS RULE: Only process pages that are in the selected set
            for page in pages:
                page_num = page.get('page_number')
                if page_num not in selected_set_page_numbers:
                    continue  # Skip pages not in selected set
                    
                if page.get('page_subtype') == 'i9_form' and page.get('extracted_values'):
                    extracted = page['extracted_values']
                    page_title = page.get('page_title', '').lower()
                    
                    # Determine section type and extract documents (prioritize Section 3 over Section 2)
                    if ('section 3' in page_title or 'sections 2 & 3' in page_title or 'sections 2 and 3' in page_title or
                        any(key in extracted for key in ['reverification', 'rehire', 'section_3_'])):
                        
                        # Extract Section 3 documents with signature dates for sorting
                        section_3_doc_info = self._extract_section_3_documents_with_dates(extracted, format_date, page_num)
                        if section_3_doc_info:
                            section_3_docs_with_dates.extend(section_3_doc_info)
                    
                    elif ('section 2' in page_title or 'sections 2' in page_title or
                          any(key in extracted for key in ['employer_signature_date', 'first_day_employment', 'list_a_document', 'section_2_'])):
                        section_2_docs.extend(self._extract_section_2_documents(extracted, format_date))
                    
                    elif 'supplement' in page_title:
                        supplement_b_docs.extend(self._extract_supplement_b_documents(extracted, format_date))
        
        # Process Section 3 documents: sort by signature date and update alien_work_until_date
        if section_3_docs_with_dates:
            # Sort by signature date (latest first), then by page number (highest first)
            section_3_docs_with_dates.sort(key=lambda x: (
                self._parse_date_for_sorting(x['signature_date']),
                x['page_num']
            ), reverse=True)
            
            # Use the latest Section 3 document's expiry date for alien_work_until_date
            latest_section_3 = section_3_docs_with_dates[0]
            if latest_section_3['expiry_date'] and latest_section_3['expiry_date'] != 'N/A':
                row['alien_work_until_date'] = format_date(latest_section_3['expiry_date'])
            
            # Extract document info strings for display
            section_3_docs = [doc['doc_info'] for doc in section_3_docs_with_dates]
        
        # Process Supplement B documents: update alien_work_until_date (highest priority)
        if supplement_b_docs:
            # Extract expiry date from Supplement B for alien_work_until_date
            for page in pages:
                page_title = page.get('page_title', '').lower()
                if 'supplement' in page_title:
                    extracted = page.get('extracted_values', {})
                    supplement_b_expiry = extracted.get('reverification_1_expiration_date')
                    if supplement_b_expiry and supplement_b_expiry != 'N/A':
                        # Supplement B has highest priority - override Section 3 date
                        row['alien_work_until_date'] = format_date(supplement_b_expiry)
                        break
        
        # Update row with extracted documents
        if section_2_docs:
            row['section_2_documents'] = ' | '.join(section_2_docs)
        if section_3_docs:
            row['section_3_documents'] = ' | '.join(section_3_docs)
        if supplement_b_docs:
            row['supplement_b_documents'] = ' | '.join(supplement_b_docs)
    
    def _extract_section_2_documents(self, extracted, format_date):
        """Extract Section 2 documents with proper formatting"""
        docs = []
        
        # Check for List A documents (including _1, _2, _3 suffix variations)
        # Handle multiple List A documents (up to 3)
        for i in range(1, 4):  # Check _1, _2, _3
            suffix = f"_{i}"
            list_a_title = (extracted.get(f'section_2_list_a_document_title{suffix}') or 
                           extracted.get(f'list_a_document_title{suffix}') or
                           extracted.get(f'list_a_document_{i}_title'))
            list_a_number = (extracted.get(f'section_2_list_a_document_number{suffix}') or 
                            extracted.get(f'list_a_document_number{suffix}') or
                            extracted.get(f'list_a_document_{i}_number'))
            list_a_expiry = (extracted.get(f'section_2_list_a_expiration_date{suffix}') or 
                            extracted.get(f'list_a_document_expiration_date{suffix}') or 
                            extracted.get(f'list_a_expiration_date{suffix}') or
                            extracted.get(f'list_a_document_{i}_expiration_date'))
            
            if list_a_title and list_a_title != 'N/A':
                doc_info = f"List A: {list_a_title}"
                if list_a_number and list_a_number != 'N/A':
                    doc_info += f" (#{list_a_number})"
                if list_a_expiry and list_a_expiry != 'N/A':
                    doc_info += f" [Expires: {format_date(list_a_expiry)}]"
                docs.append(doc_info)
        
        # Also check for the original format without suffix
        list_a_title = (extracted.get('section_2_list_a_document_title') or 
                       extracted.get('list_a_document_title'))
        list_a_number = (extracted.get('section_2_list_a_document_number') or 
                        extracted.get('list_a_document_number'))
        list_a_expiry = (extracted.get('section_2_list_a_expiration_date') or 
                        extracted.get('list_a_document_expiration_date') or 
                        extracted.get('list_a_expiration_date'))
        
        if list_a_title and list_a_title != 'N/A':
            doc_info = f"List A: {list_a_title}"
            if list_a_number and list_a_number != 'N/A':
                doc_info += f" (#{list_a_number})"
            if list_a_expiry and list_a_expiry != 'N/A':
                doc_info += f" [Expires: {format_date(list_a_expiry)}]"
            docs.append(doc_info)
        
        # Check for List B documents  
        list_b_title = extracted.get('section_2_list_b_document_title') or extracted.get('list_b_document_title')
        if list_b_title and list_b_title != 'N/A':
            docs.append(f"List B: {list_b_title}")
        
        # Check for List C documents
        list_c_title = extracted.get('section_2_list_c_document_title') or extracted.get('list_c_document_title')
        if list_c_title and list_c_title != 'N/A':
            docs.append(f"List C: {list_c_title}")
        
        return docs
    
    def _extract_section_3_documents(self, extracted, format_date):
        """Extract Section 3 documents with proper formatting"""
        docs = []
        
        # Check for reverification document details
        doc_title = (extracted.get('reverification_document_title') or 
                    extracted.get('section_3_document_title') or
                    extracted.get('reverification_1_document_title'))
        doc_number = (extracted.get('reverification_document_number') or
                     extracted.get('section_3_document_number') or
                     extracted.get('reverification_1_document_number'))
        doc_expiry = (extracted.get('reverification_expiration_date') or
                     extracted.get('section_3_expiration_date') or
                     extracted.get('reverification_1_expiration_date'))
        
        if doc_title and doc_title != 'N/A':
            doc_info = f"Reverification: {doc_title}"
            if doc_number and doc_number != 'N/A':
                doc_info += f" (#{doc_number})"
            if doc_expiry and doc_expiry != 'N/A':
                doc_info += f" [Expires: {format_date(doc_expiry)}]"
            docs.append(doc_info)
        
        return docs
    
    def _extract_section_3_documents_with_dates(self, extracted, format_date, page_num):
        """Extract Section 3 documents with signature dates for sorting"""
        docs = []
        
        # Get signature date for sorting
        signature_date = (extracted.get('employer_signature_date_reverification') or 
                         extracted.get('section_3_employer_signature_date') or
                         extracted.get('section_3_signature_date') or
                         extracted.get('employer_signature_date'))
        
        # Check for reverification document details
        doc_title = (extracted.get('reverification_document_title') or 
                    extracted.get('section_3_document_title') or
                    extracted.get('reverification_1_document_title'))
        doc_number = (extracted.get('reverification_document_number') or
                     extracted.get('section_3_document_number') or
                     extracted.get('reverification_1_document_number'))
        doc_expiry = (extracted.get('reverification_expiration_date') or
                     extracted.get('section_3_expiration_date') or
                     extracted.get('reverification_1_expiration_date'))
        
        if doc_title and doc_title != 'N/A':
            doc_info = f"Reverification: {doc_title}"
            if doc_number and doc_number != 'N/A':
                doc_info += f" (#{doc_number})"
            if doc_expiry and doc_expiry != 'N/A':
                doc_info += f" [Expires: {format_date(doc_expiry)}]"
            
            # Store with signature date and expiry date for sorting and alien_work_until_date update
            docs.append({
                'doc_info': doc_info,
                'signature_date': signature_date,
                'expiry_date': doc_expiry,
                'page_num': page_num
            })
        
        return docs
    
    def _extract_supplement_b_documents(self, extracted, format_date):
        """Extract Supplement B documents with proper formatting"""
        docs = []
        
        # Check for reverification documents in Supplement B
        doc_title = extracted.get('reverification_1_document_title')
        doc_number = extracted.get('reverification_1_document_number')
        doc_expiry = extracted.get('reverification_1_expiration_date')
        
        if doc_title and doc_title != 'N/A':
            doc_info = f"Supplement B: {doc_title}"
            if doc_number and doc_number != 'N/A':
                doc_info += f" (#{doc_number})"
            if doc_expiry and doc_expiry != 'N/A':
                doc_info += f" [Expires: {format_date(doc_expiry)}]"
            docs.append(doc_info)
        
        return docs
    
    def _parse_date_for_sorting(self, date_str):
        """Parse date string for sorting (returns datetime object or minimum date)"""
        from datetime import datetime
        
        if not date_str or date_str in ['N/A', '', None]:
            return datetime.min
        
        try:
            # Try different date formats
            for fmt in ['%m/%d/%Y', '%Y-%m-%d', '%d/%m/%Y']:
                try:
                    return datetime.strptime(str(date_str), fmt)
                except ValueError:
                    continue
            return datetime.min
        except:
            return datetime.min
    
    def _extract_from_selected_catalog_pages(self, row, catalog_data, selected_set_page_numbers, format_date, safe_get):
        """Extract additional data from catalog pages but only from selected set pages"""
        pages = catalog_data.get('pages', [])
        
        # Extract signature dates from selected set pages only
        for page in pages:
            page_num = page.get('page_number')
            if page_num not in selected_set_page_numbers:
                continue  # Skip pages not in selected set
                
            if page.get('page_subtype') == 'i9_form' and page.get('extracted_values'):
                extracted = page['extracted_values']
                page_title = page.get('page_title', '').lower()
                
                # Extract employer signature dates from Section 2
                if ('section 2' in page_title or 'sections 2' in page_title):
                    employer_sig_date = (extracted.get('employer_signature_date') or 
                                       extracted.get('today_s_date_section_2') or
                                       extracted.get('section_2_certification_date'))
                    if employer_sig_date and employer_sig_date != 'N/A' and row['employer_signature_date'] == 'N/A':
                        row['employer_signature_date'] = format_date(employer_sig_date)
                
                # Extract Section 3 signature dates
                elif ('section 3' in page_title or 'sections 2 & 3' in page_title):
                    section_3_sig_date = (extracted.get('employer_signature_date_reverification') or 
                                         extracted.get('section_3_employer_signature_date') or
                                         extracted.get('section_3_signature_date'))
                    if section_3_sig_date and section_3_sig_date != 'N/A' and row['section_3_signature_date'] == 'N/A':
                        row['section_3_signature_date'] = format_date(section_3_sig_date)
    
    def _validate_expiry_date_matching(self, row, latest_section_1_data, catalog_data, selected_set_page_numbers, format_date):
        """Validate that work authorization expiry date matches supporting document expiry dates"""
        
        # Get work authorization expiry date from the row (already updated with latest document)
        work_auth_expiry = row.get('alien_work_until_date')
        if not work_auth_expiry or work_auth_expiry == 'N/A':
            # Fallback to Section 1 if no date in row
            if latest_section_1_data:
                work_auth_expiry = (latest_section_1_data.get('alien_work_until_date') or 
                                   latest_section_1_data.get('alien_authorized_to_work_until') or
                                   latest_section_1_data.get('work_authorization_until_date'))
            if not work_auth_expiry or work_auth_expiry == 'N/A':
                return
        
        # Normalize the work auth expiry date
        work_auth_expiry_normalized = format_date(work_auth_expiry)
        if work_auth_expiry_normalized == 'N/A':
            return
        
        # Collect supporting document expiry dates from ALL I-9 form pages (not just selected set)
        supporting_doc_expiry_dates = []
        pages = catalog_data.get('pages', [])
        
        for page in pages:
            # For expiry date matching, check ALL I-9 form pages, not just selected set
            if page.get('page_subtype') == 'i9_form' and page.get('extracted_values'):
                extracted = page['extracted_values']
                page_title = page.get('page_title', '').lower()
                
                # Extract expiry dates from Section 2, Section 3, and Supplement B
                expiry_fields = [
                    'list_a_document_expiration_date', 'list_a_document_1_expiration_date',
                    'list_a_document_2_expiration_date', 'list_a_document_3_expiration_date',
                    'list_a_expiration_date_1', 'list_a_expiration_date_2', 'list_a_expiration_date_3',
                    'section_2_list_a_expiration_date', 'section_3_expiration_date',
                    'reverification_expiration_date', 'reverification_1_expiration_date'
                ]
                
                for field in expiry_fields:
                    expiry_date = extracted.get(field)
                    if expiry_date and expiry_date != 'N/A':
                        normalized_date = format_date(expiry_date)
                        if normalized_date != 'N/A':
                            supporting_doc_expiry_dates.append(normalized_date)
        
        # Check for matches
        matches = []
        for doc_expiry in supporting_doc_expiry_dates:
            if doc_expiry == work_auth_expiry_normalized:
                matches.append(doc_expiry)
        
        # Update expiry_date_matches field
        if matches:
            row['expiry_date_matches'] = f"Work Auth: {work_auth_expiry_normalized} matches {len(matches)} supporting document(s)"
        else:
            if supporting_doc_expiry_dates:
                row['expiry_date_matches'] = f"Work Auth: {work_auth_expiry_normalized} does NOT match supporting docs: {' | '.join(set(supporting_doc_expiry_dates))}"
            else:
                row['expiry_date_matches'] = f"Work Auth: {work_auth_expiry_normalized} - No supporting document expiry dates found"
    
    def _extract_from_enhanced_processor_results(self, scenario_result, processing_status: str) -> dict:
        """Extract comprehensive data from enhanced processor results"""
        
        # Get filename from scenario result
        filename = getattr(scenario_result, 'scenario_name', 'Unknown')
        original_filename = filename
        
        # Get enhanced processor results - try multiple approaches
        enhanced_data = None
        
        # First, try direct filename match
        if filename in self.enhanced_results:
            enhanced_data = self.enhanced_results[filename]
        
        # Try without path prefix (e.g., "data/input/Wu, Qianyi 9963.pdf" -> "Wu, Qianyi 9963.pdf")
        elif filename.startswith('data/input/'):
            short_filename = filename.replace('data/input/', '')
            if short_filename in self.enhanced_results:
                enhanced_data = self.enhanced_results[short_filename]
        
        # If no direct match and filename is generic, try to find a match
        if not enhanced_data and (filename == 'No Forms Detected' or filename == 'Unknown'):
            # Try to find any available enhanced results
            available_files = [f for f in self.enhanced_results.keys() if '.pdf' in f]
            if available_files:
                filename = available_files[0]  # Take the first available
                enhanced_data = self.enhanced_results[filename]
        
        logger.info(f"🔍 Processing filename: {original_filename} -> {filename}")
        
        if not enhanced_data:
            logger.warning(f"No enhanced processor results found for {filename}")
            return self._create_no_forms_row(filename, processing_status)
        
        # Helper functions (reuse from main extraction method)
        def format_date(date_str):
            if not date_str or date_str in ['N/A', '', None]:
                return 'N/A'
            try:
                from datetime import datetime
                if '/' in str(date_str):
                    return str(date_str)
                elif '-' in str(date_str):
                    parts = str(date_str).split('-')
                    if len(parts) == 3:
                        if len(parts[0]) == 4:  # YYYY-MM-DD
                            return f"{parts[1]}/{parts[2]}/{parts[0]}"
                        else:  # MM-DD-YYYY
                            return f"{parts[0]}/{parts[1]}/{parts[2]}"
                return str(date_str)
            except:
                return 'N/A'
        
        def format_list(items):
            if not items:
                return 'N/A'
            if isinstance(items, list):
                return ' | '.join(str(item) for item in items if item)
            return str(items) if items else 'N/A'
        
        def safe_get(obj, attr, default='N/A'):
            try:
                if isinstance(obj, dict):
                    value = obj.get(attr, default)
                else:
                    value = getattr(obj, attr, default)
                # Handle null values and empty strings
                if value is None or value == '' or value == 'null':
                    return default
                return str(value)
            except:
                return default
        
        def safe_get_multiple(obj, *attrs, default='N/A'):
            """Try multiple attribute names and return the first valid one"""
            for attr in attrs:
                value = safe_get(obj, attr, None)
                if value is not None and value != 'N/A':
                    return value
            return default
        
        # Extract data from enhanced processor results
        selected_set = enhanced_data.get('selected_set')
        validation_result = enhanced_data.get('validation_result')
        pdf_analysis = enhanced_data.get('pdf_analysis')
        i9_sets = enhanced_data.get('i9_sets', [])
        catalog_data = enhanced_data.get('catalog_data', {})
        
        # Initialize row with all required fields
        row = {
            'filename': filename,
            'employee_last_name': 'N/A',
            'employee_first_name': 'N/A', 
            'employee_middle_initial': 'N/A',
            'employee_other_names': 'N/A',
            'employee_date_of_birth': 'N/A',
            'employee_ssn': 'N/A',
            'citizenship_status': 'N/A',
            'alien_registration_number': 'N/A',
            'alien_work_until_date': 'N/A',
            'total_i9_sets_detected': str(len(i9_sets)) if i9_sets else '0',
            'i9_sets_summary': 'N/A',
            'selected_set_pages': 'N/A',
            'selected_set_type': 'N/A',
            'selection_reasoning': 'N/A',
            'priority_hierarchy_applied': 'N/A',
            'has_section_1': 'False',
            'has_section_2': 'False',
            'has_section_3': 'False',
            'has_supplement_b': 'False',
            'validation_status': 'N/A',
            'section_2_documents': 'N/A',
            'section_3_documents': 'N/A',
            'supplement_b_documents': 'N/A',
            'expiry_date_matches': 'N/A',
            'total_pages': str(catalog_data.get('total_pages', 'N/A')),
            'employee_signature_date': 'N/A',
            'employer_signature_date': 'N/A',
            'first_day_employment': 'N/A',
            'section_3_signature_date': 'N/A',
            'latest_signature_date': 'N/A',
            'document_expiry_dates': 'N/A',
            'supporting_documents_count': 'N/A',
            'validation_score': 'N/A',
            'validation_rules_applied': 'N/A',
            'validation_failures': 'N/A',
            'validation_warnings': 'N/A',
            'critical_issues': 'N/A',
            'minor_issues': 'N/A',
            'processing_status': processing_status,
            'error_messages': 'N/A',
            'warnings': 'N/A',
            'recommendations': 'N/A',
            'manual_review_required': 'False',
            'quality_score': 'N/A',
            'document_references_found': '0',
            'document_matches_found': '0',
            'unmatched_documents': 'N/A',
            'attachment_validation_results': 'N/A'
        }
        
        # Extract data from selected I-9 set with business rule compliance
        if selected_set:
            row['selected_set_type'] = safe_get(selected_set, 'set_type', 'NEW_HIRE')
            row['has_section_1'] = str(bool(getattr(selected_set, 'section_1_pages', [])))
            row['has_section_2'] = str(bool(getattr(selected_set, 'section_2_pages', [])))
            row['has_section_3'] = str(bool(getattr(selected_set, 'section_3_pages', [])))
            row['has_supplement_b'] = str(bool(getattr(selected_set, 'supplement_b_pages', [])))
            
            # Format page numbers
            all_pages = []
            selected_set_page_numbers = set()
            
            if hasattr(selected_set, 'section_1_pages'):
                pages = getattr(selected_set, 'section_1_pages', [])
                page_nums = [p.page_number if hasattr(p, 'page_number') else p for p in pages]
                all_pages.extend(page_nums)
                selected_set_page_numbers.update(page_nums)
            if hasattr(selected_set, 'section_2_pages'):
                pages = getattr(selected_set, 'section_2_pages', [])
                page_nums = [p.page_number if hasattr(p, 'page_number') else p for p in pages]
                all_pages.extend(page_nums)
                selected_set_page_numbers.update(page_nums)
            if hasattr(selected_set, 'section_3_pages'):
                pages = getattr(selected_set, 'section_3_pages', [])
                page_nums = [p.page_number if hasattr(p, 'page_number') else p for p in pages]
                all_pages.extend(page_nums)
                selected_set_page_numbers.update(page_nums)
            if hasattr(selected_set, 'supplement_b_pages'):
                pages = getattr(selected_set, 'supplement_b_pages', [])
                page_nums = [p.page_number if hasattr(p, 'page_number') else p for p in pages]
                all_pages.extend(page_nums)
                selected_set_page_numbers.update(page_nums)
            
            if all_pages:
                row['selected_set_pages'] = format_list(sorted(set(all_pages)))
            
            # SIMPLIFIED APPROACH: Always extract employee data from best available Section 1 page
            # This ensures reliable employee data extraction while maintaining business rule compliance for documents
            latest_section_1_data = self._get_fallback_section_1_data(catalog_data)
            
            if latest_section_1_data:
                row['employee_last_name'] = safe_get(latest_section_1_data, 'employee_last_name')
                row['employee_first_name'] = safe_get(latest_section_1_data, 'employee_first_name')
                row['employee_middle_initial'] = safe_get(latest_section_1_data, 'employee_middle_initial')
                row['employee_other_names'] = safe_get_multiple(latest_section_1_data, 'employee_other_last_names_used', 'other_last_names_used')
                row['employee_date_of_birth'] = format_date(safe_get_multiple(latest_section_1_data, 'employee_date_of_birth', 'date_of_birth', 'employee_dob'))
                row['employee_ssn'] = safe_get_multiple(latest_section_1_data, 'employee_social_security_number', 'social_security_number')
                row['citizenship_status'] = safe_get(latest_section_1_data, 'citizenship_status')
                row['alien_registration_number'] = safe_get(latest_section_1_data, 'alien_registration_number_uscis_number')
                # Get work authorization expiry date from the enhanced processor result if available
                enhanced_expiry_date = None
                if hasattr(selected_set, 'form_data') and selected_set.form_data:
                    enhanced_expiry_date = safe_get(selected_set.form_data, 'authorized_to_work_until')
                
                if enhanced_expiry_date:
                    row['alien_work_until_date'] = format_date(enhanced_expiry_date)
                else:
                    # Fallback to catalog extraction
                    row['alien_work_until_date'] = format_date(safe_get_multiple(latest_section_1_data, 'alien_work_until_date', 'alien_authorized_to_work_until', 'work_authorization_until_date'))
                row['employee_signature_date'] = format_date(safe_get_multiple(latest_section_1_data, 'employee_signature_date', 'date_of_signature'))
            
            # BUSINESS RULE: Extract documents ONLY from selected set pages
            self._extract_selected_set_documents(row, selected_set, catalog_data, selected_set_page_numbers, format_date, safe_get)
            
            # BUSINESS RULE: Validate expiry date matching between Section 1 and supporting documents
            self._validate_expiry_date_matching(row, latest_section_1_data, catalog_data, selected_set_page_numbers, format_date)
            
            # Extract other dates from selected set
            if hasattr(selected_set, 'form_data') and selected_set.form_data:
                form_data = selected_set.form_data
                
                # Get employer signature date from the actual selected pages (Section 3 or Supplement B)
                employer_sig_date = self._get_employer_signature_date_from_selected_set(selected_set)
                if employer_sig_date:
                    row['employer_signature_date'] = format_date(employer_sig_date)
                else:
                    row['employer_signature_date'] = format_date(safe_get(form_data, 'employer_signature_date'))
                
                row['first_day_employment'] = format_date(safe_get(form_data, 'first_day_employment'))
            else:
                # Fallback to catalog extraction but only from selected set pages
                self._extract_from_selected_catalog_pages(row, catalog_data, selected_set_page_numbers, format_date, safe_get)
        
        # Extract validation results
        if validation_result:
            # Extract validation status and score
            row['validation_status'] = safe_get(validation_result, 'validation_status')
            
            overall_score = safe_get(validation_result, 'overall_score')
            if overall_score != 'N/A':
                row['validation_score'] = f"{overall_score}%"
            
            # Extract issue counts
            critical_issues = safe_get(validation_result, 'critical_issues')
            error_issues = safe_get(validation_result, 'error_issues') 
            warning_issues = safe_get(validation_result, 'warning_issues')
            
            if critical_issues != 'N/A' and int(critical_issues) > 0:
                row['critical_issues'] = f"{critical_issues} critical issues found"
                row['manual_review_required'] = 'True'
            
            if error_issues != 'N/A' and int(error_issues) > 0:
                row['minor_issues'] = f"{error_issues} errors found"
            
            if warning_issues != 'N/A' and int(warning_issues) > 0:
                row['validation_warnings'] = f"{warning_issues} warnings found"
            
            # Extract validation issues details
            validation_issues = safe_get(validation_result, 'validation_issues')
            if validation_issues != 'N/A' and validation_issues:
                issue_descriptions = []
                for issue in validation_issues:
                    if hasattr(issue, 'message'):
                        issue_descriptions.append(issue.message)
                    elif isinstance(issue, dict):
                        issue_descriptions.append(issue.get('message', str(issue)))
                    else:
                        # Clean up the string representation
                        issue_str = str(issue)
                        if 'ValidationIssue' in issue_str and 'message=' in issue_str:
                            # Extract just the message part
                            try:
                                start = issue_str.find("message='") + 9
                                end = issue_str.find("'", start)
                                if start > 8 and end > start:
                                    clean_message = issue_str[start:end]
                                    issue_descriptions.append(clean_message)
                                else:
                                    # Fallback: try to extract a readable summary
                                    if 'Missing Section 2' in issue_str:
                                        issue_descriptions.append('Missing Section 2 (Employer Review and Verification)')
                                    else:
                                        issue_descriptions.append('Validation issue detected')
                            except:
                                issue_descriptions.append('Validation issue detected')
                        else:
                            issue_descriptions.append(issue_str)
                
                if issue_descriptions:
                    row['validation_failures'] = format_list(issue_descriptions)
            
            # Extract document matching info from validation_result.document_matches
            # Use direct getattr to avoid string conversion of lists
            document_matches = getattr(validation_result, 'document_matches', None) if validation_result else None
            if document_matches is not None and isinstance(document_matches, list):
                # Count matched vs unmatched documents
                matched_docs = [m for m in document_matches if hasattr(m, 'supporting_document') and m.supporting_document is not None]
                unmatched_docs = [m for m in document_matches if hasattr(m, 'supporting_document') and m.supporting_document is None]
                
                row['document_references_found'] = str(len(document_matches))
                row['document_matches_found'] = str(len(matched_docs))
                
                if unmatched_docs:
                    unmatched_list = []
                    for doc in unmatched_docs:
                        if hasattr(doc, 'reference') and hasattr(doc.reference, 'document_type'):
                            doc_type = doc.reference.document_type.value if hasattr(doc.reference.document_type, 'value') else str(doc.reference.document_type)
                            doc_num = getattr(doc.reference, 'document_number', 'N/A')
                            unmatched_list.append(f"{doc_type} (#{doc_num})")
                    row['unmatched_documents'] = ' | '.join(unmatched_list) if unmatched_list else 'N/A'
                else:
                    row['unmatched_documents'] = 'N/A'
                
                # Attachment validation results
                if matched_docs:
                    attachment_results = []
                    for doc in matched_docs:
                        if hasattr(doc, 'supporting_document') and doc.supporting_document:
                            doc_type = doc.reference.document_type.value if hasattr(doc.reference.document_type, 'value') else str(doc.reference.document_type)
                            page_num = getattr(doc.supporting_document, 'page_number', 'N/A')
                            confidence = getattr(doc, 'match_confidence', 0.0)
                            attachment_results.append(f"{doc_type} found on page {page_num} (confidence: {confidence:.1f})")
                    row['attachment_validation_results'] = ' | '.join(attachment_results) if attachment_results else 'N/A'
                else:
                    row['attachment_validation_results'] = 'No documents attached' if unmatched_docs else 'N/A'
            else:
                row['document_references_found'] = '0'
                row['document_matches_found'] = '0'
                row['unmatched_documents'] = 'N/A'
                row['attachment_validation_results'] = 'N/A'
            
            # Extract component scores
            set_completeness = safe_get(validation_result, 'set_completeness_score')
            document_matching = safe_get(validation_result, 'document_matching_score')
            compliance = safe_get(validation_result, 'compliance_score')
            
            score_details = []
            if set_completeness != 'N/A':
                score_details.append(f"Completeness: {set_completeness}%")
            if document_matching != 'N/A':
                score_details.append(f"Document Matching: {document_matching}%")
            if compliance != 'N/A':
                score_details.append(f"Compliance: {compliance}%")
            
            if score_details:
                row['validation_rules_applied'] = format_list(score_details)
        
        # Extract additional metadata
        if i9_sets:
            set_summaries = []
            for i9_set in i9_sets:
                set_type = safe_get(i9_set, 'set_type', 'UNKNOWN')
                set_id = safe_get(i9_set, 'set_id', 'unknown')
                set_summaries.append(f"{set_id}({set_type})")
            row['i9_sets_summary'] = format_list(set_summaries)
            
            # Priority hierarchy information
            if selected_set:
                if hasattr(selected_set, 'supplement_b_pages') and selected_set.supplement_b_pages:
                    row['priority_hierarchy_applied'] = 'Supplement B (Highest Priority)'
                elif hasattr(selected_set, 'section_3_pages') and selected_set.section_3_pages:
                    row['priority_hierarchy_applied'] = 'Section 3 (Second Priority)'
                elif hasattr(selected_set, 'section_2_pages') and selected_set.section_2_pages:
                    row['priority_hierarchy_applied'] = 'Section 2 (Third Priority)'
                else:
                    row['priority_hierarchy_applied'] = 'Section 1 Only (Lowest Priority)'
        
        # Extract supporting documents count
        if catalog_data:
            supporting_docs = 0
            for page in catalog_data.get('pages', []):
                if page.get('page_subtype') not in ['i9_form']:
                    supporting_docs += 1
            row['supporting_documents_count'] = str(supporting_docs)
        
        # Set quality score based on data completeness
        filled_fields = sum(1 for value in row.values() if value != 'N/A' and value != 'False')
        total_fields = len(row)
        quality_percentage = (filled_fields / total_fields) * 100
        row['quality_score'] = f"{quality_percentage:.1f}%"
        
        return row
    
    # OLD METHOD REMOVED - This was causing business rule violations by extracting from ALL pages
    # Now using _extract_selected_set_documents and _get_latest_section_1_data instead
    
    def _create_no_forms_row(self, filename: str, processing_status: str) -> dict:
        """Create a row for cases where no forms were detected"""
        return {
            'filename': filename,
            'employee_last_name': 'N/A',
            'employee_first_name': 'N/A', 
            'employee_middle_initial': 'N/A',
            'employee_other_names': 'N/A',
            'employee_date_of_birth': 'N/A',
            'employee_ssn': 'N/A',
            'citizenship_status': 'N/A',
            'alien_registration_number': 'N/A',
            'alien_work_until_date': 'N/A',
            'total_i9_sets_detected': '0',
            'i9_sets_summary': 'No I-9 forms detected',
            'selected_set_pages': 'N/A',
            'selected_set_type': 'N/A',
            'selection_reasoning': 'No I-9 forms detected in the document',
            'priority_hierarchy_applied': 'N/A',
            'has_section_1': 'False',
            'has_section_2': 'False',
            'has_section_3': 'False',
            'has_supplement_b': 'False',
            'validation_status': 'NO_FORMS',
            'section_2_documents': 'N/A',
            'section_3_documents': 'N/A',
            'supplement_b_documents': 'N/A',
            'expiry_date_matches': 'N/A',
            'total_pages': 'N/A',
            'employee_signature_date': 'N/A',
            'employer_signature_date': 'N/A',
            'first_day_employment': 'N/A',
            'section_3_signature_date': 'N/A',
            'latest_signature_date': 'N/A',
            'document_expiry_dates': 'N/A',
            'supporting_documents_count': 'N/A',
            'validation_score': '0',
            'validation_rules_applied': 'N/A',
            'validation_failures': 'N/A',
            'validation_warnings': 'N/A',
            'critical_issues': 'N/A',
            'minor_issues': 'N/A',
            'processing_status': processing_status,
            'error_messages': 'N/A',
            'warnings': 'N/A',
            'recommendations': 'N/A',
            'manual_review_required': 'False',
            'quality_score': 'N/A',
            'document_references_found': '0',
            'document_matches_found': '0',
            'unmatched_documents': 'N/A',
            'attachment_validation_results': 'N/A'
        }
    
    def _create_error_row(self, error_info: dict) -> dict:
        """Create a CSV row for error cases"""
        
        # Initialize error row with all required fields set to N/A
        row = {
            'filename': error_info.get('pdf_file', 'Unknown'),
            'employee_last_name': 'N/A',
            'employee_first_name': 'N/A', 
            'employee_middle_initial': 'N/A',
            'employee_other_names': 'N/A',
            'employee_date_of_birth': 'N/A',
            'employee_ssn': 'N/A',
            'citizenship_status': 'N/A',
            'alien_registration_number': 'N/A',
            'alien_work_until_date': 'N/A',
            'total_i9_sets_detected': '0',
            'i9_sets_summary': 'N/A',
            'selected_set_pages': 'N/A',
            'selected_set_type': 'N/A',
            'selection_reasoning': 'N/A',
            'priority_hierarchy_applied': 'N/A',
            'has_section_1': 'False',
            'has_section_2': 'False',
            'has_section_3': 'False',
            'has_supplement_b': 'False',
            'validation_status': 'ERROR',
            'section_2_documents': 'N/A',
            'section_3_documents': 'N/A',
            'supplement_b_documents': 'N/A',
            'expiry_date_matches': 'N/A',
            'total_pages': 'N/A',
            'employee_signature_date': 'N/A',
            'employer_signature_date': 'N/A',
            'first_day_employment': 'N/A',
            'section_3_signature_date': 'N/A',
            'latest_signature_date': 'N/A',
            'document_expiry_dates': 'N/A',
            'supporting_documents_count': 'N/A',
            'validation_score': '0',
            'validation_rules_applied': 'N/A',
            'validation_failures': 'N/A',
            'validation_warnings': 'N/A',
            'critical_issues': error_info.get('error', 'Processing failed'),
            'minor_issues': 'N/A',
            'processing_status': 'ERROR',
            'error_messages': error_info.get('error', 'Processing failed'),
            'warnings': 'N/A',
            'recommendations': 'Manual review required',
            'manual_review_required': 'True',
            'quality_score': '0',
            'document_references_found': '0',
            'document_matches_found': '0',
            'unmatched_documents': 'N/A',
            'attachment_validation_results': 'N/A'
        }
        
        return row
    
    def _select_best_i9_set(self, i9_sets: List[Any]) -> Any:
        """Select the best I-9 set using existing priority logic"""
        
        # The I9SetGrouper already applied priority hierarchy and validation
        # Just select the first (highest priority) set
        if i9_sets:
            selected_set = i9_sets[0]
            logger.info(f"🟢 Selected I-9 set: {selected_set.set_id} (highest priority from grouper)")
            return selected_set
        else:
            logger.warning("No I-9 sets available for selection")
            return None
    
    def process_all_files(self, input_folder: str) -> Dict[str, Any]:
        """
        Process all PDF files in input folder
        
        Args:
            input_folder: Path to directory containing PDF files
            
        Returns:
            Dictionary containing processing results and statistics
        """
        logger.info(f"🔄 Starting batch processing for folder: {input_folder}")
        
        # Discover files
        file_mappings = self.discover_pdf_files(input_folder)
        
        if not file_mappings:
            logger.error("No PDF files found or no corresponding catalogs available")
            return {
                'success_results': [],
                'partial_results': [],
                'error_results': [],
                'statistics': {
                    'total_files': 0,
                    'processed': 0,
                    'success': 0,
                    'partial': 0,
                    'errors': 0
                }
            }
        
        # Process each file
        success_results = []
        partial_results = []
        error_results = []
        
        for pdf_path, catalog_path in file_mappings:
            if not Path(catalog_path).exists():
                error_results.append({
                    'pdf_file': Path(pdf_path).name,
                    'error': 'Missing catalog file',
                    'catalog_path': catalog_path
                })
                continue
            
            result = self.process_single_catalog(catalog_path, pdf_path)
            
            logger.info(f"🔍 [BATCH] Result for {Path(pdf_path).name}: type={type(result)}, is_list={isinstance(result, list)}, len={len(result) if isinstance(result, list) else 'N/A'}")
            
            if result is None:
                error_results.append({
                    'pdf_file': Path(pdf_path).name,
                    'error': 'Processing failed',
                    'catalog_path': catalog_path
                })
            elif isinstance(result, list) and len(result) > 0:
                # Handle list of ScenarioResult objects
                primary_result = result[0]  # Use first result as primary
                logger.info(f"🔍 [BATCH] Primary result has_status={hasattr(primary_result, 'status')}, status={getattr(primary_result, 'status', 'NO_STATUS')}")
                if hasattr(primary_result, 'status'):
                    from .core.models import ProcessingStatus
                    logger.info(f"🔍 [BATCH] Comparing status: {primary_result.status} == {ProcessingStatus.COMPLETE_SUCCESS}? {primary_result.status == ProcessingStatus.COMPLETE_SUCCESS}")
                    if primary_result.status == ProcessingStatus.COMPLETE_SUCCESS:
                        logger.info(f"✅ [BATCH] Adding to success_results: {Path(pdf_path).name}")
                        success_results.append(result)
                    elif primary_result.status == ProcessingStatus.PARTIAL_SUCCESS:
                        logger.info(f"⚠️ [BATCH] Adding to partial_results: {Path(pdf_path).name}")
                        partial_results.append(result)
                    else:
                        # Treat as successful processing even if status is not perfect
                        logger.info(f"✅ [BATCH] Adding to success_results (other status): {Path(pdf_path).name}")
                        success_results.append(result)
                else:
                    # No status attribute, treat as successful
                    logger.info(f"✅ [BATCH] Adding to success_results (no status): {Path(pdf_path).name}")
                    success_results.append(result)
            else:
                error_results.append({
                    'pdf_file': Path(pdf_path).name,
                    'error': 'Unexpected result format',
                    'catalog_path': catalog_path
                })
        
        # Calculate statistics
        total_files = len(file_mappings)
        processed = len(success_results) + len(partial_results)
        
        statistics = {
            'total_files': total_files,
            'processed': processed,
            'success': len(success_results),
            'partial': len(partial_results),
            'errors': len(error_results)
        }
        
        # Generate main CSV file
        self._generate_main_csv(success_results, partial_results, error_results)
        
        logger.info(f"📊 Batch processing complete:")
        logger.info(f"   - Total files: {statistics['total_files']}")
        logger.info(f"   - Successfully processed: {statistics['success']}")
        logger.info(f"   - Partially processed: {statistics['partial']}")
        logger.info(f"   - Errors: {statistics['errors']}")
        
        return {
            'success_results': success_results,
            'partial_results': partial_results,
            'error_results': error_results,
            'statistics': statistics
        }
    
    def consolidate_results(self, batch_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Consolidate all processing results into format suitable for comprehensive report
        
        Args:
            batch_results: Results from process_all_files()
            
        Returns:
            List of consolidated result dictionaries
        """
        consolidated_data = []
        
        # Process success results
        for result in batch_results['success_results']:
            consolidated_record = self._convert_processing_result_to_dict(result, 'COMPLETE_SUCCESS')
            consolidated_data.append(consolidated_record)
        
        # Process partial results
        for result in batch_results['partial_results']:
            consolidated_record = self._convert_processing_result_to_dict(result, 'PARTIAL_SUCCESS')
            consolidated_data.append(consolidated_record)
        
        # Process error results
        for error_info in batch_results['error_results']:
            error_record = {
                'employee_id': '',
                'first_name': '',
                'last_name': '',
                'middle_initial': 'N/A',
                'date_of_birth': '',
                'ssn': '',
                'is_us_citizen': 'Unknown',
                'citizenship_status': 'UNKNOWN',
                'work_authorization_expiry_date': '',
                'alien_authorized_to_work_until': '',
                'section_2_documents': '',
                'section_3_documents': '',
                'supplement_b_documents': '',
                'supporting_documents_found': '',
                'supporting_documents_attached': '',
                'supporting_documents_not_attached': '',
                'expiry_date_matches': 0,
                'expiry_date_mismatches': 0,
                'document_attachment_status': 'ERROR',
                'document_reference_matches': '0/0',
                'form_type_selected': 'NONE',
                'selection_reason': 'processing_error',
                'processing_status': 'ERROR',
                'validation_score': 'N/A',
                'employee_signature_date': '',
                'employer_signature_date': '',
                'pdf_file_name': error_info['pdf_file'],
                'input_file_path': f"input/{error_info['pdf_file']}",
                'processing_time': '0s',
                'notes': error_info['error']
            }
            consolidated_data.append(error_record)
        
        logger.info(f"📋 Consolidated {len(consolidated_data)} records for comprehensive report")
        return consolidated_data
    
    def _convert_processing_result_to_dict(self, result: ProcessingResult, status: str) -> Dict[str, Any]:
        """Convert ProcessingResult to dictionary format for CSV export"""
        
        # Extract basic employee information
        employee_data = {
            'employee_id': getattr(result.primary_i9_data, 'employee_id', '') if result.primary_i9_data else '',
            'first_name': getattr(result.primary_i9_data, 'first_name', '') if result.primary_i9_data else '',
            'last_name': getattr(result.primary_i9_data, 'last_name', '') if result.primary_i9_data else '',
            'middle_initial': getattr(result.primary_i9_data, 'middle_initial', 'N/A') if result.primary_i9_data else 'N/A',
            'date_of_birth': getattr(result.primary_i9_data, 'date_of_birth', '') if result.primary_i9_data else '',
            'ssn': getattr(result.primary_i9_data, 'ssn', '') if result.primary_i9_data else '',
            'is_us_citizen': 'Yes' if getattr(result.primary_i9_data, 'citizenship_status', '').lower().find('citizen') != -1 else 'No' if result.primary_i9_data else 'Unknown',
            'citizenship_status': str(getattr(result.primary_i9_data, 'citizenship_status', 'UNKNOWN')) if result.primary_i9_data else 'UNKNOWN',
            'work_authorization_expiry_date': getattr(result.primary_i9_data, 'authorized_to_work_until', '') if result.primary_i9_data else '',
            'alien_authorized_to_work_until': getattr(result.primary_i9_data, 'authorized_to_work_until', '') if result.primary_i9_data else '',
            'employee_signature_date': getattr(result.primary_i9_data, 'employee_signature_date', '') if result.primary_i9_data else '',
            'employer_signature_date': getattr(result.primary_i9_data, 'employer_signature_date', '') if result.primary_i9_data else ''
        }
        
        # Extract document information
        section_2_docs = []
        section_3_docs = []
        supplement_b_docs = []
        
        if result.primary_i9_data:
            if hasattr(result.primary_i9_data, 'section_2_documents') and result.primary_i9_data.section_2_documents:
                section_2_docs = [f"{doc.document_type} ({doc.document_number}) [Exp: {doc.expiration_date}]" 
                                for doc in result.primary_i9_data.section_2_documents]
            
            if hasattr(result.primary_i9_data, 'section_3_documents') and result.primary_i9_data.section_3_documents:
                section_3_docs = [f"{doc.document_type} ({doc.document_number}) [Exp: {doc.expiration_date}]" 
                                for doc in result.primary_i9_data.section_3_documents]
            
            if hasattr(result.primary_i9_data, 'supplement_b_documents') and result.primary_i9_data.supplement_b_documents:
                supplement_b_docs = [f"{doc.document_type} ({doc.document_number}) [Exp: {doc.expiration_date}]" 
                                   for doc in result.primary_i9_data.supplement_b_documents]
        
        # Determine supporting documents based on form type
        supporting_docs = section_3_docs or supplement_b_docs or section_2_docs
        
        # Build final record
        record = {
            **employee_data,
            'section_2_documents': ', '.join(section_2_docs),
            'section_3_documents': ', '.join(section_3_docs),
            'supplement_b_documents': ', '.join(supplement_b_docs),
            'supporting_documents_found': ', '.join(supporting_docs),
            'supporting_documents_attached': ', '.join(supporting_docs) if supporting_docs else 'None',
            'supporting_documents_not_attached': 'None',
            'expiry_date_matches': result.expiration_matches or 0,
            'expiry_date_mismatches': 0,
            'document_attachment_status': result.document_attachment_status or 'UNKNOWN',
            'document_reference_matches': f"{result.document_matches_found or 0}/{result.supporting_documents_count or 0}",
            'form_type_selected': result.form_type_selected or '',
            'selection_reason': result.selection_reason or '',
            'processing_status': status,
            'validation_score': 'N/A',  # Not available in ProcessingResult
            'pdf_file_name': f"{employee_data['last_name']}.pdf" if employee_data['last_name'] else 'Unknown.pdf',
            'input_file_path': f"input/{employee_data['last_name']}.pdf" if employee_data['last_name'] else 'input/Unknown.pdf',
            'processing_time': '0s',
            'notes': result.notes or ''
        }
        
        return record
    
    def _get_employer_signature_date_from_selected_set(self, selected_set):
        """Get the employer signature date from the actual selected Section 3 or Supplement B pages"""
        
        # Check Section 3 pages first (higher priority in business rules)
        if hasattr(selected_set, 'section_3_pages') and selected_set.section_3_pages:
            latest_date = None
            for sec3_page in selected_set.section_3_pages:
                if hasattr(sec3_page, 'extracted_values'):
                    # Try Section 3 specific employer signature date fields
                    employer_sig_date = (
                        sec3_page.extracted_values.get('reverification_signature_date') or
                        sec3_page.extracted_values.get('employer_signature_date') or
                        sec3_page.extracted_values.get('section_3_employer_signature_date') or
                        sec3_page.extracted_values.get('reverification_employer_signature_date')
                    )
                    
                    if employer_sig_date and employer_sig_date != 'N/A':
                        if not latest_date or employer_sig_date > latest_date:
                            latest_date = employer_sig_date
            
            if latest_date:
                return latest_date
        
        # Check Supplement B pages
        if hasattr(selected_set, 'supplement_b_pages') and selected_set.supplement_b_pages:
            latest_date = None
            for supp_b_page in selected_set.supplement_b_pages:
                if hasattr(supp_b_page, 'extracted_values'):
                    # Try Supplement B specific employer signature date fields
                    employer_sig_date = (
                        supp_b_page.extracted_values.get('employer_signature_date') or
                        supp_b_page.extracted_values.get('employer_signature_date_1') or
                        supp_b_page.extracted_values.get('employer_signature_date_2') or
                        supp_b_page.extracted_values.get('supplement_b_employer_signature_date') or
                        supp_b_page.extracted_values.get('rehire_employer_signature_date')
                    )
                    
                    if employer_sig_date and employer_sig_date != 'N/A':
                        if not latest_date or employer_sig_date > latest_date:
                            latest_date = employer_sig_date
            
            if latest_date:
                return latest_date
        
        # Fallback to Section 2 pages if no Section 3 or Supplement B
        if hasattr(selected_set, 'section_2_page') and selected_set.section_2_page:
            if hasattr(selected_set.section_2_page, 'extracted_values'):
                return selected_set.section_2_page.extracted_values.get('employer_signature_date')
        
        return None
