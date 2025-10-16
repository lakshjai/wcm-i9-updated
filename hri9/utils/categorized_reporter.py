#!/usr/bin/env python3
"""
Categorized Reporter for I-9 Processing Results

This module creates separate output files for different processing result categories:
- SUCCESS: Fully successful processing
- PARTIAL_SUCCESS: Partial success with some issues
- ERROR: Failed processing with errors
"""

import os
import csv
from typing import Dict, List, Any
from pathlib import Path
from datetime import datetime

from .logging_config import logger


class CategorizedReporter:
    """Creates categorized output files based on processing results"""
    
    def __init__(self, output_dir: str):
        """
        Initialize the categorized reporter
        
        Args:
            output_dir: Directory to save categorized output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize output files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        self.success_file = self.output_dir / f"i9_success_{timestamp}.csv"
        self.partial_file = self.output_dir / f"i9_partial_{timestamp}.csv"
        self.error_file = self.output_dir / f"i9_errors_{timestamp}.csv"
        
        # CSV headers
        self.headers = [
            'employee_id', 'pdf_file_name', 'processing_status', 'validation_score',
            'first_name', 'last_name', 'citizenship_status', 'employee_signature_date',
            'total_forms_detected', 'form_type_selected', 'selection_reason',
            'critical_issues', 'total_validations', 'passed_validations', 'failed_validations',
            'document_matches_found', 'supporting_documents_count', 'expiration_matches',
            'documents_mentioned_count', 'documents_attached_count', 'documents_missing_count', 'document_attachment_status',
            'business_rules_applied', 'scenario_results', 'validation_details',
            'input_file_path', 'catalog_file_path', 'processing_time', 'notes'
        ]
        
        # Initialize CSV files
        self._initialize_csv_files()
        
        # Counters
        self.success_count = 0
        self.partial_count = 0
        self.error_count = 0
        
        logger.info(f"Categorized reporter initialized with output directory: {output_dir}")
    
    def _initialize_csv_files(self):
        """Initialize CSV files with headers"""
        
        for file_path in [self.success_file, self.partial_file, self.error_file]:
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(self.headers)
    
    def add_result(self, employee_id: str, pdf_path: str, processing_result: Any, 
                   validation_result: Any = None, catalog_data: Dict = None):
        """
        Add a processing result to the appropriate category file
        
        Args:
            employee_id: Employee ID
            pdf_path: Path to the processed PDF
            processing_result: Result from enhanced processor
            validation_result: Comprehensive validation result
            catalog_data: Catalog data for the document
        """
        
        try:
            # Determine category based on processing result
            status = processing_result.status.value if hasattr(processing_result, 'status') else 'ERROR'
            
            if status == 'COMPLETE_SUCCESS':
                category = 'SUCCESS'
                self.success_count += 1
                output_file = self.success_file
            elif status in ['PARTIAL_SUCCESS', 'PARTIAL']:
                category = 'PARTIAL_SUCCESS'
                self.partial_count += 1
                output_file = self.partial_file
            else:
                category = 'ERROR'
                self.error_count += 1
                output_file = self.error_file
            
            # Extract data for CSV row
            row_data = self._extract_row_data(
                employee_id, pdf_path, processing_result, validation_result, catalog_data, category
            )
            
            # Write to appropriate file
            with open(output_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(row_data)
            
            logger.debug(f"Added {category} result for employee {employee_id}")
            
        except Exception as e:
            logger.error(f"Error adding result for employee {employee_id}: {e}")
            # Add to error file as fallback
            self._add_error_result(employee_id, pdf_path, str(e))
    
    def _extract_row_data(self, employee_id: str, pdf_path: str, processing_result: Any,
                         validation_result: Any, catalog_data: Dict, category: str) -> List[str]:
        """Extract data for CSV row"""
        
        # Basic information
        pdf_name = os.path.basename(pdf_path)
        
        # Processing result data
        if hasattr(processing_result, 'primary_i9_data') and processing_result.primary_i9_data:
            i9_data = processing_result.primary_i9_data
            first_name = getattr(i9_data, 'first_name', '')
            last_name = getattr(i9_data, 'last_name', '')
            citizenship_status = getattr(i9_data, 'citizenship_status', '')
            employee_signature_date = getattr(i9_data, 'employee_signature_date', '')
        else:
            first_name = last_name = citizenship_status = employee_signature_date = ''
        
        # Validation data
        if validation_result:
            validation_score = f"{validation_result.overall_score:.1f}%"
            critical_issues = validation_result.critical_issues
            total_validations = validation_result.total_issues
            passed_validations = total_validations - validation_result.critical_issues - validation_result.error_issues
            failed_validations = validation_result.critical_issues + validation_result.error_issues
            document_matches = len(validation_result.document_matches) if validation_result.document_matches else 0
        else:
            validation_score = "N/A"
            critical_issues = total_validations = passed_validations = failed_validations = 0
            document_matches = 0
        
        # Processing metadata
        total_forms = getattr(processing_result, 'total_forms_detected', 0)
        form_type = getattr(processing_result, 'form_type_selected', '')
        selection_reason = getattr(processing_result, 'selection_reason', '')
        notes = getattr(processing_result, 'notes', '')
        
        # Catalog data
        catalog_file_path = catalog_data.get('catalog_file_path', '') if catalog_data else ''
        processing_time = catalog_data.get('processing_time', 0) if catalog_data else 0
        
        return [
            employee_id,
            pdf_name,
            category,
            validation_score,
            first_name,
            last_name,
            citizenship_status,
            employee_signature_date,
            total_forms,
            form_type,
            selection_reason,
            critical_issues,
            total_validations,
            passed_validations,
            failed_validations,
            getattr(processing_result, 'document_matches_found', document_matches),  # document_matches_found
            getattr(processing_result, 'supporting_documents_count', 0),  # supporting_documents_count
            getattr(processing_result, 'expiration_matches', 0),  # expiration_matches ← FIXED!
            getattr(processing_result, 'documents_mentioned_count', 0),  # documents_mentioned_count ← NEW!
            getattr(processing_result, 'documents_attached_count', 0),  # documents_attached_count ← NEW!
            getattr(processing_result, 'documents_missing_count', 0),  # documents_missing_count ← NEW!
            getattr(processing_result, 'document_attachment_status', 'UNKNOWN'),  # document_attachment_status ← NEW!
            "Applied",  # business_rules_applied
            "",  # scenario_results
            "",  # validation_details
            pdf_path,
            catalog_file_path,
            processing_time,
            notes
        ]
    
    def _add_error_result(self, employee_id: str, pdf_path: str, error_message: str):
        """Add an error result"""
        
        row_data = [
            employee_id,
            os.path.basename(pdf_path),
            'ERROR',
            '0.0%',
            '', '', '', '',  # Empty I-9 data
            0, '', '',  # Processing metadata
            1, 1, 0, 1,  # Validation counts (1 critical error)
            0, 0, 'N/A',  # Document data
            'Failed', '', '',  # Business rules
            pdf_path, '', 0,  # File paths and timing
            f"Error: {error_message}"
        ]
        
        with open(self.error_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(row_data)
        
        self.error_count += 1
    
    def get_summary(self) -> Dict[str, int]:
        """Get processing summary"""
        return {
            'success': self.success_count,
            'partial': self.partial_count,
            'error': self.error_count,
            'total': self.success_count + self.partial_count + self.error_count
        }
    
    def finalize(self):
        """Finalize reporting and log summary"""
        
        summary = self.get_summary()
        
        logger.info("=== Categorized Processing Summary ===")
        logger.info(f"SUCCESS: {summary['success']} documents")
        logger.info(f"PARTIAL_SUCCESS: {summary['partial']} documents") 
        logger.info(f"ERROR: {summary['error']} documents")
        logger.info(f"TOTAL: {summary['total']} documents")
        logger.info("=====================================")
        
        logger.info(f"Results saved to:")
        logger.info(f"  - Success: {self.success_file}")
        logger.info(f"  - Partial: {self.partial_file}")
        logger.info(f"  - Errors: {self.error_file}")
        
        return summary
