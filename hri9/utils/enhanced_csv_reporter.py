#!/usr/bin/env python3
"""
Enhanced CSV Reporter for Comprehensive I-9 Data Export

This module generates detailed CSV reports with all requested fields including:
- Personal information (name, DOB, SSN)
- Citizenship status and work authorization
- Supporting document details and attachment status
- Expiration date matching analysis
"""

import csv
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

from ..core.models import ProcessingResult, I9FormData, DocumentInfo
from ..utils.logging_config import logger
from ..config import settings


class EnhancedCSVReporter:
    """Enhanced CSV reporter for comprehensive I-9 data export"""
    
    def __init__(self, output_dir: str = None):
        """
        Initialize the enhanced CSV reporter.
        
        Args:
            output_dir (str, optional): Output directory for CSV files.
        """
        self.output_dir = Path(output_dir) if output_dir else Path("workdir/enhanced_reports")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def generate_comprehensive_report(self, results: List[Dict[str, Any]], filename: str = None) -> str:
        """
        Generate comprehensive CSV report with all requested fields.
        
        Args:
            results (List[Dict]): Processing results with I-9 data
            filename (str, optional): Custom filename for the report
            
        Returns:
            str: Path to generated CSV file
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"comprehensive_i9_report_{timestamp}.csv"
        
        output_path = self.output_dir / filename
        
        # Define comprehensive CSV headers
        headers = [
            # Personal Information
            'employee_id',
            'first_name',
            'last_name',
            'middle_initial',
            'date_of_birth',
            'ssn',
            
            # Citizenship and Work Authorization
            'is_us_citizen',
            'citizenship_status',
            'work_authorization_expiry_date',
            'alien_authorized_to_work_until',
            
            # Document Information
            'section_2_documents',
            'section_3_documents', 
            'supplement_b_documents',
            'supporting_documents_found',
            'supporting_documents_attached',
            'supporting_documents_not_attached',
            
            # Document Matching and Validation
            'expiry_date_matches',
            'expiry_date_mismatches',
            'document_attachment_status',
            'document_reference_matches',
            
            # Processing Metadata
            'form_type_selected',
            'selection_reason',
            'processing_status',
            'validation_score',
            'employee_signature_date',
            'employer_signature_date',
            
            # File Information
            'pdf_file_name',
            'input_file_path',
            'processing_time',
            'notes'
        ]
        
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                
                for result in results:
                    row = self._extract_comprehensive_row(result)
                    writer.writerow(row)
            
            logger.info(f"Generated comprehensive CSV report: {output_path}")
            logger.info(f"Report contains {len(results)} records with {len(headers)} fields")
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error generating comprehensive CSV report: {e}")
            raise
    
    def _extract_comprehensive_row(self, result: Dict[str, Any]) -> Dict[str, str]:
        """
        Extract comprehensive row data from processing result.
        
        Args:
            result (Dict): Processing result data (can be flat dict or structured objects)
            
        Returns:
        """
        # Check if this is flat dictionary data (from actual processing results)
        if 'first_name' in result and 'primary_i9_data' not in result:
            return self._extract_from_flat_data(result)
        
        # Get primary I-9 data (original object-based format)
        i9_data = result.get('primary_i9_data')
        processing_result = result.get('processing_result')
        
        # Extract personal information (respect PII settings)
        def format_pii_field(value: str) -> str:
            """Format PII field based on settings"""
            if not value or value in ['[PII_REDACTED]', '[DATE_REDACTED]', 'N/A', '']:
                return '' if settings.CATALOG_INCLUDE_PII else '[REDACTED]'
            return value if settings.CATALOG_INCLUDE_PII else '[REDACTED]'
        
        row = {
            'employee_id': result.get('employee_id', ''),
            'first_name': format_pii_field(i9_data.first_name if i9_data else ''),
            'last_name': format_pii_field(i9_data.last_name if i9_data else ''),
            'middle_initial': format_pii_field(i9_data.middle_initial if i9_data else ''),
            'date_of_birth': format_pii_field(i9_data.date_of_birth if i9_data else ''),
            'ssn': self._format_ssn(i9_data.ssn if i9_data else ''),
        }
        
        # Extract citizenship information
        if i9_data:
            row.update({
                'is_us_citizen': 'Yes' if i9_data.citizenship_status.name == 'US_CITIZEN' else 'No',
                'citizenship_status': i9_data.citizenship_status.name,
                'work_authorization_expiry_date': format_pii_field(i9_data.authorized_to_work_until or ''),
                'alien_authorized_to_work_until': format_pii_field(i9_data.get_alien_expiration_date() or ''),
            })
        else:
            row.update({
                'is_us_citizen': '',
                'citizenship_status': '',
                'work_authorization_expiry_date': '',
                'alien_authorized_to_work_until': '',
            })
        
        # Extract document information
        doc_info = self._extract_document_information(i9_data, result)
        row.update(doc_info)
        
        # Extract matching and validation information
        matching_info = self._extract_matching_information(result)
        row.update(matching_info)
        
        # Extract processing metadata
        if processing_result:
            row.update({
                'form_type_selected': processing_result.form_type_selected,
                'selection_reason': processing_result.selection_reason,
                'processing_status': processing_result.status.name,
                'validation_score': f"{getattr(processing_result, 'validation_success_rate', 0):.1f}%",
                'processing_time': f"{getattr(processing_result, 'processing_time', 0):.1f}s",
            })
        else:
            row.update({
                'form_type_selected': '',
                'selection_reason': '',
                'validation_score': '',
                'processing_time': '',
            })
        
        # Extract signature dates
        row.update({
            'employee_signature_date': format_pii_field(i9_data.employee_signature_date if i9_data else ''),
            'employer_signature_date': format_pii_field(i9_data.employer_signature_date if i9_data else ''),
        })
        
        # Extract file information
        row.update({
            'pdf_file_name': result.get('pdf_file_name', ''),
            'input_file_path': result.get('input_file_path', ''),
            'notes': processing_result.notes if processing_result else '',
        })
        
        return row
    
    def _extract_from_flat_data(self, result: Dict[str, Any]) -> Dict[str, str]:
        """
        Extract comprehensive row data from flat dictionary (actual processing results).
        
        Args:
            result (Dict): Flat dictionary with processing result data
            
        Returns:
            Dict[str, str]: Formatted row data for CSV
        """
        # Map flat data directly to CSV columns
        return {
            'employee_id': str(result.get('employee_id', '')),
            'first_name': str(result.get('first_name', '')),
            'last_name': str(result.get('last_name', '')),
            'middle_initial': str(result.get('middle_initial', '')),
            'date_of_birth': str(result.get('date_of_birth', '')),
            'ssn': str(result.get('ssn', '')),
            'is_us_citizen': str(result.get('is_us_citizen', '')),
            'citizenship_status': str(result.get('citizenship_status', '')),
            'work_authorization_expiry_date': str(result.get('work_authorization_expiry_date', '')),
            'alien_authorized_to_work_until': str(result.get('alien_authorized_to_work_until', '')),
            'section_2_documents': str(result.get('section_2_documents', '')),
            'section_3_documents': str(result.get('section_3_documents', '')),
            'supplement_b_documents': str(result.get('supplement_b_documents', '')),
            'supporting_documents_found': str(result.get('supporting_documents_found', '')),
            'supporting_documents_attached': str(result.get('supporting_documents_attached', '')),
            'supporting_documents_not_attached': str(result.get('supporting_documents_not_attached', '')),
            'expiry_date_matches': str(result.get('expiry_date_matches', '')),
            'expiry_date_mismatches': str(result.get('expiry_date_mismatches', '')),
            'document_attachment_status': str(result.get('document_attachment_status', '')),
            'document_reference_matches': str(result.get('document_reference_matches', '')),
            'form_type_selected': str(result.get('form_type_selected', '')),
            'selection_reason': str(result.get('selection_reason', '')),
            'processing_status': str(result.get('processing_status', '')),
            'validation_score': str(result.get('validation_score', '')),
            'employee_signature_date': str(result.get('employee_signature_date', '')),
            'employer_signature_date': str(result.get('employer_signature_date', '')),
            'pdf_file_name': str(result.get('pdf_file_name', '')),
            'input_file_path': str(result.get('input_file_path', '')),
            'processing_time': str(result.get('processing_time', '')),
            'notes': str(result.get('notes', ''))
        }
    
    def _extract_document_information(self, i9_data: I9FormData, result: Dict) -> Dict[str, str]:
        """Extract document information from the SELECTED I-9 form only (based on priority hierarchy)."""
        
        doc_info = {
            'section_2_documents': '',
            'section_3_documents': '',
            'supplement_b_documents': '',
            'supporting_documents_found': '',
            'supporting_documents_attached': '',
            'supporting_documents_not_attached': '',
        }
        
        if not i9_data:
            return doc_info
        
        # Determine which form type was selected based on processing result
        processing_result = result.get('processing_result')
        form_type_selected = processing_result.form_type_selected if processing_result else "new_hire"
        
        # Only extract documents from the SELECTED form type based on priority hierarchy
        if form_type_selected == "rehire_supplement_b":
            # Highest Priority: Supplement B form selected
            supplement_b_docs = []
            for doc in i9_data.supplement_b_documents:
                doc_str = f"{doc.document_type}"
                if doc.document_number and doc.document_number != "Not visible":
                    doc_str += f" ({doc.document_number})"
                if doc.expiration_date and doc.expiration_date != "Not visible":
                    doc_str += f" [Exp: {doc.expiration_date}]"
                supplement_b_docs.append(doc_str)
            doc_info['supplement_b_documents'] = "; ".join(supplement_b_docs)
            
            # Use Supplement B documents as the primary source
            primary_docs = i9_data.supplement_b_documents
            
        elif form_type_selected == "reverification_section_3":
            # Second Priority: Section 3 form selected
            section_3_docs = []
            for doc in i9_data.section_3_documents:
                doc_str = f"{doc.document_type}"
                if doc.document_number and doc.document_number != "Not visible":
                    doc_str += f" ({doc.document_number})"
                if doc.expiration_date and doc.expiration_date != "Not visible":
                    doc_str += f" [Exp: {doc.expiration_date}]"
                section_3_docs.append(doc_str)
            doc_info['section_3_documents'] = "; ".join(section_3_docs)
            
            # Use Section 3 documents as the primary source
            primary_docs = i9_data.section_3_documents
            
        else:
            # Lowest Priority: Basic Section 1+2 form selected (new_hire)
            section_2_docs = []
            for doc in i9_data.section_2_documents:
                doc_str = f"{doc.document_type}"
                if doc.document_number and doc.document_number != "Not visible":
                    doc_str += f" ({doc.document_number})"
                if doc.expiration_date and doc.expiration_date != "Not visible":
                    doc_str += f" [Exp: {doc.expiration_date}]"
                section_2_docs.append(doc_str)
            doc_info['section_2_documents'] = "; ".join(section_2_docs)
            
            # Use Section 2 documents as the primary source
            primary_docs = i9_data.section_2_documents
        
        # Extract supporting document attachment status ONLY from the selected form
        attached_docs = [doc.document_type for doc in primary_docs if doc.is_attached]
        not_attached_docs = [doc.document_type for doc in primary_docs if not doc.is_attached]
        
        doc_info['supporting_documents_found'] = str(len(primary_docs))
        doc_info['supporting_documents_attached'] = "; ".join(attached_docs) if attached_docs else "None"
        doc_info['supporting_documents_not_attached'] = "; ".join(not_attached_docs) if not_attached_docs else "None"
        
        logger.debug(f"Extracted documents from {form_type_selected}: {len(primary_docs)} documents found")
        
        return doc_info
    
    def _extract_matching_information(self, result: Dict) -> Dict[str, str]:
        """Extract document matching and validation information."""
        
        matching_info = {
            'expiry_date_matches': '',
            'expiry_date_mismatches': '',
            'document_attachment_status': '',
            'document_reference_matches': '',
        }
        
        processing_result = result.get('processing_result')
        if not processing_result:
            return matching_info
        
        # Extract expiry matching information
        expiry_matches = getattr(processing_result, 'expiration_matches', 0)
        matching_info['expiry_date_matches'] = str(expiry_matches)
        
        # Extract document matching information
        doc_matches = getattr(processing_result, 'document_matches_found', 0)
        total_docs = getattr(processing_result, 'supporting_documents_count', 0)
        doc_mismatches = max(0, total_docs - doc_matches)
        
        matching_info['expiry_date_mismatches'] = str(doc_mismatches)
        matching_info['document_reference_matches'] = f"{doc_matches}/{total_docs}"
        
        # Document attachment status summary
        if expiry_matches > 0:
            matching_info['document_attachment_status'] = "VERIFIED"
        elif doc_matches > 0:
            matching_info['document_attachment_status'] = "PARTIAL"
        else:
            matching_info['document_attachment_status'] = "NOT_VERIFIED"
        
        return matching_info
    
    def _format_ssn(self, ssn: str) -> str:
        """Format SSN for display (mask for privacy unless PII is enabled)."""
        if not ssn or ssn in ['[PII_REDACTED]', 'N/A', '']:
            return '[REDACTED]' if not settings.CATALOG_INCLUDE_PII else ''
        
        # If PII is enabled, show full SSN
        if settings.CATALOG_INCLUDE_PII:
            return ssn
        
        # Otherwise, mask SSN for privacy (show only last 4 digits)
        if len(ssn) >= 4:
            return f"***-**-{ssn[-4:]}"
        else:
            return '[REDACTED]'
    
    def generate_summary_report(self, results: List[Dict[str, Any]]) -> str:
        """Generate a summary report with key statistics."""
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        summary_path = self.output_dir / f"i9_processing_summary_{timestamp}.csv"
        
        # Calculate summary statistics
        total_processed = len(results)
        us_citizens = sum(1 for r in results if self._is_us_citizen(r))
        non_citizens = total_processed - us_citizens
        
        # Document attachment statistics
        fully_attached = sum(1 for r in results if self._has_all_documents_attached(r))
        partially_attached = sum(1 for r in results if self._has_some_documents_attached(r))
        no_attachments = total_processed - fully_attached - partially_attached
        
        # Expiry date matching statistics
        expiry_matches = sum(1 for r in results if self._has_expiry_matches(r))
        
        summary_data = [
            {'Metric': 'Total Employees Processed', 'Count': total_processed, 'Percentage': '100.0%'},
            {'Metric': 'US Citizens', 'Count': us_citizens, 'Percentage': f'{(us_citizens/total_processed*100):.1f}%'},
            {'Metric': 'Non-US Citizens', 'Count': non_citizens, 'Percentage': f'{(non_citizens/total_processed*100):.1f}%'},
            {'Metric': 'Fully Attached Documents', 'Count': fully_attached, 'Percentage': f'{(fully_attached/total_processed*100):.1f}%'},
            {'Metric': 'Partially Attached Documents', 'Count': partially_attached, 'Percentage': f'{(partially_attached/total_processed*100):.1f}%'},
            {'Metric': 'No Document Attachments', 'Count': no_attachments, 'Percentage': f'{(no_attachments/total_processed*100):.1f}%'},
            {'Metric': 'Expiry Date Matches Found', 'Count': expiry_matches, 'Percentage': f'{(expiry_matches/total_processed*100):.1f}%'},
        ]
        
        with open(summary_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['Metric', 'Count', 'Percentage'])
            writer.writeheader()
            writer.writerows(summary_data)
        
        logger.info(f"Generated summary report: {summary_path}")
        return str(summary_path)
    
    def _is_us_citizen(self, result: Dict) -> bool:
        """Check if employee is US citizen."""
        i9_data = result.get('primary_i9_data')
        return i9_data and i9_data.citizenship_status.name == 'US_CITIZEN'
    
    def _has_all_documents_attached(self, result: Dict) -> bool:
        """Check if all documents are attached."""
        i9_data = result.get('primary_i9_data')
        if not i9_data:
            return False
        all_docs = i9_data.get_all_documents()
        return all(doc.is_attached for doc in all_docs) if all_docs else False
    
    def _has_some_documents_attached(self, result: Dict) -> bool:
        """Check if some documents are attached."""
        i9_data = result.get('primary_i9_data')
        if not i9_data:
            return False
        all_docs = i9_data.get_all_documents()
        return any(doc.is_attached for doc in all_docs) if all_docs else False
    
    def _has_expiry_matches(self, result: Dict) -> bool:
        """Check if expiry date matches were found."""
        processing_result = result.get('processing_result')
        return processing_result and getattr(processing_result, 'expiration_matches', 0) > 0
