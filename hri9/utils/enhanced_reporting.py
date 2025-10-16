#!/usr/bin/env python3
"""
Enhanced reporting utilities for I-9 detection with catalog integration.

This module provides functionality for generating comprehensive reports that include
catalog data, extracted information, and processing details.
"""

import os
import csv
import json
import time
from typing import Dict, List, Any, Optional
from ..utils.logging_config import logger

class EnhancedReporter:
    """Enhanced reporter class with catalog integration."""
    
    @staticmethod
    def initialize_enhanced_csv(csv_path, include_catalog_data=True):
        """
        Initialize an enhanced CSV file with comprehensive headers.
        
        Args:
            csv_path (str): Path to the CSV file.
            include_catalog_data (bool): Whether to include catalog data columns.
            
        Returns:
            tuple: (csv_file, csv_writer) or (None, None) if error.
        """
        try:
            headers = [
                # Basic Processing Info
                'Employee ID', 
                'PDF File Name', 
                'I-9 Forms Found', 
                'Pages Removed', 
                'Success', 
                'Processing Status',
                
                # Personal Information (Section 1)
                'First Name',
                'Last Name', 
                'Middle Initial',
                'Other Last Names Used',
                'Address',
                'Apt Number',
                'City',
                'State',
                'ZIP Code',
                'Date of Birth',
                'Social Security Number',
                'Email Address',
                'Phone Number',
                
                # Citizenship and Work Authorization
                'Citizenship Status',
                'Is US Citizen',
                'Is Non-US Citizen Authorized to Work',
                'Work Authorization Expiry Date',
                'Alien Registration Number',
                'I-94 Admission Number',
                'Foreign Passport Number',
                'Country of Issuance',
                
                # Form Details
                'Employee Signature Date',
                'Form Version',
                'I-9 Page Number',
                
                # Document Validation
                'Supporting Documents Found',
                'Document Expiry Matches Work Auth',
                'Document Validation Status',
                
                # File Paths
                'Extracted I-9 Path',
                'Input File Path',
                'Processed File Path'
            ]
            
            if include_catalog_data:
                headers.extend([
                    # Catalog and Processing Details
                    'Total Pages Cataloged',
                    'Catalog Processing Time (s)',
                    'Catalog API Calls',
                    'Document Classification',
                    'I9 Forms Count',
                    'Latest I9 Page',
                    'Manual Review Required',
                    'High Confidence Pages',
                    'Low Confidence Pages',
                    'Extracted Fields Count',
                    'Primary Document Type',
                    
                    # Business Rules Results
                    'Business Rules Status',
                    'Validation Success Rate',
                    'Critical Issues Count',
                    'Total Validations',
                    'Passed Validations',
                    'Failed Validations',
                    'Primary Scenario',
                    
                    # Technical Details
                    'Catalog JSON Data Extracts',
                    'Catalog File Text Path',
                    'Catalog File JSON Path'
                ])
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)
            
            # Open file and initialize writer
            csv_file = open(csv_path, 'w', newline='', encoding='utf-8')
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(headers)
            csv_file.flush()
            
            logger.info(f"Initialized enhanced CSV report at {csv_path}")
            return csv_file, csv_writer
        except Exception as e:
            logger.error(f"Error initializing enhanced CSV report: {e}")
            return None, None
    
    @staticmethod
    def write_enhanced_csv_row(csv_writer, csv_file, base_data, catalog_entry=None, 
                              catalog_files=None, processing_metrics=None):
        """
        Write an enhanced row to CSV with comprehensive I-9 data extraction.
        
        Args:
            csv_writer: CSV writer object.
            csv_file: CSV file object.
            base_data (dict): Base processing data.
            catalog_entry: Document catalog entry.
            catalog_files (dict): Paths to catalog files.
            processing_metrics (dict): Processing metrics.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            # Extract I-9 personal data from catalog
            i9_data = EnhancedReporter._extract_i9_personal_data(catalog_entry)
            
            # Log extraction results for debugging
            if i9_data and any(i9_data.values()):
                logger.info(f"Successfully extracted I-9 data: {len([k for k, v in i9_data.items() if v])} fields populated")
            else:
                logger.warning(f"No I-9 personal data extracted from catalog_entry type: {type(catalog_entry)}")
                if catalog_entry and hasattr(catalog_entry, 'pages'):
                    logger.warning(f"Catalog has {len(catalog_entry.pages)} pages")
                    for i, page in enumerate(catalog_entry.pages[:3]):  # Check first 3 pages
                        logger.warning(f"Page {i+1}: subtype='{getattr(page, 'page_subtype', 'N/A')}', has_data={bool(getattr(page, 'extracted_values', {}))}")
            
            # Base row data with personal information
            row_data = [
                # Basic Processing Info
                base_data.get('employee_id', ''),
                base_data.get('pdf_file_name', ''),
                base_data.get('i9_forms_found', 'No'),
                base_data.get('pages_removed', 0),
                base_data.get('success', 'No'),
                base_data.get('business_rules_status', 'ERROR'),
                
                # Personal Information (Section 1)
                i9_data.get('first_name', ''),
                i9_data.get('last_name', ''),
                i9_data.get('middle_initial', ''),
                i9_data.get('other_last_names', ''),
                i9_data.get('address', ''),
                i9_data.get('apt_number', ''),
                i9_data.get('city', ''),
                i9_data.get('state', ''),
                i9_data.get('zip_code', ''),
                i9_data.get('date_of_birth', ''),
                i9_data.get('social_security_number', ''),
                i9_data.get('email_address', ''),
                i9_data.get('phone_number', ''),
                
                # Citizenship and Work Authorization
                i9_data.get('citizenship_status', ''),
                i9_data.get('is_us_citizen', ''),
                i9_data.get('is_authorized_to_work', ''),
                i9_data.get('work_auth_expiry', ''),
                i9_data.get('alien_registration_number', ''),
                i9_data.get('i94_admission_number', ''),
                i9_data.get('foreign_passport_number', ''),
                i9_data.get('country_of_issuance', ''),
                
                # Form Details
                i9_data.get('employee_signature_date', ''),
                i9_data.get('form_version', ''),
                i9_data.get('i9_page_number', ''),
                
                # Document Validation
                i9_data.get('supporting_documents', ''),
                i9_data.get('expiry_matches', ''),
                i9_data.get('document_validation_status', ''),
                
                # File Paths
                base_data.get('extracted_i9_path', ''),
                base_data.get('input_file_path', ''),
                base_data.get('processed_file_path', '')
            ]
            
            # Add catalog data if available
            if catalog_entry:
                catalog_stats = EnhancedReporter._extract_catalog_stats(catalog_entry)
                
                row_data.extend([
                    # Catalog and Processing Details
                    catalog_stats.get('total_pages', 0),
                    catalog_stats.get('processing_time', 0.0),
                    catalog_stats.get('api_calls', 0),
                    catalog_stats.get('document_classification', ''),
                    catalog_stats.get('i9_forms_count', 0),
                    catalog_stats.get('latest_i9_page', ''),
                    catalog_stats.get('manual_review_required', False),
                    catalog_stats.get('high_confidence_pages', 0),
                    catalog_stats.get('low_confidence_pages', 0),
                    catalog_stats.get('extracted_fields_count', 0),
                    catalog_stats.get('primary_document_type', ''),
                    
                    # Business Rules Results
                    base_data.get('business_rules_status', 'ERROR'),
                    base_data.get('validation_success_rate', '0.0%'),
                    base_data.get('critical_issues', 0),
                    base_data.get('total_validations', 0),
                    base_data.get('passed_validations', 0),
                    base_data.get('failed_validations', 0),
                    base_data.get('primary_scenario', 'None'),
                    
                    # Technical Details
                    json.dumps(catalog_stats.get('extracted_data', {}), separators=(',', ':')),
                    catalog_files.get('text_path', '') if catalog_files else '',
                    catalog_files.get('json_path', '') if catalog_files else ''
                ])
            else:
                # Fill with empty values if no catalog data
                empty_catalog_data = [''] * 21  # 21 catalog columns
                row_data.extend(empty_catalog_data)
            
            csv_writer.writerow(row_data)
            csv_file.flush()
            return True
        except Exception as e:
            logger.error(f"Error writing enhanced CSV row: {e}")
            return False
    
    @staticmethod
    def _extract_i9_personal_data(catalog_entry):
        """Extract I-9 personal data from catalog entry"""
        i9_data = {}
        
        if not catalog_entry:
            return i9_data
        
        # Handle both dict (JSON) and object formats
        pages = []
        if isinstance(catalog_entry, dict):
            pages = catalog_entry.get('pages', [])
        elif hasattr(catalog_entry, 'pages'):
            pages = catalog_entry.pages
        else:
            return i9_data
        
        # Find the first I-9 form page with extracted values
        for page in pages:
            if isinstance(page, dict):
                page_subtype = page.get('page_subtype', '')
                extracted_data = page.get('extracted_values', {})
                page_number = page.get('page_number', '')
            else:
                page_subtype = getattr(page, 'page_subtype', '')
                extracted_data = getattr(page, 'extracted_values', {})  # Fixed: use extracted_values not extracted_data
                page_number = getattr(page, 'page_number', '')
            
            if page_subtype == 'i9_form' and extracted_data:
                
                extracted = extracted_data
                
                # Personal Information - handle multiple field name formats
                i9_data['first_name'] = (extracted.get('employee_first_name') or 
                                        extracted.get('first_name', ''))
                i9_data['last_name'] = (extracted.get('employee_last_name') or 
                                       extracted.get('last_name', ''))
                i9_data['middle_initial'] = (extracted.get('employee_middle_initial') or 
                                            extracted.get('middle_initial', ''))
                i9_data['other_last_names'] = (extracted.get('employee_other_last_names') or 
                                              extracted.get('other_last_names_used', ''))
                i9_data['address'] = (extracted.get('employee_address') or 
                                     extracted.get('address', ''))
                i9_data['apt_number'] = (extracted.get('employee_apt_number') or 
                                        extracted.get('apt_number', ''))
                i9_data['city'] = (extracted.get('employee_city') or 
                                  extracted.get('city', ''))
                i9_data['state'] = (extracted.get('employee_state') or 
                                   extracted.get('state', ''))
                i9_data['zip_code'] = (extracted.get('employee_zip_code') or 
                                      extracted.get('zip_code', ''))
                i9_data['date_of_birth'] = (extracted.get('employee_dob') or 
                                           extracted.get('employee_date_of_birth') or
                                           extracted.get('date_of_birth', ''))
                i9_data['social_security_number'] = (extracted.get('employee_social_security_number') or 
                                                    extracted.get('social_security_number', ''))
                i9_data['email_address'] = (extracted.get('employee_email') or 
                                           extracted.get('employee_email_address') or
                                           extracted.get('email_address', ''))
                i9_data['phone_number'] = (extracted.get('employee_telephone_number') or 
                                          extracted.get('telephone_number') or
                                          extracted.get('section_1_telephone_number') or
                                          extracted.get('employee_phone', ''))
                
                # Citizenship and Work Authorization
                citizenship_status = extracted.get('citizenship_status', '')
                i9_data['citizenship_status'] = citizenship_status
                i9_data['is_us_citizen'] = 'Yes' if citizenship_status in ['us_citizen', 'citizen'] else 'No'
                i9_data['is_authorized_to_work'] = 'Yes' if 'authorized' in citizenship_status.lower() else 'No'
                i9_data['work_auth_expiry'] = (extracted.get('work_authorization_expiration_date') or 
                                              extracted.get('alien_authorized_to_work_until') or
                                              extracted.get('alien_authorized_until_date') or 
                                              extracted.get('alien_expiration_date', ''))
                i9_data['alien_registration_number'] = extracted.get('alien_registration_number', '')
                i9_data['i94_admission_number'] = extracted.get('form_i94_admission_number', '')
                i9_data['foreign_passport_number'] = extracted.get('foreign_passport_number', '')
                i9_data['country_of_issuance'] = extracted.get('country_of_issuance', '')
                
                # Form Details
                i9_data['employee_signature_date'] = extracted.get('employee_signature_date', '')
                i9_data['form_version'] = extracted.get('form_version', '')
                i9_data['i9_page_number'] = str(page_number) if page_number else ''
                
                # Document Validation (will be enhanced later)
                i9_data['supporting_documents'] = 'Found' if extracted.get('list_a_document_1') or extracted.get('list_b_document_1') else 'Not Found'
                i9_data['expiry_matches'] = 'Pending Validation'
                i9_data['document_validation_status'] = 'Needs Review'
                
                break  # Use first I-9 form found
        
        return i9_data
    
    @staticmethod
    def _extract_catalog_stats(catalog_entry):
        """Extract catalog statistics from catalog entry"""
        stats = {}
        
        if not catalog_entry:
            return stats
        
        # Handle both dict (JSON) and object formats
        if isinstance(catalog_entry, dict):
            pages = catalog_entry.get('pages', [])
            classification = catalog_entry.get('document_classification', {})
            processing = catalog_entry.get('processing_summary', {})
        else:
            pages = getattr(catalog_entry, 'pages', [])
            classification = getattr(catalog_entry, 'document_classification', None)
            processing = getattr(catalog_entry, 'processing_summary', None)
        
        # Basic stats
        stats['total_pages'] = len(pages)
        stats['processing_time'] = 0.0
        stats['api_calls'] = 0
        stats['document_classification'] = ''
        stats['i9_forms_count'] = 0
        stats['latest_i9_page'] = ''
        stats['manual_review_required'] = False
        stats['high_confidence_pages'] = 0
        stats['low_confidence_pages'] = 0
        stats['extracted_fields_count'] = 0
        stats['primary_document_type'] = ''
        stats['extracted_data'] = {}
        
        # Extract from processing data
        if isinstance(processing, dict):
            stats['processing_time'] = processing.get('processing_time_seconds', 0.0)
            stats['api_calls'] = processing.get('api_calls_made', 0)
            stats['manual_review_required'] = processing.get('manual_review_required', False)
            stats['high_confidence_pages'] = processing.get('high_confidence_pages', 0)
            stats['low_confidence_pages'] = processing.get('low_confidence_pages', 0)
        elif processing:  # Object format
            stats['processing_time'] = getattr(processing, 'processing_time_seconds', 0.0)
            stats['api_calls'] = getattr(processing, 'api_calls_made', 0)
            stats['manual_review_required'] = getattr(processing, 'manual_review_required', False)
            stats['high_confidence_pages'] = getattr(processing, 'high_confidence_pages', 0)
            stats['low_confidence_pages'] = getattr(processing, 'low_confidence_pages', 0)
        
        # Extract from classification data
        if isinstance(classification, dict):
            stats['document_classification'] = classification.get('primary_document_type', '')
            stats['i9_forms_count'] = classification.get('i9_form_count', 0)
            stats['latest_i9_page'] = classification.get('latest_i9_page', '')
            stats['primary_document_type'] = classification.get('primary_document_type', '')
        elif classification:  # Object format
            stats['document_classification'] = getattr(classification, 'primary_document_type', '')
            stats['i9_forms_count'] = getattr(classification, 'i9_form_count', 0)
            stats['latest_i9_page'] = getattr(classification, 'latest_i9_page', '')
            stats['primary_document_type'] = getattr(classification, 'primary_document_type', '')
        
        # Count extracted fields
        field_count = 0
        for page in pages:
            if isinstance(page, dict):
                extracted_values = page.get('extracted_values', {})
                if extracted_values:
                    field_count += len(extracted_values)
            elif hasattr(page, 'extracted_values') and page.extracted_values:
                field_count += len(page.extracted_values)
        stats['extracted_fields_count'] = field_count
        
        return stats
    
    @staticmethod
    def _extract_catalog_json_data(catalog_entry):
        """
        Extract key JSON data from catalog entry for CSV storage.
        
        Args:
            catalog_entry: Document catalog entry.
            
        Returns:
            dict: Extracted data suitable for JSON serialization.
        """
        try:
            extracted_data = {
                'document_id': catalog_entry.document_id,
                'document_name': catalog_entry.document_name,
                'total_pages': catalog_entry.total_pages,
                'processing_timestamp': catalog_entry.processing_timestamp
            }
            
            # Add document classification
            if catalog_entry.document_classification:
                extracted_data['classification'] = {
                    'primary_type': catalog_entry.document_classification.primary_document_type,
                    'i9_form_count': catalog_entry.document_classification.i9_form_count,
                    'latest_i9_page': catalog_entry.document_classification.latest_i9_page,
                    'contains_government_forms': catalog_entry.document_classification.contains_government_forms,
                    'contains_identity_documents': catalog_entry.document_classification.contains_identity_documents,
                    'contains_employment_records': catalog_entry.document_classification.contains_employment_records
                }
            
            # Add processing summary
            if catalog_entry.processing_summary:
                extracted_data['processing'] = {
                    'pages_analyzed': catalog_entry.processing_summary.total_pages_analyzed,
                    'api_calls': catalog_entry.processing_summary.api_calls_made,
                    'processing_time': catalog_entry.processing_summary.processing_time_seconds,
                    'high_confidence_pages': catalog_entry.processing_summary.high_confidence_pages,
                    'low_confidence_pages': catalog_entry.processing_summary.low_confidence_pages,
                    'error_count': catalog_entry.processing_summary.error_count,
                    'manual_review_required': catalog_entry.processing_summary.manual_review_required
                }
            
            # Add page summaries
            if catalog_entry.pages:
                page_summaries = []
                for page in catalog_entry.pages:
                    if page:
                        page_summary = {
                            'page_number': page.page_number,
                            'page_type': page.page_type,
                            'page_subtype': page.page_subtype,
                            'confidence_score': page.confidence_score,
                            'page_title': page.page_title
                        }
                        
                        # Add extracted values if available
                        if page.extracted_values:
                            # Only include non-sensitive extracted values
                            safe_values = {}
                            for key, value in page.extracted_values.items():
                                if not EnhancedReporter._is_sensitive_field(key):
                                    safe_values[key] = value
                            if safe_values:
                                page_summary['extracted_values'] = safe_values
                        
                        page_summaries.append(page_summary)
                
                extracted_data['pages'] = page_summaries
            
            return extracted_data
        except Exception as e:
            logger.error(f"Error extracting catalog JSON data: {e}")
            return {}
    
    @staticmethod
    def _is_sensitive_field(field_name):
        """
        Check if a field name indicates sensitive data.
        
        Args:
            field_name (str): Field name to check.
            
        Returns:
            bool: True if field is potentially sensitive.
        """
        sensitive_keywords = [
            'ssn', 'social_security', 'passport', 'license', 'id_number',
            'phone', 'address', 'email', 'birth', 'dob', 'alien_number'
        ]
        
        field_lower = field_name.lower()
        return any(keyword in field_lower for keyword in sensitive_keywords)
    
    @staticmethod
    def _count_extracted_fields(catalog_entry):
        """
        Count total extracted fields across all pages.
        
        Args:
            catalog_entry: Document catalog entry.
            
        Returns:
            int: Total number of extracted fields.
        """
        try:
            total_fields = 0
            if catalog_entry.pages:
                for page in catalog_entry.pages:
                    if page and page.extracted_values:
                        total_fields += len(page.extracted_values)
            return total_fields
        except Exception as e:
            logger.error(f"Error counting extracted fields: {e}")
            return 0
    
    @staticmethod
    def generate_catalog_summary_report(catalog_entries, output_path):
        """
        Generate a comprehensive summary report of catalog data.
        
        Args:
            catalog_entries (list): List of catalog entries.
            output_path (str): Path for the summary report.
            
        Returns:
            bool: True if report generated successfully.
        """
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write("CATALOG PROCESSING SUMMARY REPORT\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total Documents: {len(catalog_entries)}\n\n")
                
                # Document type distribution
                type_counts = {}
                i9_counts = {'with_i9': 0, 'without_i9': 0, 'multiple_i9': 0}
                total_pages = 0
                total_api_calls = 0
                total_processing_time = 0.0
                
                for entry in catalog_entries:
                    if entry.document_classification:
                        doc_type = entry.document_classification.primary_document_type
                        type_counts[doc_type] = type_counts.get(doc_type, 0) + 1
                        
                        i9_count = entry.document_classification.i9_form_count
                        if i9_count == 0:
                            i9_counts['without_i9'] += 1
                        elif i9_count == 1:
                            i9_counts['with_i9'] += 1
                        else:
                            i9_counts['multiple_i9'] += 1
                    
                    total_pages += entry.total_pages
                    if entry.processing_summary:
                        total_api_calls += entry.processing_summary.api_calls_made
                        total_processing_time += entry.processing_summary.processing_time_seconds
                
                f.write("DOCUMENT TYPE DISTRIBUTION\n")
                f.write("-" * 30 + "\n")
                for doc_type, count in sorted(type_counts.items()):
                    f.write(f"{doc_type}: {count}\n")
                
                f.write("\nI-9 FORM DISTRIBUTION\n")
                f.write("-" * 30 + "\n")
                f.write(f"Documents with I-9 forms: {i9_counts['with_i9']}\n")
                f.write(f"Documents without I-9 forms: {i9_counts['without_i9']}\n")
                f.write(f"Documents with multiple I-9 forms: {i9_counts['multiple_i9']}\n")
                
                f.write("\nPROCESSING STATISTICS\n")
                f.write("-" * 30 + "\n")
                f.write(f"Total pages processed: {total_pages}\n")
                f.write(f"Total API calls made: {total_api_calls}\n")
                f.write(f"Total processing time: {total_processing_time:.2f} seconds\n")
                f.write(f"Average pages per document: {total_pages / len(catalog_entries):.1f}\n")
                f.write(f"Average processing time per document: {total_processing_time / len(catalog_entries):.2f} seconds\n")
                
            logger.info(f"Generated catalog summary report: {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error generating catalog summary report: {e}")
            return False