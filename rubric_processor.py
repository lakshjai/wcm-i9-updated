#!/usr/bin/env python3
"""
I-9 Rubric-Based Processor
Standalone implementation that applies business rules rubric to catalog files
without modifying existing codebase.
"""

import json
import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import logging
from fuzzy_field_matcher import FuzzyFieldMatcher
from rubric_audit_logger import RubricAuditLogger
from json_flattener import flatten_extracted_values

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class I9RubricProcessor:
    """
    Processes I-9 catalog files using business rules rubric
    """
    
    def __init__(self):
        self.results = []
        self.fuzzy_matcher = FuzzyFieldMatcher(similarity_threshold=0.6)
        
    def process_catalog_file(self, catalog_path: str) -> Dict:
        """
        Process a single catalog file and return rubric-based results
        """
        logger.info(f"Processing catalog: {catalog_path}")
        
        try:
            with open(catalog_path, 'r', encoding='utf-8') as f:
                catalog_data = json.load(f)
            
            filename = Path(catalog_path).stem.replace('.catalog', '')
            
            # Initialize audit logger
            audit_logger = RubricAuditLogger(filename)
            
            # Apply rubric buckets
            bucket_1_score, bucket_1_data = self._bucket_1_personal_data(catalog_data)
            bucket_2_score, bucket_2_data = self._bucket_2_i9_detection(catalog_data)
            bucket_3_score, bucket_3_data = self._bucket_3_business_rules(catalog_data)
            bucket_4_score, bucket_4_data = self._bucket_4_work_authorization(catalog_data, bucket_3_data)
            bucket_5_score, bucket_5_data = self._bucket_5_document_tracking(catalog_data, bucket_3_data)
            
            # Extract comprehensive business fields
            business_fields = self._extract_comprehensive_business_fields(catalog_data, bucket_2_data, bucket_3_data)
            
            # Generate scoring reasons
            scoring_reasons = self._generate_scoring_reasons(
                bucket_1_score, bucket_1_data,
                bucket_2_score, bucket_2_data, 
                bucket_3_score, bucket_3_data,
                bucket_4_score, bucket_4_data,
                bucket_5_score, bucket_5_data
            )
            
            # Log bucket results to audit
            audit_logger.log_bucket_1_personal_data(bucket_1_score, bucket_1_data, 
                                                     scoring_reasons.get('bucket_1_reasons', '').split(' | '))
            audit_logger.log_bucket_2_i9_detection(bucket_2_score, bucket_2_data,
                                                   scoring_reasons.get('bucket_2_reasons', '').split(' | '))
            audit_logger.log_bucket_3_business_rules(bucket_3_score, bucket_3_data,
                                                     scoring_reasons.get('bucket_3_reasons', '').split(' | '))
            audit_logger.log_bucket_4_work_authorization(bucket_4_score, bucket_4_data,
                                                         scoring_reasons.get('bucket_4_reasons', '').split(' | '))
            audit_logger.log_bucket_5_document_tracking(bucket_5_score, bucket_5_data,
                                                        scoring_reasons.get('bucket_5_reasons', '').split(' | '))
            
            # Log business fields
            audit_logger.log_business_fields(business_fields)
            
            # Calculate total score and status
            total_score = bucket_1_score + bucket_2_score + bucket_3_score + bucket_4_score + bucket_5_score
            status, criteria_met, reasoning = self._determine_status_with_audit(total_score, bucket_1_score, bucket_2_score, bucket_3_score, business_fields)
            
            # Log status determination
            citizenship = business_fields.get('citizenship_status', 'UNKNOWN')
            audit_logger.log_status_determination(status, citizenship, criteria_met, reasoning)
            
            # Add summary
            audit_logger.add_summary(total_score)
            
            # Save audit log
            audit_output_path = f"workdir/audit_logs/{filename}_audit.json"
            Path("workdir/audit_logs").mkdir(parents=True, exist_ok=True)
            audit_logger.save(audit_output_path)
            
            result = {
                'filename': filename,
                'status': status,
                # Business fields first
                **business_fields,
                # Scoring details last
                'total_score': total_score,
                'bucket_1_personal_data_score': bucket_1_score,
                'bucket_2_i9_detection_score': bucket_2_score,
                'bucket_3_business_rules_score': bucket_3_score,
                'bucket_4_work_authorization_score': bucket_4_score,
                'bucket_5_document_tracking_score': bucket_5_score,
                **scoring_reasons
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing {catalog_path}: {e}")
            return self._create_error_result(filename, str(e))
    
    def _bucket_1_personal_data(self, catalog_data: Dict) -> Tuple[int, Dict]:
        """
        BUCKET 1: PERSONAL DATA EXTRACTION (25 points)
        Extract personal information from catalog
        """
        score = 0
        data = {
            'first_name': '',
            'middle_name': '',
            'last_name': '',
            'date_of_birth': '',
            'ssn': '',
            'citizenship_status': ''
        }
        
        pages = catalog_data.get('document_catalog', {}).get('pages', catalog_data.get('pages', []))
        
        # Look for personal data in Section 1 pages
        for page in pages:
            page_title = page.get('page_title', '').lower()
            extracted = page.get('extracted_values', {})
            
            if 'section 1' in page_title or 'employment eligibility' in page_title:
                # First Name (5 points)
                if not data['first_name']:
                    first_name = (extracted.get('first_name') or 
                                extracted.get('employee_first_name') or 
                                extracted.get('section_1_first_name'))
                    if first_name and first_name not in ['N/A', '', None]:
                        data['first_name'] = str(first_name)
                        score += 5
                
                # Last Name (5 points)
                if not data['last_name']:
                    last_name = (extracted.get('last_name') or 
                               extracted.get('employee_last_name') or 
                               extracted.get('section_1_last_name'))
                    if last_name and last_name not in ['N/A', '', None]:
                        data['last_name'] = str(last_name)
                        score += 5
                
                # Middle Name (3 points)
                if not data['middle_name']:
                    middle_name = (extracted.get('middle_name') or 
                                 extracted.get('employee_middle_name') or 
                                 extracted.get('employee_middle_initial') or
                                 extracted.get('middle_initial'))
                    if middle_name and middle_name not in ['N/A', '', None]:
                        data['middle_name'] = str(middle_name)
                        score += 3
                
                # Date of Birth (5 points)
                if not data['date_of_birth']:
                    dob = (extracted.get('date_of_birth') or 
                          extracted.get('employee_date_of_birth') or 
                          extracted.get('employee_dob') or
                          extracted.get('birth_date'))
                    if dob and dob not in ['N/A', '', None]:
                        data['date_of_birth'] = str(dob)
                        score += 5
                
                # SSN (4 points)
                if not data['ssn']:
                    ssn = (extracted.get('ssn') or 
                          extracted.get('employee_ssn') or 
                          extracted.get('us_social_security_number') or
                          extracted.get('employee_social_security_number') or
                          extracted.get('social_security_number') or
                          extracted.get('section_1_ssn') or
                          extracted.get('section_1_social_security_number'))
                    if ssn and ssn not in ['N/A', '', None, 'null']:
                        data['ssn'] = str(ssn)
                        score += 4
                
                # Citizenship Status (3 points)
                if not data['citizenship_status']:
                    citizenship = (extracted.get('citizenship_status') or 
                                 extracted.get('employee_citizenship_status'))
                    if citizenship and citizenship not in ['N/A', '', None]:
                        # Map to standard format
                        citizenship_lower = str(citizenship).lower()
                        if any(term in citizenship_lower for term in ['citizen of the united states', 'us citizen', 'citizen']) and 'non' not in citizenship_lower:
                            data['citizenship_status'] = 'US Citizen'
                        elif any(term in citizenship_lower for term in ['alien_authorized_to_work', 'noncitizen_authorized_to_work', 'authorized', 'noncitizen']):
                            data['citizenship_status'] = 'NON Citizen'
                        else:
                            data['citizenship_status'] = str(citizenship)
                        score += 3
        
        logger.info(f"Bucket 1 (Personal Data): {score}/25 points")
        return score, data
    
    def _bucket_2_i9_detection(self, catalog_data: Dict) -> Tuple[int, Dict]:
        """
        BUCKET 2: I-9 FORM DETECTION & CLASSIFICATION (20 points + bonus)
        """
        score = 0
        bonus_points = 0
        data = {
            'i9_forms_detected': False,
            'section_1_pages_found': 0,
            'section_2_pages_found': 0,
            'section_3_supplement_pages_found': 0,
            'form_type_detected': 'NONE'
        }
        
        pages = catalog_data.get('document_catalog', {}).get('pages', catalog_data.get('pages', []))
        
        i9_pages = []
        section_1_count = 0
        section_2_count = 0
        section_3_supplement_count = 0
        
        # Detect I-9 forms and classify sections
        for page in pages:
            page_title = page.get('page_title', '').lower()
            extracted = page.get('extracted_values', {})
            
            # Check if it's an I-9 form
            is_i9 = ('i-9' in page_title or 'form i-9' in page_title or 
                    'employment eligibility' in page_title)
            
            if is_i9:
                i9_pages.append(page)
                
                # Classify sections - Only count actual Section 1 pages
                if 'section 1' in page_title:
                    section_1_count += 1
                
                if 'section 2' in page_title or any(k in extracted for k in ['employer_signature_date', 'list_a_document']):
                    section_2_count += 1
                
                if ('section 3' in page_title or 'supplement' in page_title or 
                    'reverification' in page_title or 
                    any(k in extracted for k in ['reverification_document_title', 'section_3_document_title'])):
                    section_3_supplement_count += 1
        
        # Scoring
        if i9_pages:
            data['i9_forms_detected'] = True
            score += 8  # I-9 Forms Detected
            
            if section_1_count > 0:
                data['section_1_pages_found'] = section_1_count
                score += 4  # Section 1 Pages Found
            
            if section_2_count > 0:
                data['section_2_pages_found'] = section_2_count
                score += 4  # Section 2 Pages Found
            
            if section_3_supplement_count > 0:
                data['section_3_supplement_pages_found'] = section_3_supplement_count
                score += 4  # Section 3/Supplement B Found
        
        # Form Type Classification with Bonus Points - Only consider forms with valid signatures
        supplement_b_with_signatures = False
        section_3_with_signatures = False
        
        for page in i9_pages:
            page_title = page.get('page_title', '').lower()
            extracted = page.get('extracted_values', {})
            
            # Check for valid signatures
            has_signature = any([
                extracted.get('employer_signature_date'),
                extracted.get('reverification_signature_date'),
                extracted.get('reverification_date_signed'),
                extracted.get('date_of_employer_signature')
            ])
            
            if has_signature:
                if 'supplement b' in page_title:
                    supplement_b_with_signatures = True
                elif 'section 3' in page_title or 'reverification' in page_title:
                    section_3_with_signatures = True
        
        # Classify based on forms with valid signatures
        if supplement_b_with_signatures:
            data['form_type_detected'] = 'rehire_supplement_b'
            bonus_points = 5
        elif section_3_with_signatures:
            data['form_type_detected'] = 'reverification_section_3'
            bonus_points = 3
        elif section_2_count > 0:
            data['form_type_detected'] = 'new_hire'
            bonus_points = 1
        
        total_score = score + bonus_points
        logger.info(f"Bucket 2 (I-9 Detection): {score}/20 points + {bonus_points} bonus = {total_score}")
        return total_score, data
    
    def _bucket_3_business_rules(self, catalog_data: Dict) -> Tuple[int, Dict]:
        """
        BUCKET 3: BUSINESS RULES HIERARCHY WITH VALIDATION (25 points)
        """
        score = 0
        data = {
            'correct_form_type_selected': False,
            'latest_signature_selected': False,
            'selected_form_signature_date': '',
            'business_rules_applied': False,
            'validation_applied': False,
            'invalid_forms_skipped': 0,
            'selected_form_validity': 'Unknown',
            'selected_form_type': 'new_hire',  # Default to new hire
            'form_type_decision_basis': '',
            'form_type_source_page': ''
        }
        
        pages = catalog_data.get('document_catalog', {}).get('pages', catalog_data.get('pages', []))
        
        # Find I-9 pages with valid signatures (only these are considered valid documents)
        supplement_b_pages = []
        section_3_pages = []
        standard_i9_pages = []
        
        # Use fuzzy matcher for robust signature detection
        matcher = FuzzyFieldMatcher(similarity_threshold=0.6)
        
        for page in pages:
            page_title = page.get('page_title', '').lower()
            extracted = page.get('extracted_values', {})
            
            # UNIVERSAL SOLUTION: Flatten any nested JSON structure
            # This handles reverification_blocks, section_3_blocks, or ANY future nested structure
            # No need to check for specific structure names - just flatten everything!
            flattened = flatten_extracted_values(extracted)
            
            # Use fuzzy matching on the flattened structure
            # Now ALL fields are accessible at top level, regardless of original nesting
            signature_matches = matcher.find_signature_fields(flattened)
            
            # Find the latest signature date from all matches
            latest_sig = None
            if signature_matches:
                for field_name, date_value, confidence in signature_matches:
                    if date_value and date_value not in ['N/A', '', None]:
                        # CRITICAL: Validate it's actually a date format (MM/DD/YYYY)
                        # This prevents names like "Fatima Yamin" from being treated as dates
                        if not self._is_valid_date_format(str(date_value)):
                            continue
                        
                        # Also exclude non-signature date fields
                        if any(term in field_name.lower() for term in ['birth', 'dob', 'expir', 'valid', '_name']):
                            continue
                        
                        # Parse date for comparison
                        try:
                            if not latest_sig or date_value > latest_sig:
                                latest_sig = date_value
                        except:
                            # If comparison fails, just use the first valid date
                            if not latest_sig:
                                latest_sig = date_value
            
            # Only consider pages with valid signatures as valid documents
            if latest_sig:
                if 'supplement b' in page_title:
                    supplement_b_pages.append({'page': page, 'signature_date': latest_sig})
                elif 'section 3' in page_title or 'reverification' in page_title:
                    # Section 3 or Reverification page
                    section_3_pages.append({'page': page, 'signature_date': latest_sig})
                elif 'i-9' in page_title or 'employment eligibility' in page_title:
                    # Check if this is actually a Section 3 page with reverification data
                    # Use fuzzy matching to detect reverification fields
                    doc_title_matches = matcher.find_document_title_fields(extracted)
                    has_reverification_doc = any('reverification' in field.lower() for field, _, _ in doc_title_matches)
                    
                    if has_reverification_doc or any('reverification' in key.lower() for key in extracted.keys()):
                        # This is a Section 3 page even if title doesn't say so
                        section_3_pages.append({'page': page, 'signature_date': latest_sig})
                    else:
                        # Standard I-9 page
                        standard_i9_pages.append({'page': page, 'signature_date': latest_sig})
        
        # Apply Enhanced Business Rules Hierarchy with Validation
        selected_pages = None
        selected_type = None
        
        # Validate each form type (only pages with signatures are considered)
        valid_supplement_b = self._validate_supplement_b_pages(supplement_b_pages)
        valid_section_3 = self._validate_section_3_pages(section_3_pages)
        valid_standard_i9 = self._validate_standard_i9_pages(standard_i9_pages)
        
        data['validation_applied'] = True
        
        # Priority 1: Supplement B (if valid) - RE-HIRE
        if valid_supplement_b and supplement_b_pages:
            selected_pages = supplement_b_pages
            selected_type = 'Supplement B'
            data['selected_form_type'] = 'rehire_supplement_b'
            data['form_type_decision_basis'] = 'Valid Supplement B form with signatures and meaningful content'
            # Get source page info
            latest_page_info = max(supplement_b_pages, key=lambda x: self._parse_date_for_comparison(x['signature_date']))
            data['form_type_source_page'] = f"Page {latest_page_info['page'].get('page_number', 'Unknown')}"
            score += 10  # Valid Form Type Selected
            data['correct_form_type_selected'] = True
            data['selected_form_validity'] = 'Valid'
            
            # Bonus for skipping invalid forms
            if section_3_pages and not valid_section_3:
                data['invalid_forms_skipped'] += 1
            if standard_i9_pages and not valid_standard_i9:
                data['invalid_forms_skipped'] += 1
                
        # Priority 2: Section 3 (if valid and Supplement B not valid) - RE-VERIFICATION
        elif valid_section_3 and section_3_pages:
            selected_pages = section_3_pages
            selected_type = 'Section 3'
            data['selected_form_type'] = 'reverification_section_3'
            data['form_type_decision_basis'] = 'Valid Section 3/Reverification form with signatures and meaningful content'
            # Get source page info
            latest_page_info = max(section_3_pages, key=lambda x: self._parse_date_for_comparison(x['signature_date']))
            data['form_type_source_page'] = f"Page {latest_page_info['page'].get('page_number', 'Unknown')}"
            score += 10  # Valid Form Type Selected
            data['correct_form_type_selected'] = True
            data['selected_form_validity'] = 'Valid'
            
            # Bonus for correctly skipping invalid Supplement B
            if supplement_b_pages and not valid_supplement_b:
                score += 2  # Invalid form skipped correctly
                data['invalid_forms_skipped'] += 1
            if standard_i9_pages and not valid_standard_i9:
                data['invalid_forms_skipped'] += 1
                
        # Priority 3: Standard I-9 (if valid and others not valid) - NEW HIRE
        elif valid_standard_i9 and standard_i9_pages:
            selected_pages = standard_i9_pages
            selected_type = 'Standard I-9'
            data['selected_form_type'] = 'new_hire'
            data['form_type_decision_basis'] = 'Valid Standard I-9 form with signatures and meaningful content'
            # Get source page info
            latest_page_info = max(standard_i9_pages, key=lambda x: self._parse_date_for_comparison(x['signature_date']))
            data['form_type_source_page'] = f"Page {latest_page_info['page'].get('page_number', 'Unknown')}"
            score += 10  # Valid Form Type Selected
            data['correct_form_type_selected'] = True
            data['selected_form_validity'] = 'Valid'
            
            # Bonus for correctly skipping invalid higher priority forms
            if supplement_b_pages and not valid_supplement_b:
                score += 2  # Invalid form skipped correctly
                data['invalid_forms_skipped'] += 1
            if section_3_pages and not valid_section_3:
                score += 2  # Invalid form skipped correctly
                data['invalid_forms_skipped'] += 1
                
        # Fallback: Use best available even if invalid (with penalty)
        elif supplement_b_pages:
            selected_pages = supplement_b_pages
            selected_type = 'Supplement B'
            data['selected_form_type'] = 'rehire_supplement_b'
            data['form_type_decision_basis'] = 'Supplement B form found but validation failed (using with penalty)'
            latest_page_info = max(supplement_b_pages, key=lambda x: self._parse_date_for_comparison(x['signature_date']))
            data['form_type_source_page'] = f"Page {latest_page_info['page'].get('page_number', 'Unknown')}"
            score += 5  # Partial points for form selection (penalty applied)
            data['correct_form_type_selected'] = True
            data['selected_form_validity'] = 'Invalid'
            score -= 5  # Penalty for using invalid form
        elif section_3_pages:
            selected_pages = section_3_pages
            selected_type = 'Section 3'
            data['selected_form_type'] = 'reverification_section_3'
            data['form_type_decision_basis'] = 'Section 3/Reverification form found but validation failed (using with penalty)'
            latest_page_info = max(section_3_pages, key=lambda x: self._parse_date_for_comparison(x['signature_date']))
            data['form_type_source_page'] = f"Page {latest_page_info['page'].get('page_number', 'Unknown')}"
            score += 5  # Partial points for form selection (penalty applied)
            data['correct_form_type_selected'] = True
            data['selected_form_validity'] = 'Invalid'
            score -= 5  # Penalty for using invalid form
        elif standard_i9_pages:
            selected_pages = standard_i9_pages
            selected_type = 'Standard I-9'
            data['selected_form_type'] = 'new_hire'
            data['form_type_decision_basis'] = 'Standard I-9 form found but validation failed (using with penalty)'
            latest_page_info = max(standard_i9_pages, key=lambda x: self._parse_date_for_comparison(x['signature_date']))
            data['form_type_source_page'] = f"Page {latest_page_info['page'].get('page_number', 'Unknown')}"
            score += 5  # Partial points for form selection (penalty applied)
            data['correct_form_type_selected'] = True
            data['selected_form_validity'] = 'Invalid'
            score -= 5  # Penalty for using invalid form
        else:
            # No valid forms found
            data['selected_form_type'] = 'new_hire'  # Default
            data['form_type_decision_basis'] = 'No I-9 forms with valid signatures found - defaulting to new hire'
            data['form_type_source_page'] = 'None'
            score -= 10  # Major penalty
            data['selected_form_validity'] = 'None Found'
        
        # Select latest signature within selected type
        if selected_pages:
            latest_page_info = max(selected_pages, key=lambda x: self._parse_date_for_comparison(x['signature_date']))
            data['selected_form_signature_date'] = latest_page_info['signature_date']
            data['latest_signature_selected'] = True
            score += 8  # Latest Signature Date Selected
            
            # Additional scoring for proper selection
            score += 4  # Proper Section 1 Selection
            score += 3  # Proper Document Section Selection
            data['business_rules_applied'] = True
        
        # Store page counts for reporting
        data['supplement_b_pages_count'] = len(supplement_b_pages)
        data['section_3_pages_count'] = len(section_3_pages)
        data['standard_i9_pages_count'] = len(standard_i9_pages)
        
        logger.info(f"Bucket 3 (Business Rules): {score}/25 points - Selected: {selected_type} ({data['selected_form_validity']})")
        return score, data
    
    def _validate_supplement_b_pages(self, supplement_b_pages: List[Dict]) -> bool:
        """
        Validate if Supplement B pages contain meaningful data using fuzzy matching
        Uses universal JSON flattening to handle ANY nested structure
        """
        if not supplement_b_pages:
            return False
        
        matcher = FuzzyFieldMatcher(similarity_threshold=0.6)
        
        for page_info in supplement_b_pages:
            page = page_info['page']
            extracted = page.get('extracted_values', {})
            
            # Flatten any nested structure - handles reverification_blocks or ANY future structure
            flattened = flatten_extracted_values(extracted)
            
            # Use fuzzy matching on flattened data
            signature_matches = matcher.find_signature_fields(flattened)
            has_signature = len(signature_matches) > 0
            
            # Use fuzzy matching to find content fields
            document_matches = matcher.find_document_title_fields(flattened)
            name_matches = [
                matcher.find_name_field(flattened, 'first_name'),
                matcher.find_name_field(flattened, 'last_name')
            ]
            expiry_matches = matcher.find_expiry_date_fields(flattened)
            
            has_content = (len(document_matches) > 0 or 
                          any(name_matches) or 
                          len(expiry_matches) > 0)
            
            if has_signature and has_content:
                logger.info(f"Supplement B validation: VALID - Found {len(signature_matches)} signature(s), content fields present")
                return True
        
        logger.info(f"Supplement B validation: INVALID - Missing required fields")
        return False
    
    def _validate_section_3_pages(self, section_3_pages: List[Dict]) -> bool:
        """
        Validate if Section 3 pages contain meaningful data using fuzzy matching
        Uses universal JSON flattening to handle ANY nested structure
        """
        if not section_3_pages:
            return False
        
        matcher = FuzzyFieldMatcher(similarity_threshold=0.6)
        
        for page_info in section_3_pages:
            page = page_info['page']
            extracted = page.get('extracted_values', {})
            
            # Flatten any nested structure - handles section_3_blocks or ANY future structure
            flattened = flatten_extracted_values(extracted)
            
            # Use fuzzy matching on flattened data
            signature_matches = matcher.find_signature_fields(flattened)
            has_signature = len(signature_matches) > 0
            
            # Use fuzzy matching to find content fields
            document_matches = matcher.find_document_title_fields(flattened)
            expiry_matches = matcher.find_expiry_date_fields(flattened)
            
            has_content = len(document_matches) > 0 or len(expiry_matches) > 0
            
            if has_signature and has_content:
                logger.info(f"Section 3 validation: VALID - Found {len(signature_matches)} signature(s), content fields present")
                return True
        
        logger.info(f"Section 3 validation: INVALID - Missing required fields")
        return False
    
    def _validate_standard_i9_pages(self, standard_i9_pages: List[Dict]) -> bool:
        """
        Validate if Standard I-9 pages contain meaningful data using fuzzy matching
        Uses universal JSON flattening to handle ANY nested structure
        """
        if not standard_i9_pages:
            return False
        
        matcher = FuzzyFieldMatcher(similarity_threshold=0.6)
        
        for page_info in standard_i9_pages:
            page = page_info['page']
            extracted = page.get('extracted_values', {})
            
            # Flatten any nested structure for consistency
            flattened = flatten_extracted_values(extracted)
            
            # Use fuzzy matching on flattened data
            signature_matches = matcher.find_signature_fields(flattened)
            has_signature = len(signature_matches) > 0
            
            # Use fuzzy matching to find content fields
            document_matches = matcher.find_document_title_fields(flattened)
            name_matches = [
                matcher.find_name_field(flattened, 'first_name'),
                matcher.find_name_field(flattened, 'last_name')
            ]
            
            has_content = len(document_matches) > 0 or any(name_matches)
            
            if has_signature and has_content:
                logger.info(f"Standard I-9 validation: VALID - Found {len(signature_matches)} signature(s), content fields present")
                return True
        
        logger.info(f"Standard I-9 validation: INVALID - Missing required fields")
        return False
    
    def _bucket_4_work_authorization(self, catalog_data: Dict, bucket_3_data: Dict) -> Tuple[int, Dict]:
        """
        BUCKET 4: WORK AUTHORIZATION & EXPIRY MATCHING (15 points)
        """
        score = 0
        data = {
            'work_auth_expiry_found': False,
            'work_auth_expiry_date': '',
            'document_expiry_found': False,
            'document_expiry_date': '',
            'expiry_dates_match': False,
            'expiry_match_details': ''
        }
        
        pages = catalog_data.get('document_catalog', {}).get('pages', catalog_data.get('pages', []))
        form_type = bucket_3_data.get('selected_form_type', 'new_hire')
        selected_signature_date = bucket_3_data.get('selected_form_signature_date', '')
        
        # For Section 3 cases, find the Section 1 page that corresponds to the selected Section 3
        if form_type == 'reverification_section_3' and selected_signature_date:
            # Find the Section 3 page with the selected signature date
            section3_page_num = None
            for i, page in enumerate(pages):
                page_title = page.get('page_title', '').lower()
                extracted = page.get('extracted_values', {})
                
                if 'section 3' in page_title or 'reverification' in page_title or any('reverif' in k.lower() for k in extracted.keys()):
                    # Check if this page has the selected signature date
                    sig_matches = self.fuzzy_matcher.find_signature_fields(extracted)
                    for _, sig_date, _ in sig_matches:
                        if sig_date == selected_signature_date:
                            section3_page_num = i
                            break
                    if section3_page_num is not None:
                        break
            
            # Find the closest Section 1 page before the Section 3 page
            if section3_page_num is not None:
                for i in range(section3_page_num - 1, -1, -1):  # Search backwards from Section 3
                    page = pages[i]
                    page_title = page.get('page_title', '').lower()
                    extracted = page.get('extracted_values', {})
                    
                    if 'section 1' in page_title:
                        work_auth_fields = [
                            extracted.get('work_auth_expiration_date'),
                            extracted.get('work_until_date'),
                            extracted.get('work_authorization_expiration_date'),
                            extracted.get('alien_authorized_to_work_until'),
                            extracted.get('alien_authorized_to_work_until_date'),
                            extracted.get('alien_expiration_date')
                        ]
                        
                        for work_auth in work_auth_fields:
                            if work_auth and work_auth not in ['N/A', '', None]:
                                data['work_auth_expiry_date'] = str(work_auth)
                                data['work_auth_expiry_found'] = True
                                score += 8  # Work Auth Expiry Date Found
                                break
                        
                        if data['work_auth_expiry_found']:
                            break
        
        # If not found (or not Section 3), use standard Section 1 search
        if not data['work_auth_expiry_found']:
            for page in pages:
                page_title = page.get('page_title', '').lower()
                extracted = page.get('extracted_values', {})
                
                if 'section 1' in page_title or 'employment eligibility' in page_title:
                    work_auth_fields = [
                        extracted.get('work_auth_expiration_date'),
                        extracted.get('work_until_date'),
                        extracted.get('work_authorization_expiration_date'),
                        extracted.get('alien_authorized_to_work_until'),
                        extracted.get('alien_authorized_to_work_until_date'),
                        extracted.get('alien_expiration_date')
                    ]
                    
                    for work_auth in work_auth_fields:
                        if work_auth and work_auth not in ['N/A', '', None]:
                            data['work_auth_expiry_date'] = str(work_auth)
                            data['work_auth_expiry_found'] = True
                            score += 8  # Work Auth Expiry Date Found
                            break
                    
                    if data['work_auth_expiry_found']:
                        break
        
        # Find document expiry from appropriate sections
        form_type = bucket_3_data.get('form_type_detected', 'NONE')
        
        for page in pages:
            page_title = page.get('page_title', '').lower()
            extracted = page.get('extracted_values', {})
            
            doc_expiry = None
            
            if form_type == 'rehire_supplement_b' and 'supplement b' in page_title:
                doc_expiry = extracted.get('reverification_1_expiration_date')
            elif form_type == 'reverification_section_3' and ('section 3' in page_title or 'reverification' in page_title):
                doc_expiry = (extracted.get('reverification_expiration_date') or 
                            extracted.get('section_3_expiration_date'))
            elif 'section 2' in page_title:
                doc_expiry = (extracted.get('list_a_expiration_date') or 
                            extracted.get('list_b_expiration_date') or 
                            extracted.get('list_c_expiration_date') or
                            # De Lima format variations
                            extracted.get('list_a_document_1_expiration_date') or
                            extracted.get('list_a_document_2_expiration_date') or
                            extracted.get('list_a_document_3_expiration_date'))
            
            if doc_expiry and doc_expiry not in ['N/A', '', None]:
                data['document_expiry_date'] = str(doc_expiry)
                data['document_expiry_found'] = True
                score += 4  # Document Expiry Date Found
                break
        
        # Check for expiry date matching (with fuzzy matching)
        if data['work_auth_expiry_found'] and data['document_expiry_found']:
            work_auth_date = data['work_auth_expiry_date']
            doc_date = data['document_expiry_date']
            
            # Exact match
            if work_auth_date == doc_date:
                data['expiry_dates_match'] = True
                data['expiry_match_details'] = f"Work Auth: {work_auth_date} matches Document: {doc_date}"
                score += 3  # Expiry Dates Match
            else:
                # Try fuzzy matching (±30 days)
                try:
                    from datetime import datetime, timedelta
                    work_dt = datetime.strptime(work_auth_date, '%m/%d/%Y')
                    doc_dt = datetime.strptime(doc_date, '%m/%d/%Y')
                    diff = abs((work_dt - doc_dt).days)
                    
                    if diff <= 30:
                        data['expiry_dates_match'] = True
                        data['expiry_match_details'] = f"Work Auth: {work_auth_date} matches Document: {doc_date} (±{diff} days)"
                        score += 2  # Partial match
                    else:
                        data['expiry_match_details'] = f"Work Auth: {work_auth_date} - Document: {doc_date} (NO MATCH, {diff} days apart)"
                except:
                    data['expiry_match_details'] = f"Work Auth: {work_auth_date} - Document: {doc_date} (NO MATCH)"
        elif data['work_auth_expiry_found']:
            data['expiry_match_details'] = f"Work Auth: {data['work_auth_expiry_date']} - No document expiry found"
        else:
            data['expiry_match_details'] = "No work authorization expiry date found"
        
        logger.info(f"Bucket 4 (Work Authorization): {score}/15 points")
        return score, data
    
    def _bucket_5_document_tracking(self, catalog_data: Dict, bucket_3_data: Dict) -> Tuple[int, Dict]:
        """
        BUCKET 5: DOCUMENT TRACKING & VERIFICATION (15 points)
        """
        score = 0
        data = {
            'documents_listed_correctly': False,
            'documents_listed': '',
            'document_numbers_extracted': False,
            'supporting_documents_found': 0,
            'document_attachment_status': 'Unknown'
        }
        
        pages = catalog_data.get('document_catalog', {}).get('pages', catalog_data.get('pages', []))
        form_type = bucket_3_data.get('form_type_detected', 'NONE')
        
        documents = []
        
        # Extract documents from appropriate sections based on form type
        for page in pages:
            page_title = page.get('page_title', '').lower()
            extracted = page.get('extracted_values', {})
            
            should_extract = False
            
            if form_type == 'rehire_supplement_b' and 'supplement b' in page_title:
                should_extract = True
            elif form_type == 'reverification_section_3' and ('section 3' in page_title or 'reverification' in page_title):
                should_extract = True
            elif 'section 2' in page_title or 'employment eligibility' in page_title:
                should_extract = True
            
            if should_extract:
                # Check various document fields (comprehensive field variations)
                doc_fields = [
                    'reverification_document_title', 'section_3_document_title',
                    'list_a_document_title', 'list_b_document_title', 'list_c_document_title',
                    'list_a_document_title_1', 'list_a_document_title_2', 'list_a_document_title_3',
                    'list_b_document_title_1', 'list_b_document_title_2', 'list_b_document_title_3',
                    'list_c_document_title_1', 'list_c_document_title_2', 'list_c_document_title_3',
                    # De Lima format variations
                    'list_a_document_1_title', 'list_a_document_2_title', 'list_a_document_3_title',
                    'list_b_document_1_title', 'list_b_document_2_title', 'list_b_document_3_title',
                    'list_c_document_1_title', 'list_c_document_2_title', 'list_c_document_3_title',
                    'additional_information_document_title_2', 'additional_information_document_title_3'
                ]
                
                for field in doc_fields:
                    doc_title = extracted.get(field)
                    if doc_title and doc_title not in ['N/A', '', None]:
                        doc_number = extracted.get(field.replace('title', 'number'), '')
                        if doc_number and doc_number not in ['N/A', '', None]:
                            documents.append(f"{doc_title} (#{doc_number})")
                            data['document_numbers_extracted'] = True
                        else:
                            documents.append(doc_title)
        
        # Scoring
        if documents:
            data['documents_listed'] = ' | '.join(documents)
            data['documents_listed_correctly'] = True
            score += 6  # Documents Listed from Correct Section
            
            if data['document_numbers_extracted']:
                score += 3  # Document Numbers Extracted
        
        # Count supporting documents (non-I-9 pages)
        supporting_docs = 0
        for page in pages:
            page_title = page.get('page_title', '').lower()
            if not ('i-9' in page_title or 'employment eligibility' in page_title):
                supporting_docs += 1
        
        data['supporting_documents_found'] = supporting_docs
        if supporting_docs > 0:
            score += 3  # Supporting Documents Found
        
        # Document attachment status (simplified)
        if documents and supporting_docs > 0:
            data['document_attachment_status'] = 'ATTACHED'
            score += 3  # Document Attachment Status
        elif documents:
            data['document_attachment_status'] = 'LISTED_ONLY'
            score += 1
        else:
            data['document_attachment_status'] = 'NONE'
        
        logger.info(f"Bucket 5 (Document Tracking): {score}/15 points")
        return score, data
    
    def _extract_comprehensive_business_fields(self, catalog_data: Dict, bucket_2_data: Dict, bucket_3_data: Dict) -> Dict:
        """
        Extract comprehensive business fields for CSV output
        """
        pages = catalog_data.get('document_catalog', {}).get('pages', catalog_data.get('pages', []))
        
        # Initialize business fields
        business_fields = {
            # Personal Information
            'employee_first_name': '',
            'employee_middle_name': '',
            'employee_last_name': '',
            'employee_date_of_birth': '',
            'employee_ssn': '',
            'citizenship_status': '',
            
            # Form Classification (Business Fields) - Use validated form type from business rules
            'form_type_detected': self._get_validated_form_type(bucket_2_data, bucket_3_data),
            'form_type_decision_basis': bucket_3_data.get('form_type_decision_basis', ''),
            'form_type_source_page': bucket_3_data.get('form_type_source_page', ''),
            'total_i9_sets_found': 0,
            'section_1_pages_count': bucket_2_data.get('section_1_pages_found', 0),
            'section_2_pages_count': bucket_2_data.get('section_2_pages_found', 0),
            'section_3_pages_count': bucket_3_data.get('section_3_pages_count', 0),
            'supplement_b_pages_count': bucket_3_data.get('supplement_b_pages_count', 0),
            
            # Primary I-9 Set Information
            'primary_i9_set_signature_date': bucket_3_data.get('selected_form_signature_date', ''),
            'primary_i9_set_type': bucket_2_data.get('form_type_detected', 'NONE'),
            'primary_i9_set_pages': '',
            
            # Work Authorization
            'work_authorization_expiry_date': '',
            'work_authorization_source': '',
            
            # Document Information
            'documents_in_primary_set': '',
            'document_count_in_primary_set': 0,
            'supporting_documents_count': 0,
            'document_expiry_dates': '',
            'expiry_date_matches': '',
            
            # Support Document Validation
            'matching_support_documents_attached': '',
            'matching_support_documents_not_attached': '',
            
            # Additional Fields
            'employee_signature_date': '',
            'employer_signature_date': bucket_3_data.get('selected_form_signature_date', ''),
            'alien_registration_number': '',
            'first_day_of_employment': '',
            
            # I-9 Sets Details
            'all_i9_sets_found': '',  # All signature dates and their details
        }
        
        # Extract personal information from ALL I-9 pages (not just Section 1)
        for page in pages:
            page_title = page.get('page_title', '').lower()
            extracted = page.get('extracted_values', {})
            
            # Look in all I-9 related pages for personal data
            if any(term in page_title for term in ['i-9', 'employment eligibility', 'section 1', 'section 2']):
                # Names
                if not business_fields['employee_first_name']:
                    first_name = (extracted.get('first_name') or extracted.get('employee_first_name') or 
                                extracted.get('section_1_first_name'))
                    if first_name and first_name not in ['N/A', '', None]:
                        business_fields['employee_first_name'] = str(first_name)
                
                if not business_fields['employee_middle_name']:
                    middle_name = (extracted.get('middle_name') or extracted.get('employee_middle_name') or 
                                 extracted.get('employee_middle_initial') or extracted.get('middle_initial'))
                    if middle_name and middle_name not in ['N/A', '', None]:
                        business_fields['employee_middle_name'] = str(middle_name)
                
                if not business_fields['employee_last_name']:
                    last_name = (extracted.get('last_name') or extracted.get('employee_last_name') or 
                               extracted.get('section_1_last_name'))
                    if last_name and last_name not in ['N/A', '', None]:
                        business_fields['employee_last_name'] = str(last_name)
                
                # Date of Birth
                if not business_fields['employee_date_of_birth']:
                    dob = (extracted.get('date_of_birth') or extracted.get('employee_date_of_birth') or 
                          extracted.get('employee_dob') or extracted.get('birth_date'))
                    if dob and dob not in ['N/A', '', None]:
                        business_fields['employee_date_of_birth'] = str(dob)
                
                # SSN
                if not business_fields['employee_ssn']:
                    ssn = (extracted.get('ssn') or extracted.get('employee_ssn') or 
                          extracted.get('us_social_security_number') or extracted.get('employee_social_security_number') or
                          extracted.get('social_security_number'))
                    if ssn and ssn not in ['N/A', '', None, 'null']:
                        business_fields['employee_ssn'] = str(ssn)
                
                # Citizenship
                if not business_fields['citizenship_status']:
                    citizenship = (extracted.get('citizenship_status') or extracted.get('employee_citizenship_status'))
                    if citizenship and citizenship not in ['N/A', '', None]:
                        citizenship_lower = str(citizenship).lower()
                        if any(term in citizenship_lower for term in ['citizen of the united states', 'us citizen', 'citizen']) and 'non' not in citizenship_lower:
                            business_fields['citizenship_status'] = 'US Citizen'
                        elif any(term in citizenship_lower for term in ['alien_authorized_to_work', 'noncitizen_authorized_to_work', 'authorized', 'noncitizen']):
                            business_fields['citizenship_status'] = 'NON Citizen'
                        else:
                            business_fields['citizenship_status'] = str(citizenship)
                
                # Work Authorization - collect all dates and find the latest
                work_auth_fields = [
                    extracted.get('work_auth_expiration_date'),
                    extracted.get('work_until_date'),
                    extracted.get('work_authorization_expiration_date'),
                    extracted.get('alien_authorized_to_work_until'),
                    extracted.get('alien_authorized_to_work_until_date'),
                    extracted.get('alien_expiration_date'),
                    extracted.get('section2_list_a_expiration_date'),
                    extracted.get('reverification_expiration_date')
                ]
                
                for work_auth in work_auth_fields:
                    if work_auth and work_auth not in ['N/A', '', None]:
                        # Convert to comparable format and check if it's later than current
                        if self._is_later_date(str(work_auth), business_fields['work_authorization_expiry_date']):
                            business_fields['work_authorization_expiry_date'] = str(work_auth)
                            business_fields['work_authorization_source'] = 'Section 1'
                
                # Employee Signature Date
                if not business_fields['employee_signature_date']:
                    emp_sig = (extracted.get('employee_signature_date') or extracted.get('date_of_employee_signature'))
                    if emp_sig and emp_sig not in ['N/A', '', None]:
                        business_fields['employee_signature_date'] = str(emp_sig)
                
                # Alien Registration Number
                if not business_fields['alien_registration_number']:
                    alien_reg = (extracted.get('alien_registration_number') or extracted.get('uscis_a_number'))
                    if alien_reg and alien_reg not in ['N/A', '', None]:
                        business_fields['alien_registration_number'] = str(alien_reg)
        
        # Enhanced I-9 set detection - group pages by signature dates and form sets
        i9_sets = {}  # signature_date -> {pages: [], type: '', documents: []}
        i9_form_pages = {}  # Track I-9 pages by form version/date to group Section 1 + Section 2
        
        # First pass: identify all I-9 pages and their relationships
        for page in pages:
            page_title = page.get('page_title', '').lower()
            extracted = page.get('extracted_values', {})
            page_num = page.get('page_number')
            form_version = extracted.get('form_version', '')
            
            # Only process I-9 related pages
            if any(term in page_title for term in ['i-9', 'employment eligibility', 'section 1', 'section 2', 'section 3', 'supplement']):
                # Use fuzzy matching to find ALL signature date fields
                sig_fields_raw = self.fuzzy_matcher.find_signature_fields(extracted)
                # Filter to only include actual date values (MM/DD/YYYY format)
                # AND exclude non-signature date fields like date_of_birth
                sig_fields = [(fn, val, conf) for fn, val, conf in sig_fields_raw 
                             if self._is_valid_date_format(str(val)) and 
                             'birth' not in fn.lower() and 'dob' not in fn.lower() and
                             'expir' not in fn.lower() and 'valid' not in fn.lower()]
                
                # Track form sets by version to group Section 1 + Section 2
                if 'section 1' in page_title or 'section 2' in page_title:
                    if form_version not in i9_form_pages:
                        i9_form_pages[form_version] = {'section_1': None, 'section_2': None, 'pages': []}
                    
                    if 'section 1' in page_title:
                        i9_form_pages[form_version]['section_1'] = page
                    if 'section 2' in page_title:
                        i9_form_pages[form_version]['section_2'] = page
                    i9_form_pages[form_version]['pages'].append(page_num)
                
                for field_name, sig_date, confidence in sig_fields:
                    if sig_date and sig_date not in ['N/A', '', None]:
                        # Ensure sig_date is a string (not dict or other type)
                        sig_date = str(sig_date) if not isinstance(sig_date, str) else sig_date
                        if sig_date not in i9_sets:
                            i9_sets[sig_date] = {
                                'pages': [],
                                'type': 'Standard I-9',
                                'documents': [],
                                'signature_field': field_name
                            }
                        
                        i9_sets[sig_date]['pages'].append(f"Page {page_num}")
                        
                        # Determine set type
                        if 'supplement b' in page_title:
                            i9_sets[sig_date]['type'] = 'Supplement B'
                        elif 'section 3' in page_title or 'reverification' in page_title:
                            i9_sets[sig_date]['type'] = 'Section 3'
        
        # Second pass: Add form sets where Section 1 is unsigned but Section 2 is signed
        for form_version, form_data in i9_form_pages.items():
            if form_data['section_2']:
                # Section 2 exists, check if it has a signature
                sec2_extracted = form_data['section_2'].get('extracted_values', {})
                sec2_sig = sec2_extracted.get('employer_signature_date') or sec2_extracted.get('date_of_employer_signature')
                
                if sec2_sig and sec2_sig not in ['N/A', '', None]:
                    # This is a valid I-9 set even if Section 1 is unsigned
                    if sec2_sig not in i9_sets:
                        i9_sets[sec2_sig] = {
                            'pages': [f"Page {p}" for p in form_data['pages']],
                            'type': 'Standard I-9',
                            'documents': [],
                            'signature_field': 'employer_signature_date'
                        }
        
        business_fields['total_i9_sets_found'] = len(i9_sets)
        
        # Create detailed summary of all I-9 sets
        all_sets_details = []
        for sig_date, set_info in sorted(i9_sets.items()):
            set_summary = f"{sig_date} ({set_info['type']}) - {len(set_info['pages'])} pages"
            all_sets_details.append(set_summary)
        
        business_fields['all_i9_sets_found'] = ' | '.join(all_sets_details)
        
        # Find the primary (latest) I-9 set
        # CRITICAL: Use bucket_3 selection if available (it uses proper business rules)
        # Only fall back to max date if bucket_3 didn't select anything
        if bucket_3_data.get('selected_form_signature_date'):
            # Use bucket 3's selection (it already applied business rules and validation)
            business_fields['primary_i9_set_signature_date'] = bucket_3_data.get('selected_form_signature_date')
            # Find the corresponding set info
            selected_date = bucket_3_data.get('selected_form_signature_date')
            if selected_date in i9_sets:
                primary_set = i9_sets[selected_date]
                business_fields['primary_i9_set_type'] = primary_set['type']
                business_fields['primary_i9_set_pages'] = ' | '.join(set(primary_set['pages']))
            else:
                # Set info not found, use bucket 3 data
                business_fields['primary_i9_set_type'] = bucket_3_data.get('selected_form_type', 'NONE')
                business_fields['primary_i9_set_pages'] = bucket_3_data.get('form_type_source_page', '')
        elif i9_sets:
            # Fallback: find latest using proper date comparison
            latest_date = max(i9_sets.keys(), key=lambda d: self._parse_date_for_comparison(d))
            primary_set = i9_sets[latest_date]
            business_fields['primary_i9_set_signature_date'] = latest_date
            business_fields['primary_i9_set_type'] = primary_set['type']
            business_fields['primary_i9_set_pages'] = ' | '.join(set(primary_set['pages']))  # Remove duplicates
        else:
            business_fields['primary_i9_set_signature_date'] = ''
            business_fields['primary_i9_set_type'] = 'NONE'
            business_fields['primary_i9_set_pages'] = ''
        
        # Extract documents from primary set
        form_type = business_fields['form_type_detected']
        documents = []
        
        # For Section 3 and Supplement B, we need to extract ONLY from the LATEST page
        # Build a list of pages to extract from based on form type
        pages_to_extract = []
        
        if form_type == 'reverification_section_3' or form_type == 're-verification':
            # Find all Section 3 pages with signature dates
            section3_pages = []
            for page in pages:
                page_title = page.get('page_title', '').lower()
                extracted = page.get('extracted_values', {})
                if 'section 3' in page_title or 'reverification' in page_title:
                    # Use fuzzy matching to find signature dates
                    sig_matches = self.fuzzy_matcher.find_signature_fields(extracted)
                    # Filter to only valid dates (exclude names like "Fatima Yamin")
                    sig_dates = [sig_date for field_name, sig_date, _ in sig_matches 
                                if self._is_valid_date_format(str(sig_date)) and '_name' not in field_name.lower()]
                    latest_sig = None
                    for sig_date in sig_dates:
                        if sig_date and sig_date not in ['N/A', '', None]:
                            if not latest_sig or self._parse_date_for_comparison(sig_date) > self._parse_date_for_comparison(latest_sig):
                                latest_sig = sig_date
                    
                    if latest_sig:
                        section3_pages.append({'page': page, 'signature_date': latest_sig})
            
            # Select ONLY the page with the latest signature date
            if section3_pages:
                latest_section3 = max(section3_pages, key=lambda x: self._parse_date_for_comparison(x['signature_date']))
                pages_to_extract = [latest_section3['page']]
        
        elif form_type == 'rehire_supplement_b' or form_type == 're-hire':
            # Find all Supplement B pages with signature dates
            supplement_b_pages = []
            for page in pages:
                page_title = page.get('page_title', '').lower()
                extracted = page.get('extracted_values', {})
                if 'supplement b' in page_title:
                    # Use fuzzy matching to find signature dates
                    sig_matches = self.fuzzy_matcher.find_signature_fields(extracted)
                    # Filter to only valid dates (exclude names)
                    sig_dates = [sig_date for field_name, sig_date, _ in sig_matches 
                                if self._is_valid_date_format(str(sig_date)) and '_name' not in field_name.lower()]
                    latest_sig = None
                    for sig_date in sig_dates:
                        if sig_date and sig_date not in ['N/A', '', None]:
                            if not latest_sig or self._parse_date_for_comparison(sig_date) > self._parse_date_for_comparison(latest_sig):
                                latest_sig = sig_date
                    
                    if latest_sig:
                        supplement_b_pages.append({'page': page, 'signature_date': latest_sig})
            
            # Select ONLY the page with the latest signature date
            if supplement_b_pages:
                latest_supplement_b = max(supplement_b_pages, key=lambda x: self._parse_date_for_comparison(x['signature_date']))
                pages_to_extract = [latest_supplement_b['page']]
        
        else:
            # For Standard I-9, extract from all Section 2 pages
            for page in pages:
                page_title = page.get('page_title', '').lower()
                if 'section 2' in page_title or 'employment eligibility' in page_title:
                    pages_to_extract.append(page)
        
        # Now extract documents from the selected pages
        for page in pages_to_extract:
            page_title = page.get('page_title', '').lower()
            extracted = page.get('extracted_values', {})
            
            # Comprehensive document field variations found in analysis
            # Use fuzzy matching to find ALL document title fields
            doc_matches_raw = self.fuzzy_matcher.find_document_title_fields(extracted)
            
            # Filter out document number fields and date values (they should not be treated as separate documents)
            doc_matches = [(fn, val, conf) for fn, val, conf in doc_matches_raw 
                          if 'number' not in fn.lower() and 
                          'issuing' not in fn.lower() and
                          not self._is_valid_date_format(str(val)) and  # Exclude date values
                          not self._is_only_number_or_code(str(val))]  # Exclude pure numbers/codes
            
            for field_name, doc_title, confidence in doc_matches:
                if doc_title and doc_title not in ['N/A', '', None]:
                    # Try to find corresponding document number using fuzzy matching
                    # Look for fields that are similar to the title field but with 'number' instead
                    doc_number = ''
                    
                    # Generate possible number field variations
                    number_field_candidates = [
                        field_name.replace('title', 'number'),
                        field_name.replace('_title', '_number'),
                        field_name.replace('document_title', 'document_number'),
                        'document_number'  # Generic fallback
                    ]
                    
                    for num_field in number_field_candidates:
                        num_val = extracted.get(num_field)
                        if num_val and num_val not in ['N/A', '', None]:
                            doc_number = num_val
                            break
                    
                    if doc_number:
                        documents.append(f"{doc_title} (#{doc_number})")
                    else:
                        documents.append(doc_title)
        
        business_fields['documents_in_primary_set'] = ' | '.join(documents)
        business_fields['document_count_in_primary_set'] = len(documents)
        
        # Count supporting documents
        supporting_docs = 0
        for page in pages:
            page_title = page.get('page_title', '').lower()
            if not ('i-9' in page_title or 'employment eligibility' in page_title):
                supporting_docs += 1
        
        business_fields['supporting_documents_count'] = supporting_docs
        
        # Extract document expiry dates and matching information - Check ALL pages including support documents
        doc_expiry_dates = []
        for page in pages:
            page_title = page.get('page_title', '').lower()
            extracted = page.get('extracted_values', {})
            
            # Check Section 2 pages for List A/B/C document expiry dates
            if 'section 2' in page_title or 'employment eligibility' in page_title:
                expiry_fields = [
                    'list_a_document_1_expiration_date', 'list_a_document_2_expiration_date', 'list_a_document_3_expiration_date',
                    'list_a_expiration_date', 'list_b_expiration_date', 'list_c_expiration_date'
                ]
                for field in expiry_fields:
                    expiry = extracted.get(field)
                    if expiry and expiry not in ['N/A', '', None]:
                        doc_expiry_dates.append(expiry)
            
            # Check DS-2019 forms for program end dates
            if 'ds-2019' in page_title or 'certificate of eligibility' in page_title:
                ds2019_expiry_fields = [
                    'form_period_to', 'form_covers_period_to', 'program_end_date'
                ]
                for field in ds2019_expiry_fields:
                    expiry = extracted.get(field)
                    if expiry and expiry not in ['N/A', '', None]:
                        # Normalize date format from MM-DD-YYYY to MM/DD/YYYY
                        expiry_normalized = str(expiry).replace('-', '/')
                        doc_expiry_dates.append(expiry_normalized)
            
            # Check Supplement B for document expiry dates
            if 'supplement b' in page_title:
                supp_b_expiry_fields = [
                    'document_expiration_date', 'reverification_1_document_expiration_date',
                    'reverification_2_document_expiration_date', 'reverification_3_document_expiration_date'
                ]
                for field in supp_b_expiry_fields:
                    expiry = extracted.get(field)
                    if expiry and expiry not in ['N/A', '', None]:
                        doc_expiry_dates.append(expiry)
        
        business_fields['document_expiry_dates'] = ' | '.join(doc_expiry_dates)
        
        # Set expiry matching details
        work_auth = business_fields['work_authorization_expiry_date']
        if work_auth and doc_expiry_dates:
            matches = []
            for doc_expiry in doc_expiry_dates:
                if work_auth == doc_expiry:
                    matches.append(f"MATCH: {work_auth}")
                else:
                    matches.append(f"NO MATCH: Work Auth {work_auth} vs Doc {doc_expiry}")
            business_fields['expiry_date_matches'] = ' | '.join(matches)
        elif work_auth:
            business_fields['expiry_date_matches'] = f"Work Auth: {work_auth} - No document expiry found"
        else:
            business_fields['expiry_date_matches'] = "No work authorization expiry found"
        
        # Extract primary I-9 set pages information
        primary_pages = []
        for page in pages:
            page_title = page.get('page_title', '').lower()
            extracted = page.get('extracted_values', {})
            
            # Check if this page belongs to the primary set based on signature date
            sig_dates = [
                extracted.get('employer_signature_date'),
                extracted.get('date_of_employer_signature'),
                extracted.get('reverification_date_signed')
            ]
            
            for sig_date in sig_dates:
                if sig_date == business_fields['primary_i9_set_signature_date']:
                    primary_pages.append(f"Page {page.get('page_number')}")
                    break
        
        business_fields['primary_i9_set_pages'] = ' | '.join(primary_pages)
        
        # Validate support document attachments
        attached_docs, not_attached_docs = self._validate_support_documents(pages, documents, business_fields['primary_i9_set_signature_date'])
        business_fields['matching_support_documents_attached'] = ' | '.join(attached_docs)
        business_fields['matching_support_documents_not_attached'] = ' | '.join(not_attached_docs)
        
        return business_fields
    
    def _validate_support_documents(self, pages: List[Dict], i9_documents: List[str], primary_signature_date: str) -> Tuple[List[str], List[str]]:
        """
        Validate if support documents listed in I-9 are actually attached as supporting pages
        """
        attached_docs = []
        not_attached_docs = []
        
        # Get all supporting document pages (non-I-9 pages)
        supporting_pages = []
        for page in pages:
            page_title = page.get('page_title', '').lower()
            page_num = page.get('page_number')
            
            # Skip I-9 form pages and lists of acceptable documents
            # CRITICAL: Use word boundaries to distinguish "i-9" from "i-94"
            import re
            
            # Check if page contains "i-9" but NOT "i-94" using word boundaries
            # This ensures "Form I-94" is NOT matched as "Form I-9"
            has_i9 = bool(re.search(r'\bi-9\b', page_title))  # Word boundary ensures exact match
            has_i94 = bool(re.search(r'\bi-94\b', page_title))  # Also use word boundary for i-94
            
            is_i9_form = (
                has_i9 and not has_i94  # Has i-9 but not i-94
            )
            
            is_i9_related = (
                is_i9_form or
                ('section 1' in page_title and has_i9) or
                ('section 2' in page_title and has_i9) or
                ('section 3' in page_title and has_i9) or
                'supplement a' in page_title or
                'supplement b' in page_title or
                'lists of acceptable' in page_title or
                'preparer and/or translator' in page_title
            )
            
            if not is_i9_related:
                supporting_pages.append({
                    'page_num': page_num,
                    'title': page_title,
                    'extracted': page.get('extracted_values', {})
                })
        
        # Check each I-9 document for matching supporting pages
        for doc_entry in i9_documents:
            # Parse document entry (format: "Document Title (#Number)")
            if '(#' in doc_entry:
                doc_title = doc_entry.split('(#')[0].strip()
                doc_number = doc_entry.split('(#')[1].rstrip(')')
            else:
                doc_title = doc_entry.strip()
                doc_number = ''
            
            doc_title_lower = doc_title.lower()
            
            # Find matching supporting pages
            matches = []
            for page in supporting_pages:
                page_title = page['title'].lower()
                
                # Document type matching logic
                if self._is_document_match(doc_title_lower, page_title):
                    matches.append(f"Page {page['page_num']}")
            
            if matches:
                attached_docs.append(f"{doc_title} → {', '.join(matches)}")
            else:
                not_attached_docs.append(doc_title)
        
        return attached_docs, not_attached_docs
    
    def _is_document_match(self, i9_doc_title: str, support_page_title: str) -> bool:
        """
        Check if an I-9 document title matches a supporting page title
        """
        # Normalize titles
        i9_doc = i9_doc_title.lower().strip()
        page_title = support_page_title.lower().strip()
        
        # Passport matching
        if 'passport' in i9_doc and 'passport' in page_title:
            return True
        
        # I-94 matching
        if 'i-94' in i9_doc and ('i-94' in page_title or 'i94' in page_title):
            return True
        
        # DS-2019 matching
        if 'ds-2019' in i9_doc and ('ds-2019' in page_title or 'ds2019' in page_title or 'certificate of eligibility' in page_title):
            return True
        
        # EAD/I-766 matching
        if ('ead' in i9_doc or 'i-766' in i9_doc or 'employment authorization' in i9_doc) and \
           ('ead' in page_title or 'i-766' in page_title or 'employment authorization' in page_title):
            return True
        
        # Permanent Resident Card matching
        if 'permanent resident' in i9_doc and ('permanent resident' in page_title or 'green card' in page_title):
            return True
        
        # Driver's License matching
        if 'driver' in i9_doc and 'license' in i9_doc and ('driver' in page_title and 'license' in page_title):
            return True
        
        # Social Security Card matching
        if 'ssn' in i9_doc or 'social security' in i9_doc:
            if 'social security' in page_title or 'ssn' in page_title:
                return True
        
        # Generic document matching (if specific types don't match, try broader matching)
        # Split both titles into words and check for significant overlap
        i9_words = set(i9_doc.replace('-', ' ').split())
        page_words = set(page_title.replace('-', ' ').split())
        
        # Remove common words that don't help with matching
        common_words = {'form', 'document', 'card', 'with', 'and', 'a', 'the', 'page', 'data', 'biographic', 'issued', 'by'}
        i9_words -= common_words
        page_words -= common_words
        
        # Check for significant word overlap (at least 2 meaningful words)
        overlap = i9_words & page_words
        if len(overlap) >= 2:
            return True
        
        return False
    
    def _is_only_number_or_code(self, value: str) -> bool:
        """Check if string is only a number or alphanumeric code (not a document title)"""
        import re
        if not value or not isinstance(value, str):
            return False
        
        value = value.strip()
        
        # Check if it's purely numeric (with optional dashes/spaces)
        # Examples: "31311478", "200-76-8781", "532212165"
        if re.match(r'^[\d\s\-]+$', value):
            return True
        
        # Check if it's a short alphanumeric code (less than 15 chars, mostly numbers)
        # Examples: "A02730709", "RE0920344079", "FZ586052"
        if len(value) < 15:
            # Count digits vs letters
            digit_count = sum(c.isdigit() for c in value)
            letter_count = sum(c.isalpha() for c in value)
            # If mostly numbers (>60% digits), it's likely a code
            if digit_count > 0 and digit_count / len(value) > 0.6:
                return True
        
        return False
    
    def _is_valid_date_format(self, date_str: str) -> bool:
        """Check if string is a valid date in MM/DD/YYYY or MM-DD-YYYY format"""
        import re
        if not date_str or not isinstance(date_str, str):
            return False
        
        # Check for MM/DD/YYYY or MM-DD-YYYY format
        date_pattern = r'^\d{1,2}[/-]\d{1,2}[/-]\d{4}$'
        if not re.match(date_pattern, date_str):
            return False
        
        # Try to parse it to ensure it's a valid date
        try:
            # Normalize separators
            normalized = date_str.replace('-', '/')
            datetime.strptime(normalized, '%m/%d/%Y')
            return True
        except:
            return False
    
    def _parse_date_for_comparison(self, date_str: str):
        """Parse date string for comparison purposes"""
        try:
            return datetime.strptime(date_str, '%m/%d/%Y')
        except:
            return datetime.min
    
    def _get_validated_form_type(self, bucket_2_data: Dict, bucket_3_data: Dict) -> str:
        """
        Get the validated form type based on business rules validation
        """
        # Use the selected form type from bucket 3 business rules which has proper validation
        selected_form_type = bucket_3_data.get('selected_form_type', 'new_hire')
        
        # Map internal codes to user-friendly names
        type_mapping = {
            'rehire_supplement_b': 're-hire',
            'reverification_section_3': 're-verification', 
            'new_hire': 'new hire'
        }
        
        return type_mapping.get(selected_form_type, 'new hire')
    
    def _is_later_date(self, new_date: str, current_date: str) -> bool:
        """
        Check if new_date is later than current_date
        """
        if not current_date:
            return True  # Any date is better than no date
        
        try:
            from datetime import datetime
            
            # Try different date formats
            date_formats = ['%m/%d/%Y', '%m/%d/%y', '%Y-%m-%d', '%d/%m/%Y']
            
            new_dt = None
            current_dt = None
            
            for fmt in date_formats:
                try:
                    new_dt = datetime.strptime(new_date, fmt)
                    break
                except:
                    continue
            
            for fmt in date_formats:
                try:
                    current_dt = datetime.strptime(current_date, fmt)
                    break
                except:
                    continue
            
            if new_dt and current_dt:
                return new_dt > current_dt
            
        except:
            pass
        
        # Fallback: string comparison (works for YYYY-MM-DD format)
        return new_date > current_date
    
    def _generate_scoring_reasons(self, bucket_1_score: int, bucket_1_data: Dict,
                                bucket_2_score: int, bucket_2_data: Dict,
                                bucket_3_score: int, bucket_3_data: Dict,
                                bucket_4_score: int, bucket_4_data: Dict,
                                bucket_5_score: int, bucket_5_data: Dict) -> Dict:
        """
        Generate detailed reasons for scoring
        """
        reasons = {}
        
        # Bucket 1 Reasons (Personal Data)
        bucket_1_reasons = []
        if not bucket_1_data.get('first_name'):
            bucket_1_reasons.append("Missing first name (-5 pts)")
        if not bucket_1_data.get('last_name'):
            bucket_1_reasons.append("Missing last name (-5 pts)")
        if not bucket_1_data.get('middle_name'):
            bucket_1_reasons.append("Missing middle name (-3 pts)")
        if not bucket_1_data.get('date_of_birth'):
            bucket_1_reasons.append("Missing date of birth (-5 pts)")
        if not bucket_1_data.get('ssn'):
            bucket_1_reasons.append("Missing SSN (-4 pts)")
        if not bucket_1_data.get('citizenship_status'):
            bucket_1_reasons.append("Missing citizenship status (-3 pts)")
        
        if not bucket_1_reasons:
            bucket_1_reasons.append("All personal data fields found")
        
        reasons['bucket_1_reasons'] = ' | '.join(bucket_1_reasons)
        
        # Bucket 2 Reasons (I-9 Detection)
        bucket_2_reasons = []
        if not bucket_2_data.get('i9_forms_detected'):
            bucket_2_reasons.append("No I-9 forms detected (-8 pts)")
        if bucket_2_data.get('section_1_pages_found', 0) == 0:
            bucket_2_reasons.append("No Section 1 pages found (-4 pts)")
        if bucket_2_data.get('section_2_pages_found', 0) == 0:
            bucket_2_reasons.append("No Section 2 pages found (-4 pts)")
        if bucket_2_data.get('section_3_supplement_pages_found', 0) == 0:
            bucket_2_reasons.append("No Section 3/Supplement pages found (-4 pts)")
        
        form_type = bucket_2_data.get('form_type_detected', 'NONE')
        if form_type == 'rehire_supplement_b':
            bucket_2_reasons.append("Supplement B detected (+5 bonus)")
        elif form_type == 'reverification_section_3':
            bucket_2_reasons.append("Section 3 detected (+3 bonus)")
        elif form_type == 'new_hire':
            bucket_2_reasons.append("Standard I-9 detected (+1 bonus)")
        
        if not bucket_2_reasons:
            bucket_2_reasons.append("All I-9 detection criteria met")
        
        reasons['bucket_2_reasons'] = ' | '.join(bucket_2_reasons)
        
        # Bucket 3 Reasons (Business Rules with Validation)
        bucket_3_reasons = []
        
        validity = bucket_3_data.get('selected_form_validity', 'Unknown')
        invalid_skipped = bucket_3_data.get('invalid_forms_skipped', 0)
        
        if not bucket_3_data.get('correct_form_type_selected'):
            bucket_3_reasons.append("Form type selection failed (-10 pts)")
        elif validity == 'Valid':
            bucket_3_reasons.append("Valid form type selected (+10 pts)")
            if invalid_skipped > 0:
                bucket_3_reasons.append(f"Correctly skipped {invalid_skipped} invalid forms (+{invalid_skipped*2} bonus pts)")
        elif validity == 'Invalid':
            bucket_3_reasons.append("Invalid form selected (-5 pts penalty)")
        elif validity == 'None Found':
            bucket_3_reasons.append("No valid forms found (-10 pts)")
            
        if not bucket_3_data.get('latest_signature_selected'):
            bucket_3_reasons.append("Latest signature selection failed (-8 pts)")
        if not bucket_3_data.get('business_rules_applied'):
            bucket_3_reasons.append("Business rules not properly applied (-7 pts)")
        
        if bucket_3_data.get('validation_applied'):
            bucket_3_reasons.append("Enhanced validation applied")
        
        if bucket_3_score >= 25:
            bucket_3_reasons.append("All business rules correctly applied")
        
        reasons['bucket_3_reasons'] = ' | '.join(bucket_3_reasons)
        
        # Bucket 4 Reasons (Work Authorization)
        bucket_4_reasons = []
        if not bucket_4_data.get('work_auth_expiry_found'):
            bucket_4_reasons.append("No work authorization expiry found (-8 pts)")
        if not bucket_4_data.get('document_expiry_found'):
            bucket_4_reasons.append("No document expiry found (-4 pts)")
        if bucket_4_data.get('work_auth_expiry_found') and bucket_4_data.get('document_expiry_found'):
            if bucket_4_data.get('expiry_dates_match'):
                bucket_4_reasons.append("Expiry dates match (+3 pts)")
            else:
                bucket_4_reasons.append("Expiry dates do not match (-3 pts)")
        
        if not bucket_4_reasons:
            bucket_4_reasons.append("No work authorization data available")
        
        reasons['bucket_4_reasons'] = ' | '.join(bucket_4_reasons)
        
        # Bucket 5 Reasons (Document Tracking)
        bucket_5_reasons = []
        if not bucket_5_data.get('documents_listed_correctly'):
            bucket_5_reasons.append("Documents not properly listed (-6 pts)")
        if not bucket_5_data.get('document_numbers_extracted'):
            bucket_5_reasons.append("Document numbers not extracted (-3 pts)")
        if bucket_5_data.get('supporting_documents_found', 0) == 0:
            bucket_5_reasons.append("No supporting documents found (-3 pts)")
        
        attachment_status = bucket_5_data.get('document_attachment_status', 'Unknown')
        if attachment_status == 'NONE':
            bucket_5_reasons.append("No document attachments (-3 pts)")
        elif attachment_status == 'LISTED_ONLY':
            bucket_5_reasons.append("Documents listed but not all attached (-2 pts)")
        
        if bucket_5_score == 15:
            bucket_5_reasons.append("All document tracking criteria met")
        
        reasons['bucket_5_reasons'] = ' | '.join(bucket_5_reasons)
        
        return reasons
    
    def _all_documents_attached(self, business_fields: Dict) -> bool:
        """
        Check if all listed documents are attached to PDF
        Returns True if matching_support_documents_not_attached is empty
        """
        not_attached = str(business_fields.get('matching_support_documents_not_attached', ''))
        return not_attached in ['', 'nan', 'N/A', 'None'] or not not_attached
    
    def _has_expiry_match(self, business_fields: Dict) -> bool:
        """
        Check if work authorization expiry matches document expiry
        Returns True if expiry_date_matches contains "MATCH:"
        """
        expiry_matches = str(business_fields.get('expiry_date_matches', ''))
        return 'MATCH:' in expiry_matches
    
    def _determine_status(self, total_score: int, bucket_1_score: int, bucket_2_score: int, bucket_3_score: int, business_fields: Dict = None) -> str:
        """
        Determine overall status based on citizenship-specific criteria
        
        US Citizen Requirements:
        1. First name
        2. Last name
        3. Date of birth
        4. All listed documents attached
        
        Non-Citizen Requirements:
        1. First name
        2. Last name
        3. Date of birth
        4. Expiry date matches
        5. All listed documents attached
        """
        # Critical requirement: Must have I-9 forms
        has_i9_forms = bucket_2_score >= 10
        if not has_i9_forms:
            return "NO_I9_FOUND"
        
        # If business_fields not provided, fall back to old logic
        if business_fields is None:
            has_personal_data = bucket_1_score >= 12
            follows_business_rules = bucket_3_score >= 15
            
            if total_score >= 85 and has_personal_data and follows_business_rules:
                return "COMPLETE_SUCCESS"
            elif total_score >= 60:
                return "PARTIAL_SUCCESS"
            elif total_score >= 40:
                return "ERROR"
            else:
                return "NO_I9_FOUND"
        
        # New citizenship-based logic
        # Check basic required fields
        first_name = business_fields.get('employee_first_name', '')
        last_name = business_fields.get('employee_last_name', '')
        dob = business_fields.get('employee_date_of_birth', '')
        
        has_first_name = first_name and str(first_name) not in ['', 'N/A', 'nan', 'None']
        has_last_name = last_name and str(last_name) not in ['', 'N/A', 'nan', 'None']
        has_dob = dob and str(dob) not in ['', 'N/A', 'nan', 'None']
        
        # Check if all documents are attached
        all_docs_attached = self._all_documents_attached(business_fields)
        
        # Determine citizenship
        citizenship = str(business_fields.get('citizenship_status', '')).lower()
        is_us_citizen = 'us citizen' in citizenship or 'citizen of the united states' in citizenship
        
        if is_us_citizen:
            # US Citizen: 4 criteria
            if has_first_name and has_last_name and has_dob and all_docs_attached:
                return 'COMPLETE_SUCCESS'
            else:
                return 'PARTIAL_SUCCESS'
        else:
            # Non-Citizen: 5 criteria
            has_expiry_match = self._has_expiry_match(business_fields)
            
            if has_first_name and has_last_name and has_dob and has_expiry_match and all_docs_attached:
                return 'COMPLETE_SUCCESS'
            else:
                return 'PARTIAL_SUCCESS'
    
    def _determine_status_with_audit(self, total_score: int, bucket_1_score: int, bucket_2_score: int, bucket_3_score: int, business_fields: Dict = None) -> Tuple[str, Dict, str]:
        """
        Determine status and return detailed audit information
        Returns: (status, criteria_met, reasoning)
        """
        # Critical requirement: Must have I-9 forms
        has_i9_forms = bucket_2_score >= 10
        if not has_i9_forms:
            return "NO_I9_FOUND", {}, "No I-9 forms detected in the document"
        
        if business_fields is None:
            status = self._determine_status(total_score, bucket_1_score, bucket_2_score, bucket_3_score, None)
            return status, {}, "Legacy status determination (no business fields)"
        
        # Check basic required fields
        first_name = business_fields.get('employee_first_name', '')
        last_name = business_fields.get('employee_last_name', '')
        dob = business_fields.get('employee_date_of_birth', '')
        
        has_first_name = first_name and str(first_name) not in ['', 'N/A', 'nan', 'None']
        has_last_name = last_name and str(last_name) not in ['', 'N/A', 'nan', 'None']
        has_dob = dob and str(dob) not in ['', 'N/A', 'nan', 'None']
        
        # Check if all documents are attached
        all_docs_attached = self._all_documents_attached(business_fields)
        not_attached = business_fields.get('matching_support_documents_not_attached', '')
        
        # Determine citizenship
        citizenship = str(business_fields.get('citizenship_status', '')).lower()
        is_us_citizen = 'us citizen' in citizenship or 'citizen of the united states' in citizenship
        
        criteria_met = {
            'has_first_name': has_first_name,
            'has_last_name': has_last_name,
            'has_dob': has_dob,
            'all_docs_attached': all_docs_attached,
            'first_name_value': first_name,
            'last_name_value': last_name,
            'dob_value': dob,
            'docs_not_attached': not_attached
        }
        
        if is_us_citizen:
            # US Citizen: 4 criteria
            criteria_met['citizenship_type'] = 'US Citizen'
            criteria_met['required_criteria_count'] = 4
            
            reasons = []
            reasons.append(f"Citizenship: US Citizen")
            reasons.append(f"1. First Name: {'✅ ' + str(first_name) if has_first_name else '❌ NOT FOUND'}")
            reasons.append(f"2. Last Name: {'✅ ' + str(last_name) if has_last_name else '❌ NOT FOUND'}")
            reasons.append(f"3. Date of Birth: {'✅ ' + str(dob) if has_dob else '❌ NOT FOUND'}")
            reasons.append(f"4. All Docs Attached: {'✅ YES' if all_docs_attached else '❌ NO - Missing: ' + str(not_attached)}")
            
            if has_first_name and has_last_name and has_dob and all_docs_attached:
                status = 'COMPLETE_SUCCESS'
                reasons.append("Result: ALL 4 CRITERIA MET → COMPLETE_SUCCESS")
            else:
                status = 'PARTIAL_SUCCESS'
                failed_count = sum([not has_first_name, not has_last_name, not has_dob, not all_docs_attached])
                reasons.append(f"Result: {failed_count} CRITERIA FAILED → PARTIAL_SUCCESS")
            
            reasoning = ' | '.join(reasons)
            
        else:
            # Non-Citizen: 5 criteria
            has_expiry_match = self._has_expiry_match(business_fields)
            expiry_match_detail = business_fields.get('expiry_date_matches', '')
            
            criteria_met['citizenship_type'] = 'Non-Citizen'
            criteria_met['required_criteria_count'] = 5
            criteria_met['has_expiry_match'] = has_expiry_match
            criteria_met['expiry_match_detail'] = expiry_match_detail
            
            reasons = []
            reasons.append(f"Citizenship: Non-Citizen")
            reasons.append(f"1. First Name: {'✅ ' + str(first_name) if has_first_name else '❌ NOT FOUND'}")
            reasons.append(f"2. Last Name: {'✅ ' + str(last_name) if has_last_name else '❌ NOT FOUND'}")
            reasons.append(f"3. Date of Birth: {'✅ ' + str(dob) if has_dob else '❌ NOT FOUND'}")
            reasons.append(f"4. Expiry Match: {'✅ YES - ' + str(expiry_match_detail) if has_expiry_match else '❌ NO - ' + str(expiry_match_detail)}")
            reasons.append(f"5. All Docs Attached: {'✅ YES' if all_docs_attached else '❌ NO - Missing: ' + str(not_attached)}")
            
            if has_first_name and has_last_name and has_dob and has_expiry_match and all_docs_attached:
                status = 'COMPLETE_SUCCESS'
                reasons.append("Result: ALL 5 CRITERIA MET → COMPLETE_SUCCESS")
            else:
                status = 'PARTIAL_SUCCESS'
                failed_count = sum([not has_first_name, not has_last_name, not has_dob, not has_expiry_match, not all_docs_attached])
                reasons.append(f"Result: {failed_count} CRITERIA FAILED → PARTIAL_SUCCESS")
            
            reasoning = ' | '.join(reasons)
        
        return status, criteria_met, reasoning
    
    def _create_error_result(self, filename: str, error_msg: str) -> Dict:
        """Create error result for failed processing"""
        return {
            'filename': filename,
            'status': 'ERROR',
            'total_score': 0,
            'error_message': error_msg,
            'bucket_1_personal_data_score': 0,
            'bucket_2_i9_detection_score': 0,
            'bucket_3_business_rules_score': 0,
            'bucket_4_work_authorization_score': 0,
            'bucket_5_document_tracking_score': 0
        }
    
    def process_all_catalogs(self, catalog_directory: str) -> List[Dict]:
        """
        Process all catalog files in a directory
        """
        catalog_dir = Path(catalog_directory)
        catalog_files = list(catalog_dir.glob("*.catalog.json"))
        
        logger.info(f"Found {len(catalog_files)} catalog files to process")
        
        results = []
        for catalog_file in catalog_files:
            result = self.process_catalog_file(str(catalog_file))
            results.append(result)
        
        return results
    
    def export_to_csv(self, results: List[Dict], output_path: str):
        """
        Export results to CSV file with comprehensive business fields first, then scoring details
        """
        if not results:
            logger.warning("No results to export")
            return
        
        # Define CSV columns in the requested order: Business fields first, then scoring details
        fieldnames = [
            # Basic identification
            'filename', 'status',
            
            # Personal Information (Business Fields)
            'employee_first_name', 'employee_middle_name', 'employee_last_name',
            'employee_date_of_birth', 'employee_ssn', 'citizenship_status',
            
            # Form Analysis (Business Fields)
            'form_type_detected', 'form_type_decision_basis', 'form_type_source_page',
            'total_i9_sets_found', 'all_i9_sets_found',
            'section_1_pages_count', 'section_2_pages_count', 'section_3_pages_count', 'supplement_b_pages_count',
            
            # Primary I-9 Set Information (Business Fields)
            'primary_i9_set_type', 'primary_i9_set_signature_date', 'primary_i9_set_pages',
            
            # Work Authorization (Business Fields)
            'work_authorization_expiry_date', 'work_authorization_source',
            
            # Document Information (Business Fields)
            'documents_in_primary_set', 'document_count_in_primary_set',
            'supporting_documents_count', 'document_expiry_dates', 'expiry_date_matches',
            
            # Support Document Validation (Business Fields)
            'matching_support_documents_attached', 'matching_support_documents_not_attached',
            
            # Additional Business Fields
            'employee_signature_date', 'employer_signature_date',
            'alien_registration_number', 'first_day_of_employment',
            
            # Scoring Details (at the end as requested)
            'total_score',
            'bucket_1_personal_data_score', 'bucket_2_i9_detection_score', 
            'bucket_3_business_rules_score', 'bucket_4_work_authorization_score', 
            'bucket_5_document_tracking_score',
            
            # Scoring Reasons (detailed explanations)
            'bucket_1_reasons', 'bucket_2_reasons', 'bucket_3_reasons',
            'bucket_4_reasons', 'bucket_5_reasons'
        ]
        
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore', 
                                   quoting=csv.QUOTE_ALL)  # Quote all fields
            writer.writeheader()
            
            for result in results:
                writer.writerow(result)
        
        logger.info(f"Results exported to: {output_path}")


def main():
    """
    Main function to run the rubric processor
    """
    processor = I9RubricProcessor()
    
    # Process all catalogs
    catalog_directory = "workdir/catalogs"
    results = processor.process_all_catalogs(catalog_directory)
    
    # Export to CSV
    output_path = "workdir/rubric_based_results.csv"
    processor.export_to_csv(results, output_path)
    
    # Print summary
    print(f"\n🎯 RUBRIC-BASED PROCESSING COMPLETE")
    print(f"📁 Processed: {len(results)} catalog files")
    print(f"📊 Results exported to: {output_path}")
    
    # Status summary
    status_counts = {}
    for result in results:
        status = result.get('status', 'UNKNOWN')
        status_counts[status] = status_counts.get(status, 0) + 1
    
    print(f"\n📋 STATUS SUMMARY:")
    for status, count in status_counts.items():
        print(f"   {status}: {count}")


if __name__ == "__main__":
    main()
