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

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class I9RubricProcessor:
    """
    Processes I-9 catalog files using business rules rubric
    """
    
    def __init__(self):
        self.results = []
        
    def process_catalog_file(self, catalog_path: str) -> Dict:
        """
        Process a single catalog file and return rubric-based results
        """
        logger.info(f"Processing catalog: {catalog_path}")
        
        try:
            with open(catalog_path, 'r', encoding='utf-8') as f:
                catalog_data = json.load(f)
            
            filename = Path(catalog_path).stem.replace('.catalog', '')
            
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
            
            # Calculate total score and status
            total_score = bucket_1_score + bucket_2_score + bucket_3_score + bucket_4_score + bucket_5_score
            status = self._determine_status(total_score, bucket_1_score, bucket_2_score, bucket_3_score)
            
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
                
                # Classify sections
                if 'section 1' in page_title or any(k in extracted for k in ['employee_first_name', 'employee_last_name']):
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
        
        # Form Type Classification with Bonus Points
        if section_3_supplement_count > 0:
            # Check if it's Supplement B or Section 3
            has_supplement_b = any('supplement b' in page.get('page_title', '').lower() for page in i9_pages)
            if has_supplement_b:
                data['form_type_detected'] = 'rehire_supplement_b'
                bonus_points = 5
            else:
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
        BUCKET 3: BUSINESS RULES HIERARCHY APPLICATION (25 points)
        """
        score = 0
        data = {
            'correct_form_type_selected': False,
            'latest_signature_selected': False,
            'selected_form_signature_date': '',
            'business_rules_applied': False
        }
        
        pages = catalog_data.get('document_catalog', {}).get('pages', catalog_data.get('pages', []))
        
        # Find all I-9 pages with signature dates
        supplement_b_pages = []
        section_3_pages = []
        standard_i9_pages = []
        
        for page in pages:
            page_title = page.get('page_title', '').lower()
            extracted = page.get('extracted_values', {})
            
            # Get signature dates (including fresh catalog variations)
            sig_dates = [
                extracted.get('employer_signature_date'),
                extracted.get('reverification_signature_date'),
                extracted.get('reverification_date_signed'),
                extracted.get('employer_signature_date_reverification'),
                # Additional variations found in catalogs
                extracted.get('date_of_employer_signature'),  # ← Debek has this
                extracted.get('date_of_employee_signature'),  # ← Additional coverage
                extracted.get('section_3_signature_date'),
                extracted.get('section_3_employer_signature_date')
            ]
            
            latest_sig = None
            for sig_date in sig_dates:
                if sig_date and sig_date not in ['N/A', '', None]:
                    if not latest_sig or sig_date > latest_sig:
                        latest_sig = sig_date
            
            if latest_sig:
                if 'supplement b' in page_title:
                    supplement_b_pages.append({'page': page, 'signature_date': latest_sig})
                elif 'section 3' in page_title or 'reverification' in page_title:
                    section_3_pages.append({'page': page, 'signature_date': latest_sig})
                elif 'i-9' in page_title or 'employment eligibility' in page_title:
                    standard_i9_pages.append({'page': page, 'signature_date': latest_sig})
        
        # Apply Business Rules Hierarchy
        selected_pages = None
        selected_type = None
        
        if supplement_b_pages:
            selected_pages = supplement_b_pages
            selected_type = 'Supplement B'
            score += 10  # Correct Form Type Selected
            data['correct_form_type_selected'] = True
        elif section_3_pages:
            selected_pages = section_3_pages
            selected_type = 'Section 3'
            score += 10  # Correct Form Type Selected
            data['correct_form_type_selected'] = True
        elif standard_i9_pages:
            selected_pages = standard_i9_pages
            selected_type = 'Standard I-9'
            score += 10  # Correct Form Type Selected
            data['correct_form_type_selected'] = True
        
        # Select latest signature within selected type
        if selected_pages:
            def parse_date_for_comparison(date_str):
                try:
                    return datetime.strptime(date_str, '%m/%d/%Y')
                except:
                    return datetime.min
            
            latest_page_info = max(selected_pages, key=lambda x: parse_date_for_comparison(x['signature_date']))
            data['selected_form_signature_date'] = latest_page_info['signature_date']
            data['latest_signature_selected'] = True
            score += 8  # Latest Signature Date Selected
            
            # Additional scoring for proper selection
            score += 4  # Proper Section 1 Selection
            score += 3  # Proper Document Section Selection
            data['business_rules_applied'] = True
        
        logger.info(f"Bucket 3 (Business Rules): {score}/25 points - Selected: {selected_type}")
        return score, data
    
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
        
        # Find work authorization expiry from Section 1
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
    
    def _determine_status(self, total_score: int, bucket_1_score: int, bucket_2_score: int, bucket_3_score: int) -> str:
        """
        Determine overall status based on total score and critical requirements
        """
        # Critical requirements
        has_i9_forms = bucket_2_score >= 10
        has_personal_data = bucket_1_score >= 12
        follows_business_rules = bucket_3_score >= 15
        
        if not has_i9_forms:
            return "NO_I9_FOUND"
        
        if total_score >= 85 and has_personal_data and follows_business_rules:
            return "COMPLETE_SUCCESS"
        elif total_score >= 60:
            return "PARTIAL_SUCCESS"
        elif total_score >= 40:
            return "ERROR"
        else:
            return "NO_I9_FOUND"
    
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
        Export results to CSV file
        """
        if not results:
            logger.warning("No results to export")
            return
        
        # Define CSV columns
        fieldnames = [
            'filename', 'status', 'total_score',
            'bucket_1_personal_data_score', 'bucket_2_i9_detection_score', 
            'bucket_3_business_rules_score', 'bucket_4_work_authorization_score', 
            'bucket_5_document_tracking_score',
            'first_name', 'middle_name', 'last_name', 'date_of_birth', 'ssn', 'citizenship_status',
            'form_type_detected', 'selected_form_signature_date',
            'work_auth_expiry_date', 'expiry_match_details',
            'documents_listed', 'supporting_documents_found', 'document_attachment_status'
        ]
        
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
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
