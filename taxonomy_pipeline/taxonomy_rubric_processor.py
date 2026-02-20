#!/usr/bin/env python3
"""
Taxonomy-Based Rubric Processor
Processes taxonomy-normalized catalogs and applies business rules
"""
import json
import csv
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import datetime
import logging

import config

logging.basicConfig(level=logging.INFO if config.VERBOSE_LOGGING else logging.WARNING)
logger = logging.getLogger(__name__)


class TaxonomyRubricProcessor:
    """
    Processes taxonomy-normalized I-9 catalogs with business rules
    """
    
    def __init__(self):
        """Initialize processor"""
        self.results = []
    
    def process_catalog(self, catalog_path: str) -> Dict:
        """
        Process a single taxonomy catalog
        
        Args:
            catalog_path: Path to taxonomy catalog JSON
        
        Returns:
            Processing result dict
        """
        logger.info(f"Processing catalog: {Path(catalog_path).name}")
        
        with open(catalog_path) as f:
            catalog = json.load(f)
        
        result = {
            'filename': catalog['filename'],
            'taxonomy_version': catalog.get('taxonomy_version'),
            'extraction_provider': catalog['extraction_metadata']['llm_provider'],
            'extraction_model': catalog['extraction_metadata']['llm_model'],
            'total_pages': len(catalog['pages']),
            'processing_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Extract personal data
        personal_data = self._extract_personal_data(catalog)
        result.update(personal_data)
        
        # Detect form type
        form_type_data = self._detect_form_type(catalog)
        result.update(form_type_data)
        
        # Extract documents
        document_data = self._extract_documents(catalog)
        result.update(document_data)
        
        # Extract work authorization
        work_auth_data = self._extract_work_authorization(catalog)
        result.update(work_auth_data)
        
        # Determine status
        status_data = self._determine_status(result)
        result.update(status_data)
        
        # Calculate quality score
        quality_score = self._calculate_quality_score(catalog, result)
        result['quality_score'] = quality_score
        
        return result
    
    def _extract_personal_data(self, catalog: Dict) -> Dict:
        """Extract personal information from catalog"""
        data = {
            'first_name': '',
            'last_name': '',
            'middle_name': '',
            'date_of_birth': '',
            'employee_ssn': ''
        }
        
        # Search all pages for personal data
        for page in catalog['pages']:
            fields = page.get('fields', {})
            
            # Extract names
            if not data['first_name'] and 'first_name' in fields:
                data['first_name'] = fields['first_name'].get('value', '')
            if not data['first_name'] and 'employee_first_name' in fields:
                data['first_name'] = fields['employee_first_name'].get('value', '')
            
            if not data['last_name'] and 'last_name' in fields:
                data['last_name'] = fields['last_name'].get('value', '')
            if not data['last_name'] and 'employee_last_name' in fields:
                data['last_name'] = fields['employee_last_name'].get('value', '')
            
            if not data['middle_name'] and 'middle_name' in fields:
                data['middle_name'] = fields['middle_name'].get('value', '')
            
            # Extract DOB
            if not data['date_of_birth'] and 'date_of_birth' in fields:
                data['date_of_birth'] = fields['date_of_birth'].get('value', '')
            if not data['date_of_birth'] and 'employee_date_of_birth' in fields:
                data['date_of_birth'] = fields['employee_date_of_birth'].get('value', '')
            
            # Extract SSN
            if not data['employee_ssn'] and 'social_security_number' in fields:
                data['employee_ssn'] = fields['social_security_number'].get('value', '')
            if not data['employee_ssn'] and 'ssn' in fields:
                data['employee_ssn'] = fields['ssn'].get('value', '')
        
        return data
    
    def _detect_form_type(self, catalog: Dict) -> Dict:
        """Detect form type using taxonomy canonical names"""
        data = {
            'form_type_detected': 'unknown',
            'form_type_confidence': 0.0,
            'form_type_source_page': '',
            'supplement_b_pages': 0,
            'section_3_pages': 0,
            'standard_i9_pages': 0
        }
        
        supplement_b_pages = []
        section_3_pages = []
        standard_i9_pages = []
        
        for page in catalog['pages']:
            page_type = page.get('page_type', {})
            canonical = page_type.get('canonical') or ''
            canonical = canonical.lower() if canonical else ''
            confidence = page_type.get('confidence', 0.0)
            
            # Supplement B detection
            if 'supplement_b' in canonical or 'supplement b' in canonical:
                supplement_b_pages.append({
                    'page': page['page_number'],
                    'confidence': confidence
                })
            
            # Section 3 detection
            elif 'section_3' in canonical or 'section 3' in canonical or 'reverification' in canonical:
                section_3_pages.append({
                    'page': page['page_number'],
                    'confidence': confidence
                })
            
            # Standard I-9 detection
            elif 'i9' in canonical or 'i-9' in canonical or 'employment eligibility' in canonical:
                standard_i9_pages.append({
                    'page': page['page_number'],
                    'confidence': confidence
                })
        
        data['supplement_b_pages'] = len(supplement_b_pages)
        data['section_3_pages'] = len(section_3_pages)
        data['standard_i9_pages'] = len(standard_i9_pages)
        
        # Priority hierarchy: Supplement B > Section 3 > Standard I-9
        if supplement_b_pages:
            best = max(supplement_b_pages, key=lambda x: x['confidence'])
            data['form_type_detected'] = 're-hire'
            data['form_type_confidence'] = best['confidence']
            data['form_type_source_page'] = f"Page {best['page']}"
        
        elif section_3_pages:
            best = max(section_3_pages, key=lambda x: x['confidence'])
            data['form_type_detected'] = 're-verification'
            data['form_type_confidence'] = best['confidence']
            data['form_type_source_page'] = f"Page {best['page']}"
        
        elif standard_i9_pages:
            best = max(standard_i9_pages, key=lambda x: x['confidence'])
            data['form_type_detected'] = 'new hire'
            data['form_type_confidence'] = best['confidence']
            data['form_type_source_page'] = f"Page {best['page']}"
        
        return data
    
    def _extract_documents(self, catalog: Dict) -> Dict:
        """Extract document information"""
        data = {
            'list_a_documents': [],
            'list_b_documents': [],
            'list_c_documents': [],
            'total_documents': 0
        }
        
        all_documents = []
        
        for page in catalog['pages']:
            documents = page.get('documents', [])
            for doc in documents:
                canonical = doc.get('canonical', '')
                doc_list = doc.get('list', '')
                confidence = doc.get('confidence', 0.0)
                
                if confidence >= config.CONFIDENCE_THRESHOLD:
                    doc_info = {
                        'canonical': canonical,
                        'original_text': doc.get('original_text', ''),
                        'confidence': confidence,
                        'page': page['page_number']
                    }
                    
                    all_documents.append(doc_info)
                    
                    if doc_list == 'A':
                        data['list_a_documents'].append(canonical)
                    elif doc_list == 'B':
                        data['list_b_documents'].append(canonical)
                    elif doc_list == 'C':
                        data['list_c_documents'].append(canonical)
        
        data['total_documents'] = len(all_documents)
        data['list_a_documents'] = ', '.join(data['list_a_documents'])
        data['list_b_documents'] = ', '.join(data['list_b_documents'])
        data['list_c_documents'] = ', '.join(data['list_c_documents'])
        
        return data
    
    def _extract_work_authorization(self, catalog: Dict) -> Dict:
        """Extract work authorization expiry date"""
        data = {
            'work_authorization_expiry_date': '',
            'work_auth_confidence': 0.0
        }
        
        # Look for expiration dates in documents
        for page in catalog['pages']:
            documents = page.get('documents', [])
            for doc in documents:
                if doc.get('expiration_date'):
                    data['work_authorization_expiry_date'] = doc['expiration_date']
                    data['work_auth_confidence'] = doc.get('confidence', 0.0)
                    break
            
            # Also check fields
            fields = page.get('fields', {})
            for field_name, field_data in fields.items():
                if 'expir' in field_name.lower() and field_data.get('value'):
                    if not data['work_authorization_expiry_date']:
                        data['work_authorization_expiry_date'] = field_data['value']
                        data['work_auth_confidence'] = field_data.get('confidence', 0.0)
        
        return data
    
    def _determine_status(self, result: Dict) -> Dict:
        """Determine completion status"""
        data = {
            'status': 'UNKNOWN',
            'status_reason': []
        }
        
        reasons = []
        
        # Check required fields
        has_first_name = bool(result.get('first_name'))
        has_last_name = bool(result.get('last_name'))
        has_dob = bool(result.get('date_of_birth'))
        has_documents = result.get('total_documents', 0) > 0
        
        if has_first_name and has_last_name and has_dob and has_documents:
            data['status'] = 'COMPLETE_SUCCESS'
            reasons.append('All required fields present')
        elif has_first_name and has_last_name:
            data['status'] = 'PARTIAL_SUCCESS'
            reasons.append('Basic info present, missing some fields')
        else:
            data['status'] = 'INCOMPLETE'
            reasons.append('Missing critical fields')
        
        data['status_reason'] = '; '.join(reasons)
        
        return data
    
    def _calculate_quality_score(self, catalog: Dict, result: Dict) -> float:
        """Calculate overall quality score based on confidence"""
        confidences = []
        
        # Collect all confidence scores
        for page in catalog['pages']:
            page_type = page.get('page_type', {})
            if page_type.get('confidence'):
                confidences.append(page_type['confidence'])
            
            fields = page.get('fields', {})
            for field_data in fields.values():
                if field_data.get('confidence'):
                    confidences.append(field_data['confidence'])
            
            documents = page.get('documents', [])
            for doc in documents:
                if doc.get('confidence'):
                    confidences.append(doc['confidence'])
        
        if confidences:
            return round(sum(confidences) / len(confidences), 3)
        else:
            return 0.0
    
    def process_all_catalogs(self, catalog_dir: str) -> List[Dict]:
        """
        Process all catalogs in a directory
        
        Args:
            catalog_dir: Directory containing taxonomy catalogs
        
        Returns:
            List of results
        """
        catalog_dir = Path(catalog_dir)
        catalog_files = list(catalog_dir.glob('*_taxonomy.json'))
        
        logger.info(f"Found {len(catalog_files)} taxonomy catalogs")
        
        results = []
        for catalog_file in catalog_files:
            try:
                result = self.process_catalog(str(catalog_file))
                results.append(result)
                logger.info(f"  ✓ {catalog_file.name}: {result['status']}")
            except Exception as e:
                logger.error(f"  ✗ {catalog_file.name}: {e}")
        
        self.results = results
        return results
    
    def save_csv(self, output_path: str):
        """Save results to CSV"""
        if not self.results:
            logger.warning("No results to save")
            return
        
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Get all field names
        fieldnames = list(self.results[0].keys())
        
        with open(output_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.results)
        
        logger.info(f"✓ CSV saved: {output_path}")
    
    def save_audit_logs(self, output_dir: str):
        """Save individual audit logs"""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        for result in self.results:
            filename = Path(result['filename']).stem
            audit_path = output_dir / f"{filename}_audit.json"
            
            with open(audit_path, 'w') as f:
                json.dump(result, f, indent=2)
        
        logger.info(f"✓ Audit logs saved: {output_dir}")


def main():
    """Test the processor"""
    processor = TaxonomyRubricProcessor()
    
    # Process all catalogs
    results = processor.process_all_catalogs(config.CATALOGS_DIR)
    
    # Save outputs
    processor.save_csv(config.CSV_DIR / 'taxonomy_results.csv')
    processor.save_audit_logs(config.AUDIT_DIR)
    
    print(f"\n✓ Processing complete!")
    print(f"  Processed: {len(results)} files")
    print(f"  CSV: {config.CSV_DIR / 'taxonomy_results.csv'}")
    print(f"  Audits: {config.AUDIT_DIR}")


if __name__ == "__main__":
    main()
