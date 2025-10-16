#!/usr/bin/env python3
"""
Supporting Document Matching Logic

This module implements the logic to match Section 2/3 document references
to actual supporting document pages within the same PDF.
"""

from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum
import re
from datetime import datetime

from hri9.core.set_grouping import I9Set, I9Page, I9SectionType
from hri9.utils.logging_config import logger


class DocumentType(Enum):
    """Types of supporting documents"""
    PASSPORT = "passport"
    I94 = "i94"
    DS2019 = "ds2019"
    EAD = "ead"
    DRIVERS_LICENSE = "drivers_license"
    SSN_CARD = "ssn_card"
    OTHER = "other"


@dataclass
class DocumentReference:
    """Document reference from Section 2/3 of I-9 form"""
    document_type: DocumentType
    document_number: str
    expiration_date: Optional[str]
    issuing_authority: Optional[str]
    list_category: str  # A, B, or C
    source_section: str  # section_2, section_3, supplement_b


@dataclass
class SupportingDocument:
    """Actual supporting document page"""
    page_number: int
    document_type: DocumentType
    document_number: str
    expiration_date: Optional[str]
    issuing_authority: Optional[str]
    extracted_values: Dict[str, Any]
    confidence_score: float


@dataclass
class DocumentMatch:
    """Result of matching a reference to a supporting document"""
    reference: DocumentReference
    supporting_document: Optional[SupportingDocument]
    match_confidence: float
    match_reasons: List[str]
    validation_errors: List[str]


class DocumentMatcher:
    """Matches I-9 document references to supporting documents"""
    
    def __init__(self):
        self.logger = logger
    
    def match_documents_for_set(self, i9_set: I9Set) -> List[DocumentMatch]:
        """Match document references to supporting documents for an I-9 set"""
        
        # Debug logging
        logger.info(f"DocumentMatcher: Processing I-9 set {i9_set.set_id}")
        logger.info(f"DocumentMatcher: Supporting doc pages count: {len(i9_set.supporting_doc_pages)}")
        
        # Extract document references from I-9 forms
        references = self._extract_document_references(i9_set)
        logger.info(f"DocumentMatcher: Extracted {len(references)} document references")
        
        # Parse supporting documents
        supporting_docs = self._parse_supporting_documents(i9_set.supporting_doc_pages)
        logger.info(f"DocumentMatcher: Parsed {len(supporting_docs)} supporting documents")
        
        # Match references to supporting documents
        matches = self._match_references_to_documents(references, supporting_docs)
        
        logger.info(f"Matched {len([m for m in matches if m.supporting_document])} of {len(references)} document references")
        
        return matches
    
    def _extract_document_references(self, i9_set: I9Set) -> List[DocumentReference]:
        """Extract document references from Section 2/3 pages"""
        
        references = []
        
        # Extract from Section 2
        if i9_set.section_2_page:
            section_2_refs = self._extract_section_2_references(i9_set.section_2_page)
            references.extend(section_2_refs)
        
        # Extract from Section 3 pages
        for section_3_page in i9_set.section_3_pages:
            section_3_refs = self._extract_section_3_references(section_3_page)
            references.extend(section_3_refs)
        
        # Extract from Supplement B pages
        for supp_b_page in i9_set.supplement_b_pages:
            supp_b_refs = self._extract_supplement_b_references(supp_b_page)
            references.extend(supp_b_refs)
        
        logger.info(f"Extracted {len(references)} document references")
        return references
    
    def _extract_section_2_references(self, section_2_page: I9Page) -> List[DocumentReference]:
        """Extract document references from Section 2"""
        
        references = []
        extracted = section_2_page.extracted_values
        
        # List A documents - use flexible field extraction like CSV system
        list_a_docs = [
            {
                'title': (extracted.get('section_2_list_a_document_title') or 
                         extracted.get('list_a_document_title') or 
                         extracted.get('list_a_document_title_1')),
                'number': (extracted.get('section_2_list_a_document_number') or 
                          extracted.get('list_a_document_number') or 
                          extracted.get('list_a_document_number_1')),
                'expiry': (extracted.get('section_2_list_a_expiration_date') or 
                          extracted.get('list_a_document_expiration_date') or 
                          extracted.get('list_a_expiration_date') or
                          extracted.get('list_a_expiration_date_1')),
                'authority': (extracted.get('section_2_list_a_issuing_authority') or 
                             extracted.get('list_a_issuing_authority') or
                             extracted.get('list_a_issuing_authority_1'))
            },
            {
                'title': (extracted.get('list_a_document_title_2') or 
                         extracted.get('list_a_document_2_title')),
                'number': (extracted.get('list_a_document_number_2') or 
                          extracted.get('list_a_document_2_number')),
                'expiry': (extracted.get('list_a_expiration_date_2') or 
                          extracted.get('list_a_document_2_expiration_date')),
                'authority': (extracted.get('list_a_issuing_authority_2') or 
                             extracted.get('list_a_document_2_issuing_authority'))
            },
            {
                'title': (extracted.get('list_a_document_title_3') or 
                         extracted.get('list_a_document_3_title')),
                'number': (extracted.get('list_a_document_number_3') or 
                          extracted.get('list_a_document_3_number')),
                'expiry': (extracted.get('list_a_expiration_date_3') or 
                          extracted.get('list_a_document_3_expiration_date')),
                'authority': (extracted.get('list_a_issuing_authority_3') or 
                             extracted.get('list_a_document_3_issuing_authority'))
            }
        ]
        
        for doc in list_a_docs:
            if doc['title'] and doc['title'] != 'N/A':  # Only require title, like CSV system
                doc_type = self._determine_document_type(doc['title'])
                reference = DocumentReference(
                    document_type=doc_type,
                    document_number=doc['number'] or 'N/A',  # Allow missing numbers
                    expiration_date=doc['expiry'],
                    issuing_authority=doc['authority'],
                    list_category='A',
                    source_section='section_2'
                )
                references.append(reference)
        
        # List B and C documents (if present)
        # Add similar logic for List B and C documents
        
        return references
    
    def _extract_section_3_references(self, section_3_page: I9Page) -> List[DocumentReference]:
        """Extract document references from Section 3 (reverification)"""
        
        references = []
        extracted = section_3_page.extracted_values
        
        # Use flexible field extraction like CSV system for Section 3/reverification documents
        doc_title = (extracted.get('reverification_document_title') or 
                    extracted.get('section_3_document_title') or
                    extracted.get('reverification_1_document_title') or
                    extracted.get('new_document_title'))
        doc_number = (extracted.get('reverification_document_number') or
                     extracted.get('section_3_document_number') or
                     extracted.get('reverification_1_document_number') or
                     extracted.get('new_document_number'))
        doc_expiry = (extracted.get('reverification_expiration_date') or
                     extracted.get('section_3_expiration_date') or
                     extracted.get('reverification_1_expiration_date') or
                     extracted.get('new_document_expiration'))
        doc_authority = (extracted.get('reverification_issuing_authority') or
                        extracted.get('section_3_issuing_authority') or
                        extracted.get('new_document_authority'))
        
        if doc_title and doc_title != 'N/A':  # Only require title, like CSV system
            doc_type = self._determine_document_type(doc_title)
            reference = DocumentReference(
                document_type=doc_type,
                document_number=doc_number or 'N/A',  # Allow missing numbers
                expiration_date=doc_expiry,
                issuing_authority=doc_authority,
                list_category='A',  # Default to A for reverification
                source_section='section_3'
            )
            references.append(reference)
        
        return references
    
    def _extract_supplement_b_references(self, supp_b_page: I9Page) -> List[DocumentReference]:
        """Extract document references from Supplement B (rehire)"""
        
        references = []
        extracted = supp_b_page.extracted_values
        
        # Use flexible field extraction like CSV system for Supplement B documents
        doc_title = extracted.get('reverification_1_document_title')
        doc_number = extracted.get('reverification_1_document_number')
        doc_expiry = extracted.get('reverification_1_expiration_date')
        doc_authority = extracted.get('reverification_1_issuing_authority')
        
        if doc_title and doc_title != 'N/A':  # Only require title, like CSV system
            doc_type = self._determine_document_type(doc_title)
            reference = DocumentReference(
                document_type=doc_type,
                document_number=doc_number or 'N/A',  # Allow missing numbers
                expiration_date=doc_expiry,
                issuing_authority=doc_authority,
                list_category='A',  # Default to A for Supplement B
                source_section='supplement_b'
            )
            references.append(reference)
        
        return references
    
    def _determine_document_type(self, document_title: str) -> DocumentType:
        """Determine document type from title/description"""
        
        if not document_title:
            return DocumentType.OTHER
        
        title_lower = document_title.lower()
        
        if 'passport' in title_lower:
            return DocumentType.PASSPORT
        elif 'i-94' in title_lower or 'i94' in title_lower:
            return DocumentType.I94
        elif 'ds-2019' in title_lower or 'ds2019' in title_lower:
            return DocumentType.DS2019
        elif 'ead' in title_lower or 'employment authorization' in title_lower:
            return DocumentType.EAD
        elif 'driver' in title_lower or 'license' in title_lower:
            return DocumentType.DRIVERS_LICENSE
        elif 'social security' in title_lower or 'ssn' in title_lower:
            return DocumentType.SSN_CARD
        else:
            return DocumentType.OTHER
    
    def _parse_supporting_documents(self, supporting_doc_pages: List[I9Page]) -> List[SupportingDocument]:
        """Parse supporting document pages into structured format"""
        
        supporting_docs = []
        
        for page in supporting_doc_pages:
            extracted = page.extracted_values
            
            # Determine document type from page title and content
            doc_type = self._determine_document_type(page.page_title)
            
            # Extract document number (varies by document type)
            doc_number = self._extract_document_number(doc_type, extracted)
            
            # Extract expiration date
            expiry_date = self._extract_expiration_date(doc_type, extracted)
            
            # Extract issuing authority
            authority = self._extract_issuing_authority(doc_type, extracted)
            
            supporting_doc = SupportingDocument(
                page_number=page.page_number,
                document_type=doc_type,
                document_number=doc_number,
                expiration_date=expiry_date,
                issuing_authority=authority,
                extracted_values=extracted,
                confidence_score=page.confidence_score
            )
            
            supporting_docs.append(supporting_doc)
        
        return supporting_docs
    
    def _extract_document_number(self, doc_type: DocumentType, extracted: Dict[str, Any]) -> str:
        """Extract document number based on document type"""
        
        if doc_type == DocumentType.PASSPORT:
            return extracted.get('passport_number', '')
        elif doc_type == DocumentType.I94:
            return (extracted.get('admission_record_number') or 
                   extracted.get('admission_number') or 
                   extracted.get('i94_number') or '')
        elif doc_type == DocumentType.DS2019:
            return extracted.get('sevis_id', '') or extracted.get('program_number', '')
        else:
            # Try common field names
            return (extracted.get('document_number') or 
                   extracted.get('number') or 
                   extracted.get('id_number', ''))
    
    def _extract_expiration_date(self, doc_type: DocumentType, extracted: Dict[str, Any]) -> Optional[str]:
        """Extract expiration date based on document type"""
        
        if doc_type == DocumentType.PASSPORT:
            return extracted.get('date_of_expiry')
        elif doc_type == DocumentType.DS2019:
            return extracted.get('form_covers_period_to')
        else:
            return (extracted.get('expiration_date') or 
                   extracted.get('expires') or 
                   extracted.get('valid_until'))
    
    def _extract_issuing_authority(self, doc_type: DocumentType, extracted: Dict[str, Any]) -> Optional[str]:
        """Extract issuing authority based on document type"""
        
        if doc_type == DocumentType.PASSPORT:
            return extracted.get('issuing_country_code') or extracted.get('issuing_authority')
        elif doc_type == DocumentType.DS2019:
            return extracted.get('program_sponsor')
        else:
            return extracted.get('issuing_authority')
    
    def _match_references_to_documents(self, references: List[DocumentReference], 
                                     supporting_docs: List[SupportingDocument]) -> List[DocumentMatch]:
        """Match document references to supporting documents"""
        
        matches = []
        
        for reference in references:
            best_match = None
            best_confidence = 0.0
            match_reasons = []
            validation_errors = []
            
            # Try to find matching supporting document
            for supporting_doc in supporting_docs:
                confidence, reasons = self._calculate_match_confidence(reference, supporting_doc)
                
                if confidence > best_confidence:
                    best_confidence = confidence
                    best_match = supporting_doc
                    match_reasons = reasons
            
            # Validate the match
            if best_match:
                validation_errors = self._validate_document_match(reference, best_match)
            else:
                validation_errors.append(f"No supporting document found for {reference.document_type.value} {reference.document_number}")
            
            match = DocumentMatch(
                reference=reference,
                supporting_document=best_match,
                match_confidence=best_confidence,
                match_reasons=match_reasons,
                validation_errors=validation_errors
            )
            
            matches.append(match)
        
        return matches
    
    def _calculate_match_confidence(self, reference: DocumentReference, 
                                  supporting_doc: SupportingDocument) -> Tuple[float, List[str]]:
        """Calculate confidence score for matching a reference to a supporting document"""
        
        confidence = 0.0
        reasons = []
        
        # Document type match (increased weight to make type-only matches viable)
        if reference.document_type == supporting_doc.document_type:
            confidence += 0.6  # Increased from 0.4 to make type-only matches more viable
            reasons.append("Document type matches")
        
        # Document number match
        if reference.document_number and supporting_doc.document_number:
            if reference.document_number == supporting_doc.document_number:
                confidence += 0.5
                reasons.append("Document number exact match")
            elif self._fuzzy_match_numbers(reference.document_number, supporting_doc.document_number):
                confidence += 0.3
                reasons.append("Document number fuzzy match")
        
        # Expiration date match
        if reference.expiration_date and supporting_doc.expiration_date:
            if self._dates_match(reference.expiration_date, supporting_doc.expiration_date):
                confidence += 0.1
                reasons.append("Expiration date matches")
        
        return confidence, reasons
    
    def _fuzzy_match_numbers(self, ref_number: str, doc_number: str) -> bool:
        """Perform fuzzy matching on document numbers"""
        
        # Remove spaces and special characters
        ref_clean = re.sub(r'[^A-Za-z0-9]', '', ref_number.upper())
        doc_clean = re.sub(r'[^A-Za-z0-9]', '', doc_number.upper())
        
        # Check if one contains the other
        if ref_clean in doc_clean or doc_clean in ref_clean:
            return True
        
        # For similar length numbers, check if they're mostly the same (allow 1-2 character differences)
        if len(ref_clean) >= 8 and len(doc_clean) >= 8:
            # Count matching characters in the same positions
            min_len = min(len(ref_clean), len(doc_clean))
            matches = sum(1 for i in range(min_len) if ref_clean[i] == doc_clean[i])
            
            # Consider it a match if 80% or more characters match
            if matches / min_len >= 0.8:
                return True
        
        return False
    
    def _dates_match(self, date1: str, date2: str) -> bool:
        """Check if two date strings represent the same date"""
        
        try:
            # Try different date formats
            formats = ['%m/%d/%Y', '%m-%d-%Y', '%Y-%m-%d', '%d/%m/%Y']
            
            parsed_date1 = None
            parsed_date2 = None
            
            for fmt in formats:
                try:
                    if not parsed_date1:
                        parsed_date1 = datetime.strptime(date1, fmt)
                except ValueError:
                    continue
                
                try:
                    if not parsed_date2:
                        parsed_date2 = datetime.strptime(date2, fmt)
                except ValueError:
                    continue
            
            if parsed_date1 and parsed_date2:
                return parsed_date1.date() == parsed_date2.date()
            
        except Exception:
            pass
        
        return False
    
    def _validate_document_match(self, reference: DocumentReference, 
                               supporting_doc: SupportingDocument) -> List[str]:
        """Validate a document match and return any errors"""
        
        errors = []
        
        # Document expiration validation removed - system should only extract dates, not validate expiration
        # The business logic should not determine if documents are "expired" relative to current date
        
        # Check document number consistency
        if (reference.document_number and supporting_doc.document_number and 
            reference.document_number != supporting_doc.document_number):
            if not self._fuzzy_match_numbers(reference.document_number, supporting_doc.document_number):
                errors.append(f"Document number mismatch: {reference.document_number} vs {supporting_doc.document_number}")
        
        return errors
