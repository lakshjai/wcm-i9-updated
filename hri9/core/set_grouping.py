#!/usr/bin/env python3
"""
I-9 Set Grouping Logic

This module implements the logic to group pages into logical I-9 sets based on
the processing pipeline definition:
- I-9 set: A logically related group of pages for one transaction
- Always includes Section 1 and Section 2
- May include Section 3 or Supplement B for reverification/rehire
"""

from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

from hri9.core.models import PDFAnalysis, I9FormData
from hri9.utils.logging_config import logger


class I9SetType(Enum):
    """Types of I-9 sets"""
    NEW_HIRE = "new_hire"
    REVERIFICATION = "reverification"
    REHIRE = "rehire"
    INCOMPLETE = "incomplete"


class I9SectionType(Enum):
    """I-9 form section types"""
    SECTION_1 = "section_1"
    SECTION_2 = "section_2"
    SECTION_3 = "section_3"
    SUPPLEMENT_B = "supplement_b"
    SUPPORTING_DOC = "supporting_doc"


@dataclass
class I9Page:
    """Represents a single I-9 related page"""
    page_number: int
    section_type: I9SectionType
    form_data: Optional[I9FormData]
    employee_signature_date: Optional[str]
    employer_signature_date: Optional[str]
    page_title: str
    confidence_score: float
    extracted_values: Dict[str, Any]


@dataclass
class I9Set:
    """Represents a complete I-9 set (transaction)"""
    set_id: str
    set_type: I9SetType
    employee_signature_date: str
    section_1_page: Optional[I9Page]
    section_2_page: Optional[I9Page]
    section_3_pages: List[I9Page]
    supplement_b_pages: List[I9Page]
    supporting_doc_pages: List[I9Page]
    is_complete: bool
    validation_errors: List[str]
    confidence_score: float


class I9SetGrouper:
    """Groups pages into logical I-9 sets"""
    
    def __init__(self):
        self.logger = logger
    
    def group_pages_into_sets(self, pdf_analysis: PDFAnalysis) -> List[I9Set]:
        """
        Main method to group pages into I-9 sets
        
        Args:
            pdf_analysis: Complete PDF analysis with catalog data
            
        Returns:
            List of I9Set objects representing logical groupings
        """
        
        logger.info("Starting I-9 set grouping process")
        
        # Step 1: Parse all pages and identify I-9 related content
        i9_pages = self._parse_i9_pages(pdf_analysis)
        supporting_docs = self._parse_supporting_documents(pdf_analysis)
        
        # Step 2: Group Section 1 and Section 2 pages by signature dates
        core_sets = self._group_core_sections(i9_pages)
        
        # Step 3: Associate Section 3 and Supplement B pages with core sets
        complete_sets = self._associate_additional_sections(core_sets, i9_pages)
        
        # Step 3.5: Create dedicated Section 3 sets for unassociated Section 3 pages
        complete_sets = self._create_dedicated_section_3_sets(complete_sets, i9_pages)
        
        # Step 4: Associate supporting documents with sets
        final_sets = self._associate_supporting_documents(complete_sets, supporting_docs)
        
        # Step 5: Validate set completeness and identify latest set
        validated_sets = self._validate_and_score_sets(final_sets)
        
        total_pages = len(pdf_analysis.catalog_data.get('catalog_entry', {}).pages) if pdf_analysis.catalog_data.get('catalog_entry') else 0
        logger.info(f"Grouped {len(validated_sets)} I-9 sets from {total_pages} pages")
        
        return validated_sets
    
    def _parse_i9_pages(self, pdf_analysis: PDFAnalysis) -> List[I9Page]:
        """Parse catalog pages and identify I-9 form sections"""
        
        i9_pages = []
        
        # Get catalog entry from the stored catalog data
        catalog_entry = pdf_analysis.catalog_data.get('catalog_entry')
        if not catalog_entry or not hasattr(catalog_entry, 'pages'):
            logger.warning("No catalog entry or pages found in PDF analysis")
            return i9_pages
        
        # Process each page from the catalog entry
        for page_analysis in catalog_entry.pages:
            # Check if this is an I-9 form page using the catalog structure
            if not hasattr(page_analysis, 'page_type'):
                continue
                
            # Look for I-9 forms in the catalog data
            page_type = getattr(page_analysis, 'page_type', '')
            page_subtype = getattr(page_analysis, 'page_subtype', '')
            
            if not (page_type == 'government_form' and page_subtype == 'i9_form'):
                continue
                
            page_number = page_analysis.page_number
            page_title = getattr(page_analysis, 'page_title', 'I-9 Form')
            extracted_values = getattr(page_analysis, 'extracted_values', {})
            confidence_score = getattr(page_analysis, 'confidence_score', 0.0)
            
            # Determine section type from page title and extracted data
            section_type = self._determine_section_type(page_title, extracted_values)
            
            # Extract signature dates
            employee_sig_date = extracted_values.get('employee_signature_date')
            employer_sig_date = extracted_values.get('employer_signature_date')
            
            # Create I9Page object
            i9_page = I9Page(
                page_number=page_number,
                section_type=section_type,
                form_data=None,  # Will be populated later if needed
                employee_signature_date=employee_sig_date,
                employer_signature_date=employer_sig_date,
                page_title=page_title,
                confidence_score=confidence_score,
                extracted_values=extracted_values
            )
            
            i9_pages.append(i9_page)
            
        logger.info(f"Parsed {len(i9_pages)} I-9 form pages")
        return i9_pages
    
    def _parse_supporting_documents(self, pdf_analysis: PDFAnalysis) -> List[I9Page]:
        """Parse supporting documents (passport, I-94, DS-2019, etc.)"""
        
        supporting_docs = []
        
        # Get catalog entry from the stored catalog data
        catalog_entry = pdf_analysis.catalog_data.get('catalog_entry')
        if not catalog_entry or not hasattr(catalog_entry, 'pages'):
            logger.warning("No catalog entry or pages found for supporting documents")
            return supporting_docs
        
        # Process each page from the catalog entry
        for page_analysis in catalog_entry.pages:
            # Check if this is a supporting document using catalog structure
            if not hasattr(page_analysis, 'page_type'):
                continue
                
            page_type = getattr(page_analysis, 'page_type', '')
            page_subtype = getattr(page_analysis, 'page_subtype', '')
            
            # Identify supporting documents by their subtype
            # Supporting documents are government forms but NOT I-9 forms
            if page_type == 'government_form':
                # Include supporting document subtypes but exclude I-9 forms
                supporting_subtypes = ['i94_form', 'i94_record', 'ds_2019_form', 'passport', 'drivers_license', 'other']
                if page_subtype not in supporting_subtypes:
                    continue
            else:
                # Also include non-government forms that are supporting documents
                # Handle compound subtypes like 'social_security_card|drivers_license'
                if page_type not in ['identity_document', 'work_authorization', 'passport', 'drivers_license', 'other_document']:
                    continue
                
            page_number = page_analysis.page_number
            page_title = getattr(page_analysis, 'page_title', 'Supporting Document')
            extracted_values = getattr(page_analysis, 'extracted_values', {})
            confidence_score = getattr(page_analysis, 'confidence_score', 0.0)
            
            # Create supporting document I9Page object
            supporting_doc = I9Page(
                page_number=page_number,
                section_type=I9SectionType.SUPPORTING_DOC,
                form_data=None,
                employee_signature_date=None,
                employer_signature_date=None,
                page_title=page_title,
                confidence_score=confidence_score,
                extracted_values=extracted_values
            )
            
            supporting_docs.append(supporting_doc)
        
        logger.info(f"Parsed {len(supporting_docs)} supporting document pages")
        return supporting_docs
    
    def _determine_section_type(self, page_title: str, extracted_values: Dict = None) -> I9SectionType:
        """Determine I-9 section type from page title and extracted data"""
        
        title_lower = page_title.lower()
        
        # PRIORITY 1: Check for Supplement B first (before Section 3)
        supplement_b_indicators = [
            'supplement b', 'supplementb', 'supplement-b', 'i-9 supplement b',
            'form i-9 supplement b', 'reverification and rehire'
        ]
        
        if any(indicator in title_lower for indicator in supplement_b_indicators):
            logger.debug(f"Detected Supplement B from title: {page_title}")
            return I9SectionType.SUPPLEMENT_B
        
        # PRIORITY 2: Enhanced Section 3 detection (excluding Supplement B terms)
        section_3_indicators = [
            'section 3', 'employment authorization expiration',
            'new employment authorization document', 'updated information',
            'section iii', 'part 3'
        ]
        
        # PRIORITY 2b: Check for Section 3 reverification (but NOT Supplement B)
        if ('reverification' in title_lower or 'rehires' in title_lower) and 'supplement' not in title_lower:
            logger.debug(f"Detected Section 3 reverification (non-Supplement B) from title: {page_title}")
            return I9SectionType.SECTION_3
        
        # Enhanced Section 1 detection
        section_1_indicators = [
            'section 1', 'employee information', 'employee attestation',
            'section i', 'part 1', 'employee data'
        ]
        
        # Enhanced Section 2 detection  
        section_2_indicators = [
            'section 2', 'employer', 'employer review', 'document verification',
            'section ii', 'part 2', 'employer verification'
        ]
        
        # Check title patterns (Supplement B already checked above)
        if any(indicator in title_lower for indicator in section_3_indicators):
            logger.debug(f"Detected Section 3 from title: {page_title}")
            return I9SectionType.SECTION_3
        elif any(indicator in title_lower for indicator in section_1_indicators):
            return I9SectionType.SECTION_1
        elif any(indicator in title_lower for indicator in section_2_indicators):
            return I9SectionType.SECTION_2
        
        # Enhanced detection using extracted field patterns
        if extracted_values:
            # Section 3 specific fields (including reverification fields)
            section_3_fields = [
                'new_name', 'rehire_date', 'new_employment_authorization_document',
                'document_title_list_a_or_list_c', 'document_number_list_a_or_c',
                'expiration_date_if_any_list_a_or_c', 'section_3_employee_signature_date',
                'reverification_date', 'new_i94_number',
                # Reverification fields that indicate Section 3
                'reverification_document_title', 'reverification_document_number', 
                'reverification_expiration_date', 'rehire_last_name', 'rehire_first_name',
                'rehire_middle_initial',
                # Section 3 specific fields with section_3_ prefix
                'section_3_date_of_rehire', 'section_3_document_title', 'section_3_document_number',
                'section_3_expiration_date', 'section_3_new_first_name', 'section_3_new_middle_initial',
                'section_3_employer_name', 'section_3_date_signed'
            ]
            
            # Section 1 specific fields
            section_1_fields = [
                'employee_last_name', 'employee_first_name', 'employee_middle_initial',
                'other_last_names_used', 'employee_address', 'date_of_birth',
                'us_social_security_number', 'employee_email_address',
                'employee_telephone_number', 'citizenship_status'
            ]
            
            # Section 2 specific fields (including _1 suffix variations)
            section_2_fields = [
                'list_a_document_title', 'list_b_document_title', 'list_c_document_title',
                'list_a_document_title_1', 'list_b_document_title_1', 'list_c_document_title_1',
                'employer_signature_date', 'employee_first_day_of_employment',
                'employer_name', 'employer_address'
            ]
            
            # Count field matches
            sec3_matches = sum(1 for field in section_3_fields if field in extracted_values)
            sec1_matches = sum(1 for field in section_1_fields if field in extracted_values)
            sec2_matches = sum(1 for field in section_2_fields if field in extracted_values)
            
            # CRITICAL FIX: Check if page has employee name fields (indicates Section 1)
            # This handles combined Section 1+2 forms correctly
            has_employee_name = bool(extracted_values.get('employee_first_name') or 
                                   extracted_values.get('employee_last_name'))
            
            # Determine section based on field matches
            # Priority: Section 3 > Section 1 (if has employee name) > Section 2 > Section 1 (fallback)
            if sec3_matches > 0:
                logger.debug(f"Detected Section 3 based on {sec3_matches} field matches")
                return I9SectionType.SECTION_3
            elif has_employee_name and sec1_matches > 0:
                # Combined Section 1+2 forms: Prioritize as Section 1 when employee name is present
                logger.debug(f"Detected Section 1 (combined form) based on employee name presence and {sec1_matches} field matches")
                return I9SectionType.SECTION_1
            elif sec2_matches > 0:
                # Check for critical Section 2 document fields (including _1 suffix and List B/C)
                critical_sec2_fields = ['list_a_document_title', 'list_a_document_number', 
                                       'list_a_document_title_1', 'list_a_document_number_1',
                                       'list_b_document_title', 'list_b_document_number',
                                       'list_c_document_title', 'list_c_document_number']
                has_critical_sec2 = any(field in extracted_values and extracted_values[field] and extracted_values[field] != 'N/A' 
                                      for field in critical_sec2_fields)
                if has_critical_sec2:
                    logger.debug(f"Detected Section 2 based on {sec2_matches} field matches with critical document fields")
                    return I9SectionType.SECTION_2
            
            # Fall back to Section 1 if no critical Section 2 fields
            if sec1_matches > 0:
                return I9SectionType.SECTION_1
        
        # Default fallback - check for generic I-9 indicators
        if 'i-9' in title_lower or 'form i9' in title_lower or 'employment eligibility' in title_lower:
            # Default to Section 1 for generic I-9 forms
            return I9SectionType.SECTION_1
        
        # Final fallback
        return I9SectionType.SECTION_1
    
    def _group_core_sections(self, i9_pages: List[I9Page]) -> List[I9Set]:
        """Group Section 1 and Section 2 pages by expiry date matching, then signature dates and proximity"""
        
        # Priority 1: Group by expiry date matching (most important for COMPLETE_SUCCESS)
        expiry_groups = self._group_by_expiry_dates(i9_pages)
        
        # Priority 2: Group remaining pages by signature dates
        remaining_pages = [p for p in i9_pages if not self._page_in_groups(p, expiry_groups)]
        signature_groups = {}
        ungrouped_pages = []
        
        # Enhanced signature grouping: handle both traditional and "both signatures on Section 2" cases
        for page in remaining_pages:
            if page.section_type in [I9SectionType.SECTION_1, I9SectionType.SECTION_2]:
                sig_date = page.employee_signature_date or page.employer_signature_date
                if sig_date:
                    # Check if there's an existing group with a close date (within 7 days)
                    found_close_group = False
                    for existing_date in list(signature_groups.keys()):
                        if self._dates_are_close(sig_date, existing_date, days=7):
                            signature_groups[existing_date].append(page)
                            found_close_group = True
                            break
                    
                    if not found_close_group:
                        if sig_date not in signature_groups:
                            signature_groups[sig_date] = []
                        signature_groups[sig_date].append(page)
                else:
                    ungrouped_pages.append(page)
        
        # Special handling: If Section 2 page has both signatures, group with nearby Section 1 pages
        for sig_date, pages in list(signature_groups.items()):
            section_2_pages = [p for p in pages if p.section_type == I9SectionType.SECTION_2]
            for sec2_page in section_2_pages:
                if (sec2_page.employee_signature_date and sec2_page.employer_signature_date and
                    self._dates_are_close(sec2_page.employee_signature_date, sec2_page.employer_signature_date, days=7)):
                    
                    # Find nearby Section 1 pages from ungrouped pages
                    nearby_section_1 = []
                    for ungrouped_page in ungrouped_pages[:]:  # Copy list to modify during iteration
                        if (ungrouped_page.section_type == I9SectionType.SECTION_1 and
                            abs(ungrouped_page.page_number - sec2_page.page_number) <= 3):
                            nearby_section_1.append(ungrouped_page)
                            ungrouped_pages.remove(ungrouped_page)
                    
                    # Add nearby Section 1 pages to this signature group
                    signature_groups[sig_date].extend(nearby_section_1)
                    logger.info(f"✅ Enhanced grouping: Section 2 page {sec2_page.page_number} with both signatures grouped with {len(nearby_section_1)} nearby Section 1 pages")
        
        # Priority 3: Group ungrouped pages by proximity (consecutive page numbers)
        proximity_groups = self._group_by_proximity(ungrouped_pages)
        
        # Priority 4: Create Section 1 + Section 3 sets for consecutive pages with close expiry dates
        section_1_3_groups = self._group_section_1_and_3_by_proximity(i9_pages)
        
        # Merge all groups with expiry groups having highest priority
        all_groups = expiry_groups + list(signature_groups.values()) + proximity_groups + section_1_3_groups
        
        # Create I9Set objects for each group
        core_sets = []
        for group_idx, pages in enumerate(all_groups):
            
            # Find Section 1, Section 2, and Section 3 pages
            section_1_pages_in_group = [p for p in pages if p.section_type == I9SectionType.SECTION_1]
            
            # If multiple Section 1 pages, prefer the one with the latest expiry date
            if len(section_1_pages_in_group) > 1:
                section_1_page = self._select_best_section_1_page(section_1_pages_in_group)
                logger.info(f"Multiple Section 1 pages found, selected page {section_1_page.page_number if section_1_page else 'None'} with latest expiry date")
            else:
                section_1_page = next((p for p in pages if p.section_type == I9SectionType.SECTION_1), None)
            section_2_page = next((p for p in pages if p.section_type == I9SectionType.SECTION_2), None)
            section_3_pages = [p for p in pages if p.section_type == I9SectionType.SECTION_3]
            
            # Determine set type
            if section_3_pages:
                set_type = I9SetType.REVERIFICATION
            elif section_1_page and section_2_page:
                set_type = I9SetType.NEW_HIRE
            else:
                set_type = I9SetType.INCOMPLETE
            
            # Create set ID based on available information
            if pages and pages[0].employee_signature_date:
                set_id = f"set_{pages[0].employee_signature_date.replace('/', '_')}"
            elif pages and pages[0].employer_signature_date:
                set_id = f"set_{pages[0].employer_signature_date.replace('/', '_')}"
            else:
                # Use page numbers for proximity-based groups
                page_numbers = sorted([p.page_number for p in pages])
                set_id = f"set_pages_{'-'.join(map(str, page_numbers))}"
            
            # Determine signature date for the set
            sig_date = None
            if pages:
                sig_date = pages[0].employee_signature_date or pages[0].employer_signature_date
                if not sig_date and len(pages) > 1:
                    sig_date = pages[1].employee_signature_date or pages[1].employer_signature_date
            
            # Create set
            i9_set = I9Set(
                set_id=set_id,
                set_type=set_type,
                employee_signature_date=sig_date,
                section_1_page=section_1_page,
                section_2_page=section_2_page,
                section_3_pages=section_3_pages,
                supplement_b_pages=[],
                supporting_doc_pages=[],
                is_complete=(section_1_page is not None and (section_2_page is not None or section_3_pages)),
                validation_errors=[],
                confidence_score=0.0
            )
            
            core_sets.append(i9_set)
        
        logger.info(f"Created {len(core_sets)} core I-9 sets")
        return core_sets
    
    def _group_by_signature_dates(self, i9_pages: List[I9Page]) -> List[List[I9Page]]:
        """Group pages by matching signature dates"""
        
        signature_groups = []
        section_1_pages = [p for p in i9_pages if p.section_type == I9SectionType.SECTION_1]
        section_2_3_pages = [p for p in i9_pages if p.section_type in [I9SectionType.SECTION_2, I9SectionType.SECTION_3]]
        
        logger.info(f"Grouping by signature dates: {len(section_1_pages)} Section 1 pages, {len(section_2_3_pages)} Section 2/3 pages")
        
        # Method 1: Traditional matching (Section 1 employee sig + Section 2 employer sig)
        for sec1_page in section_1_pages:
            if not sec1_page.employee_signature_date:
                continue
                
            matching_pages = [sec1_page]
            
            for sec23_page in section_2_3_pages:
                # Check if dates are close (within 30 days)
                if (sec23_page.employer_signature_date and 
                    self._dates_are_close(sec1_page.employee_signature_date, sec23_page.employer_signature_date, days=30)):
                    matching_pages.append(sec23_page)
                    logger.info(f"✅ Found signature date match: Section 1 page {sec1_page.page_number} ({sec1_page.employee_signature_date}) ~ Section 2/3 page {sec23_page.page_number} ({sec23_page.employer_signature_date})")
            
            if len(matching_pages) > 1:
                signature_groups.append(matching_pages)
                logger.info(f"Created signature-based group with pages: {[p.page_number for p in matching_pages]}")
        
        # Method 2: Handle cases where both signatures are on Section 2 page
        # Group Section 1 pages with Section 2 pages that have both signatures
        for sec23_page in section_2_3_pages:
            if (sec23_page.employee_signature_date and sec23_page.employer_signature_date and
                self._dates_are_close(sec23_page.employee_signature_date, sec23_page.employer_signature_date, days=7)):
                
                # Find nearby Section 1 pages (within 3 pages)
                nearby_section_1_pages = []
                for sec1_page in section_1_pages:
                    if abs(sec1_page.page_number - sec23_page.page_number) <= 3:
                        nearby_section_1_pages.append(sec1_page)
                
                if nearby_section_1_pages:
                    # Create group with Section 2 page + nearby Section 1 pages
                    group_pages = [sec23_page] + nearby_section_1_pages
                    
                    # Check if this group already exists in signature_groups
                    group_exists = False
                    for existing_group in signature_groups:
                        if any(p in existing_group for p in group_pages):
                            group_exists = True
                            break
                    
                    if not group_exists:
                        signature_groups.append(group_pages)
                        logger.info(f"✅ Found Section 2 with both signatures: page {sec23_page.page_number} ({sec23_page.employee_signature_date}/{sec23_page.employer_signature_date}) + nearby Section 1 pages {[p.page_number for p in nearby_section_1_pages]}")
        
        logger.info(f"Created {len(signature_groups)} signature-based groups")
        return signature_groups
    
    def _dates_are_close(self, date1: str, date2: str, days: int = 30) -> bool:
        """Check if two dates are within the specified number of days of each other"""
        try:
            from datetime import datetime, timedelta
            
            # Parse dates - try multiple formats
            formats = ['%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y', '%d/%m/%Y']
            
            parsed_date1 = None
            parsed_date2 = None
            
            for fmt in formats:
                try:
                    if not parsed_date1:
                        parsed_date1 = datetime.strptime(date1, fmt)
                    if not parsed_date2:
                        parsed_date2 = datetime.strptime(date2, fmt)
                    if parsed_date1 and parsed_date2:
                        break
                except ValueError:
                    continue
            
            if not parsed_date1 or not parsed_date2:
                logger.debug(f"Could not parse dates: {date1}, {date2}")
                return False
            
            # Check if dates are within the specified range
            diff = abs((parsed_date1 - parsed_date2).days)
            return diff <= days
            
        except Exception as e:
            logger.debug(f"Error comparing dates {date1} and {date2}: {e}")
            return False
    
    def _group_by_expiry_dates(self, i9_pages: List[I9Page]) -> List[List[I9Page]]:
        """Group pages by matching expiry dates between Section 1 and Section 2/3"""
        
        expiry_groups = []
        section_1_pages = [p for p in i9_pages if p.section_type == I9SectionType.SECTION_1]
        section_2_3_pages = [p for p in i9_pages if p.section_type in [I9SectionType.SECTION_2, I9SectionType.SECTION_3]]
        
        logger.info(f"Looking for expiry date matches: {len(section_1_pages)} Section 1 pages, {len(section_2_3_pages)} Section 2/3 pages")
        
        # Debug: Log all Section 2/3 pages and their expiry dates
        for page in section_2_3_pages:
            expiry = self._extract_section_23_expiry(page)
            logger.info(f"Section {page.section_type.value} page {page.page_number} has expiry: {expiry}")
        
        for sec1_page in section_1_pages:
            # Extract Section 1 expiry date
            sec1_expiry = self._extract_section_1_expiry(sec1_page)
            if not sec1_expiry:
                continue
                
            logger.info(f"Section 1 page {sec1_page.page_number} has expiry: {sec1_expiry}")
            
            # Find matching Section 2/3 pages
            matching_pages = [sec1_page]  # Start with the Section 1 page
            
            for sec23_page in section_2_3_pages:
                # Get all possible expiry dates from Section 2/3 page
                sec23_expiry_dates = self._extract_all_section_23_expiry_dates(sec23_page)
                
                # Check if any of the expiry dates match Section 1
                for sec23_expiry in sec23_expiry_dates:
                    if sec23_expiry and sec23_expiry == sec1_expiry:
                        matching_pages.append(sec23_page)
                        logger.info(f"✅ Found expiry match: Section 1 page {sec1_page.page_number} ({sec1_expiry}) = Section 2/3 page {sec23_page.page_number} ({sec23_expiry})")
                        break  # Found a match, no need to check other expiry dates from this page
            
            # If we found matches, create a group
            if len(matching_pages) > 1:
                expiry_groups.append(matching_pages)
                logger.info(f"Created expiry-based group with pages: {[p.page_number for p in matching_pages]}")
        
        logger.info(f"Created {len(expiry_groups)} expiry-based groups")
        return expiry_groups
    
    def _extract_section_1_expiry(self, page: I9Page) -> Optional[str]:
        """Extract expiry date from Section 1 page"""
        if not page.extracted_values:
            return None
            
        return (
            page.extracted_values.get('work_authorization_expiration_date') or  # ← PRIORITIZE this field (has 2025 date)
            page.extracted_values.get('section_1_work_authorization_expiration_date') or
            page.extracted_values.get('section_1_alien_authorized_to_work_until') or
            page.extracted_values.get('alien_authorized_to_work_until') or  # ← Lower priority (has 2024 date)
            page.extracted_values.get('alien_authorized_to_work_until_date')
        )
    
    def _select_best_section_1_page(self, section_1_pages: List[I9Page]) -> Optional[I9Page]:
        """Select the best Section 1 page from multiple candidates, preferring latest expiry date"""
        if not section_1_pages:
            return None
        
        if len(section_1_pages) == 1:
            return section_1_pages[0]
        
        # Find the page with the latest expiry date
        best_page = None
        latest_expiry_date = None
        
        for page in section_1_pages:
            expiry_date_str = self._extract_section_1_expiry(page)
            if not expiry_date_str:
                continue
            
            try:
                # Parse date string to compare
                expiry_date = datetime.strptime(expiry_date_str, '%m/%d/%Y')
                
                if not latest_expiry_date or expiry_date > latest_expiry_date:
                    latest_expiry_date = expiry_date
                    best_page = page
                    logger.info(f"Section 1 page {page.page_number} has later expiry date: {expiry_date_str}")
                    
            except ValueError:
                logger.warning(f"Could not parse expiry date: {expiry_date_str}")
                continue
        
        # If no valid expiry dates found, return the first page
        if not best_page:
            best_page = section_1_pages[0]
            logger.info(f"No valid expiry dates found, selecting first Section 1 page: {best_page.page_number}")
        
        return best_page
    
    def _update_section_1_for_supplement_b(self, i9_set: I9Set, all_i9_pages: List[I9Page]) -> None:
        """Update Section 1 page selection for Supplement B cases to prefer latest expiry date"""
        
        # Find all Section 1 pages in the document
        all_section_1_pages = [p for p in all_i9_pages if p.section_type == I9SectionType.SECTION_1]
        
        if len(all_section_1_pages) <= 1:
            return  # No need to change if only one or no Section 1 pages
        
        # Select the Section 1 page with the latest expiry date
        best_section_1_page = self._select_best_section_1_page(all_section_1_pages)
        
        if best_section_1_page and best_section_1_page != i9_set.section_1_page:
            old_page_num = i9_set.section_1_page.page_number if i9_set.section_1_page else 'None'
            new_page_num = best_section_1_page.page_number
            
            old_expiry = self._extract_section_1_expiry(i9_set.section_1_page) if i9_set.section_1_page else 'None'
            new_expiry = self._extract_section_1_expiry(best_section_1_page)
            
            i9_set.section_1_page = best_section_1_page
            
            logger.info(f"🔄 SUPPLEMENT B FIX: Updated Section 1 page from {old_page_num} ({old_expiry}) to {new_page_num} ({new_expiry}) for latest expiry date")
    
    def _extract_section_23_expiry(self, page: I9Page) -> Optional[str]:
        """Extract expiry date from Section 2 or Section 3 page"""
        if not page.extracted_values:
            return None
        
        # Check all possible expiry date fields that could appear in Section 2 or Section 3 pages
        expiry_date = (
            # Section 3 specific fields
            page.extracted_values.get('reverification_expiration_date') or
            page.extracted_values.get('rehire_expiration_date') or
            page.extracted_values.get('section_3_expiration_date') or
            
            # Section 2 fields that contain expiry dates (List A, B, C documents)
            page.extracted_values.get('list_a_expiration_date') or
            page.extracted_values.get('list_a_expiration_date_1') or
            page.extracted_values.get('list_a_document_1_expiration_date') or
            page.extracted_values.get('list_a_document_2_expiration_date') or
            page.extracted_values.get('list_a_document_3_expiration_date') or
            page.extracted_values.get('list_b_expiration_date') or
            page.extracted_values.get('list_c_expiration_date') or
            
            # Generic document expiry fields
            page.extracted_values.get('document_expiration_date') or
            page.extracted_values.get('expiration_date_if_any_list_a_or_c') or
            
            # Additional variations that might appear in combined Section 2/3 pages
            page.extracted_values.get('employer_signature_date') if page.section_type == I9SectionType.SECTION_3 else None
        )
        
        # Debug logging to track what fields are available
        if page.page_number in [19, 12]:  # Focus on key pages
            available_fields = {k: v for k, v in page.extracted_values.items() if 'expir' in k.lower() or 'date' in k.lower()}
            logger.info(f"Page {page.page_number} ({page.section_type.value}) available date fields: {available_fields}")
            logger.info(f"Page {page.page_number} extracted expiry: {expiry_date}")
        
        return expiry_date
    
    def _extract_all_section_23_expiry_dates(self, page: I9Page) -> List[str]:
        """Extract all possible expiry dates from Section 2 or Section 3 page"""
        if not page.extracted_values:
            return []
        
        expiry_dates = []
        
        # All possible expiry date fields
        expiry_fields = [
            # Section 3 specific fields
            'reverification_expiration_date',
            'rehire_expiration_date', 
            'section_3_expiration_date',
            
            # Section 2 fields (List A, B, C documents)
            'list_a_expiration_date',
            'list_a_expiration_date_1',
            'list_a_document_1_expiration_date',
            'list_a_document_2_expiration_date', 
            'list_a_document_3_expiration_date',
            'list_b_expiration_date',
            'list_c_expiration_date',
            
            # Generic document expiry fields
            'document_expiration_date',
            'expiration_date_if_any_list_a_or_c'
        ]
        
        for field in expiry_fields:
            value = page.extracted_values.get(field)
            if value and value != 'N/A' and value not in expiry_dates:
                expiry_dates.append(value)
        
        return expiry_dates
    
    def _page_in_groups(self, page: I9Page, groups: List[List[I9Page]]) -> bool:
        """Check if a page is already in any of the groups"""
        for group in groups:
            if page in group:
                return True
        return False
    
    def _group_by_proximity(self, pages: List[I9Page]) -> List[List[I9Page]]:
        """Group pages by proximity (consecutive page numbers) with enhanced logic"""
        
        if not pages:
            return []
        
        # Sort pages by page number
        sorted_pages = sorted(pages, key=lambda p: p.page_number)
        
        logger.debug(f"Grouping {len(sorted_pages)} ungrouped pages by proximity")
        
        groups = []
        current_group = [sorted_pages[0]]
        
        for i in range(1, len(sorted_pages)):
            current_page = sorted_pages[i]
            prev_page = sorted_pages[i-1]
            
            # If pages are consecutive or within 2 pages of each other, group them
            page_gap = current_page.page_number - prev_page.page_number
            if page_gap <= 2:
                current_group.append(current_page)
                logger.debug(f"Added page {current_page.page_number} to group (gap: {page_gap})")
            else:
                # Start a new group
                if len(current_group) > 0:
                    groups.append(current_group)
                    logger.debug(f"Created proximity group with pages: {[p.page_number for p in current_group]}")
                current_group = [current_page]
        
        # Add the last group
        if len(current_group) > 0:
            groups.append(current_group)
            logger.debug(f"Created final proximity group with pages: {[p.page_number for p in current_group]}")
        
        # Enhanced filtering: prioritize groups with both Section 1 and Section 2
        valid_groups = []
        for group in groups:
            section_types = [p.section_type for p in group]
            
            # Priority 1: Groups with both Section 1 and Section 2
            if (I9SectionType.SECTION_1 in section_types and 
                I9SectionType.SECTION_2 in section_types):
                valid_groups.append(group)
                logger.info(f"Valid proximity group found: pages {[p.page_number for p in group]} "
                           f"(Section 1 + Section 2)")
            
            # Priority 2: Groups with Section 1 and Section 3 (Section 2 might be on same page)
            elif (I9SectionType.SECTION_1 in section_types and 
                  I9SectionType.SECTION_3 in section_types):
                # Check if any page has both Section 2 and Section 3 content
                has_section_2_content = any(
                    self._page_has_section_2_content(p) for p in group
                )
                if has_section_2_content:
                    valid_groups.append(group)
                    logger.info(f"Valid proximity group found: pages {[p.page_number for p in group]} "
                               f"(Section 1 + Section 3 with Section 2 content)")
        
        logger.info(f"Created {len(valid_groups)} valid proximity-based groups")
        return valid_groups
    
    def _page_has_section_2_content(self, page: I9Page) -> bool:
        """Check if a page contains Section 2 content even if not classified as Section 2"""
        
        if not hasattr(page, 'extracted_values') or not page.extracted_values:
            return False
        
        # Look for Section 2 specific fields
        section_2_indicators = [
            'list_a_document_title',
            'list_a_document_number', 
            'list_a_expiration_date',
            'list_b_document_title',
            'list_c_document_title',
            'employer_signature_date',
            'employee_first_day_of_employment'
        ]
        
        for indicator in section_2_indicators:
            if page.extracted_values.get(indicator):
                return True
        
        return False
    
    def _associate_additional_sections(self, core_sets: List[I9Set], i9_pages: List[I9Page]) -> List[I9Set]:
        """Associate Section 3 and Supplement B pages with core sets"""
        
        # Find Section 3 and Supplement B pages
        section_3_pages = [p for p in i9_pages if p.section_type == I9SectionType.SECTION_3]
        supplement_b_pages = [p for p in i9_pages if p.section_type == I9SectionType.SUPPLEMENT_B]
        
        for i9_set in core_sets:
            # Associate Section 3 pages with enhanced logic (signature date + proximity)
            self._associate_section_3_pages(i9_set, section_3_pages)
            
            # Associate Supplement B pages with enhanced logic
            self._associate_supplement_b_pages(i9_set, supplement_b_pages, i9_pages)
        
        return core_sets
    
    def _create_dedicated_section_3_sets(self, existing_sets: List[I9Set], i9_pages: List[I9Page]) -> List[I9Set]:
        """Create dedicated I-9 sets for Section 3 pages that weren't associated with core sets"""
        
        # Find all Section 3 pages
        section_3_pages = [p for p in i9_pages if p.section_type == I9SectionType.SECTION_3]
        
        # Find Section 3 pages that are not yet associated with any set
        associated_section_3_pages = set()
        for i9_set in existing_sets:
            for sec3_page in i9_set.section_3_pages:
                associated_section_3_pages.add(sec3_page.page_number)
        
        unassociated_section_3_pages = [p for p in section_3_pages if p.page_number not in associated_section_3_pages]
        
        if not unassociated_section_3_pages:
            logger.info("All Section 3 pages are already associated with existing sets")
            return existing_sets
        
        logger.info(f"Creating dedicated sets for {len(unassociated_section_3_pages)} unassociated Section 3 pages")
        
        # Group Section 3 pages by their employer signature dates
        section_3_groups = {}
        for sec3_page in unassociated_section_3_pages:
            # Get Section 3 specific employer signature date
            employer_sig_date = (
                sec3_page.extracted_values.get('reverification_signature_date') or
                sec3_page.extracted_values.get('employer_signature_date') or
                sec3_page.employer_signature_date or
                'unknown'
            )
            
            if employer_sig_date not in section_3_groups:
                section_3_groups[employer_sig_date] = []
            section_3_groups[employer_sig_date].append(sec3_page)
        
        # Create dedicated I-9 sets for each Section 3 group
        new_sets = []
        for employer_sig_date, sec3_pages_group in section_3_groups.items():
            if employer_sig_date == 'unknown':
                continue
                
            # Try to find a nearby Section 1 page for this Section 3 group
            section_1_page = self._find_section_1_for_section_3_group(sec3_pages_group, i9_pages)
            
            # Create new I-9 set
            set_id = f"section_3_{employer_sig_date.replace('/', '_')}"
            new_set = I9Set(
                set_id=set_id,
                set_type=I9SetType.REVERIFICATION,
                employee_signature_date=employer_sig_date,  # Use employer sig date as set identifier
                section_1_page=section_1_page,
                section_2_page=None,
                section_3_pages=sec3_pages_group,
                supplement_b_pages=[],
                supporting_doc_pages=[],
                is_complete=True,  # Section 3 can be complete without Section 2
                validation_errors=[],
                confidence_score=0.8  # Moderate confidence for dedicated Section 3 sets
            )
            
            new_sets.append(new_set)
            logger.info(f"Created dedicated Section 3 set '{set_id}' with {len(sec3_pages_group)} pages "
                       f"(employer signature: {employer_sig_date})")
        
        return existing_sets + new_sets
    
    def _find_section_1_for_section_3_group(self, sec3_pages: List[I9Page], all_i9_pages: List[I9Page]) -> Optional[I9Page]:
        """Find the most appropriate Section 1 page for a Section 3 group"""
        
        section_1_pages = [p for p in all_i9_pages if p.section_type == I9SectionType.SECTION_1]
        
        if not section_1_pages:
            return None
        
        # Find Section 1 page closest to the Section 3 pages
        sec3_page_numbers = [p.page_number for p in sec3_pages]
        min_sec3_page = min(sec3_page_numbers)
        
        # Look for Section 1 pages near the Section 3 pages (within 5 pages)
        nearby_section_1_pages = []
        for sec1_page in section_1_pages:
            distance = abs(sec1_page.page_number - min_sec3_page)
            if distance <= 5:  # Within 5 pages
                nearby_section_1_pages.append((sec1_page, distance))
        
        if nearby_section_1_pages:
            # Return the closest Section 1 page
            nearby_section_1_pages.sort(key=lambda x: x[1])  # Sort by distance
            return nearby_section_1_pages[0][0]
        
        # If no nearby Section 1, return the latest Section 1 page
        return max(section_1_pages, key=lambda p: p.page_number)
    
    def _associate_section_3_pages(self, i9_set: I9Set, section_3_pages: List[I9Page]) -> None:
        """Associate Section 3 pages using signature date matching and proximity fallback"""
        
        associated_pages = []
        
        # Method 1: Match by signature date (enhanced for Section 3 specific fields)
        for sec3_page in section_3_pages:
            # Get Section 3 specific employer signature date (prioritize reverification_signature_date)
            sec3_employer_sig = (
                sec3_page.extracted_values.get('reverification_signature_date') or
                sec3_page.extracted_values.get('employer_signature_date') or
                sec3_page.employer_signature_date
            )
            
            # Try to match with various signature dates in the set
            signature_match = (
                sec3_page.employee_signature_date == i9_set.employee_signature_date or
                sec3_employer_sig == i9_set.employee_signature_date or
                sec3_page.employer_signature_date == i9_set.employee_signature_date or
                # Also try matching Section 3 employer signature with set's employer signature if available
                (hasattr(i9_set, 'employer_signature_date') and sec3_employer_sig == getattr(i9_set, 'employer_signature_date', None))
            )
            
            if signature_match:
                associated_pages.append(sec3_page)
                logger.debug(f"Associated Section 3 page {sec3_page.page_number} by signature date match "
                           f"(sec3_employer_sig: {sec3_employer_sig}, set_employee_sig: {i9_set.employee_signature_date})")
        
        # Method 2: Proximity-based association (if no signature matches)
        if not associated_pages:
            set_page_range = self._get_set_page_range(i9_set)
            if set_page_range:
                for sec3_page in section_3_pages:
                    # Check if Section 3 is within proximity of the set
                    if self._is_page_in_proximity(sec3_page.page_number, set_page_range, max_distance=2):
                        associated_pages.append(sec3_page)
                        logger.info(f"Associated Section 3 page {sec3_page.page_number} by proximity "
                                   f"to set pages {set_page_range}")
        
        # Method 3: Same page association (Section 2 + Section 3 on same page)
        if not associated_pages and i9_set.section_2_page:
            for sec3_page in section_3_pages:
                if sec3_page.page_number == i9_set.section_2_page.page_number:
                    associated_pages.append(sec3_page)
                    logger.info(f"Associated Section 3 page {sec3_page.page_number} "
                               f"(same page as Section 2)")
        
        # Update the set
        if associated_pages:
            i9_set.section_3_pages.extend(associated_pages)
            i9_set.set_type = I9SetType.REVERIFICATION
            logger.info(f"Set {i9_set.set_id} updated to REVERIFICATION type "
                       f"with {len(associated_pages)} Section 3 pages")
    
    def _associate_supplement_b_pages(self, i9_set: I9Set, supplement_b_pages: List[I9Page], all_i9_pages: List[I9Page]) -> None:
        """Associate Supplement B pages using signature date matching and proximity fallback"""
        
        if not supplement_b_pages:
            return
        
        # Filter out blank/empty Supplement B pages
        filled_supplement_b_pages = [page for page in supplement_b_pages if self._is_supplement_b_filled(page)]
        
        if not filled_supplement_b_pages:
            logger.info(f"All {len(supplement_b_pages)} Supplement B pages are blank/empty - ignoring them")
            return
            
        logger.info(f"Attempting to associate {len(filled_supplement_b_pages)} filled Supplement B pages with set {i9_set.set_id}")
        
        associated_pages = []
        
        # Method 1: Match by signature date (exact match)
        for supp_page in filled_supplement_b_pages:
            logger.debug(f"Checking Supplement B page {supp_page.page_number}: "
                        f"employee_sig='{supp_page.employee_signature_date}', "
                        f"employer_sig='{supp_page.employer_signature_date}', "
                        f"set_sig='{i9_set.employee_signature_date}'")
            
            if (supp_page.employee_signature_date == i9_set.employee_signature_date or
                supp_page.employer_signature_date == i9_set.employee_signature_date):
                associated_pages.append(supp_page)
                logger.info(f"✅ Associated Supplement B page {supp_page.page_number} by signature date match")
        
        # Method 2: Proximity-based association (if no signature matches)
        if not associated_pages:
            logger.info("No signature date matches found, trying proximity matching...")
            set_page_range = self._get_set_page_range(i9_set)
            logger.info(f"Set page range: {set_page_range}")
            
            if set_page_range:
                for supp_page in filled_supplement_b_pages:
                    # Check if Supplement B is within proximity of the set
                    if self._is_page_in_proximity(supp_page.page_number, set_page_range, max_distance=2):
                        associated_pages.append(supp_page)
                        logger.info(f"✅ Associated Supplement B page {supp_page.page_number} by proximity "
                                   f"to set pages {set_page_range}")
                    else:
                        logger.debug(f"❌ Supplement B page {supp_page.page_number} not in proximity to {set_page_range}")
        
        # Method 3: Fallback - associate all filled Supplement B pages if no other match
        if not associated_pages:
            logger.warning("No proximity matches found, associating all filled Supplement B pages as fallback")
            associated_pages = filled_supplement_b_pages.copy()
        
        # Update the set
        if associated_pages:
            i9_set.supplement_b_pages.extend(associated_pages)
            i9_set.set_type = I9SetType.REHIRE
            
            # CRITICAL FIX: When Supplement B is found, re-evaluate Section 1 page selection
            # to prefer the one with the latest expiry date
            self._update_section_1_for_supplement_b(i9_set, all_i9_pages)
            
            logger.info(f"✅ Set {i9_set.set_id} updated to REHIRE type "
                       f"with {len(associated_pages)} Supplement B pages")
        else:
            logger.warning(f"❌ Failed to associate any Supplement B pages with set {i9_set.set_id}")
    
    def _is_supplement_b_filled(self, supp_b_page: I9Page) -> bool:
        """Check if a Supplement B page has meaningful data (not blank)"""
        
        if not supp_b_page.extracted_values:
            logger.debug(f"Supplement B page {supp_b_page.page_number} has no extracted values - considering blank")
            return False
        
        extracted_values = supp_b_page.extracted_values
        
        # Check for meaningful data in various field formats
        meaningful_fields = []
        
        # Check reverification_block_X structure (newest format - nested blocks)
        for i in range(1, 4):
            block_name = f'reverification_block_{i}'
            block_data = extracted_values.get(block_name, {})
            if isinstance(block_data, dict):
                for field_name, field_value in block_data.items():
                    if field_value and field_value not in ['N/A', '', None, '[PII_REDACTED]', 'null']:
                        meaningful_fields.append(f'{block_name}_{field_name}')
        
        # Check reverification_blocks structure (array format)
        reverification_blocks = extracted_values.get('reverification_blocks', [])
        if isinstance(reverification_blocks, list):
            for i, block in enumerate(reverification_blocks):
                if isinstance(block, dict):
                    for field_name, field_value in block.items():
                        if field_value and field_value not in ['N/A', '', None, '[PII_REDACTED]', 'null']:
                            meaningful_fields.append(f'reverification_block_{i}_{field_name}')
        
        # Check block field structure: document_title_block1, document_number_block1, etc. (newest format - no underscore)
        for i in range(1, 4):
            doc_title_block = extracted_values.get(f'document_title_block{i}')
            doc_number_block = extracted_values.get(f'document_number_block{i}')
            expiry_date_block = extracted_values.get(f'expiration_date_block{i}')
            rehire_date_block = extracted_values.get(f'rehire_date_block{i}')
            
            if any(field and field not in ['N/A', '', None, '[PII_REDACTED]', 'null'] 
                   for field in [doc_title_block, doc_number_block, expiry_date_block, rehire_date_block]):
                meaningful_fields.append(f'reverification_block{i}')
        
        # Check block field structure with underscore: document_title_block_1, etc. (fallback)
        for i in range(1, 4):
            doc_title_block = extracted_values.get(f'document_title_block_{i}')
            doc_number_block = extracted_values.get(f'document_number_block_{i}')
            expiry_date_block = extracted_values.get(f'expiration_date_block_{i}')
            rehire_date_block = extracted_values.get(f'rehire_date_block_{i}')
            
            if any(field and field not in ['N/A', '', None, '[PII_REDACTED]', 'null'] 
                   for field in [doc_title_block, doc_number_block, expiry_date_block, rehire_date_block]):
                meaningful_fields.append(f'reverification_block_{i}')
        
        # Check flat field structure: document_title_1, document_number_1, etc.
        for i in range(1, 4):
            doc_title = extracted_values.get(f'document_title_{i}')
            doc_number = extracted_values.get(f'document_number_{i}')
            expiry_date = extracted_values.get(f'expiration_date_{i}')
            rehire_date = extracted_values.get(f'date_of_rehire_{i}')
            
            if any(field and field not in ['N/A', '', None, '[PII_REDACTED]', 'null'] 
                   for field in [doc_title, doc_number, expiry_date, rehire_date]):
                meaningful_fields.append(f'reverification_{i}')
        
        # Check nested structure
        for section_name in ['reverification_section_1', 'reverification_section_2', 'reverification_section_3']:
            section_data = extracted_values.get(section_name, {})
            if isinstance(section_data, dict):
                for field_name, field_value in section_data.items():
                    if field_value and field_value not in ['N/A', '', None, '[PII_REDACTED]']:
                        meaningful_fields.append(section_name)
                        break
        
        # Check reverification_X_ field structure (newest format - flat with numbers)
        for i in range(1, 4):
            reverif_fields = [
                f'reverification_{i}_document_title', f'reverification_{i}_document_number', 
                f'reverification_{i}_expiration_date', f'reverification_{i}_date_of_rehire',
                f'reverification_{i}_employer_name', f'reverification_{i}_employer_signature',
                f'reverification_{i}_date_signed'
            ]
            for field in reverif_fields:
                value = extracted_values.get(field)
                if value and value not in ['N/A', '', None, '[PII_REDACTED]', 'null']:
                    meaningful_fields.append(field)
        
        # Check simple field structure: document_title, document_expiration_date, etc.
        simple_fields = [
            'document_title', 'document_number', 'document_expiration_date', 'rehire_date',
            'employer_name', 'employer_signature', 'date_signed'
        ]
        for field in simple_fields:
            value = extracted_values.get(field)
            if value and value not in ['N/A', '', None, '[PII_REDACTED]', 'null']:
                meaningful_fields.append(field)
        
        # Check other common Supplement B fields (multiple formats)
        other_fields = [
            'employee_signature_date', 'employer_signature_date', 
            'todays_date_1', 'today_date_block1', 'today_date_block_1',
            'employer_name_or_authorized_representative_1', 'employer_name_or_authorized_representative_block1',
            'employer_signature_block1', 'employer_signature_block_1',
            'additional_information_1', 'additional_information_block1'
        ]
        for field in other_fields:
            value = extracted_values.get(field)
            if value and value not in ['N/A', '', None, '[PII_REDACTED]', 'null']:
                meaningful_fields.append(field)
        
        is_filled = len(meaningful_fields) > 0
        
        if is_filled:
            logger.info(f"✅ Supplement B page {supp_b_page.page_number} has meaningful data in fields: {meaningful_fields}")
        else:
            logger.error(f"❌ Supplement B page {supp_b_page.page_number} is blank/empty - ignoring")
            logger.error(f"DEBUG: Available extracted_values keys: {list(extracted_values.keys())}")
            logger.error(f"DEBUG: reverification_block_1 data: {extracted_values.get('reverification_block_1', 'NOT_FOUND')}")
            logger.error(f"DEBUG: Total meaningful fields found: {len(meaningful_fields)}")
            logger.error(f"DEBUG: All meaningful fields: {meaningful_fields}")
        
        return is_filled
    
    def _group_section_1_and_3_by_proximity(self, i9_pages: List[I9Page]) -> List[List[I9Page]]:
        """Group Section 1 and Section 3 pages by proximity when they have close expiry dates"""
        
        section_1_pages = [p for p in i9_pages if p.section_type == I9SectionType.SECTION_1]
        section_3_pages = [p for p in i9_pages if p.section_type == I9SectionType.SECTION_3]
        
        if not section_1_pages or not section_3_pages:
            return []
        
        groups = []
        
        for sec1_page in section_1_pages:
            for sec3_page in section_3_pages:
                # Check if pages are consecutive or close (within 2 pages)
                page_distance = abs(sec1_page.page_number - sec3_page.page_number)
                if page_distance <= 2:
                    # Check if expiry dates are close or matching
                    sec1_expiry = self._extract_section_1_expiry(sec1_page)
                    sec3_expiry = self._extract_section_23_expiry(sec3_page)
                    
                    if sec1_expiry and sec3_expiry:
                        # Allow for small date differences (within 7 days)
                        try:
                            from datetime import datetime, timedelta
                            sec1_date = datetime.strptime(sec1_expiry, '%m/%d/%Y')
                            sec3_date = datetime.strptime(sec3_expiry, '%m/%d/%Y')
                            date_diff = abs((sec1_date - sec3_date).days)
                            
                            if date_diff <= 7:  # Within 7 days
                                groups.append([sec1_page, sec3_page])
                                logger.info(f"✅ Created Section 1+3 group: pages {sec1_page.page_number}, {sec3_page.page_number} "
                                           f"(expiry dates: {sec1_expiry}, {sec3_expiry})")
                        except (ValueError, TypeError):
                            # If date parsing fails, still group if pages are consecutive
                            if page_distance == 1:
                                groups.append([sec1_page, sec3_page])
                                logger.info(f"✅ Created Section 1+3 group by proximity: pages {sec1_page.page_number}, {sec3_page.page_number}")
        
        logger.info(f"Created {len(groups)} Section 1+3 groups")
        return groups
    
    def _extract_section_1_expiry(self, page: I9Page) -> Optional[str]:
        """Extract expiry date from Section 1 page"""
        if not page.extracted_values:
            return None
        
        # Check all possible Section 1 expiry date fields
        expiry_date = (
            page.extracted_values.get('section_1_alien_authorized_to_work_until') or
            page.extracted_values.get('work_auth_expiration_date') or
            page.extracted_values.get('alien_authorized_to_work_until_date') or
            page.extracted_values.get('alien_authorized_to_work_until') or
            page.extracted_values.get('work_authorization_expiration_date') or
            page.extracted_values.get('work_until_date') or
            page.extracted_values.get('alien_expiration_date')
        )
        
        return expiry_date
    
    def _get_set_page_range(self, i9_set: I9Set) -> Optional[Tuple[int, int]]:
        """Get the page range (min, max) for an I-9 set"""
        
        page_numbers = []
        if i9_set.section_1_page:
            page_numbers.append(i9_set.section_1_page.page_number)
        if i9_set.section_2_page:
            page_numbers.append(i9_set.section_2_page.page_number)
        
        if page_numbers:
            return (min(page_numbers), max(page_numbers))
        
        return None
    
    def _is_page_in_proximity(self, page_number: int, page_range: Tuple[int, int], max_distance: int = 2) -> bool:
        """Check if a page is within proximity of a page range"""
        
        min_page, max_page = page_range
        
        # Check if page is within max_distance of the range
        return (min_page - max_distance <= page_number <= max_page + max_distance)
    
    def _associate_supporting_documents(self, i9_sets: List[I9Set], supporting_docs: List[I9Page]) -> List[I9Set]:
        """Associate supporting documents with I-9 sets using intelligent matching"""
        
        for i9_set in i9_sets:
            # Enhanced document matching with expiry date validation
            matched_docs = self._match_documents_to_set(i9_set, supporting_docs)
            i9_set.supporting_doc_pages = matched_docs
            
            # Validate work authorization expiry dates
            self._validate_work_authorization_expiry(i9_set)
        
        return i9_sets
    
    def _match_documents_to_set(self, i9_set: I9Set, supporting_docs: List[I9Page]) -> List[I9Page]:
        """Match supporting documents to an I-9 set based on document references and proximity"""
        
        matched_docs = []
        
        # Method 1: Match by document references (I-94, passport numbers, etc.)
        doc_references = self._extract_document_references_from_set(i9_set)
        if doc_references:
            for doc_ref in doc_references:
                for support_doc in supporting_docs:
                    if self._document_matches_reference(support_doc, doc_ref):
                        matched_docs.append(support_doc)
                        logger.info(f"Matched supporting document page {support_doc.page_number} "
                                   f"to reference: {doc_ref}")
        
        # Method 2: Proximity-based matching (if no specific references matched)
        if not matched_docs:
            set_page_range = self._get_set_page_range(i9_set)
            if set_page_range:
                for support_doc in supporting_docs:
                    if self._is_page_in_proximity(support_doc.page_number, set_page_range, max_distance=3):
                        matched_docs.append(support_doc)
                        logger.debug(f"Matched supporting document page {support_doc.page_number} "
                                    f"by proximity to set pages {set_page_range}")
        
        # Method 3: Fallback - associate all supporting docs if no matches found
        if not matched_docs:
            matched_docs = supporting_docs.copy()
            logger.warning(f"No specific document matches found for set {i9_set.set_id}, "
                          f"associating all {len(supporting_docs)} supporting documents")
        
        return matched_docs
    
    def _extract_document_references_from_set(self, i9_set: I9Set) -> List[Dict]:
        """Extract document references from all sections of an I-9 set"""
        
        references = []
        
        # Extract from Section 1
        if i9_set.section_1_page and i9_set.section_1_page.extracted_values:
            sec1_refs = self._extract_references_from_page(i9_set.section_1_page, "section_1")
            references.extend(sec1_refs)
        
        # Extract from Section 2
        if i9_set.section_2_page and i9_set.section_2_page.extracted_values:
            sec2_refs = self._extract_references_from_page(i9_set.section_2_page, "section_2")
            references.extend(sec2_refs)
        
        # Extract from Section 3
        for sec3_page in i9_set.section_3_pages:
            if sec3_page.extracted_values:
                sec3_refs = self._extract_references_from_page(sec3_page, "section_3")
                references.extend(sec3_refs)
        
        # Extract from Supplement B
        for supp_page in i9_set.supplement_b_pages:
            if supp_page.extracted_values:
                supp_refs = self._extract_references_from_page(supp_page, "supplement_b")
                references.extend(supp_refs)
        
        logger.debug(f"Extracted {len(references)} document references from set {i9_set.set_id}")
        return references
    
    def _extract_references_from_page(self, page: I9Page, section_type: str) -> List[Dict]:
        """Extract document references from a specific page"""
        
        references = []
        extracted = page.extracted_values
        
        # I-94 references
        i94_number = (extracted.get('form_i94_admission_number') or 
                     extracted.get('i94_admission_number') or
                     extracted.get('list_a_document_number'))
        if i94_number and i94_number not in ['N/A', '', None]:
            references.append({
                'type': 'i94',
                'number': i94_number,
                'source_page': page.page_number,
                'source_section': section_type
            })
        
        # Passport references
        passport_number = (extracted.get('foreign_passport_number') or
                          extracted.get('passport_number') or
                          extracted.get('list_a_document_number'))
        if passport_number and passport_number not in ['N/A', '', None]:
            country = extracted.get('country_of_issuance', 'Unknown')
            references.append({
                'type': 'passport',
                'number': passport_number,
                'country': country,
                'source_page': page.page_number,
                'source_section': section_type
            })
        
        # Alien Registration Number / USCIS Number
        alien_reg = (extracted.get('alien_registration_number_uscis_number') or
                    extracted.get('uscis_number') or
                    extracted.get('alien_registration_number'))
        if alien_reg and alien_reg not in ['N/A', '', None]:
            references.append({
                'type': 'alien_registration',
                'number': alien_reg,
                'source_page': page.page_number,
                'source_section': section_type
            })
        
        return references
    
    def _document_matches_reference(self, support_doc: I9Page, doc_ref: Dict) -> bool:
        """Check if a supporting document matches a document reference"""
        
        if not hasattr(support_doc, 'extracted_values') or not support_doc.extracted_values:
            return False
        
        doc_data = support_doc.extracted_values
        ref_type = doc_ref.get('type')
        ref_number = doc_ref.get('number', '').strip()
        
        if not ref_number:
            return False
        
        # Match I-94 documents
        if ref_type == 'i94':
            doc_numbers = [
                doc_data.get('i94_number', ''),
                doc_data.get('admission_number', ''),
                doc_data.get('document_number', ''),
                str(doc_data.get('form_number', ''))
            ]
            for doc_num in doc_numbers:
                if doc_num and str(doc_num).strip() == ref_number:
                    return True
        
        # Match passport documents
        elif ref_type == 'passport':
            doc_numbers = [
                doc_data.get('passport_number', ''),
                doc_data.get('document_number', ''),
                doc_data.get('foreign_passport_number', '')
            ]
            for doc_num in doc_numbers:
                if doc_num and str(doc_num).strip() == ref_number:
                    # Also check country if available
                    ref_country = doc_ref.get('country', '').upper()
                    doc_country = doc_data.get('country_of_issuance', '').upper()
                    if not ref_country or not doc_country or ref_country == doc_country:
                        return True
        
        # Match alien registration documents
        elif ref_type == 'alien_registration':
            doc_numbers = [
                doc_data.get('alien_registration_number', ''),
                doc_data.get('uscis_number', ''),
                doc_data.get('registration_number', '')
            ]
            for doc_num in doc_numbers:
                if doc_num and str(doc_num).strip() == ref_number:
                    return True
        
        return False
    
    def _validate_work_authorization_expiry(self, i9_set: I9Set) -> None:
        """Validate work authorization expiry dates across sections and supporting documents"""
        
        # Get work authorization expiry from Section 1 (primary source)
        section_1_expiry = None
        if i9_set.section_1_page and i9_set.section_1_page.extracted_values:
            section_1_expiry = (
                i9_set.section_1_page.extracted_values.get('alien_authorized_to_work_until_date') or
                i9_set.section_1_page.extracted_values.get('work_authorization_expiration_date') or
                i9_set.section_1_page.extracted_values.get('alien_expiration_date')
            )
        
        if not section_1_expiry or section_1_expiry in ['[PII_REDACTED]', '[DATE_REDACTED]', 'N/A', '']:
            logger.warning(f"No work authorization expiry date found in Section 1 for set {i9_set.set_id}")
            return
        
        logger.info(f"Validating work authorization expiry date: {section_1_expiry}")
        
        # Priority 1: Check Supplement B expiry dates
        if i9_set.supplement_b_pages:
            for supp_page in i9_set.supplement_b_pages:
                self._compare_expiry_dates(section_1_expiry, supp_page, "Supplement B")
        
        # Priority 2: Check Section 3 expiry dates
        elif i9_set.section_3_pages:
            for sec3_page in i9_set.section_3_pages:
                self._compare_expiry_dates(section_1_expiry, sec3_page, "Section 3")
        
        # Priority 3: Check Section 2 expiry dates
        elif i9_set.section_2_page:
            self._compare_expiry_dates(section_1_expiry, i9_set.section_2_page, "Section 2")
        
        # Also validate supporting documents
        for support_doc in i9_set.supporting_doc_pages:
            self._compare_expiry_dates(section_1_expiry, support_doc, "Supporting Document")
    
    def _compare_expiry_dates(self, section_1_expiry: str, page: I9Page, page_type: str) -> None:
        """Compare Section 1 expiry date with expiry date from another page"""
        
        if not hasattr(page, 'extracted_values') or not page.extracted_values:
            return
        
        page_expiry_fields = [
            'expiration_date',
            'document_expiration_date',
            'list_a_expiration_date',
            'work_authorization_expiration_date',
            'alien_authorized_to_work_until_date'
        ]
        
        for field in page_expiry_fields:
            page_expiry = page.extracted_values.get(field)
            if page_expiry and page_expiry not in ['[PII_REDACTED]', '[DATE_REDACTED]', 'N/A', '']:
                if page_expiry == section_1_expiry:
                    logger.info(f"✅ Work auth expiry MATCH: Section 1 ({section_1_expiry}) = "
                               f"{page_type} page {page.page_number} ({page_expiry})")
                else:
                    logger.warning(f"❌ Work auth expiry MISMATCH: Section 1 ({section_1_expiry}) ≠ "
                                  f"{page_type} page {page.page_number} ({page_expiry})")
                break
    
    def _validate_and_score_sets(self, i9_sets: List[I9Set]) -> List[I9Set]:
        """Validate set completeness and calculate confidence scores"""
        
        for i9_set in i9_sets:
            validation_errors = []
            
            # Validate core sections
            if not i9_set.section_1_page:
                validation_errors.append("E001: Missing Section 1 (Employee Information)")
            
            if not i9_set.section_2_page:
                validation_errors.append("E002: Missing Section 2 (Employer Verification)")
            
            # Validate reverification requirements
            if i9_set.section_3_pages and not i9_set.section_1_page:
                validation_errors.append("E003: Section 3 found without corresponding Section 1")
            
            # Calculate confidence score
            confidence_scores = []
            if i9_set.section_1_page:
                confidence_scores.append(i9_set.section_1_page.confidence_score)
            if i9_set.section_2_page:
                confidence_scores.append(i9_set.section_2_page.confidence_score)
            
            i9_set.confidence_score = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0.0
            i9_set.validation_errors = validation_errors
            i9_set.is_complete = len(validation_errors) == 0
        
        # Sort by employee signature date (latest first) with robust error handling
        def safe_date_sort_key(i9_set):
            """Safely extract date for sorting with fallback logic"""
            try:
                if i9_set.employee_signature_date and i9_set.employee_signature_date != "[PII_REDACTED]":
                    return datetime.strptime(i9_set.employee_signature_date, '%m/%d/%Y')
                else:
                    # Fallback: use page number as proxy for recency (higher page = more recent)
                    if i9_set.section_1_page:
                        return datetime(2000, 1, 1) + timedelta(days=i9_set.section_1_page.page_number)
                    else:
                        return datetime.min
            except (ValueError, TypeError) as e:
                logger.warning(f"Failed to parse signature date '{i9_set.employee_signature_date}': {e}")
                # Fallback to page number or minimum date
                if i9_set.section_1_page:
                    return datetime(2000, 1, 1) + timedelta(days=i9_set.section_1_page.page_number)
                else:
                    return datetime.min
        
        i9_sets.sort(key=safe_date_sort_key, reverse=True)
        
        # Apply I-9 form priority hierarchy
        i9_sets = self._apply_priority_hierarchy(i9_sets)
        
        return i9_sets
    
    def _apply_priority_hierarchy(self, i9_sets: List[I9Set]) -> List[I9Set]:
        """Apply I-9 form selection priority hierarchy"""
        
        if not i9_sets:
            return i9_sets
        
        logger.info(f"Applying I-9 form priority hierarchy to {len(i9_sets)} sets")
        
        # Step 1: Check for Supplement B forms (highest priority)
        supplement_b_sets = [s for s in i9_sets if s.supplement_b_pages]
        if supplement_b_sets:
            logger.info(f"Found {len(supplement_b_sets)} sets with Supplement B - applying highest priority")
            selected_set = self._select_latest_by_form_type(supplement_b_sets, "supplement_b")
            if selected_set:
                # Enhance Section 1 association for Supplement B
                selected_set = self._enhance_section_1_association(selected_set, "supplement_b")
                return [selected_set] + [s for s in i9_sets if s != selected_set]
        
        # Step 2: Check for Section 3 forms (second priority) 
        section_3_sets = [s for s in i9_sets if s.section_3_pages]
        if section_3_sets:
            logger.info(f"Found {len(section_3_sets)} sets with Section 3 - applying second priority")
            selected_set = self._select_latest_by_form_type(section_3_sets, "section_3")
            if selected_set:
                # Enhance Section 1 association for Section 3
                selected_set = self._enhance_section_1_association(selected_set, "section_3")
                return [selected_set] + [s for s in i9_sets if s != selected_set]
        
        # Step 3: Fall back to Section 1+2 forms (lowest priority)
        basic_sets = [s for s in i9_sets if s.section_1_page and s.section_2_page and 
                     not s.section_3_pages and not s.supplement_b_pages]
        if basic_sets:
            logger.info(f"Found {len(basic_sets)} basic Section 1+2 sets - applying lowest priority")
            selected_set = self._select_latest_by_form_type(basic_sets, "section_1")
            if selected_set:
                return [selected_set] + [s for s in i9_sets if s != selected_set]
        
        # If no clear priority, return original order
        logger.warning("No clear priority hierarchy could be applied")
        return i9_sets
    
    def _select_latest_by_form_type(self, sets: List[I9Set], form_type: str) -> Optional[I9Set]:
        """Select the latest set based on employee signature date from specific form type"""
        
        valid_sets = []
        
        for i9_set in sets:
            signature_date = None
            
            if form_type == "supplement_b" and i9_set.supplement_b_pages:
                # Get employee signature date from latest Supplement B page
                for supp_page in sorted(i9_set.supplement_b_pages, key=lambda x: x.page_number, reverse=True):
                    if supp_page.employee_signature_date and supp_page.employee_signature_date != "[PII_REDACTED]":
                        signature_date = supp_page.employee_signature_date
                        logger.debug(f"Found Supplement B signature date: {signature_date} on page {supp_page.page_number}")
                        break
            
            elif form_type == "section_3" and i9_set.section_3_pages:
                # Get signature date from latest Section 3 page (employee or employer)
                for sec3_page in sorted(i9_set.section_3_pages, key=lambda x: x.page_number, reverse=True):
                    if sec3_page.employee_signature_date and sec3_page.employee_signature_date != "[PII_REDACTED]":
                        signature_date = sec3_page.employee_signature_date
                        logger.debug(f"Found Section 3 employee signature date: {signature_date} on page {sec3_page.page_number}")
                        break
                    elif sec3_page.employer_signature_date and sec3_page.employer_signature_date != "[PII_REDACTED]":
                        signature_date = sec3_page.employer_signature_date
                        logger.debug(f"Found Section 3 employer signature date: {signature_date} on page {sec3_page.page_number}")
                        break
            
            elif form_type == "section_1" and i9_set.section_1_page:
                # Get employee signature date from Section 1 page
                if i9_set.section_1_page.employee_signature_date and i9_set.section_1_page.employee_signature_date != "[PII_REDACTED]":
                    signature_date = i9_set.section_1_page.employee_signature_date
                    logger.debug(f"Found Section 1 signature date: {signature_date} on page {i9_set.section_1_page.page_number}")
            
            if signature_date:
                try:
                    date_obj = datetime.strptime(signature_date, '%m/%d/%Y')
                    valid_sets.append((i9_set, date_obj, signature_date))
                except ValueError as e:
                    logger.warning(f"Failed to parse signature date '{signature_date}': {e}")
                    # Still include but with low priority
                    valid_sets.append((i9_set, datetime.min, signature_date))
            else:
                # No signature date found, use page number as fallback
                if form_type == "supplement_b" and i9_set.supplement_b_pages:
                    page_num = max(p.page_number for p in i9_set.supplement_b_pages)
                elif form_type == "section_3" and i9_set.section_3_pages:
                    page_num = max(p.page_number for p in i9_set.section_3_pages)
                elif form_type == "section_1" and i9_set.section_1_page:
                    page_num = i9_set.section_1_page.page_number
                else:
                    page_num = 0
                
                fallback_date = datetime(2000, 1, 1) + timedelta(days=page_num)
                valid_sets.append((i9_set, fallback_date, f"page_{page_num}"))
                logger.debug(f"Using page number fallback for set {i9_set.set_id}: page {page_num}")
        
        if valid_sets:
            # Sort by date (latest first) and return the latest set
            valid_sets.sort(key=lambda x: x[1], reverse=True)
            selected_set, selected_date, signature_info = valid_sets[0]
            logger.info(f"Selected set '{selected_set.set_id}' with {form_type} signature: {signature_info}")
            return selected_set
        
        return None
    
    def _enhance_section_1_association(self, i9_set: I9Set, form_type: str) -> I9Set:
        """Enhance Section 1 association by searching ±2 pages around Supplement B/Section 3"""
        
        if form_type == "supplement_b" and i9_set.supplement_b_pages:
            # Find Section 1 by searching ±2 pages around latest Supplement B
            latest_supp_b = max(i9_set.supplement_b_pages, key=lambda x: x.page_number)
            target_pages = self._get_search_page_range(latest_supp_b.page_number, 2)
            
            logger.info(f"Searching for Section 1 around Supplement B page {latest_supp_b.page_number}, "
                       f"checking pages: {target_pages}")
            
            # This would require access to all catalog pages - for now, log the intent
            logger.info(f"Should search pages {target_pages} for Section 1 to associate with "
                       f"Supplement B on page {latest_supp_b.page_number}")
            
            # Update the set's employee signature date to use Supplement B signature
            if latest_supp_b.employee_signature_date and latest_supp_b.employee_signature_date != "[PII_REDACTED]":
                i9_set.employee_signature_date = latest_supp_b.employee_signature_date
                logger.info(f"Updated set signature date to Supplement B date: {latest_supp_b.employee_signature_date}")
        
        elif form_type == "section_3" and i9_set.section_3_pages:
            # Find Section 1 by searching ±2 pages around latest Section 3
            latest_sec3 = max(i9_set.section_3_pages, key=lambda x: x.page_number)
            target_pages = self._get_search_page_range(latest_sec3.page_number, 2)
            
            logger.info(f"Searching for Section 1 around Section 3 page {latest_sec3.page_number}, "
                       f"checking pages: {target_pages}")
            
            # This would require access to all catalog pages - for now, log the intent
            logger.info(f"Should search pages {target_pages} for Section 1 to associate with "
                       f"Section 3 on page {latest_sec3.page_number}")
            
            # Update the set's employee signature date to use Section 3 signature
            if latest_sec3.employee_signature_date and latest_sec3.employee_signature_date != "[PII_REDACTED]":
                i9_set.employee_signature_date = latest_sec3.employee_signature_date
                logger.info(f"Updated set signature date to Section 3 date: {latest_sec3.employee_signature_date}")
        
        return i9_set
    
    def _get_search_page_range(self, center_page: int, distance: int) -> List[int]:
        """Get list of page numbers to search within ±distance of center page"""
        
        start_page = max(1, center_page - distance)
        end_page = center_page + distance
        
        return list(range(start_page, end_page + 1))
    
    def get_latest_i9_set(self, i9_sets: List[I9Set]) -> Optional[I9Set]:
        """Get the latest I-9 set based on priority hierarchy (already applied in group_pages_into_sets)"""
        
        if not i9_sets:
            return None
        
        # The sets are already sorted by priority hierarchy in group_pages_into_sets
        # Priority: 1) Supplement B, 2) Section 3, 3) Basic Section 1+2
        # So we should return the first set which has the highest priority
        selected_set = i9_sets[0]
        
        logger.info(f"Selected I-9 set: '{selected_set.set_id}' (type: {selected_set.set_type.value})")
        
        return selected_set
