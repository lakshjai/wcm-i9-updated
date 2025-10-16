#!/usr/bin/env python3
"""
Form Classification and Selection Logic

This module handles the classification of different I-9 form types and
selection of the most appropriate form when multiple forms are present.
"""

from typing import List, Dict, Optional, Tuple
from datetime import datetime
import re

from ..utils.logging_config import logger
from .models import I9FormData, FormType, FormSelectionResult, CitizenshipStatus
from .set_grouping import I9Set


class FormTypePatterns:
    """Patterns for detecting different I-9 form types"""
    
    SUPPLEMENT_B_INDICATORS = [
        "supplement b",
        "i-9 supplement b",
        "employment eligibility verification supplement b",
        "reverification",
        "renewal of employment authorization",
        "section 3 reverification",
        "i-9 reverification"
    ]
    
    SECTION_3_INDICATORS = [
        "section 3",
        "reverification and rehires",
        "employment authorization expiration",
        "new employment authorization document",
        "updated information",
        "section 3 reverification"
    ]
    
    STANDARD_I9_INDICATORS = [
        "employment eligibility verification",
        "form i-9",
        "section 1",
        "section 2",
        "employee information and attestation",
        "i-9 employment eligibility"
    ]


class I9FormClassifier:
    """Classifies I-9 forms by type and calculates priority scores"""
    
    def __init__(self):
        self.form_type_priorities = {
            FormType.SUPPLEMENT_B: 100,
            FormType.SECTION_3: 80,
            FormType.STANDARD_I9: 60
        }
        
    def classify_form(self, form_data: I9FormData, page_text: str, 
                     ai_analysis: Dict) -> I9FormData:
        """Classify form type and calculate priority score"""
        
        # Detect form type using multiple methods
        form_type = self._detect_form_type(page_text, ai_analysis)
        form_data.form_type = form_type
        form_data.form_confidence = self._calculate_type_confidence(
            form_type, page_text, ai_analysis
        )
        
        # Extract enhanced date information
        self._extract_enhanced_dates(form_data, ai_analysis)
        
        # Calculate priority score
        form_data.form_priority_score = self._calculate_priority_score(form_data)
        
        # Classify as renewal if applicable
        form_data.is_renewal_form = form_type in [FormType.SUPPLEMENT_B, FormType.SECTION_3]
        
        # Set citizenship status based on form data
        self._determine_citizenship_status(form_data)
        
        return form_data
    
    def _detect_form_type(self, page_text: str, ai_analysis: Dict) -> FormType:
        """Detect form type using text patterns and AI analysis"""
        
        text_lower = page_text.lower() if page_text else ""
        
        # Check AI analysis first
        ai_form_type = ai_analysis.get('form_type', '').lower()
        if ai_form_type == 'supplement_b':
            return FormType.SUPPLEMENT_B
        elif ai_form_type == 'section_3':
            return FormType.SECTION_3
        elif ai_form_type == 'standard_i9':
            return FormType.STANDARD_I9
        
        # Text pattern matching
        if any(indicator in text_lower for indicator in FormTypePatterns.SUPPLEMENT_B_INDICATORS):
            return FormType.SUPPLEMENT_B
        elif any(indicator in text_lower for indicator in FormTypePatterns.SECTION_3_INDICATORS):
            return FormType.SECTION_3
        elif any(indicator in text_lower for indicator in FormTypePatterns.STANDARD_I9_INDICATORS):
            return FormType.STANDARD_I9
        
        return FormType.STANDARD_I9  # Default fallback
    
    def _calculate_type_confidence(self, form_type: FormType, page_text: str, ai_analysis: Dict) -> float:
        """Calculate confidence in form type detection"""
        
        confidence = 0.5  # Base confidence
        
        # AI analysis confidence boost
        if ai_analysis.get('form_type') == form_type.value.lower():
            confidence += 0.3
        
        # Text pattern confidence boost
        text_lower = page_text.lower() if page_text else ""
        if form_type == FormType.SUPPLEMENT_B:
            matches = sum(1 for indicator in FormTypePatterns.SUPPLEMENT_B_INDICATORS if indicator in text_lower)
            confidence += min(matches * 0.1, 0.3)
        elif form_type == FormType.SECTION_3:
            matches = sum(1 for indicator in FormTypePatterns.SECTION_3_INDICATORS if indicator in text_lower)
            confidence += min(matches * 0.1, 0.3)
        elif form_type == FormType.STANDARD_I9:
            matches = sum(1 for indicator in FormTypePatterns.STANDARD_I9_INDICATORS if indicator in text_lower)
            confidence += min(matches * 0.1, 0.2)
        
        return min(confidence, 1.0)
    
    def _extract_enhanced_dates(self, form: I9FormData, ai_analysis: Dict):
        """Extract enhanced date information from AI analysis"""
        
        date_extraction = ai_analysis.get('date_extraction', {})
        
        # Map dates from AI analysis
        form.employer_signature_date = date_extraction.get('employer_signature_date', '')
        if not form.authorized_to_work_until:
            form.authorized_to_work_until = date_extraction.get('work_authorization_expiration', '')
        
        # Extract renewal date for renewal forms
        renewal_date = date_extraction.get('renewal_date', '')
        if renewal_date and form.is_renewal_form:
            form.renewal_context['renewal_date'] = renewal_date
    
    def _calculate_priority_score(self, form: I9FormData) -> float:
        """Calculate overall priority score for form selection"""
        
        # Base score from form type
        type_score = self.form_type_priorities.get(form.form_type, 0)
        
        # Date recency bonus (if dates are available)
        date_score = self._calculate_date_score(form)
        
        # Completeness bonus
        completeness_score = self._calculate_completeness_score(form)
        
        # Weighted total
        total_score = (
            type_score * 0.4 +           # 40% form type priority
            date_score * 0.35 +          # 35% date recency
            completeness_score * 0.25    # 25% completeness
        )
        
        return total_score
    
    def _calculate_date_score(self, form: I9FormData) -> float:
        """Calculate date-based scoring"""
        
        score = 0.0
        
        # Employee signature date (highest priority)
        if form.employee_signature_date:
            score += 35
        
        # Work authorization expiration
        if form.authorized_to_work_until:
            score += 25
        
        # Document expiration dates
        all_docs = form.get_all_documents()
        if any(doc.expiration_date and doc.expiration_date != "Not visible" for doc in all_docs):
            score += 15
        
        # Employer signature date
        if form.employer_signature_date:
            score += 10
        
        return score
    
    def _calculate_completeness_score(self, form: I9FormData) -> float:
        """Calculate form completeness score"""
        
        score = 0.0
        
        # Required fields (20 points each)
        if form.first_name:
            score += 20
        if form.last_name:
            score += 20
        if form.date_of_birth:
            score += 20
        if form.residential_status or form.citizenship_status != CitizenshipStatus.UNKNOWN:
            score += 20
        
        # Section 2 documents (15 points)
        if form.section_2_documents or form.section_2_list_a or form.section_2_list_b or form.section_2_list_c:
            score += 15
        
        # Employee signature (5 points)
        if form.employee_signature_present:
            score += 5
        
        return score
    
    def _determine_citizenship_status(self, form: I9FormData) -> None:
        """Determine citizenship status from form data"""
        
        if not form.residential_status:
            return
        
        status_lower = form.residential_status.lower()
        
        if any(term in status_lower for term in ['citizen', 'u.s. citizen', 'us citizen']):
            form.citizenship_status = CitizenshipStatus.US_CITIZEN
        elif any(term in status_lower for term in ['permanent resident', 'lpr', 'green card']):
            form.citizenship_status = CitizenshipStatus.LAWFUL_PERMANENT_RESIDENT
        elif any(term in status_lower for term in ['alien', 'authorized to work', 'work authorized']):
            form.citizenship_status = CitizenshipStatus.ALIEN_AUTHORIZED_TO_WORK
        else:
            form.citizenship_status = CitizenshipStatus.UNKNOWN


class I9FormSelector:
    """Selects the best I-9 form when multiple forms are detected"""
    
    def __init__(self):
        self.classifier = I9FormClassifier()
    
    def select_best_form(self, forms: List[I9FormData]) -> FormSelectionResult:
        """Select the best form from multiple detected forms"""
        
        if not forms:
            return FormSelectionResult(
                selected_form=I9FormData(),
                all_detected_forms=[],
                selection_criteria="no_forms_available",
                confidence_score=0.0,
                selection_notes="No forms provided for selection"
            )
        
        if len(forms) == 1:
            return FormSelectionResult(
                selected_form=forms[0],
                all_detected_forms=forms,
                selection_criteria="single_form",
                confidence_score=forms[0].form_confidence,
                selection_notes="Only one form detected"
            )
        
        # Sort forms by priority score (highest first)
        sorted_forms = sorted(forms, key=lambda f: f.form_priority_score, reverse=True)
        
        selected_form = sorted_forms[0]
        alternative_forms = sorted_forms[1:]
        
        # Determine selection criteria
        criteria = self._determine_selection_criteria(selected_form, forms)
        
        # Calculate confidence based on score difference
        confidence = self._calculate_selection_confidence(selected_form, alternative_forms)
        
        # Generate selection notes
        notes = self._generate_selection_notes(selected_form, alternative_forms, criteria)
        
        return FormSelectionResult(
            selected_form=selected_form,
            all_detected_forms=forms,
            selection_criteria=criteria,
            confidence_score=confidence,
            alternative_forms=alternative_forms,
            selection_notes=notes,
            selection_reasons=self._get_selection_reasons(selected_form)
        )
    
    def _determine_selection_criteria(self, selected_form: I9FormData, all_forms: List[I9FormData]) -> str:
        """Determine the primary criteria used for form selection"""
        
        # Check if selection was based on form type priority
        form_types = [f.form_type for f in all_forms]
        if FormType.SUPPLEMENT_B in form_types and selected_form.form_type == FormType.SUPPLEMENT_B:
            return "supplement_b_priority"
        elif FormType.SECTION_3 in form_types and selected_form.form_type == FormType.SECTION_3:
            return "section_3_priority"
        
        # Check if selection was based on signature date
        if selected_form.employee_signature_date:
            signature_dates = [f.employee_signature_date for f in all_forms if f.employee_signature_date]
            if len(signature_dates) > 1:
                return "latest_signature_date"
        
        # Check if selection was based on completeness
        completeness_scores = [self.classifier._calculate_completeness_score(f) for f in all_forms]
        if max(completeness_scores) > min(completeness_scores) + 20:  # Significant difference
            return "form_completeness"
        
        return "priority_score"
    
    def _calculate_selection_confidence(self, selected_form: I9FormData, alternatives: List[I9FormData]) -> float:
        """Calculate confidence in form selection"""
        
        if not alternatives:
            return selected_form.form_confidence
        
        # Base confidence from form classification
        base_confidence = selected_form.form_confidence
        
        # Confidence boost based on score difference
        selected_score = selected_form.form_priority_score
        best_alternative_score = max(f.form_priority_score for f in alternatives)
        
        score_difference = selected_score - best_alternative_score
        confidence_boost = min(score_difference / 100, 0.3)  # Max 30% boost
        
        return min(base_confidence + confidence_boost, 1.0)
    
    def _generate_selection_notes(self, selected_form: I9FormData, alternatives: List[I9FormData], criteria: str) -> str:
        """Generate human-readable selection notes"""
        
        notes = []
        
        notes.append(f"Selected {selected_form.form_type.value} form from page {selected_form.page_number}")
        notes.append(f"Selection criteria: {criteria}")
        notes.append(f"Priority score: {selected_form.form_priority_score:.1f}")
        
        if alternatives:
            alt_scores = [f.form_priority_score for f in alternatives]
            notes.append(f"Alternative forms: {len(alternatives)} (scores: {[f'{s:.1f}' for s in alt_scores]})")
        
        if selected_form.employee_signature_date:
            notes.append(f"Employee signature date: {selected_form.employee_signature_date}")
        
        return "; ".join(notes)
    
    def _get_selection_reasons(self, selected_form: I9FormData) -> List[str]:
        """Get detailed reasons for form selection"""
        
        reasons = []
        
        if selected_form.form_type == FormType.SUPPLEMENT_B:
            reasons.append("Supplement B forms have highest priority for renewal processing")
        elif selected_form.form_type == FormType.SECTION_3:
            reasons.append("Section 3 forms indicate employment authorization renewal")
        
        if selected_form.employee_signature_date:
            reasons.append(f"Has employee signature date: {selected_form.employee_signature_date}")
        
        if selected_form.form_confidence > 0.8:
            reasons.append("High confidence in form type detection")
        
        completeness = self.classifier._calculate_completeness_score(selected_form)
        if completeness > 80:
            reasons.append("Form has high completeness score")
        
        return reasons
    
    def select_best_i9_set(self, i9_sets: List) -> 'I9Set':
        """
        Select the best I-9 set from multiple sets using priority hierarchy
        
        Args:
            i9_sets: List of I9Set objects
            
        Returns:
            Selected I9Set object
        """
        from .set_grouping import I9Set  # Import here to avoid circular imports
        
        if not i9_sets:
            raise ValueError("No I-9 sets provided for selection")
        
        if len(i9_sets) == 1:
            logger.info(f"Only one I-9 set available: {i9_sets[0].set_id}")
            return i9_sets[0]
        
        logger.info(f"Selecting best I-9 set from {len(i9_sets)} available sets")
        
        # Priority hierarchy (per Business Rules Rubric):
        # 1. Supplement B forms (highest priority) - select latest signature date
        # 2. Section 3 forms (second priority) - select latest employer signature date in Section 3
        # 3. Standard I-9 forms (lowest priority) - select latest signature date
        
        # Priority 1: Supplement B forms
        supplement_b_sets = [s for s in i9_sets if s.supplement_b_pages]
        if supplement_b_sets:
            logger.info(f"Found {len(supplement_b_sets)} sets with Supplement B forms - highest priority")
            # Select the one with latest employer signature date from Supplement B pages
            selected = self._select_latest_supplement_b_set(supplement_b_sets)
            logger.info(f"Selected Supplement B set: {selected.set_id}")
            return selected
        
        # Priority 2: Section 3 forms
        section_3_sets = [s for s in i9_sets if s.section_3_pages]
        if section_3_sets:
            logger.info(f"Found {len(section_3_sets)} sets with Section 3 forms - second priority")
            # Select the one with latest employer signature date in Section 3 (per Business Rules Rubric)
            selected = self._select_latest_section_3_set(section_3_sets)
            logger.info(f"Selected Section 3 set: {selected.set_id}")
            return selected
        
        # Priority 3: Standard I-9 forms
        logger.info("No Supplement B or Section 3 forms found, selecting from standard I-9 forms")
        # Select the one with latest signature date
        selected = max(i9_sets, key=lambda s: s.employee_signature_date or '')
        logger.info(f"Selected standard I-9 set: {selected.set_id}")
        return selected
    
    def _select_latest_section_3_set(self, section_3_sets: List) -> 'I9Set':
        """
        Select Section 3 set with the latest employer signature date in Section 3
        (per Business Rules Rubric: "Latest employer signature in Sec 3")
        
        Args:
            section_3_sets: List of I9Set objects with Section 3 pages
            
        Returns:
            I9Set with the latest employer signature date in Section 3
        """
        def get_latest_employer_signature_date_from_set(i9_set):
            """Get the latest employer signature date from all Section 3 pages in a set"""
            latest_date = datetime.min
            
            for sec3_page in i9_set.section_3_pages:
                # Prioritize reverification_signature_date for true Section 3 pages
                # Based on Wu, Qianyi analysis: Page 19 has reverification_signature_date: 02/15/2023
                employer_sig_str = (
                    sec3_page.extracted_values.get('reverification_signature_date') or
                    sec3_page.extracted_values.get('employer_signature_date') or
                    sec3_page.extracted_values.get('section_3_employer_signature_date') or
                    sec3_page.extracted_values.get('reverification_employer_signature_date') or
                    sec3_page.extracted_values.get('section_3_signature_date') or
                    sec3_page.extracted_values.get('employer_date') or
                    ''
                )
                
                if employer_sig_str:
                    try:
                        # Parse date string to datetime object
                        employer_sig_date = datetime.strptime(employer_sig_str, '%m/%d/%Y')
                        if employer_sig_date > latest_date:
                            latest_date = employer_sig_date
                            logger.debug(f"Found newer Section 3 employer signature date: {employer_sig_str} on page {sec3_page.page_number}")
                    except ValueError:
                        logger.warning(f"Could not parse employer signature date: {employer_sig_str}")
                        continue
            
            return latest_date
        
        # Find the maximum employer signature date
        max_employer_sig_date = max(get_latest_employer_signature_date_from_set(s) for s in section_3_sets)
        
        # Get all sets with the max employer signature date
        sets_with_max_sig_date = [s for s in section_3_sets if get_latest_employer_signature_date_from_set(s) == max_employer_sig_date]
        
        # TIEBREAKER: If multiple sets have the same employer signature date, prefer the one with Section 1 data
        # (per Business Rules Rubric: "Prefer set with Section 1 employee data")
        if len(sets_with_max_sig_date) > 1:
            logger.info(f"Multiple Section 3 sets with same employer signature date - preferring set with Section 1 employee data")
            # Prefer sets that have Section 1 page with actual employee data
            for candidate_set in sets_with_max_sig_date:
                if (candidate_set.section_1_page and 
                    candidate_set.section_1_page.extracted_values and
                    (candidate_set.section_1_page.extracted_values.get('employee_first_name') or 
                     candidate_set.section_1_page.extracted_values.get('employee_last_name'))):
                    selected_set = candidate_set
                    logger.info(f"Selected set with Section 1 employee data: {selected_set.set_id}")
                    break
            else:
                # No set with employee data found, just use the first one
                selected_set = sets_with_max_sig_date[0]
                logger.info(f"No sets with Section 1 data found, selecting first set: {selected_set.set_id}")
        else:
            selected_set = sets_with_max_sig_date[0]
        
        # Log the selection details
        if max_employer_sig_date != datetime.min:
            logger.info(f"Selected Section 3 set with latest employer signature date: {max_employer_sig_date.strftime('%m/%d/%Y')}")
        else:
            logger.warning("No valid employer signature dates found, selecting first Section 3 set")
        
        return selected_set
    
    def _select_latest_supplement_b_set(self, supplement_b_sets: List[I9Set]) -> I9Set:
        """Select Supplement B set with the latest employer signature date from Supplement B pages
        
        Args:
            supplement_b_sets: List of I9Set objects with Supplement B pages
            
        Returns:
            I9Set with the latest employer signature date in Supplement B pages
        """
        def get_latest_employer_signature_date_from_supplement_b_set(i9_set):
            """Get the latest employer signature date from all Supplement B pages in a set"""
            latest_date = datetime.min
            
            for supp_b_page in i9_set.supplement_b_pages:
                # Try different employer signature date field names for Supplement B
                # Similar to Section 3, but adapted for Supplement B field names
                employer_sig_str = (
                    supp_b_page.extracted_values.get('employer_signature_date') or
                    supp_b_page.extracted_values.get('employer_signature_date_1') or
                    supp_b_page.extracted_values.get('employer_signature_date_2') or
                    supp_b_page.extracted_values.get('supplement_b_employer_signature_date') or
                    supp_b_page.extracted_values.get('rehire_employer_signature_date') or
                    supp_b_page.employer_signature_date or
                    ''
                )
                
                if employer_sig_str:
                    try:
                        # Parse date string to datetime object
                        employer_sig_date = datetime.strptime(employer_sig_str, '%m/%d/%Y')
                        if employer_sig_date > latest_date:
                            latest_date = employer_sig_date
                            logger.debug(f"Found newer Supplement B employer signature date: {employer_sig_str} on page {supp_b_page.page_number}")
                    except ValueError:
                        logger.warning(f"Could not parse Supplement B employer signature date: {employer_sig_str}")
                        continue
            
            return latest_date
        
        # Find the maximum employer signature date
        max_employer_sig_date = max(get_latest_employer_signature_date_from_supplement_b_set(s) for s in supplement_b_sets)
        
        # Get all sets with the max employer signature date
        sets_with_max_date = [
            s for s in supplement_b_sets 
            if get_latest_employer_signature_date_from_supplement_b_set(s) == max_employer_sig_date
        ]
        
        logger.info(f"Selected Supplement B set with latest employer signature date: {max_employer_sig_date.strftime('%m/%d/%Y')}")
        
        # If multiple sets have the same max date, return the first one
        return sets_with_max_date[0]
