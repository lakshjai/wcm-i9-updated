#!/usr/bin/env python3
"""
Scenario Processor for I-9 Business Rules

This module implements the 5 specific business rule scenarios for I-9 processing
as defined in the requirements.
"""

from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime
import re

from ..utils.logging_config import logger
from ..core.models import (
    I9FormData, DocumentInfo, FormType, CitizenshipStatus,
    ScenarioResult, ValidationResult, ProcessingStatus, PDFAnalysis
)
from .rule_engine import Rule, RuleContext, RuleResult, RuleStatus, RuleSeverity


class ScenarioProcessor:
    """Processes I-9 documents according to specific business rule scenarios"""
    
    def __init__(self):
        self.logger = logger
    
    def process_document(self, pdf_analysis: PDFAnalysis) -> List[ScenarioResult]:
        """
        Process a PDF document through all applicable scenarios
        
        Args:
            pdf_analysis: Complete PDF analysis with forms and documents
            
        Returns:
            List of scenario results
        """
        results = []
        
        if not pdf_analysis.form_selection_result:
            return [self._create_no_forms_result()]
        
        forms = pdf_analysis.form_selection_result.all_detected_forms
        selected_form = pdf_analysis.form_selection_result.selected_form
        
        # Determine which scenarios apply
        applicable_scenarios = self._determine_applicable_scenarios(forms, pdf_analysis)
        
        for scenario_id in applicable_scenarios:
            if scenario_id == "scenario_1":
                result = self._process_scenario_1(selected_form, pdf_analysis)
            elif scenario_id == "scenario_2":
                result = self._process_scenario_2(forms, pdf_analysis)
            elif scenario_id == "scenario_3":
                result = self._process_scenario_3(forms, pdf_analysis)
            elif scenario_id == "scenario_4":
                result = self._process_scenario_4(forms, pdf_analysis)
            elif scenario_id == "scenario_5":
                result = self._process_scenario_5(forms, pdf_analysis)
            else:
                continue
            
            results.append(result)
        
        return results if results else [self._create_no_applicable_scenarios_result()]
    
    def _determine_applicable_scenarios(self, forms: List[I9FormData], pdf_analysis: PDFAnalysis) -> List[str]:
        """Determine which scenarios apply to the document"""
        scenarios = []
        
        if not forms:
            return scenarios
        
        # Count form types
        standard_i9_forms = [f for f in forms if f.form_type == FormType.STANDARD_I9]
        supplement_b_forms = [f for f in forms if f.form_type == FormType.SUPPLEMENT_B]
        section_3_forms = [f for f in forms if f.form_type == FormType.SECTION_3]
        
        # Scenario 1: Single I-9 form
        if len(forms) == 1 and len(standard_i9_forms) == 1:
            scenarios.append("scenario_1")
        
        # Scenario 2: Multiple I-9 Section 1 forms
        elif len(standard_i9_forms) > 1:
            scenarios.append("scenario_2")
        
        # Scenario 3: I-9 with blank Supplement B
        elif len(standard_i9_forms) >= 1 and len(supplement_b_forms) >= 1:
            # Check if Supplement B is blank (this would need to be determined from AI analysis)
            scenarios.append("scenario_3")
        
        # Scenario 4: Filled Supplement B form
        elif len(supplement_b_forms) >= 1:
            scenarios.append("scenario_4")
        
        # Scenario 5: Multiple Section 3 forms
        elif len(section_3_forms) >= 1:
            scenarios.append("scenario_5")
        
        return scenarios
    
    def _process_scenario_1(self, form: I9FormData, pdf_analysis: PDFAnalysis) -> ScenarioResult:
        """
        Scenario 1: Single I-9 form processing
        - Get all details from Section 1
        - Get supporting documents from Section 2
        - Check alien expiration date matches with supporting documents
        - Check if supporting documents are attached in PDF
        """
        scenario_result = ScenarioResult(
            scenario_id="scenario_1",
            scenario_name="Single I-9 Form Processing",
            status=ProcessingStatus.COMPLETE_SUCCESS,
            primary_form=form
        )
        
        # Extract basic information
        validation_results = []
        
        # Validate basic employee information
        basic_info_validation = self._validate_basic_employee_info(form)
        validation_results.append(basic_info_validation)
        
        # Get supporting documents from Section 2
        supporting_docs = form.section_2_documents
        scenario_result.supporting_documents = supporting_docs
        
        # Check alien expiration date matching (if non-citizen)
        if form.is_non_citizen():
            date_match_validation = self._validate_alien_expiration_date_match(form, supporting_docs)
            validation_results.append(date_match_validation)
            
            alien_expiration = form.get_alien_expiration_date()
            if alien_expiration:
                scenario_result.date_matches["alien_expiration"] = self._check_date_matches_documents(
                    alien_expiration, supporting_docs
                )
        
        # Check document attachments
        attachment_validation = self._validate_document_attachments(supporting_docs, pdf_analysis)
        validation_results.append(attachment_validation)
        
        # Update attachment status
        for doc in supporting_docs:
            scenario_result.attachment_status[doc.document_type] = doc.is_attached
        
        scenario_result.validation_results = validation_results
        
        # Determine overall status
        if any(not v.is_valid and v.severity == "critical" for v in validation_results):
            scenario_result.status = ProcessingStatus.ERROR
        elif any(not v.is_valid for v in validation_results):
            scenario_result.status = ProcessingStatus.PARTIAL_SUCCESS
        
        return scenario_result
    
    def _process_scenario_2(self, forms: List[I9FormData], pdf_analysis: PDFAnalysis) -> ScenarioResult:
        """
        Scenario 2: Multiple I-9 forms - select latest by employee signature date
        """
        # Filter to standard I-9 forms only
        standard_forms = [f for f in forms if f.form_type == FormType.STANDARD_I9]
        
        if not standard_forms:
            return ScenarioResult(
                scenario_id="scenario_2",
                scenario_name="Multiple I-9 Forms - Latest Selection",
                status=ProcessingStatus.NO_I9_FOUND,
                primary_form=None,
                notes="No standard I-9 forms found"
            )
        
        # Select the latest form by signature date
        latest_form = self._select_latest_form_by_signature_date(standard_forms)
        
        # Process similar to Scenario 1
        return self._process_scenario_1(latest_form, pdf_analysis)
    
    def _process_scenario_3(self, forms: List[I9FormData], pdf_analysis: PDFAnalysis) -> ScenarioResult:
        """
        Scenario 3: I-9 with blank Supplement B - use Section 1 and 2 details
        """
        
        # Find the standard I-9 form
        standard_forms = [f for f in forms if f.form_type == FormType.STANDARD_I9]
        
        if not standard_forms:
            return ScenarioResult(
                scenario_id="scenario_3",
                scenario_name="I-9 with Blank Supplement B",
                status=ProcessingStatus.NO_I9_FOUND,
                primary_form=None,
                notes="No standard I-9 forms found"
            )
        
        # Use the latest standard form (similar to Scenario 2 logic)
        primary_form = self._select_latest_form_by_signature_date(standard_forms)
        
        # Process like Scenario 2
        return self._process_scenario_1(primary_form, pdf_analysis)
    
    def _process_scenario_4(self, forms: List[I9FormData], pdf_analysis: PDFAnalysis) -> ScenarioResult:
        """
        Scenario 4: Filled Supplement B form processing
        """
        
        # Find Supplement B forms
        supplement_b_forms = [f for f in forms if f.form_type == FormType.SUPPLEMENT_B]
        
        if not supplement_b_forms:
            return ScenarioResult(
                scenario_id="scenario_4",
                scenario_name="Supplement B Form Processing",
                status=ProcessingStatus.ERROR,
                primary_form=None,
                notes="No Supplement B forms found"
            )
        
        # Select latest Supplement B form
        latest_supplement_b = self._select_latest_form_by_signature_date(supplement_b_forms)
        
        scenario_result = ScenarioResult(
            scenario_id="scenario_4",
            scenario_name="Supplement B Form Processing",
            status=ProcessingStatus.COMPLETE_SUCCESS,
            primary_form=latest_supplement_b
        )
        scenario_result.primary_form = latest_supplement_b
        
        # Get supporting documents from Supplement B
        supporting_docs = latest_supplement_b.supplement_b_documents
        scenario_result.supporting_documents = supporting_docs
        
        validation_results = []
        
        # Validate basic info (should come from associated Section 1)
        basic_info_validation = self._validate_basic_employee_info(latest_supplement_b)
        validation_results.append(basic_info_validation)
        
        # Check alien expiration date with Supplement B documents
        if latest_supplement_b.is_non_citizen():
            date_match_validation = self._validate_alien_expiration_date_match(latest_supplement_b, supporting_docs)
            validation_results.append(date_match_validation)
        
        # Check Supplement B document attachments
        attachment_validation = self._validate_document_attachments(supporting_docs, pdf_analysis)
        validation_results.append(attachment_validation)
        
        scenario_result.validation_results = validation_results
        
        # Determine status
        if any(not v.is_valid and v.severity == "critical" for v in validation_results):
            scenario_result.status = ProcessingStatus.ERROR
        elif any(not v.is_valid for v in validation_results):
            scenario_result.status = ProcessingStatus.PARTIAL_SUCCESS
        
        return scenario_result
    
    def _process_scenario_5(self, forms: List[I9FormData], pdf_analysis: PDFAnalysis) -> ScenarioResult:
        """
        Scenario 5: Multiple Section 3 forms with Section 1 dependency
        """
        
        # Find Section 3 forms
        section_3_forms = [f for f in forms if f.form_type == FormType.SECTION_3]
        
        if not section_3_forms:
            return ScenarioResult(
                scenario_id="scenario_5",
                scenario_name="Section 3 Forms with Section 1 Dependency",
                status=ProcessingStatus.ERROR,
                primary_form=None,
                notes="No Section 3 forms found"
            )
        
        # Select latest Section 3 form by signature date
        latest_section_3 = self._select_latest_form_by_signature_date(section_3_forms)
        
        scenario_result = ScenarioResult(
            scenario_id="scenario_5",
            scenario_name="Section 3 Forms with Section 1 Dependency",
            status=ProcessingStatus.COMPLETE_SUCCESS,
            primary_form=latest_section_3
        )
        
        validation_results = []
        
        # Check if Section 1 exists before Section 3
        section_1_validation = self._validate_section_1_before_section_3(
            latest_section_3, forms, pdf_analysis
        )
        validation_results.append(section_1_validation)
        
        # If Section 1 found, get primary details from it
        if section_1_validation.is_valid:
            section_1_form = section_1_validation.details.get('section_1_form')
            if section_1_form:
                # Use Section 1 for basic info, Section 3 for documents
                basic_info_validation = self._validate_basic_employee_info(section_1_form)
                validation_results.append(basic_info_validation)
                
                # Check alien date with Section 3 documents
                section_3_docs = latest_section_3.section_3_documents
                scenario_result.supporting_documents = section_3_docs
                
                if section_1_form.is_non_citizen():
                    date_match_validation = self._validate_alien_expiration_date_match(
                        section_1_form, section_3_docs
                    )
                    validation_results.append(date_match_validation)
                
                # Check Section 3 document attachments
                attachment_validation = self._validate_document_attachments(section_3_docs, pdf_analysis)
                validation_results.append(attachment_validation)
        else:
            # Section 1 not found - log this as required
            scenario_result.status = ProcessingStatus.SECTION_1_NOT_FOUND
            scenario_result.notes = "Section 1 not found before latest Section 3 form"
        
        scenario_result.validation_results = validation_results
        
        return scenario_result
    
    # Helper methods for validation
    
    def _validate_basic_employee_info(self, form: I9FormData) -> ValidationResult:
        """Validate basic employee information completeness"""
        missing_fields = []
        
        if not form.first_name:
            missing_fields.append("first_name")
        if not form.last_name:
            missing_fields.append("last_name")
        if not form.date_of_birth:
            missing_fields.append("date_of_birth")
        if form.citizenship_status == CitizenshipStatus.UNKNOWN:
            missing_fields.append("citizenship_status")
        
        is_valid = len(missing_fields) == 0
        severity = "critical" if missing_fields else "info"
        
        message = "Basic employee information complete" if is_valid else f"Missing fields: {', '.join(missing_fields)}"
        
        return ValidationResult(
            is_valid=is_valid,
            validation_type="basic_employee_info",
            message=message,
            details={"missing_fields": missing_fields},
            severity=severity
        )
    
    def _validate_alien_expiration_date_match(self, form: I9FormData, documents: List[DocumentInfo]) -> ValidationResult:
        """Validate that alien expiration date matches supporting document expiration dates"""
        
        alien_expiration = form.get_alien_expiration_date()
        
        if not alien_expiration or alien_expiration == "Not visible":
            return ValidationResult(
                is_valid=False,
                validation_type="alien_expiration_match",
                message="Alien work authorization expiration date not found",
                severity="high"
            )
        
        # Check if any document expiration matches
        matching_docs = []
        for doc in documents:
            if doc.expiration_date and doc.expiration_date != "Not visible":
                if self._dates_match(alien_expiration, doc.expiration_date):
                    matching_docs.append(doc.document_type)
        
        is_valid = len(matching_docs) > 0
        message = f"Alien expiration date matches {len(matching_docs)} documents: {', '.join(matching_docs)}" if is_valid else "No matching document expiration dates found"
        
        return ValidationResult(
            is_valid=is_valid,
            validation_type="alien_expiration_match",
            message=message,
            details={
                "alien_expiration_date": alien_expiration,
                "matching_documents": matching_docs,
                "total_documents_checked": len(documents)
            },
            severity="medium" if is_valid else "high"
        )
    
    def _validate_document_attachments(self, documents: List[DocumentInfo], pdf_analysis: PDFAnalysis) -> ValidationResult:
        """Validate that listed documents are actually attached to the PDF"""
        
        attached_docs = []
        missing_docs = []
        
        for doc in documents:
            # Check if document type exists in PDF catalog
            if pdf_analysis.has_document_type(doc.document_type):
                doc.is_attached = True
                attached_docs.append(doc.document_type)
            else:
                doc.is_attached = False
                missing_docs.append(doc.document_type)
        
        is_valid = len(missing_docs) == 0
        message = f"All {len(documents)} documents attached" if is_valid else f"{len(missing_docs)} documents missing: {', '.join(missing_docs)}"
        
        return ValidationResult(
            is_valid=is_valid,
            validation_type="document_attachments",
            message=message,
            details={
                "attached_documents": attached_docs,
                "missing_documents": missing_docs,
                "total_documents": len(documents)
            },
            severity="medium" if is_valid else "high"
        )
    
    def _validate_section_1_before_section_3(self, section_3_form: I9FormData, all_forms: List[I9FormData], pdf_analysis: PDFAnalysis) -> ValidationResult:
        """Validate that Section 1 exists before Section 3 form"""
        
        section_3_page = section_3_form.page_number
        
        # Find Section 1 forms that appear before Section 3
        section_1_forms = [f for f in all_forms 
                          if f.form_type == FormType.STANDARD_I9 and f.page_number < section_3_page]
        
        if section_1_forms:
            # Use the closest Section 1 form before Section 3
            closest_section_1 = max(section_1_forms, key=lambda f: f.page_number)
            
            return ValidationResult(
                is_valid=True,
                validation_type="section_1_before_section_3",
                message=f"Section 1 found on page {closest_section_1.page_number} before Section 3 on page {section_3_page}",
                details={"section_1_form": closest_section_1},
                severity="info"
            )
        else:
            return ValidationResult(
                is_valid=False,
                validation_type="section_1_before_section_3",
                message=f"No Section 1 found before Section 3 on page {section_3_page}",
                details={"section_3_page": section_3_page},
                severity="critical"
            )
    
    def _select_latest_form_by_signature_date(self, forms: List[I9FormData]) -> I9FormData:
        """Select the form with the latest employee signature date"""
        
        # Filter forms with signature dates
        forms_with_dates = [f for f in forms if f.employee_signature_date]
        
        if not forms_with_dates:
            # If no signature dates, return the first form
            return forms[0] if forms else I9FormData()
        
        # Sort by signature date (latest first)
        try:
            sorted_forms = sorted(forms_with_dates, 
                                key=lambda f: datetime.strptime(f.employee_signature_date, "%m/%d/%Y"), 
                                reverse=True)
            return sorted_forms[0]
        except ValueError:
            # If date parsing fails, return first form with date
            return forms_with_dates[0]
    
    def _check_date_matches_documents(self, target_date: str, documents: List[DocumentInfo]) -> bool:
        """Check if target date matches any document expiration date"""
        for doc in documents:
            if doc.expiration_date and doc.expiration_date != "Not visible":
                if self._dates_match(target_date, doc.expiration_date):
                    return True
        return False
    
    def _dates_match(self, date1: str, date2: str) -> bool:
        """Check if two date strings match (with some tolerance for formatting)"""
        if not date1 or not date2:
            return False
        
        # Normalize dates by removing common separators and spaces
        norm_date1 = re.sub(r'[/-\s]', '', date1.strip())
        norm_date2 = re.sub(r'[/-\s]', '', date2.strip())
        
        return norm_date1 == norm_date2
    
    def _create_no_forms_result(self) -> ScenarioResult:
        """Create result when no forms are found"""
        return ScenarioResult(
            scenario_id="no_forms",
            scenario_name="No Forms Detected",
            status=ProcessingStatus.NO_I9_FOUND,
            primary_form=None,
            notes="No I-9 forms detected in the document"
        )
    
    def _create_no_applicable_scenarios_result(self) -> ScenarioResult:
        """Create result when no scenarios apply"""
        return ScenarioResult(
            scenario_id="no_scenarios",
            scenario_name="No Applicable Scenarios",
            status=ProcessingStatus.ERROR,
            primary_form=None,
            notes="Document structure does not match any defined processing scenarios"
        )
