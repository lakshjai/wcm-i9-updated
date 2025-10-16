#!/usr/bin/env python3
"""
I-9 Specific Business Rules

This module contains business rules specific to I-9 processing,
implementing the core logic migrated from I9Processor.py with enhancements.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime

from ..core.models import I9FormData, DocumentInfo, ValidationResult, FormType, CitizenshipStatus, PDFAnalysis
from ..utils.logging_config import logger
from .rule_engine import Rule, RuleContext, RuleResult, RuleStatus, RuleSeverity
from .scenario_processor import ScenarioProcessor
from ..validation.validators import ValidationFramework
from ..validation.document_validators import DocumentValidator, AttachmentValidator, DateMatchValidator
from ..validation.compliance_validators import ComplianceValidator, DateValidator


class I9BusinessRules:
    """Main class for I-9 business rule processing"""
    
    def __init__(self):
        self.scenario_processor = ScenarioProcessor()
        self.validation_framework = ValidationFramework()
        self.document_validator = DocumentValidator()
        self.attachment_validator = AttachmentValidator()
        self.date_match_validator = DateMatchValidator()
        self.compliance_validator = ComplianceValidator()
        self.date_validator = DateValidator()
        
        # Setup custom rules
        self._setup_i9_specific_rules()
    
    def _setup_i9_specific_rules(self):
        """Setup I-9 specific validation rules"""
        
        # Add custom validators for I-9 specific requirements
        self.validation_framework.add_custom_validator(self.compliance_validator)
        self.validation_framework.add_custom_validator(self.date_validator)
        
        # Add I-9 specific field validators
        self.validation_framework.add_field_validator(
            "citizenship_status",
            lambda x: x != CitizenshipStatus.UNKNOWN,
            "Citizenship status must be specified",
            "high"
        )
        
        # Add cross-field validator for alien work authorization
        self.validation_framework.add_cross_field_validator(
            "alien_work_authorization_completeness",
            self._validate_alien_work_authorization_completeness,
            "high"
        )
    
    def process_document(self, pdf_analysis: PDFAnalysis) -> Dict[str, Any]:
        """
        Main entry point for processing I-9 documents with business rules
        
        Args:
            pdf_analysis: Complete PDF analysis with forms and documents
            
        Returns:
            Dictionary containing processing results and validation outcomes
        """
        
        logger.info("Starting I-9 business rules processing")
        
        # Process through scenarios
        scenario_results = self.scenario_processor.process_document(pdf_analysis)
        
        # Perform comprehensive validation on selected form
        validation_results = []
        if pdf_analysis.form_selection_result and pdf_analysis.form_selection_result.selected_form:
            selected_form = pdf_analysis.form_selection_result.selected_form
            validation_results = self._perform_comprehensive_validation(selected_form, pdf_analysis)
        
        # Generate processing summary
        processing_summary = self._generate_processing_summary(
            scenario_results, validation_results, pdf_analysis
        )
        
        logger.info(f"I-9 processing completed with status: {processing_summary['overall_status']}")
        
        return {
            "scenario_results": scenario_results,
            "validation_results": validation_results,
            "processing_summary": processing_summary,
            "pdf_analysis": pdf_analysis
        }
    
    def _perform_comprehensive_validation(self, form_data: I9FormData, pdf_analysis: PDFAnalysis) -> List[ValidationResult]:
        """Perform comprehensive validation on I-9 form"""
        
        validation_results = []
        
        # Basic form validation
        form_validations = self.validation_framework.validate_form(form_data)
        validation_results.extend(form_validations)
        
        # Document validation
        all_documents = form_data.get_all_documents()
        if all_documents:
            doc_validation = self.document_validator.validate(all_documents)
            validation_results.append(doc_validation)
            
            # Attachment validation
            attachment_validation = self.attachment_validator.validate((all_documents, pdf_analysis))
            validation_results.append(attachment_validation)
            
            # Date matching validation (for non-citizens)
            if form_data.is_non_citizen():
                date_match_validation = self.date_match_validator.validate((form_data, all_documents))
                validation_results.append(date_match_validation)
        
        # Additional document validations
        document_validations = self.validation_framework.validate_documents(all_documents)
        validation_results.extend(document_validations)
        
        return validation_results
    
    def _validate_alien_work_authorization_completeness(self, form_data: I9FormData) -> tuple:
        """Validate completeness of alien work authorization information"""
        
        if form_data.citizenship_status != CitizenshipStatus.ALIEN_AUTHORIZED_TO_WORK:
            return (True, "Not applicable - not an alien authorized to work", {})
        
        issues = []
        
        # Check required fields for alien workers
        if not form_data.authorized_to_work_until:
            issues.append("Missing work authorization expiration date")
        
        # Check if alien work authorization info is provided
        if not form_data.alien_work_authorization_info:
            issues.append("Missing alien work authorization details")
        else:
            auth_info = form_data.alien_work_authorization_info
            if not auth_info.get("document_number"):
                issues.append("Missing work authorization document number")
            if not auth_info.get("category"):
                issues.append("Missing work authorization category")
        
        # Check supporting documents
        primary_docs = form_data.get_primary_documents()
        if not primary_docs:
            issues.append("Missing supporting documents for work authorization")
        else:
            # Check if any document supports work authorization
            work_auth_docs = [doc for doc in primary_docs 
                            if "employment authorization" in doc.document_type.lower() 
                            or "ead" in doc.document_type.lower()]
            if not work_auth_docs and form_data.form_type != FormType.SUPPLEMENT_B:
                issues.append("No work authorization documents found in supporting documents")
        
        is_valid = len(issues) == 0
        message = "Alien work authorization information complete" if is_valid else f"Issues: {'; '.join(issues)}"
        
        return (is_valid, message, {"issues": issues})
    
    def _generate_processing_summary(self, scenario_results: List, validation_results: List, 
                                   pdf_analysis: PDFAnalysis) -> Dict[str, Any]:
        """Generate comprehensive processing summary"""
        
        summary = {
            "overall_status": "COMPLETE_SUCCESS",
            "scenarios_processed": len(scenario_results),
            "validations_performed": len(validation_results),
            "critical_issues": [],
            "warnings": [],
            "recommendations": [],
            "statistics": {}
        }
        
        # Analyze scenario results
        if scenario_results:
            successful_scenarios = [s for s in scenario_results if s.status.value in ["COMPLETE_SUCCESS", "PARTIAL_SUCCESS"]]
            failed_scenarios = [s for s in scenario_results if s.has_critical_issues]
            
            summary["statistics"]["successful_scenarios"] = len(successful_scenarios)
            summary["statistics"]["failed_scenarios"] = len(failed_scenarios)
            
            # Collect issues and recommendations from scenarios
            for scenario in scenario_results:
                if scenario.has_critical_issues:
                    summary["critical_issues"].extend([
                        f"Scenario {scenario.scenario_id}: {v.message}" 
                        for v in scenario.validation_results 
                        if not v.is_valid and v.severity == "critical"
                    ])
                
                summary["recommendations"].extend(scenario.recommendations)
        
        # Analyze validation results
        if validation_results:
            passed_validations = [v for v in validation_results if v.is_valid]
            failed_validations = [v for v in validation_results if not v.is_valid]
            critical_validations = [v for v in failed_validations if v.severity == "critical"]
            
            summary["statistics"]["passed_validations"] = len(passed_validations)
            summary["statistics"]["failed_validations"] = len(failed_validations)
            summary["statistics"]["critical_validations"] = len(critical_validations)
            
            # Collect critical issues and warnings
            for validation in failed_validations:
                if validation.severity == "critical":
                    summary["critical_issues"].append(f"Critical: {validation.message}")
                elif validation.severity == "high":
                    summary["warnings"].append(f"High: {validation.message}")
                
                summary["recommendations"].extend(validation.recommendations)
        
        # Determine overall status
        if summary["critical_issues"]:
            summary["overall_status"] = "ERROR"
        elif summary["warnings"]:
            summary["overall_status"] = "PARTIAL_SUCCESS"
        elif not scenario_results:
            summary["overall_status"] = "NO_I9_FOUND"
        
        # Add form selection information
        if pdf_analysis.form_selection_result:
            selection_result = pdf_analysis.form_selection_result
            summary["form_selection"] = {
                "selected_form_type": selection_result.selected_form.form_type.value,
                "selection_criteria": selection_result.selection_criteria,
                "confidence_score": selection_result.confidence_score,
                "total_forms_detected": len(selection_result.all_detected_forms),
                "alternative_forms": len(selection_result.alternative_forms)
            }
        
        # Remove duplicates from recommendations
        summary["recommendations"] = list(set(summary["recommendations"]))
        
        return summary


class I9ValidationRule(Rule):
    """Base class for I-9 specific validation rules"""
    
    def __init__(self, rule_id: str, name: str, validation_func, **kwargs):
        super().__init__(rule_id, name, **kwargs)
        self.validation_func = validation_func
    
    def execute(self, context: RuleContext) -> RuleResult:
        """Execute I-9 validation rule"""
        
        form_data = context.get("form_data")
        if not form_data:
            return self.create_result(
                RuleStatus.ERROR,
                "No form data provided for validation"
            )
        
        try:
            is_valid, message, details = self.validation_func(form_data)
            
            status = RuleStatus.PASSED if is_valid else RuleStatus.FAILED
            
            return self.create_result(
                status,
                message,
                details
            )
            
        except Exception as e:
            return self.create_result(
                RuleStatus.ERROR,
                f"Rule execution failed: {str(e)}"
            )


class DocumentMatchingRule(Rule):
    """Rule for validating document matching between I-9 and attachments"""
    
    def __init__(self):
        super().__init__(
            "document_matching",
            "Document Matching Validation",
            RuleSeverity.HIGH
        )
    
    def execute(self, context: RuleContext) -> RuleResult:
        """Execute document matching validation"""
        
        form_data = context.get("form_data")
        pdf_analysis = context.get("pdf_analysis")
        
        if not form_data or not pdf_analysis:
            return self.create_result(
                RuleStatus.ERROR,
                "Missing required data for document matching"
            )
        
        # Get documents from form
        listed_documents = form_data.get_primary_documents()
        
        if not listed_documents:
            return self.create_result(
                RuleStatus.WARNING,
                "No documents listed in form to validate"
            )
        
        # Check each document
        attached_count = 0
        missing_documents = []
        
        for doc in listed_documents:
            if pdf_analysis.has_document_type(doc.document_type):
                attached_count += 1
                doc.is_attached = True
            else:
                missing_documents.append(doc.document_type)
                doc.is_attached = False
        
        total_docs = len(listed_documents)
        attachment_rate = (attached_count / total_docs) * 100 if total_docs > 0 else 0
        
        if missing_documents:
            status = RuleStatus.FAILED
            message = f"{len(missing_documents)} documents not attached: {', '.join(missing_documents)}"
        else:
            status = RuleStatus.PASSED
            message = f"All {total_docs} documents are attached"
        
        return self.create_result(
            status,
            message,
            {
                "total_documents": total_docs,
                "attached_documents": attached_count,
                "missing_documents": missing_documents,
                "attachment_rate": attachment_rate
            }
        )


class AlienExpirationMatchRule(Rule):
    """Rule for validating alien expiration date matches with supporting documents"""
    
    def __init__(self):
        super().__init__(
            "alien_expiration_match",
            "Alien Expiration Date Matching",
            RuleSeverity.MEDIUM
        )
    
    def execute(self, context: RuleContext) -> RuleResult:
        """Execute alien expiration date matching"""
        
        form_data = context.get("form_data")
        
        if not form_data:
            return self.create_result(
                RuleStatus.ERROR,
                "No form data provided"
            )
        
        # Only applicable to alien workers
        if form_data.citizenship_status != CitizenshipStatus.ALIEN_AUTHORIZED_TO_WORK:
            return self.create_result(
                RuleStatus.SKIPPED,
                "Not applicable - employee is not alien authorized to work"
            )
        
        alien_expiration = form_data.get_alien_expiration_date()
        if not alien_expiration:
            return self.create_result(
                RuleStatus.FAILED,
                "Alien work authorization expiration date not found"
            )
        
        # Check supporting documents
        supporting_docs = form_data.get_primary_documents()
        matching_docs = []
        
        for doc in supporting_docs:
            if doc.expiration_date and doc.expiration_date != "Not visible":
                if self._dates_match(alien_expiration, doc.expiration_date):
                    matching_docs.append(doc.document_type)
        
        if matching_docs:
            status = RuleStatus.PASSED
            message = f"Alien expiration date matches {len(matching_docs)} document(s): {', '.join(matching_docs)}"
        else:
            status = RuleStatus.FAILED
            message = f"Alien expiration date ({alien_expiration}) does not match any supporting document dates"
        
        return self.create_result(
            status,
            message,
            {
                "alien_expiration_date": alien_expiration,
                "matching_documents": matching_docs,
                "total_documents_checked": len(supporting_docs)
            }
        )
    
    def _dates_match(self, date1: str, date2: str) -> bool:
        """Check if two dates match"""
        import re
        
        if not date1 or not date2:
            return False
        
        # Normalize dates
        norm_date1 = re.sub(r'[/\-\s]', '', date1.strip())
        norm_date2 = re.sub(r'[/\-\s]', '', date2.strip())
        
        return norm_date1 == norm_date2


# Factory function for creating I-9 rule sets
def create_i9_rule_set() -> List[Rule]:
    """Create a complete set of I-9 validation rules"""
    
    rules = []
    
    # Document matching rule
    rules.append(DocumentMatchingRule())
    
    # Alien expiration matching rule
    rules.append(AlienExpirationMatchRule())
    
    # Basic field validation rules
    rules.append(I9ValidationRule(
        "first_name_required",
        "First Name Required",
        lambda form: (bool(form.first_name and form.first_name.strip()), 
                     "First name is required" if not (form.first_name and form.first_name.strip()) else "First name provided",
                     {}),
        severity=RuleSeverity.CRITICAL
    ))
    
    rules.append(I9ValidationRule(
        "last_name_required", 
        "Last Name Required",
        lambda form: (bool(form.last_name and form.last_name.strip()),
                     "Last name is required" if not (form.last_name and form.last_name.strip()) else "Last name provided",
                     {}),
        severity=RuleSeverity.CRITICAL
    ))
    
    rules.append(I9ValidationRule(
        "citizenship_status_required",
        "Citizenship Status Required", 
        lambda form: (form.citizenship_status != CitizenshipStatus.UNKNOWN,
                     "Citizenship status must be specified" if form.citizenship_status == CitizenshipStatus.UNKNOWN else "Citizenship status provided",
                     {"citizenship_status": form.citizenship_status.value}),
        severity=RuleSeverity.HIGH
    ))
    
    rules.append(I9ValidationRule(
        "employee_signature_required",
        "Employee Signature Required",
        lambda form: (form.employee_signature_present,
                     "Employee signature is required" if not form.employee_signature_present else "Employee signature present",
                     {"signature_present": form.employee_signature_present}),
        severity=RuleSeverity.HIGH
    ))
    
    return rules
