#!/usr/bin/env python3
"""
Compliance and Regulatory Validators

This module provides validators for compliance with I-9 regulations
and business rules specific to employment eligibility verification.
"""

from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import re

from ..core.models import I9FormData, DocumentInfo, ValidationResult, FormType, CitizenshipStatus
from ..utils.logging_config import logger
from .validators import BaseValidator


class ComplianceValidator(BaseValidator):
    """Validator for I-9 compliance requirements"""
    
    def __init__(self):
        super().__init__("compliance_validator", "high")
        
        # Compliance rules configuration
        self.required_fields_by_citizenship = {
            CitizenshipStatus.US_CITIZEN: [
                "first_name", "last_name", "date_of_birth", "employee_signature_present"
            ],
            CitizenshipStatus.LAWFUL_PERMANENT_RESIDENT: [
                "first_name", "last_name", "date_of_birth", "employee_signature_present"
            ],
            CitizenshipStatus.ALIEN_AUTHORIZED_TO_WORK: [
                "first_name", "last_name", "date_of_birth", "employee_signature_present", 
                "authorized_to_work_until"
            ]
        }
        
        self.document_requirements = {
            CitizenshipStatus.US_CITIZEN: {"min_docs": 1, "acceptable_lists": ["A", "B+C"]},
            CitizenshipStatus.LAWFUL_PERMANENT_RESIDENT: {"min_docs": 1, "acceptable_lists": ["A", "B+C"]},
            CitizenshipStatus.ALIEN_AUTHORIZED_TO_WORK: {"min_docs": 1, "acceptable_lists": ["A", "B+C"]}
        }
    
    def validate(self, form_data: I9FormData) -> ValidationResult:
        """Validate I-9 form for compliance with regulations"""
        
        compliance_issues = []
        recommendations = []
        
        # Check required fields based on citizenship status
        field_issues = self._validate_required_fields(form_data)
        compliance_issues.extend(field_issues)
        
        # Check document requirements
        doc_issues = self._validate_document_requirements(form_data)
        compliance_issues.extend(doc_issues)
        
        # Check signature requirements
        signature_issues = self._validate_signature_requirements(form_data)
        compliance_issues.extend(signature_issues)
        
        # Check work authorization expiration
        if form_data.citizenship_status == CitizenshipStatus.ALIEN_AUTHORIZED_TO_WORK:
            auth_issues = self._validate_work_authorization(form_data)
            compliance_issues.extend(auth_issues)
        
        # Check form completion timeline
        timeline_issues = self._validate_completion_timeline(form_data)
        compliance_issues.extend(timeline_issues)
        
        # Generate recommendations
        if compliance_issues:
            recommendations = self._generate_compliance_recommendations(compliance_issues, form_data)
        
        is_valid = len(compliance_issues) == 0
        message = "Form meets all compliance requirements" if is_valid else f"Compliance issues found: {len(compliance_issues)}"
        
        return self.create_result(
            is_valid=is_valid,
            message=message,
            details={
                "compliance_issues": compliance_issues,
                "citizenship_status": form_data.citizenship_status.value,
                "form_type": form_data.form_type.value,
                "total_issues": len(compliance_issues)
            },
            recommendations=recommendations
        )
    
    def _validate_required_fields(self, form_data: I9FormData) -> List[str]:
        """Validate required fields based on citizenship status"""
        
        issues = []
        required_fields = self.required_fields_by_citizenship.get(
            form_data.citizenship_status, 
            self.required_fields_by_citizenship[CitizenshipStatus.US_CITIZEN]
        )
        
        for field in required_fields:
            value = getattr(form_data, field, None)
            
            if field == "employee_signature_present":
                if not value:
                    issues.append(f"Missing employee signature")
            elif not value or (isinstance(value, str) and not value.strip()):
                issues.append(f"Missing required field: {field}")
        
        return issues
    
    def _validate_document_requirements(self, form_data: I9FormData) -> List[str]:
        """Validate document requirements for Section 2"""
        
        issues = []
        
        # Get all documents from appropriate section
        if form_data.form_type == FormType.SUPPLEMENT_B:
            documents = form_data.supplement_b_documents
        elif form_data.form_type == FormType.SECTION_3:
            documents = form_data.section_3_documents
        else:
            documents = form_data.section_2_documents
        
        if not documents:
            issues.append("No supporting documents provided")
            return issues
        
        # Check document list compliance (List A OR List B+C)
        list_a_docs = [d for d in documents if d.list_category == 'A']
        list_b_docs = [d for d in documents if d.list_category == 'B']
        list_c_docs = [d for d in documents if d.list_category == 'C']
        
        has_list_a = len(list_a_docs) > 0
        has_list_b_and_c = len(list_b_docs) > 0 and len(list_c_docs) > 0
        
        if not (has_list_a or has_list_b_and_c):
            issues.append("Must provide either List A document OR both List B and List C documents")
        
        # Document expiration validation removed - system should only extract dates, not validate expiration
        # The business logic should not determine if documents are "expired" relative to current date
        
        return issues
    
    def _validate_signature_requirements(self, form_data: I9FormData) -> List[str]:
        """Validate signature requirements"""
        
        issues = []
        
        # Employee signature is required
        if not form_data.employee_signature_present:
            issues.append("Employee signature is required")
        
        # Employee signature date should be provided if signature is present
        if form_data.employee_signature_present and not form_data.employee_signature_date:
            issues.append("Employee signature date is required when signature is present")
        
        # For completed forms, employer signature should also be present
        if form_data.form_type == FormType.STANDARD_I9:
            if form_data.section_2_documents and not form_data.employer_signature_date:
                issues.append("Employer signature date is required for completed Section 2")
        
        return issues
    
    def _validate_work_authorization(self, form_data: I9FormData) -> List[str]:
        """Validate work authorization for alien workers"""
        
        issues = []
        
        if not form_data.authorized_to_work_until:
            issues.append("Work authorization expiration date is required for alien workers")
            return issues
        
        # Work authorization date validation removed - system should only extract dates, not validate expiration
        # The business logic should not determine if work authorization is "expired" relative to current date
        
        return issues
    
    def _validate_completion_timeline(self, form_data: I9FormData) -> List[str]:
        """Validate I-9 completion timeline requirements"""
        
        issues = []
        
        # Section 1 should be completed by employee's first day of work
        if form_data.hire_date and form_data.employee_signature_date:
            try:
                hire_date = datetime.strptime(form_data.hire_date, "%m/%d/%Y")
                signature_date = datetime.strptime(form_data.employee_signature_date, "%m/%d/%Y")
                
                if signature_date > hire_date + timedelta(days=1):
                    days_late = (signature_date - hire_date).days
                    issues.append(f"Section 1 completed {days_late} days after hire date (should be completed by first day)")
            
            except ValueError:
                pass  # Skip validation if dates can't be parsed
        
        # Section 2 should be completed within 3 business days of hire
        if form_data.hire_date and form_data.employer_signature_date:
            try:
                hire_date = datetime.strptime(form_data.hire_date, "%m/%d/%Y")
                employer_signature_date = datetime.strptime(form_data.employer_signature_date, "%m/%d/%Y")
                
                # Allow 3 business days (approximately 5 calendar days including weekends)
                if employer_signature_date > hire_date + timedelta(days=5):
                    days_late = (employer_signature_date - hire_date).days
                    issues.append(f"Section 2 completed {days_late} days after hire date (should be within 3 business days)")
            
            except ValueError:
                pass
        
        return issues
    
    # _is_document_expired method removed - no longer validating document expiration dates
    
    def _generate_compliance_recommendations(self, issues: List[str], form_data: I9FormData) -> List[str]:
        """Generate recommendations based on compliance issues"""
        
        recommendations = []
        
        # Field-specific recommendations
        if any("Missing required field" in issue for issue in issues):
            recommendations.append("Complete all required fields before submitting the form")
        
        if any("signature" in issue.lower() for issue in issues):
            recommendations.append("Ensure both employee and employer signatures are present with dates")
        
        # Removed expired document recommendations - system no longer validates expiration dates
        
        if any("authorization expires" in issue for issue in issues):
            recommendations.append("Initiate work authorization renewal process")
            recommendations.append("Prepare for Section 3 reverification if needed")
        
        if any("timeline" in issue.lower() or "days after" in issue for issue in issues):
            recommendations.append("Ensure I-9 forms are completed within required timeframes")
            recommendations.append("Review hiring process to prevent future timeline violations")
        
        # Form-specific recommendations
        if form_data.form_type == FormType.SUPPLEMENT_B:
            recommendations.append("Verify all Supplement B requirements are met for work authorization renewal")
        
        return recommendations


class DateValidator(BaseValidator):
    """Specialized validator for date-related compliance"""
    
    def __init__(self):
        super().__init__("date_validator", "medium")
    
    def validate(self, form_data: I9FormData) -> ValidationResult:
        """Validate all dates in the I-9 form for consistency and compliance"""
        
        date_issues = []
        date_details = {}
        
        # Collect all dates from the form
        dates_to_check = {
            "date_of_birth": form_data.date_of_birth,
            "employee_signature_date": form_data.employee_signature_date,
            "employer_signature_date": form_data.employer_signature_date,
            "hire_date": form_data.hire_date,
            "authorized_to_work_until": form_data.authorized_to_work_until
        }
        
        # Validate each date format and reasonableness
        for date_name, date_value in dates_to_check.items():
            if date_value and date_value.strip():
                validation_result = self._validate_single_date(date_name, date_value)
                date_details[date_name] = validation_result
                
                if not validation_result["is_valid"]:
                    date_issues.append(validation_result["issue"])
        
        # Cross-date validations
        cross_date_issues = self._validate_date_relationships(dates_to_check)
        date_issues.extend(cross_date_issues)
        
        # Document date validations
        doc_date_issues = self._validate_document_dates(form_data)
        date_issues.extend(doc_date_issues)
        
        is_valid = len(date_issues) == 0
        message = "All dates are valid and consistent" if is_valid else f"Date validation issues: {len(date_issues)}"
        
        return self.create_result(
            is_valid=is_valid,
            message=message,
            details={
                "date_issues": date_issues,
                "date_details": date_details,
                "total_issues": len(date_issues)
            }
        )
    
    def _validate_single_date(self, date_name: str, date_value: str) -> Dict[str, Any]:
        """Validate a single date for format and reasonableness"""
        
        result = {
            "date_name": date_name,
            "date_value": date_value,
            "is_valid": True,
            "parsed_date": None,
            "issue": None
        }
        
        # Try to parse the date
        try:
            if '/' in date_value:
                parsed_date = datetime.strptime(date_value.strip(), "%m/%d/%Y")
            elif '-' in date_value:
                parsed_date = datetime.strptime(date_value.strip(), "%m-%d-%Y")
            else:
                result["is_valid"] = False
                result["issue"] = f"{date_name}: Invalid date format '{date_value}'"
                return result
            
            result["parsed_date"] = parsed_date
            
        except ValueError:
            result["is_valid"] = False
            result["issue"] = f"{date_name}: Cannot parse date '{date_value}'"
            return result
        
        # Check date reasonableness
        today = datetime.now()
        
        if date_name == "date_of_birth":
            # Birth date should be in the past and reasonable (not too old/young)
            if parsed_date >= today:
                result["is_valid"] = False
                result["issue"] = f"{date_name}: Birth date cannot be in the future"
            elif parsed_date < today - timedelta(days=365 * 100):  # 100 years old
                result["is_valid"] = False
                result["issue"] = f"{date_name}: Birth date seems unreasonably old"
            elif parsed_date > today - timedelta(days=365 * 16):  # Under 16 years old
                result["is_valid"] = False
                result["issue"] = f"{date_name}: Employee appears to be under 16 years old"
        
        elif date_name in ["employee_signature_date", "employer_signature_date", "hire_date"]:
            # These dates should be reasonable (not too far in past/future)
            if parsed_date > today + timedelta(days=30):
                result["is_valid"] = False
                result["issue"] = f"{date_name}: Date is too far in the future"
            elif parsed_date < today - timedelta(days=365 * 5):  # 5 years ago
                result["is_valid"] = False
                result["issue"] = f"{date_name}: Date is very old (over 5 years ago)"
        
        elif date_name == "authorized_to_work_until":
            # Work authorization should be in the future or recent past
            if parsed_date < today - timedelta(days=365):  # 1 year ago
                result["is_valid"] = False
                result["issue"] = f"{date_name}: Work authorization expired over a year ago"
        
        return result
    
    def _validate_date_relationships(self, dates: Dict[str, str]) -> List[str]:
        """Validate relationships between different dates"""
        
        issues = []
        
        try:
            # Parse dates that exist
            parsed_dates = {}
            for name, value in dates.items():
                if value and value.strip():
                    try:
                        if '/' in value:
                            parsed_dates[name] = datetime.strptime(value.strip(), "%m/%d/%Y")
                        elif '-' in value:
                            parsed_dates[name] = datetime.strptime(value.strip(), "%m-%d-%Y")
                    except ValueError:
                        continue
            
            # Employee signature should be on or after hire date
            if "hire_date" in parsed_dates and "employee_signature_date" in parsed_dates:
                if parsed_dates["employee_signature_date"] < parsed_dates["hire_date"]:
                    issues.append("Employee signature date cannot be before hire date")
            
            # Employer signature should be on or after employee signature
            if "employee_signature_date" in parsed_dates and "employer_signature_date" in parsed_dates:
                if parsed_dates["employer_signature_date"] < parsed_dates["employee_signature_date"]:
                    issues.append("Employer signature date cannot be before employee signature date")
            
            # Work authorization should be after hire date
            if "hire_date" in parsed_dates and "authorized_to_work_until" in parsed_dates:
                if parsed_dates["authorized_to_work_until"] <= parsed_dates["hire_date"]:
                    issues.append("Work authorization expiration should be after hire date")
            
            # Employee should be at least 16 on hire date
            if "date_of_birth" in parsed_dates and "hire_date" in parsed_dates:
                age_at_hire = parsed_dates["hire_date"] - parsed_dates["date_of_birth"]
                if age_at_hire.days < 365 * 16:  # Less than 16 years
                    issues.append("Employee appears to be under 16 years old at hire date")
        
        except Exception as e:
            issues.append(f"Error validating date relationships: {str(e)}")
        
        return issues
    
    def _validate_document_dates(self, form_data: I9FormData) -> List[str]:
        """Validate expiration dates on supporting documents"""
        
        issues = []
        
        # Document expiration validation removed - system should only extract dates, not validate expiration
        # The business logic should not determine if documents are "expired" relative to current date
        # Only validate date format if needed for processing
        
        all_documents = form_data.get_all_documents()
        
        for doc in all_documents:
            if doc.expiration_date and doc.expiration_date != "Not visible":
                try:
                    # Only validate date format, not expiration status
                    datetime.strptime(doc.expiration_date, "%m/%d/%Y")
                except ValueError:
                    issues.append(f"Document {doc.document_type} has invalid expiration date format: {doc.expiration_date}")
        
        return issues
