#!/usr/bin/env python3
"""
Core Validation Framework

This module provides the foundational validation infrastructure for
I-9 form processing with extensible validator classes.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Callable, Union
from dataclasses import dataclass
import re
from datetime import datetime

from ..core.models import I9FormData, DocumentInfo, ValidationResult, CitizenshipStatus
from ..utils.logging_config import logger


class BaseValidator(ABC):
    """Abstract base class for all validators"""
    
    def __init__(self, name: str, severity: str = "medium"):
        self.name = name
        self.severity = severity
        self.logger = logger
    
    @abstractmethod
    def validate(self, data: Any) -> ValidationResult:
        """Perform validation and return result"""
        pass
    
    def create_result(self, is_valid: bool, message: str, 
                     details: Dict[str, Any] = None,
                     recommendations: List[str] = None) -> ValidationResult:
        """Helper to create validation result"""
        return ValidationResult(
            is_valid=is_valid,
            validation_type=self.name,
            message=message,
            details=details or {},
            recommendations=recommendations or [],
            severity=self.severity
        )


class FieldValidator(BaseValidator):
    """Validator for individual form fields"""
    
    def __init__(self, field_name: str, validation_func: Callable[[Any], bool],
                 error_message: str, severity: str = "medium"):
        super().__init__(f"field_{field_name}", severity)
        self.field_name = field_name
        self.validation_func = validation_func
        self.error_message = error_message
    
    def validate(self, form_data: I9FormData) -> ValidationResult:
        """Validate a specific field in form data"""
        
        field_value = getattr(form_data, self.field_name, None)
        
        try:
            is_valid = self.validation_func(field_value)
            
            if is_valid:
                message = f"Field {self.field_name} is valid"
            else:
                message = self.error_message
            
            return self.create_result(
                is_valid=is_valid,
                message=message,
                details={
                    "field_name": self.field_name,
                    "field_value": str(field_value) if field_value else None
                }
            )
            
        except Exception as e:
            return self.create_result(
                is_valid=False,
                message=f"Validation error for field {self.field_name}: {str(e)}",
                details={"error": str(e)},
                recommendations=["Check field format and try again"]
            )


class CrossFieldValidator(BaseValidator):
    """Validator for relationships between multiple fields"""
    
    def __init__(self, name: str, validation_func: Callable[[I9FormData], tuple],
                 severity: str = "medium"):
        super().__init__(name, severity)
        self.validation_func = validation_func
    
    def validate(self, form_data: I9FormData) -> ValidationResult:
        """Validate relationships between fields"""
        
        try:
            is_valid, message, details = self.validation_func(form_data)
            
            return self.create_result(
                is_valid=is_valid,
                message=message,
                details=details or {}
            )
            
        except Exception as e:
            return self.create_result(
                is_valid=False,
                message=f"Cross-field validation error: {str(e)}",
                details={"error": str(e)}
            )


class ValidationFramework:
    """Main validation framework that orchestrates all validators"""
    
    def __init__(self):
        self.field_validators: List[FieldValidator] = []
        self.cross_field_validators: List[CrossFieldValidator] = []
        self.custom_validators: List[BaseValidator] = []
        self._setup_default_validators()
    
    def _setup_default_validators(self):
        """Setup default field validators"""
        
        # Name validators
        self.add_field_validator(
            "first_name",
            lambda x: bool(x and x.strip()),
            "First name is required",
            "high"
        )
        
        self.add_field_validator(
            "last_name", 
            lambda x: bool(x and x.strip()),
            "Last name is required",
            "high"
        )
        
        # Date validators
        self.add_field_validator(
            "date_of_birth",
            self._validate_date_format,
            "Date of birth must be in MM/DD/YYYY format",
            "high"
        )
        
        self.add_field_validator(
            "employee_signature_date",
            lambda x: not x or self._validate_date_format(x),
            "Employee signature date must be in MM/DD/YYYY format if provided",
            "medium"
        )
        
        # SSN validator (optional but format check if provided)
        self.add_field_validator(
            "ssn",
            self._validate_ssn_format,
            "SSN format is invalid (should be XXX-XX-XXXX or XXXXXXXXX)",
            "low"
        )
        
        # Cross-field validators
        self.add_cross_field_validator(
            "citizenship_work_authorization",
            self._validate_citizenship_work_authorization,
            "high"
        )
        
        self.add_cross_field_validator(
            "signature_consistency",
            self._validate_signature_consistency,
            "medium"
        )
    
    def add_field_validator(self, field_name: str, validation_func: Callable,
                           error_message: str, severity: str = "medium"):
        """Add a field validator"""
        validator = FieldValidator(field_name, validation_func, error_message, severity)
        self.field_validators.append(validator)
    
    def add_cross_field_validator(self, name: str, validation_func: Callable,
                                 severity: str = "medium"):
        """Add a cross-field validator"""
        validator = CrossFieldValidator(name, validation_func, severity)
        self.cross_field_validators.append(validator)
    
    def add_custom_validator(self, validator: BaseValidator):
        """Add a custom validator"""
        self.custom_validators.append(validator)
    
    def validate_form(self, form_data: I9FormData) -> List[ValidationResult]:
        """Validate an I-9 form using all registered validators"""
        
        results = []
        
        # Run field validators
        for validator in self.field_validators:
            result = validator.validate(form_data)
            results.append(result)
        
        # Run cross-field validators
        for validator in self.cross_field_validators:
            result = validator.validate(form_data)
            results.append(result)
        
        # Run custom validators
        for validator in self.custom_validators:
            result = validator.validate(form_data)
            results.append(result)
        
        return results
    
    def validate_documents(self, documents: List[DocumentInfo]) -> List[ValidationResult]:
        """Validate a list of documents"""
        
        results = []
        
        for i, doc in enumerate(documents):
            # Validate document type
            if not doc.document_type or doc.document_type.strip() == "":
                results.append(ValidationResult(
                    is_valid=False,
                    validation_type="document_type",
                    message=f"Document {i+1} missing document type",
                    severity="high"
                ))
            
            # Validate expiration date format if provided
            if doc.expiration_date and doc.expiration_date != "Not visible":
                if not self._validate_date_format(doc.expiration_date):
                    results.append(ValidationResult(
                        is_valid=False,
                        validation_type="document_expiration_date",
                        message=f"Invalid expiration date format for {doc.document_type}",
                        details={"expiration_date": doc.expiration_date},
                        severity="medium"
                    ))
        
        return results
    
    # Validation helper methods
    
    def _validate_date_format(self, date_str: str) -> bool:
        """Validate date is in MM/DD/YYYY format"""
        if not date_str:
            return True  # Empty dates are allowed in some contexts
        
        # Try multiple common date formats
        date_patterns = [
            r'^\d{1,2}/\d{1,2}/\d{4}$',  # MM/DD/YYYY or M/D/YYYY
            r'^\d{1,2}-\d{1,2}-\d{4}$',  # MM-DD-YYYY or M-D-YYYY
            r'^\d{4}-\d{1,2}-\d{1,2}$'   # YYYY-MM-DD or YYYY-M-D
        ]
        
        for pattern in date_patterns:
            if re.match(pattern, date_str.strip()):
                # Try to parse the date to ensure it's valid
                try:
                    if '/' in date_str:
                        datetime.strptime(date_str.strip(), "%m/%d/%Y")
                    elif '-' in date_str and date_str.startswith('2'):
                        datetime.strptime(date_str.strip(), "%Y-%m-%d")
                    else:
                        datetime.strptime(date_str.strip(), "%m-%d-%Y")
                    return True
                except ValueError:
                    continue
        
        return False
    
    def _validate_ssn_format(self, ssn: str) -> bool:
        """Validate SSN format"""
        if not ssn:
            return True  # SSN is optional
        
        # Remove spaces and check format
        clean_ssn = ssn.replace(' ', '').replace('-', '')
        
        # Should be 9 digits
        if len(clean_ssn) != 9 or not clean_ssn.isdigit():
            return False
        
        # Check for invalid patterns (all zeros, etc.)
        invalid_patterns = ['000000000', '111111111', '222222222', '333333333',
                           '444444444', '555555555', '666666666', '777777777',
                           '888888888', '999999999']
        
        return clean_ssn not in invalid_patterns
    
    def _validate_citizenship_work_authorization(self, form_data: I9FormData) -> tuple:
        """Validate citizenship status and work authorization consistency"""
        
        citizenship = form_data.citizenship_status
        work_auth_date = form_data.authorized_to_work_until
        
        # If alien authorized to work, should have work authorization date
        if citizenship == CitizenshipStatus.ALIEN_AUTHORIZED_TO_WORK:
            if not work_auth_date or work_auth_date == "Not visible":
                return (False, 
                       "Alien authorized to work must have work authorization expiration date",
                       {"citizenship_status": citizenship.value, "work_auth_date": work_auth_date})
        
        # US Citizens shouldn't have work authorization expiration
        elif citizenship == CitizenshipStatus.US_CITIZEN:
            if work_auth_date and work_auth_date != "Not visible":
                return (False,
                       "US Citizens should not have work authorization expiration date",
                       {"citizenship_status": citizenship.value, "work_auth_date": work_auth_date})
        
        return (True, "Citizenship and work authorization are consistent", {})
    
    def _validate_signature_consistency(self, form_data: I9FormData) -> tuple:
        """Validate signature presence and dates"""
        
        has_signature = form_data.employee_signature_present
        signature_date = form_data.employee_signature_date
        
        # If signature is present, should have date
        if has_signature and not signature_date:
            return (False,
                   "Employee signature present but no signature date provided",
                   {"has_signature": has_signature, "signature_date": signature_date})
        
        # If signature date provided, signature should be present
        if signature_date and not has_signature:
            return (False,
                   "Employee signature date provided but signature not marked as present",
                   {"has_signature": has_signature, "signature_date": signature_date})
        
        return (True, "Signature information is consistent", {})
    
    def get_validation_summary(self, results: List[ValidationResult]) -> Dict[str, Any]:
        """Generate a summary of validation results"""
        
        total = len(results)
        passed = sum(1 for r in results if r.is_valid)
        failed = total - passed
        
        # Group by severity
        severity_counts = {}
        for result in results:
            if not result.is_valid:
                severity = result.severity
                severity_counts[severity] = severity_counts.get(severity, 0) + 1
        
        return {
            "total_validations": total,
            "passed": passed,
            "failed": failed,
            "success_rate": (passed / total * 100) if total > 0 else 0,
            "severity_breakdown": severity_counts,
            "has_critical_issues": "critical" in severity_counts,
            "has_high_issues": "high" in severity_counts
        }
