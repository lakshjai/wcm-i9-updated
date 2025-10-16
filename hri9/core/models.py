#!/usr/bin/env python3
"""
Enhanced Data Models for I-9 Processing

This module contains the core data models migrated and enhanced from I9Processor.py
with additional functionality for complex rule processing.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Union
from datetime import datetime
from enum import Enum
import json


class CitizenshipStatus(Enum):
    """Employee citizenship status"""
    US_CITIZEN = "US_CITIZEN"
    LAWFUL_PERMANENT_RESIDENT = "LAWFUL_PERMANENT_RESIDENT"
    ALIEN_AUTHORIZED_TO_WORK = "ALIEN_AUTHORIZED_TO_WORK"
    UNKNOWN = "UNKNOWN"


class FormType(Enum):
    """I-9 form types"""
    STANDARD_I9 = "STANDARD_I9"
    SUPPLEMENT_B = "SUPPLEMENT_B"
    SECTION_3 = "SECTION_3"
    UNKNOWN = "UNKNOWN"


class ProcessingStatus(Enum):
    """Processing status enumeration"""
    COMPLETE_SUCCESS = "COMPLETE_SUCCESS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    PARTIAL_SUCCESS_MULTIPLE_FORMS = "PARTIAL_SUCCESS_MULTIPLE_FORMS"
    MISSING_SUPPORTING_DOCS = "MISSING_SUPPORTING_DOCS"
    INCOMPLETE_I9 = "INCOMPLETE_I9"
    FORM_SELECTION_UNCERTAIN = "FORM_SELECTION_UNCERTAIN"
    RENEWAL_FORM_INCOMPLETE = "RENEWAL_FORM_INCOMPLETE"
    NO_I9_FOUND = "NO_I9_FOUND"
    SECTION_1_NOT_FOUND = "SECTION_1_NOT_FOUND"
    DATE_MISMATCH = "DATE_MISMATCH"
    MISSING_ATTACHMENTS = "MISSING_ATTACHMENTS"
    ERROR = "ERROR"


@dataclass
class DocumentInfo:
    """Enhanced document information with validation support"""
    document_type: str
    document_number: str = "Not visible"
    expiration_date: str = "Not visible"
    issuing_authority: str = "Not visible"
    confidence: str = "medium"
    page_number: int = 0
    details: str = ""
    is_attached: bool = False  # Whether document is physically attached to PDF
    list_category: str = ""  # A, B, or C for Section 2 documents
    section_source: str = ""  # section_2, supplement_b, section_3
    
    def __post_init__(self):
        """Normalize document type after initialization"""
        self.document_type = self.normalize_document_type(self.document_type)
    
    @staticmethod
    def normalize_document_type(document_type: str) -> str:
        """Normalize document type names for consistent identification"""
        if not document_type:
            return document_type
        
        doc_upper = document_type.upper().strip()
        
        # Mapping of common document types
        type_mappings = {
            'I-94': "I-94 Arrival/Departure Record",
            'I94': "I-94 Arrival/Departure Record",
            'ARRIVAL/DEPARTURE': "I-94 Arrival/Departure Record",
            'DRIVER': "Driver's License",
            'LICENSE': "Driver's License",
            'DL': "Driver's License",
            'PASSPORT': "U.S. Passport",
            'PASSPORT CARD': "U.S. Passport Card",
            'SOCIAL SECURITY': "Social Security Card",
            'SS CARD': "Social Security Card",
            'SSN': "Social Security Card",
            'GREEN CARD': "Permanent Resident Card",
            'PERMANENT RESIDENT': "Permanent Resident Card",
            'EAD': "Employment Authorization Document",
            'EMPLOYMENT AUTHORIZATION': "Employment Authorization Document",
            'WORK PERMIT': "Employment Authorization Document",
            'BIRTH CERTIFICATE': "Birth Certificate",
            'BIRTH CERT': "Birth Certificate",
            'CERTIFICATE OF LIVE BIRTH': "Birth Certificate",
            'I-20': "I-20 Certificate of Eligibility for Non-Immigrant Student Status",
            'DS-2019': "DS-2019 Certificate of Eligibility for Exchange Visitor Status"
        }
        
        for key, normalized in type_mappings.items():
            if key in doc_upper:
                return normalized
        
        return document_type
    
    def matches_document(self, other: 'DocumentInfo') -> bool:
        """Check if this document matches another document"""
        if not self.document_number or not other.document_number:
            return False
        
        # Normalize document numbers
        num1 = ''.join(c for c in self.document_number if c.isalnum()).upper()
        num2 = ''.join(c for c in other.document_number if c.isalnum()).upper()
        
        return (num1 == num2 and len(num1) > 0 and 
                self.document_type == other.document_type)


@dataclass
class I9FormData:
    """Enhanced I-9 form data with citizenship status and validation support"""
    # Basic employee information
    first_name: str = ""
    last_name: str = ""
    middle_initial: str = ""
    ssn: str = ""
    date_of_birth: str = ""
    
    # Signature information
    employee_signature_present: bool = False
    employee_signature_date: str = ""
    employer_signature_date: str = ""
    
    # Citizenship and work authorization
    citizenship_status: CitizenshipStatus = CitizenshipStatus.UNKNOWN
    residential_status: str = ""
    hire_date: str = ""
    authorized_to_work_until: str = ""
    
    # Section 2 document information
    section_2_list_a: List[str] = field(default_factory=list)
    section_2_list_b: List[str] = field(default_factory=list)
    section_2_list_c: List[str] = field(default_factory=list)
    section_2_documents: List[DocumentInfo] = field(default_factory=list)
    
    # Additional form sections
    supplement_b_documents: List[DocumentInfo] = field(default_factory=list)
    section_3_documents: List[DocumentInfo] = field(default_factory=list)
    
    # Form metadata
    form_type: FormType = FormType.STANDARD_I9
    page_number: int = 0
    pdf_filename: str = ""
    extraction_confidence: str = "medium"
    
    # Additional info for LPR and alien workers
    lawful_permanent_resident_info: Dict = field(default_factory=dict)
    alien_work_authorization_info: Dict = field(default_factory=dict)
    
    # Enhanced metadata
    form_confidence: float = 0.0
    form_priority_score: float = 0.0
    is_renewal_form: bool = False
    original_form_reference: str = ""
    renewal_context: Dict = field(default_factory=dict)
    
    def get_all_documents(self) -> List[DocumentInfo]:
        """Get all documents from all sections"""
        all_docs = []
        all_docs.extend(self.section_2_documents)
        all_docs.extend(self.supplement_b_documents)
        all_docs.extend(self.section_3_documents)
        return all_docs
    
    def get_alien_expiration_date(self) -> Optional[str]:
        """Get alien work authorization expiration date"""
        if self.citizenship_status == CitizenshipStatus.ALIEN_AUTHORIZED_TO_WORK:
            return self.authorized_to_work_until
        return None
    
    def is_non_citizen(self) -> bool:
        """Check if employee is non-citizen"""
        return self.citizenship_status in [
            CitizenshipStatus.LAWFUL_PERMANENT_RESIDENT,
            CitizenshipStatus.ALIEN_AUTHORIZED_TO_WORK
        ]
    
    def get_primary_documents(self) -> List[DocumentInfo]:
        """Get primary documents based on form type"""
        if self.form_type == FormType.SUPPLEMENT_B:
            return self.supplement_b_documents
        elif self.form_type == FormType.SECTION_3:
            return self.section_3_documents
        else:
            return self.section_2_documents


@dataclass
class FormSelectionResult:
    """Result of form selection process with enhanced metadata"""
    selected_form: I9FormData
    all_detected_forms: List[I9FormData]
    selection_criteria: str
    confidence_score: float
    alternative_forms: List[I9FormData] = field(default_factory=list)
    selection_notes: str = ""
    selection_reasons: List[str] = field(default_factory=list)


@dataclass
class ValidationResult:
    """Result of validation checks"""
    is_valid: bool
    validation_type: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)
    severity: str = "medium"  # low, medium, high, critical


@dataclass
class ScenarioResult:
    """Result of scenario processing"""
    scenario_id: str
    scenario_name: str
    status: ProcessingStatus
    primary_form: Optional[I9FormData]
    supporting_documents: List[DocumentInfo] = field(default_factory=list)
    validation_results: List[ValidationResult] = field(default_factory=list)
    date_matches: Dict[str, bool] = field(default_factory=dict)
    attachment_status: Dict[str, bool] = field(default_factory=dict)
    notes: str = ""
    recommendations: List[str] = field(default_factory=list)
    
    @property
    def has_critical_issues(self) -> bool:
        """Check if scenario has critical validation issues"""
        return any(v.severity == "critical" for v in self.validation_results)
    
    @property
    def success_rate(self) -> float:
        """Calculate validation success rate"""
        if not self.validation_results:
            return 0.0
        
        passed = sum(1 for v in self.validation_results if v.is_valid)
        return (passed / len(self.validation_results)) * 100


@dataclass
class ProcessingResult:
    """Enhanced final processing result"""
    status: ProcessingStatus
    scenario_results: List[ScenarioResult] = field(default_factory=list)
    primary_i9_data: Optional[I9FormData] = None
    
    # Legacy fields for compatibility
    missing_fields: List[str] = field(default_factory=list)
    found_documents: List[str] = field(default_factory=list)
    missing_documents: List[str] = field(default_factory=list)
    expiration_match: str = "N/A"
    notes: str = ""
    selected_i9_page: int = 0
    
    # Enhanced fields
    total_forms_detected: int = 0
    form_type_selected: str = "UNKNOWN"
    selection_reason: str = ""
    alternative_forms_available: str = ""
    document_matching_strategy: str = ""
    
    # Supporting document information
    supporting_documents_count: int = 0
    document_matches_found: int = 0
    expiration_matches: int = 0
    
    # Document attachment verification
    documents_mentioned_count: int = 0
    documents_attached_count: int = 0
    documents_missing_count: int = 0
    document_attachment_status: str = "UNKNOWN"
    
    # Validation statistics
    total_validations: int = 0
    passed_validations: int = 0
    failed_validations: int = 0
    critical_issues: int = 0
    
    def add_scenario_result(self, scenario_result: ScenarioResult) -> None:
        """Add a scenario result and update summary statistics"""
        self.scenario_results.append(scenario_result)
        
        # Update validation statistics
        for validation in scenario_result.validation_results:
            self.total_validations += 1
            if validation.is_valid:
                self.passed_validations += 1
            else:
                self.failed_validations += 1
                if validation.severity == "critical":
                    self.critical_issues += 1
    
    @property
    def validation_success_rate(self) -> float:
        """Calculate overall validation success rate"""
        if self.total_validations == 0:
            return 0.0
        return (self.passed_validations / self.total_validations) * 100
    
    @property
    def has_critical_issues(self) -> bool:
        """Check if processing has critical issues"""
        return self.critical_issues > 0


@dataclass
class PDFAnalysis:
    """Enhanced PDF analysis result"""
    filename: str
    total_pages: int
    i9_pages: List[int] = field(default_factory=list)
    document_catalog: Dict[str, List[DocumentInfo]] = field(default_factory=dict)
    form_selection_result: Optional[FormSelectionResult] = None
    processing_result: Optional[ProcessingResult] = None
    catalog_data: Dict[str, Any] = field(default_factory=dict)  # Raw catalog data
    
    def get_documents_by_type(self, doc_type: str) -> List[DocumentInfo]:
        """Get all documents of a specific type"""
        normalized_type = DocumentInfo.normalize_document_type(doc_type)
        return self.document_catalog.get(normalized_type, [])
    
    def has_document_type(self, doc_type: str) -> bool:
        """Check if PDF contains documents of a specific type"""
        return len(self.get_documents_by_type(doc_type)) > 0
