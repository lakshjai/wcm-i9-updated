#!/usr/bin/env python3
"""
Comprehensive I-9 Validation & Scoring

This module implements comprehensive validation and scoring logic
according to the processing pipeline requirements.
"""

from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum
from datetime import datetime

from hri9.core.set_grouping import I9Set, I9SetType
from hri9.core.document_matching import DocumentMatch, DocumentMatcher
from hri9.core.models import ValidationResult
from hri9.utils.logging_config import logger


class ValidationSeverity(Enum):
    """Validation severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ErrorCode(Enum):
    """Standard I-9 error codes"""
    E001 = "E001"  # Missing Section 1 for reverification
    E002 = "E002"  # Missing Section 2
    E003 = "E003"  # Section 3 without corresponding Section 1
    E004 = "E004"  # Document expiration date mismatch
    E005 = "E005"  # Missing supporting documents
    E006 = "E006"  # Inconsistent employee information
    E007 = "E007"  # Invalid work authorization period
    E008 = "E008"  # Document number mismatch
    E009 = "E009"  # Expired documents
    E010 = "E010"  # Incomplete form sections


@dataclass
class ValidationIssue:
    """Represents a validation issue"""
    error_code: ErrorCode
    severity: ValidationSeverity
    message: str
    details: Dict[str, Any]
    affected_pages: List[int]
    recommendations: List[str]


@dataclass
class ComprehensiveValidationResult:
    """Result of comprehensive validation"""
    overall_score: float
    validation_status: str  # SUCCESS, PARTIAL_SUCCESS, ERROR
    total_issues: int
    critical_issues: int
    error_issues: int
    warning_issues: int
    validation_issues: List[ValidationIssue]
    document_matches: List[DocumentMatch]
    set_completeness_score: float
    document_matching_score: float
    compliance_score: float


class ComprehensiveValidator:
    """Performs comprehensive I-9 validation and scoring"""
    
    def __init__(self):
        self.logger = logger
        self.document_matcher = DocumentMatcher()
    
    def validate_i9_set(self, i9_set: I9Set) -> ComprehensiveValidationResult:
        """
        Perform comprehensive validation on an I-9 set
        
        Args:
            i9_set: Complete I-9 set to validate
            
        Returns:
            ComprehensiveValidationResult with detailed validation results
        """
        
        logger.info(f"Starting comprehensive validation for set {i9_set.set_id}")
        
        validation_issues = []
        
        # Step 1: Validate set completeness
        completeness_issues = self._validate_set_completeness(i9_set)
        validation_issues.extend(completeness_issues)
        
        # Step 2: Validate employee information consistency
        consistency_issues = self._validate_employee_consistency(i9_set)
        validation_issues.extend(consistency_issues)
        
        # Step 3: Validate work authorization
        auth_issues = self._validate_work_authorization(i9_set)
        validation_issues.extend(auth_issues)
        
        # Step 4: Perform document matching and validation
        document_matches = self.document_matcher.match_documents_for_set(i9_set)
        doc_issues = self._validate_document_matches(document_matches)
        validation_issues.extend(doc_issues)
        
        # Step 5: Calculate scores
        scores = self._calculate_scores(i9_set, validation_issues, document_matches)
        
        # Step 6: Determine overall status
        overall_status = self._determine_overall_status(validation_issues, scores)
        
        result = ComprehensiveValidationResult(
            overall_score=scores['overall'],
            validation_status=overall_status,
            total_issues=len(validation_issues),
            critical_issues=len([i for i in validation_issues if i.severity == ValidationSeverity.CRITICAL]),
            error_issues=len([i for i in validation_issues if i.severity == ValidationSeverity.ERROR]),
            warning_issues=len([i for i in validation_issues if i.severity == ValidationSeverity.WARNING]),
            validation_issues=validation_issues,
            document_matches=document_matches,
            set_completeness_score=scores['completeness'],
            document_matching_score=scores['document_matching'],
            compliance_score=scores['compliance']
        )
        
        logger.info(f"Validation completed: {overall_status} with score {scores['overall']:.1f}%")
        
        return result
    
    def _validate_set_completeness(self, i9_set: I9Set) -> List[ValidationIssue]:
        """Validate that the I-9 set has all required sections"""
        
        issues = []
        
        # Check for missing Section 1
        if not i9_set.section_1_page:
            issues.append(ValidationIssue(
                error_code=ErrorCode.E001,
                severity=ValidationSeverity.CRITICAL,
                message="Missing Section 1 (Employee Information and Attestation)",
                details={"set_id": i9_set.set_id, "set_type": i9_set.set_type.value},
                affected_pages=[],
                recommendations=["Ensure Section 1 is completed by employee before processing"]
            ))
        
        # Check for missing Section 2
        if not i9_set.section_2_page:
            issues.append(ValidationIssue(
                error_code=ErrorCode.E002,
                severity=ValidationSeverity.CRITICAL,
                message="Missing Section 2 (Employer Review and Verification)",
                details={"set_id": i9_set.set_id, "set_type": i9_set.set_type.value},
                affected_pages=[],
                recommendations=["Ensure Section 2 is completed by employer"]
            ))
        
        # Check Section 3 requirements
        if i9_set.section_3_pages and not i9_set.section_1_page:
            issues.append(ValidationIssue(
                error_code=ErrorCode.E003,
                severity=ValidationSeverity.ERROR,
                message="Section 3 found without corresponding Section 1",
                details={
                    "set_id": i9_set.set_id,
                    "section_3_pages": [p.page_number for p in i9_set.section_3_pages]
                },
                affected_pages=[p.page_number for p in i9_set.section_3_pages],
                recommendations=["Verify that Section 1 is present for reverification process"]
            ))
        
        return issues
    
    def _validate_employee_consistency(self, i9_set: I9Set) -> List[ValidationIssue]:
        """Validate consistency of employee information across sections"""
        
        issues = []
        
        if not i9_set.section_1_page or not i9_set.section_2_page:
            return issues
        
        section_1_data = i9_set.section_1_page.extracted_values
        section_2_data = i9_set.section_2_page.extracted_values
        
        # Check name consistency
        s1_first = section_1_data.get('first_name', '').upper()
        s1_last = section_1_data.get('last_name', '').upper()
        s2_first = section_2_data.get('employee_first_name', '').upper()
        s2_last = section_2_data.get('employee_last_name', '').upper()
        
        if s1_first and s2_first and s1_first != s2_first:
            issues.append(ValidationIssue(
                error_code=ErrorCode.E006,
                severity=ValidationSeverity.ERROR,
                message=f"First name inconsistency: Section 1 '{s1_first}' vs Section 2 '{s2_first}'",
                details={
                    "section_1_name": s1_first,
                    "section_2_name": s2_first,
                    "section_1_page": i9_set.section_1_page.page_number,
                    "section_2_page": i9_set.section_2_page.page_number
                },
                affected_pages=[i9_set.section_1_page.page_number, i9_set.section_2_page.page_number],
                recommendations=["Verify employee name spelling across all sections"]
            ))
        
        if s1_last and s2_last and s1_last != s2_last:
            issues.append(ValidationIssue(
                error_code=ErrorCode.E006,
                severity=ValidationSeverity.ERROR,
                message=f"Last name inconsistency: Section 1 '{s1_last}' vs Section 2 '{s2_last}'",
                details={
                    "section_1_name": s1_last,
                    "section_2_name": s2_last,
                    "section_1_page": i9_set.section_1_page.page_number,
                    "section_2_page": i9_set.section_2_page.page_number
                },
                affected_pages=[i9_set.section_1_page.page_number, i9_set.section_2_page.page_number],
                recommendations=["Verify employee name spelling across all sections"]
            ))
        
        return issues
    
    def _validate_work_authorization(self, i9_set: I9Set) -> List[ValidationIssue]:
        """Validate work authorization information"""
        
        issues = []
        
        if not i9_set.section_1_page:
            return issues
        
        section_1_data = i9_set.section_1_page.extracted_values
        citizenship_status = section_1_data.get('citizenship_status', '')
        
        # Check work authorization expiry for non-citizens
        if 'alien_authorized_to_work' in citizenship_status:
            auth_until = section_1_data.get('alien_authorized_to_work_until')
            
            if not auth_until:
                issues.append(ValidationIssue(
                    error_code=ErrorCode.E007,
                    severity=ValidationSeverity.ERROR,
                    message="Missing work authorization expiration date for alien authorized to work",
                    details={
                        "citizenship_status": citizenship_status,
                        "page": i9_set.section_1_page.page_number
                    },
                    affected_pages=[i9_set.section_1_page.page_number],
                    recommendations=["Ensure work authorization expiration date is provided"]
                ))
            else:
                # Work authorization date is present - no expiration validation needed
                # The system should only extract and report dates, not validate if they're "expired"
                pass
        
        return issues
    
    def _validate_document_matches(self, document_matches: List[DocumentMatch]) -> List[ValidationIssue]:
        """Validate document matching results"""
        
        issues = []
        
        for match in document_matches:
            # Check for missing supporting documents
            if not match.supporting_document:
                issues.append(ValidationIssue(
                    error_code=ErrorCode.E005,
                    severity=ValidationSeverity.ERROR,
                    message=f"No supporting document found for {match.reference.document_type.value} {match.reference.document_number}",
                    details={
                        "document_type": match.reference.document_type.value,
                        "document_number": match.reference.document_number,
                        "source_section": match.reference.source_section
                    },
                    affected_pages=[],
                    recommendations=[f"Ensure {match.reference.document_type.value} document is included in PDF"]
                ))
            else:
                # Check match confidence
                if match.match_confidence < 0.7:
                    issues.append(ValidationIssue(
                        error_code=ErrorCode.E008,
                        severity=ValidationSeverity.WARNING,
                        message=f"Low confidence document match ({match.match_confidence:.1%})",
                        details={
                            "reference_number": match.reference.document_number,
                            "supporting_number": match.supporting_document.document_number,
                            "confidence": match.match_confidence,
                            "match_reasons": match.match_reasons
                        },
                        affected_pages=[match.supporting_document.page_number],
                        recommendations=["Manually verify document numbers match"]
                    ))
                
                # Add validation errors from document matching
                for error in match.validation_errors:
                    issues.append(ValidationIssue(
                        error_code=ErrorCode.E008,
                        severity=ValidationSeverity.WARNING,
                        message=error,
                        details={
                            "document_type": match.reference.document_type.value,
                            "reference_number": match.reference.document_number,
                            "supporting_number": match.supporting_document.document_number if match.supporting_document else None
                        },
                        affected_pages=[match.supporting_document.page_number] if match.supporting_document else [],
                        recommendations=["Review document details for accuracy"]
                    ))
        
        return issues
    
    def _calculate_scores(self, i9_set: I9Set, validation_issues: List[ValidationIssue], 
                         document_matches: List[DocumentMatch]) -> Dict[str, float]:
        """Calculate various validation scores"""
        
        # Set completeness score
        completeness_score = 100.0
        if not i9_set.section_1_page:
            completeness_score -= 50.0
        if not i9_set.section_2_page:
            completeness_score -= 50.0
        
        # Document matching score
        if document_matches:
            matched_docs = len([m for m in document_matches if m.supporting_document])
            document_matching_score = (matched_docs / len(document_matches)) * 100.0
        else:
            document_matching_score = 0.0
        
        # Compliance score (based on validation issues)
        compliance_score = 100.0
        for issue in validation_issues:
            if issue.severity == ValidationSeverity.CRITICAL:
                compliance_score -= 25.0
            elif issue.severity == ValidationSeverity.ERROR:
                compliance_score -= 15.0
            elif issue.severity == ValidationSeverity.WARNING:
                compliance_score -= 5.0
        
        compliance_score = max(0.0, compliance_score)
        
        # Overall score (weighted average)
        overall_score = (
            completeness_score * 0.4 +
            document_matching_score * 0.3 +
            compliance_score * 0.3
        )
        
        return {
            'overall': overall_score,
            'completeness': completeness_score,
            'document_matching': document_matching_score,
            'compliance': compliance_score
        }
    
    def _determine_overall_status(self, validation_issues: List[ValidationIssue], 
                                scores: Dict[str, float]) -> str:
        """Determine overall validation status"""
        
        critical_issues = [i for i in validation_issues if i.severity == ValidationSeverity.CRITICAL]
        error_issues = [i for i in validation_issues if i.severity == ValidationSeverity.ERROR]
        
        if critical_issues:
            return "ERROR"
        elif error_issues or scores['overall'] < 80.0:
            return "PARTIAL_SUCCESS"
        else:
            return "SUCCESS"
