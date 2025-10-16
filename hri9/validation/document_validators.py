#!/usr/bin/env python3
"""
Document Validation Specialists

This module provides specialized validators for document-related validation
including attachment verification and document matching.
"""

from typing import List, Dict, Any, Optional, Tuple
import re
from datetime import datetime, timedelta

from ..core.models import DocumentInfo, I9FormData, ValidationResult, PDFAnalysis
from ..utils.logging_config import logger
from .validators import BaseValidator


class DocumentValidator(BaseValidator):
    """Validator for document-specific rules and requirements"""
    
    def __init__(self):
        super().__init__("document_validator", "medium")
        self.acceptable_list_a_docs = [
            "U.S. Passport",
            "U.S. Passport Card", 
            "Permanent Resident Card",
            "Employment Authorization Document"
        ]
        
        self.acceptable_list_b_docs = [
            "Driver's License",
            "U.S. Passport",
            "U.S. Passport Card"
        ]
        
        self.acceptable_list_c_docs = [
            "Social Security Card",
            "Birth Certificate",
            "U.S. Passport",
            "U.S. Passport Card"
        ]
    
    def validate(self, data: Any) -> ValidationResult:
        """Main validation entry point"""
        if isinstance(data, list):
            return self.validate_document_list(data)
        elif isinstance(data, DocumentInfo):
            return self.validate_single_document(data)
        else:
            return self.create_result(
                False, 
                "Invalid data type for document validation"
            )
    
    def validate_document_list(self, documents: List[DocumentInfo]) -> ValidationResult:
        """Validate a list of documents for completeness and compliance"""
        
        if not documents:
            return self.create_result(
                False,
                "No documents provided for validation",
                severity="high"
            )
        
        issues = []
        recommendations = []
        
        # Check for duplicate documents
        duplicates = self._find_duplicate_documents(documents)
        if duplicates:
            issues.append(f"Duplicate documents found: {', '.join(duplicates)}")
            recommendations.append("Remove duplicate document entries")
        
        # Validate each document
        invalid_docs = []
        for doc in documents:
            doc_validation = self.validate_single_document(doc)
            if not doc_validation.is_valid:
                invalid_docs.append(doc.document_type)
        
        if invalid_docs:
            issues.append(f"Invalid documents: {', '.join(invalid_docs)}")
        
        # Check List A/B+C combination rules
        list_combination_issues = self._validate_list_combinations(documents)
        issues.extend(list_combination_issues)
        
        is_valid = len(issues) == 0
        message = "All documents are valid" if is_valid else f"Document validation issues: {'; '.join(issues)}"
        
        return self.create_result(
            is_valid=is_valid,
            message=message,
            details={
                "total_documents": len(documents),
                "issues": issues,
                "duplicates": duplicates
            },
            recommendations=recommendations
        )
    
    def validate_single_document(self, document: DocumentInfo) -> ValidationResult:
        """Validate a single document"""
        
        issues = []
        
        # Check required fields
        if not document.document_type or document.document_type.strip() == "":
            issues.append("Missing document type")
        
        # Validate document number format if provided
        if document.document_number and document.document_number != "Not visible":
            if not self._validate_document_number_format(document):
                issues.append("Invalid document number format")
        
        # Validate expiration date format if provided (but not expiration status)
        if document.expiration_date and document.expiration_date != "Not visible":
            if not self._validate_date_format(document.expiration_date):
                issues.append("Invalid date format")
        
        # Check if document type is acceptable for its list category
        if document.list_category:
            if not self._is_acceptable_for_list(document.document_type, document.list_category):
                issues.append(f"Document not acceptable for List {document.list_category}")
        
        is_valid = len(issues) == 0
        message = f"Document {document.document_type} is valid" if is_valid else f"Issues: {'; '.join(issues)}"
        
        return self.create_result(
            is_valid=is_valid,
            message=message,
            details={
                "document_type": document.document_type,
                "issues": issues
            }
        )
    
    def _find_duplicate_documents(self, documents: List[DocumentInfo]) -> List[str]:
        """Find duplicate documents in the list"""
        seen = {}
        duplicates = []
        
        for doc in documents:
            key = (doc.document_type, doc.document_number)
            if key in seen:
                if doc.document_type not in duplicates:
                    duplicates.append(doc.document_type)
            else:
                seen[key] = doc
        
        return duplicates
    
    def _validate_document_number_format(self, document: DocumentInfo) -> bool:
        """Validate document number format based on document type"""
        
        doc_type = document.document_type.lower()
        doc_number = document.document_number
        
        # Passport numbers (6-9 alphanumeric characters)
        if "passport" in doc_type:
            return bool(re.match(r'^[A-Z0-9]{6,9}$', doc_number.upper()))
        
        # Driver's License (varies by state, but generally alphanumeric)
        elif "driver" in doc_type or "license" in doc_type:
            return bool(re.match(r'^[A-Z0-9]{5,20}$', doc_number.upper()))
        
        # Social Security (XXX-XX-XXXX or XXXXXXXXX)
        elif "social security" in doc_type:
            clean_ssn = doc_number.replace('-', '').replace(' ', '')
            return bool(re.match(r'^\d{9}$', clean_ssn))
        
        # Green Card/Permanent Resident Card
        elif "permanent resident" in doc_type or "green card" in doc_type:
            return bool(re.match(r'^[A-Z]{3}\d{10}$', doc_number.upper()))
        
        # Employment Authorization Document
        elif "employment authorization" in doc_type or "ead" in doc_type:
            return bool(re.match(r'^[A-Z0-9]{8,13}$', doc_number.upper()))
        
        # I-94 (11-digit number)
        elif "i-94" in doc_type:
            return bool(re.match(r'^\d{11}$', doc_number))
        
        # For other document types, accept any non-empty alphanumeric
        return bool(re.match(r'^[A-Z0-9\-\s]{1,50}$', doc_number.upper()))
    
    def _validate_date_format(self, date_string: str) -> bool:
        """Validate that date is properly formatted (but not expiration status)"""
        
        try:
            # Try to parse the date - only validate format, not expiration
            if '/' in date_string:
                datetime.strptime(date_string, "%m/%d/%Y")
                return True
            elif '-' in date_string:
                datetime.strptime(date_string, "%m-%d-%Y")
                return True
            else:
                return False
            
        except ValueError:
            return False
    
    def _is_acceptable_for_list(self, document_type: str, list_category: str) -> bool:
        """Check if document type is acceptable for the specified list category"""
        
        if list_category.upper() == 'A':
            return any(acceptable in document_type for acceptable in self.acceptable_list_a_docs)
        elif list_category.upper() == 'B':
            return any(acceptable in document_type for acceptable in self.acceptable_list_b_docs)
        elif list_category.upper() == 'C':
            return any(acceptable in document_type for acceptable in self.acceptable_list_c_docs)
        
        return False
    
    def _validate_list_combinations(self, documents: List[DocumentInfo]) -> List[str]:
        """Validate that document combinations follow List A or List B+C rules"""
        
        issues = []
        
        # Categorize documents by list
        list_a_docs = [d for d in documents if d.list_category == 'A']
        list_b_docs = [d for d in documents if d.list_category == 'B']
        list_c_docs = [d for d in documents if d.list_category == 'C']
        
        # Valid combinations:
        # 1. One List A document, OR
        # 2. One List B document AND one List C document
        
        has_list_a = len(list_a_docs) > 0
        has_list_b = len(list_b_docs) > 0
        has_list_c = len(list_c_docs) > 0
        
        if has_list_a:
            if has_list_b or has_list_c:
                issues.append("Cannot combine List A documents with List B or C documents")
        else:
            if not (has_list_b and has_list_c):
                issues.append("Must provide either one List A document OR both List B and List C documents")
        
        return issues


class AttachmentValidator(BaseValidator):
    """Validator for checking if documents are actually attached to the PDF"""
    
    def __init__(self):
        super().__init__("attachment_validator", "high")
    
    def validate(self, data: Tuple[List[DocumentInfo], PDFAnalysis]) -> ValidationResult:
        """Validate that listed documents are attached to the PDF"""
        
        documents, pdf_analysis = data
        
        if not documents:
            return self.create_result(
                True,
                "No documents to validate for attachment"
            )
        
        attached_count = 0
        missing_documents = []
        attachment_details = {}
        
        for doc in documents:
            is_attached = self._check_document_attachment(doc, pdf_analysis)
            doc.is_attached = is_attached
            
            if is_attached:
                attached_count += 1
            else:
                missing_documents.append(doc.document_type)
            
            attachment_details[doc.document_type] = {
                "attached": is_attached,
                "page_number": doc.page_number,
                "confidence": doc.confidence
            }
        
        total_documents = len(documents)
        attachment_rate = (attached_count / total_documents) * 100 if total_documents > 0 else 0
        
        is_valid = len(missing_documents) == 0
        
        if is_valid:
            message = f"All {total_documents} documents are attached to the PDF"
        else:
            message = f"{len(missing_documents)} out of {total_documents} documents are missing: {', '.join(missing_documents)}"
        
        recommendations = []
        if missing_documents:
            recommendations.append("Ensure all listed documents are included in the PDF file")
            recommendations.append("Verify document scanning quality and completeness")
        
        return self.create_result(
            is_valid=is_valid,
            message=message,
            details={
                "total_documents": total_documents,
                "attached_documents": attached_count,
                "missing_documents": missing_documents,
                "attachment_rate": attachment_rate,
                "attachment_details": attachment_details
            },
            recommendations=recommendations
        )
    
    def _check_document_attachment(self, document: DocumentInfo, pdf_analysis: PDFAnalysis) -> bool:
        """Check if a specific document is attached to the PDF"""
        
        # Check if document type exists in PDF catalog
        if pdf_analysis.has_document_type(document.document_type):
            
            # For more specific matching, check document numbers if available
            if document.document_number and document.document_number != "Not visible":
                matching_docs = pdf_analysis.get_documents_by_type(document.document_type)
                
                # Check if any matching document has the same number
                for pdf_doc in matching_docs:
                    if pdf_doc.document_number and pdf_doc.document_number != "Not visible":
                        if self._document_numbers_match(document.document_number, pdf_doc.document_number):
                            return True
                
                # If no number match found but document type exists, consider it attached
                # (document might be present but number not clearly visible)
                return True
            else:
                # If no document number to match, presence of document type is sufficient
                return True
        
        return False
    
    def _document_numbers_match(self, listed_number: str, pdf_number: str) -> bool:
        """Check if two document numbers match (with normalization)"""
        
        if not listed_number or not pdf_number:
            return False
        
        # Normalize both numbers (remove spaces, hyphens, convert to uppercase)
        norm_listed = re.sub(r'[\s\-]', '', listed_number.upper())
        norm_pdf = re.sub(r'[\s\-]', '', pdf_number.upper())
        
        # Exact match
        if norm_listed == norm_pdf:
            return True
        
        # Partial match for cases where PDF extraction might be incomplete
        if len(norm_listed) >= 6 and len(norm_pdf) >= 6:
            # Check if one is a substring of the other (for partial OCR results)
            return norm_listed in norm_pdf or norm_pdf in norm_listed
        
        return False


class DateMatchValidator(BaseValidator):
    """Validator for checking date consistency between forms and documents"""
    
    def __init__(self):
        super().__init__("date_match_validator", "medium")
    
    def validate(self, data: Tuple[I9FormData, List[DocumentInfo]]) -> ValidationResult:
        """Validate date consistency between I-9 form and supporting documents"""
        
        form_data, documents = data
        
        # Get alien work authorization expiration date
        alien_expiration = form_data.get_alien_expiration_date()
        
        if not alien_expiration or alien_expiration == "Not visible":
            # If no alien expiration date, validation passes (not applicable)
            return self.create_result(
                True,
                "No alien work authorization expiration date to validate"
            )
        
        matching_documents = []
        non_matching_documents = []
        date_details = {}
        
        for doc in documents:
            if doc.expiration_date and doc.expiration_date != "Not visible":
                matches = self._dates_match(alien_expiration, doc.expiration_date)
                
                date_details[doc.document_type] = {
                    "document_expiration": doc.expiration_date,
                    "matches_alien_date": matches,
                    "alien_expiration_date": alien_expiration
                }
                
                if matches:
                    matching_documents.append(doc.document_type)
                else:
                    non_matching_documents.append(doc.document_type)
        
        has_matches = len(matching_documents) > 0
        
        if has_matches:
            message = f"Alien expiration date matches {len(matching_documents)} document(s): {', '.join(matching_documents)}"
        else:
            message = f"Alien expiration date ({alien_expiration}) does not match any document expiration dates"
        
        recommendations = []
        if not has_matches and non_matching_documents:
            recommendations.append("Verify alien work authorization expiration date accuracy")
            recommendations.append("Check if document expiration dates are correctly extracted")
            recommendations.append("Consider if documents are for different authorization periods")
        
        return self.create_result(
            is_valid=has_matches,
            message=message,
            details={
                "alien_expiration_date": alien_expiration,
                "matching_documents": matching_documents,
                "non_matching_documents": non_matching_documents,
                "total_documents_with_dates": len(matching_documents) + len(non_matching_documents),
                "date_details": date_details
            },
            recommendations=recommendations
        )
    
    def _dates_match(self, date1: str, date2: str) -> bool:
        """Check if two dates match with some tolerance for formatting differences"""
        
        if not date1 or not date2:
            return False
        
        # Normalize dates by removing separators
        norm_date1 = re.sub(r'[/\-\s]', '', date1.strip())
        norm_date2 = re.sub(r'[/\-\s]', '', date2.strip())
        
        # Direct string match
        if norm_date1 == norm_date2:
            return True
        
        # Try parsing and comparing as dates (allows for different formats)
        try:
            parsed_date1 = self._parse_flexible_date(date1)
            parsed_date2 = self._parse_flexible_date(date2)
            
            if parsed_date1 and parsed_date2:
                return parsed_date1 == parsed_date2
                
        except Exception:
            pass
        
        return False
    
    def _parse_flexible_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string with multiple format attempts"""
        
        formats = [
            "%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d",
            "%m/%d/%y", "%m-%d-%y", "%y-%m-%d"
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        
        return None
