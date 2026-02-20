#!/usr/bin/env python3
"""
Fuzzy Field Matcher - Intelligent field name matching with similarity scoring
"""
from typing import Dict, List, Optional, Tuple
from difflib import SequenceMatcher
import re

class FuzzyFieldMatcher:
    """
    Matches field names using fuzzy logic instead of exact string matching
    """
    
    # Field name patterns with variations
    SIGNATURE_PATTERNS = [
        'signature_date',
        'employer_signature_date',
        'employee_signature_date',
        'reverification_signature_date',
        'date_of_signature',
        'date_of_employer_signature',
        'date_of_employee_signature',
        'signature_date_employer',
        'signature_date_employee',
        'employer_sig_date',
        'employee_sig_date',
        'date_signed',
        'reverification_date_signed',
        'section_3_signature_date',
        'supplement_b_signature_date',
        # Wu, Qianyi variations
        'section_3_employer_signature_date',
        'employer_signature_date_section_3',
        'reverification_employer_signature_date',
        'employer_signature_date_reverification',
        'signature_date_section_3',
        'reverification_signature',
        'employer_reverification_signature_date',
        'section_3_date_signed',
        'date_signed_section_3'
    ]
    
    DOCUMENT_TITLE_PATTERNS = [
        'document_title',
        'reverification_document_title',
        'document_name',
        'document_type',
        'list_a_document_title',
        'list_b_document_title',
        'list_c_document_title',
        'section_2_document_title',
        'section_3_document_title'
    ]
    
    EXPIRY_DATE_PATTERNS = [
        'expiration_date',
        'document_expiration_date',
        'expiry_date',
        'document_expiry_date',
        'work_authorization_expiry_date',
        'work_until_date',
        'alien_authorized_to_work_until_date',
        'authorization_expiry',
        'valid_until',
        'expires_on'
    ]
    
    NAME_PATTERNS = {
        'first_name': ['first_name', 'employee_first_name', 'given_name', 'first', 'fname'],
        'last_name': ['last_name', 'employee_last_name', 'family_name', 'surname', 'last', 'lname'],
        'middle_name': ['middle_name', 'employee_middle_name', 'middle_initial', 'middle', 'mname', 'mi']
    }
    
    # SSN patterns
    SSN_PATTERNS = [
        'ssn',
        'social_security_number',
        'employee_ssn',
        'us_social_security_number',
        'employee_social_security_number',
        'social_security',
        'ss_number'
    ]
    
    # Date of Birth patterns
    DOB_PATTERNS = [
        'date_of_birth',
        'dob',
        'employee_date_of_birth',
        'employee_dob',
        'birth_date',
        'birthdate'
    ]
    
    # Address patterns
    ADDRESS_PATTERNS = {
        'street': ['street_address', 'address', 'employee_address', 'street', 'address_line_1', 'address_line1'],
        'apt': ['apt_number', 'apartment_number', 'apt', 'apartment', 'unit', 'suite'],
        'city': ['city', 'employee_city', 'city_or_town'],
        'state': ['state', 'employee_state', 'state_province'],
        'zip': ['zip_code', 'zipcode', 'zip', 'postal_code', 'employee_zip_code']
    }
    
    # Citizenship patterns
    CITIZENSHIP_PATTERNS = [
        'citizenship_status',
        'employee_citizenship_status',
        'citizenship',
        'immigration_status',
        'status'
    ]
    
    # Document number patterns (for List A/B/C documents)
    DOCUMENT_NUMBER_PATTERNS = [
        'document_number',
        'document_no',
        'number',
        'issuing_authority',
        'list_a_document_number',
        'list_b_document_number',
        'list_c_document_number',
        'passport_number',
        'alien_registration_number',
        'uscis_number',
        'i94_admission_number',
        'form_i94_admission_number',
        'admission_number',
        'admission_record_number'
    ]
    
    # I-94 specific patterns
    I94_PATTERNS = [
        'form_i94_admission_number',
        'i94_admission_number',
        'admission_number',
        'admission_record_number',
        'i94_number',
        'arrival_departure_number'
    ]
    
    # Alien registration patterns
    ALIEN_REGISTRATION_PATTERNS = [
        'alien_registration_number',
        'alien_registration_number_uscis',
        'uscis_number',
        'a_number',
        'alien_number',
        'registration_number'
    ]
    
    # Foreign passport patterns
    PASSPORT_PATTERNS = [
        'foreign_passport_number',
        'passport_number',
        'passport_no',
        'country_of_issuance',
        'issuing_country'
    ]
    
    # Employment date patterns
    EMPLOYMENT_DATE_PATTERNS = [
        'first_day_of_employment',
        'employment_start_date',
        'hire_date',
        'start_date',
        'date_of_hire'
    ]
    
    def __init__(self, similarity_threshold: float = 0.6):
        """
        Initialize fuzzy matcher
        
        Args:
            similarity_threshold: Minimum similarity score (0.0 to 1.0) to consider a match
        """
        self.similarity_threshold = similarity_threshold
    
    def normalize_field_name(self, field_name: str) -> str:
        """
        Normalize field name for comparison
        - Convert to lowercase
        - Replace spaces/hyphens with underscores
        - Remove special characters
        - Strip numbered suffixes (e.g., _1, _2, _3) for better matching
        """
        if not field_name:
            return ""
        
        # Convert to lowercase
        normalized = field_name.lower()
        
        # Replace spaces and hyphens with underscores
        normalized = re.sub(r'[\s\-]+', '_', normalized)
        
        # Remove special characters except underscores and digits
        normalized = re.sub(r'[^a-z0-9_]', '', normalized)
        
        # Remove multiple consecutive underscores
        normalized = re.sub(r'_+', '_', normalized)
        
        # Remove leading/trailing underscores
        normalized = normalized.strip('_')
        
        # CRITICAL: Strip numbered suffixes like _1, _2, _3 at the end
        # This allows employer_signature_date_1 to match employer_signature_date
        normalized = re.sub(r'_\d+$', '', normalized)
        
        return normalized
    
    def calculate_similarity(self, str1: str, str2: str) -> float:
        """
        Calculate similarity between two strings using multiple methods
        Returns score between 0.0 and 1.0
        """
        if not str1 or not str2:
            return 0.0
        
        # Normalize both strings
        norm1 = self.normalize_field_name(str1)
        norm2 = self.normalize_field_name(str2)
        
        if norm1 == norm2:
            return 1.0
        
        # Method 1: Sequence matching (overall similarity)
        seq_score = SequenceMatcher(None, norm1, norm2).ratio()
        
        # Method 2: Substring matching (one contains the other)
        if norm1 in norm2 or norm2 in norm1:
            # Bonus for substring match
            substring_score = 0.8 + (0.2 * seq_score)
        else:
            substring_score = seq_score
        
        # Method 3: Word-level matching (split by underscore)
        words1 = set(norm1.split('_'))
        words2 = set(norm2.split('_'))
        
        if words1 and words2:
            common_words = words1.intersection(words2)
            word_score = len(common_words) / max(len(words1), len(words2))
        else:
            word_score = 0.0
        
        # Weighted combination
        final_score = (seq_score * 0.4) + (substring_score * 0.3) + (word_score * 0.3)
        
        return min(final_score, 1.0)
    
    def find_best_match(self, field_name: str, patterns: List[str]) -> Tuple[Optional[str], float]:
        """
        Find the best matching pattern for a field name
        
        Returns:
            (best_pattern, similarity_score) or (None, 0.0) if no match above threshold
        """
        best_match = None
        best_score = 0.0
        
        for pattern in patterns:
            score = self.calculate_similarity(field_name, pattern)
            if score > best_score:
                best_score = score
                best_match = pattern
        
        if best_score >= self.similarity_threshold:
            return best_match, best_score
        else:
            return None, 0.0
    
    def find_signature_fields(self, extracted_values: Dict) -> List[Tuple[str, str, float]]:
        """
        Find all signature-related fields in extracted values
        
        Returns:
            List of (field_name, value, confidence_score) tuples
        """
        matches = []
        
        for field_name, value in extracted_values.items():
            if not value or value in ['N/A', '', None]:
                continue
            
            pattern, score = self.find_best_match(field_name, self.SIGNATURE_PATTERNS)
            if pattern:
                matches.append((field_name, value, score))
        
        return matches
    
    def find_document_title_fields(self, extracted_values: Dict) -> List[Tuple[str, str, float]]:
        """
        Find all document title fields in extracted values
        
        Returns:
            List of (field_name, value, confidence_score) tuples
        """
        matches = []
        
        for field_name, value in extracted_values.items():
            if not value or value in ['N/A', '', None]:
                continue
            
            pattern, score = self.find_best_match(field_name, self.DOCUMENT_TITLE_PATTERNS)
            if pattern:
                matches.append((field_name, value, score))
        
        return matches
    
    def find_expiry_date_fields(self, extracted_values: Dict) -> List[Tuple[str, str, float]]:
        """
        Find all expiry date fields in extracted values
        
        Returns:
            List of (field_name, value, confidence_score) tuples
        """
        matches = []
        
        for field_name, value in extracted_values.items():
            if not value or value in ['N/A', '', None]:
                continue
            
            pattern, score = self.find_best_match(field_name, self.EXPIRY_DATE_PATTERNS)
            if pattern:
                matches.append((field_name, value, score))
        
        return matches
    
    def find_ssn_field(self, extracted_values: Dict) -> Optional[Tuple[str, str, float]]:
        """Find SSN field in extracted values"""
        matches = self.find_fields_by_patterns(extracted_values, self.SSN_PATTERNS)
        return matches[0] if matches else None
    
    def find_dob_field(self, extracted_values: Dict) -> Optional[Tuple[str, str, float]]:
        """Find date of birth field in extracted values"""
        matches = self.find_fields_by_patterns(extracted_values, self.DOB_PATTERNS)
        return matches[0] if matches else None
    
    def find_address_field(self, extracted_values: Dict, address_type: str) -> Optional[Tuple[str, str, float]]:
        """Find address field (street, apt, city, state, zip) in extracted values"""
        if address_type not in self.ADDRESS_PATTERNS:
            return None
        patterns = self.ADDRESS_PATTERNS[address_type]
        matches = self.find_fields_by_patterns(extracted_values, patterns)
        return matches[0] if matches else None
    
    def find_citizenship_field(self, extracted_values: Dict) -> Optional[Tuple[str, str, float]]:
        """Find citizenship status field in extracted values"""
        matches = self.find_fields_by_patterns(extracted_values, self.CITIZENSHIP_PATTERNS)
        return matches[0] if matches else None
    
    def find_i94_field(self, extracted_values: Dict) -> Optional[Tuple[str, str, float]]:
        """Find I-94 admission number field in extracted values"""
        matches = self.find_fields_by_patterns(extracted_values, self.I94_PATTERNS)
        return matches[0] if matches else None
    
    def find_alien_registration_field(self, extracted_values: Dict) -> Optional[Tuple[str, str, float]]:
        """Find alien registration number field in extracted values"""
        matches = self.find_fields_by_patterns(extracted_values, self.ALIEN_REGISTRATION_PATTERNS)
        return matches[0] if matches else None
    
    def find_passport_number_field(self, extracted_values: Dict) -> Optional[Tuple[str, str, float]]:
        """Find passport number field in extracted values"""
        matches = self.find_fields_by_patterns(extracted_values, self.PASSPORT_PATTERNS)
        return matches[0] if matches else None
    
    def find_employment_date_field(self, extracted_values: Dict) -> Optional[Tuple[str, str, float]]:
        """Find first day of employment field in extracted values"""
        matches = self.find_fields_by_patterns(extracted_values, self.EMPLOYMENT_DATE_PATTERNS)
        return matches[0] if matches else None
    
    def find_document_number_fields(self, extracted_values: Dict) -> List[Tuple[str, str, float]]:
        """Find all document number fields in extracted values"""
        return self.find_fields_by_patterns(extracted_values, self.DOCUMENT_NUMBER_PATTERNS)
    
    def find_fields_by_patterns(self, extracted_values: Dict, patterns: List[str]) -> List[Tuple[str, str, float]]:
        """Generic method to find fields matching a list of patterns"""
        matches = []
        for field_name, value in extracted_values.items():
            if not value or value in ['N/A', '', None]:
                continue
            
            pattern, score = self.find_best_match(field_name, patterns)
            if pattern:
                matches.append((field_name, value, score))
        
        return matches
    
    def find_name_field(self, extracted_values: Dict, name_type: str) -> Optional[Tuple[str, str, float]]:
        """
        Find a specific name field (first_name, last_name, middle_name)
        
        Returns:
            (field_name, value, confidence_score) or None
        """
        if name_type not in self.NAME_PATTERNS:
            return None
        
        patterns = self.NAME_PATTERNS[name_type]
        matches = self.find_fields_by_patterns(extracted_values, patterns)
        return matches[0] if matches else None
    
    def extract_any_signature_date(self, extracted_values: Dict) -> Optional[Tuple[str, str, float]]:
        """
        Extract any signature date from extracted values
        
        Returns:
            (field_name, date_value, confidence_score) or None
        """
        matches = self.find_signature_fields(extracted_values)
        
        if not matches:
            return None
        
        # Return the match with highest confidence
        return max(matches, key=lambda x: x[2])
    
    def extract_any_document_title(self, extracted_values: Dict) -> Optional[Tuple[str, str, float]]:
        """
        Extract any document title from extracted values
        
        Returns:
            (field_name, title_value, confidence_score) or None
        """
        matches = self.find_document_title_fields(extracted_values)
        
        if not matches:
            return None
        
        # Return the match with highest confidence
        return max(matches, key=lambda x: x[2])
    
    def has_meaningful_content(self, extracted_values: Dict, 
                              required_fields: List[str] = None) -> Tuple[bool, float]:
        """
        Check if extracted values have meaningful content
        
        Args:
            extracted_values: Dictionary of extracted field values
            required_fields: List of field types to check (e.g., ['signature', 'document_title', 'name'])
        
        Returns:
            (has_content, confidence_score)
        """
        if not required_fields:
            required_fields = ['signature', 'document_title']
        
        found_fields = []
        scores = []
        
        for field_type in required_fields:
            if field_type == 'signature':
                result = self.extract_any_signature_date(extracted_values)
                if result:
                    found_fields.append(field_type)
                    scores.append(result[2])
            
            elif field_type == 'document_title':
                result = self.extract_any_document_title(extracted_values)
                if result:
                    found_fields.append(field_type)
                    scores.append(result[2])
            
            elif field_type in ['first_name', 'last_name', 'middle_name']:
                result = self.find_name_field(extracted_values, field_type)
                if result:
                    found_fields.append(field_type)
                    scores.append(result[2])
        
        has_content = len(found_fields) >= len(required_fields) * 0.5  # At least 50% of required fields
        avg_confidence = sum(scores) / len(scores) if scores else 0.0
        
        return has_content, avg_confidence


# Convenience functions for backward compatibility
def fuzzy_find_signature(extracted_values: Dict, threshold: float = 0.6) -> Optional[str]:
    """Find any signature date field using fuzzy matching"""
    matcher = FuzzyFieldMatcher(similarity_threshold=threshold)
    result = matcher.extract_any_signature_date(extracted_values)
    return result[1] if result else None


def fuzzy_find_document_title(extracted_values: Dict, threshold: float = 0.6) -> Optional[str]:
    """Find any document title field using fuzzy matching"""
    matcher = FuzzyFieldMatcher(similarity_threshold=threshold)
    result = matcher.extract_any_document_title(extracted_values)
    return result[1] if result else None
