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
        'supplement_b_signature_date'
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
        """
        if not field_name:
            return ""
        
        # Convert to lowercase
        normalized = field_name.lower()
        
        # Replace spaces and hyphens with underscores
        normalized = re.sub(r'[\s\-]+', '_', normalized)
        
        # Remove special characters except underscores
        normalized = re.sub(r'[^a-z0-9_]', '', normalized)
        
        # Remove multiple consecutive underscores
        normalized = re.sub(r'_+', '_', normalized)
        
        # Remove leading/trailing underscores
        normalized = normalized.strip('_')
        
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
    
    def find_name_field(self, extracted_values: Dict, name_type: str) -> Optional[Tuple[str, str, float]]:
        """
        Find a specific name field (first_name, last_name, middle_name)
        
        Returns:
            (field_name, value, confidence_score) or None
        """
        if name_type not in self.NAME_PATTERNS:
            return None
        
        patterns = self.NAME_PATTERNS[name_type]
        best_match = None
        best_score = 0.0
        best_field = None
        
        for field_name, value in extracted_values.items():
            if not value or value in ['N/A', '', None]:
                continue
            
            pattern, score = self.find_best_match(field_name, patterns)
            if pattern and score > best_score:
                best_score = score
                best_match = value
                best_field = field_name
        
        if best_match:
            return (best_field, best_match, best_score)
        else:
            return None
    
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
