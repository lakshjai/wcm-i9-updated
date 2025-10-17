#!/usr/bin/env python3
"""
Taxonomy Loader - Loads and provides query methods for I-9 taxonomy
"""
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from difflib import SequenceMatcher

class TaxonomyLoader:
    """
    Loads I-9 taxonomy and provides intelligent matching methods
    """
    
    def __init__(self, taxonomy_path: str = "docs/i9_taxonomy.json"):
        """Initialize and load taxonomy"""
        self.taxonomy_path = Path(taxonomy_path)
        self.taxonomy = self._load_taxonomy()
        self.match_strategies = self.taxonomy.get('metadata', {}).get('match_strategies', {})
        self.confidence_scoring = self.taxonomy.get('metadata', {}).get('confidence_scoring', {})
        
        # Build lookup indexes for fast matching
        self._build_indexes()
    
    def _load_taxonomy(self) -> Dict:
        """Load taxonomy from JSON file"""
        with open(self.taxonomy_path) as f:
            return json.load(f)
    
    def _build_indexes(self):
        """Build indexes for fast lookups"""
        # Document index: maps variations to canonical names
        self.document_index = {}
        
        # Index List A documents
        for doc_key, doc_data in self.taxonomy['taxonomy'].get('list_a_documents', {}).items():
            canonical = doc_data.get('canonical', doc_key)
            
            # Add identifiers
            for identifier in doc_data.get('identifiers', []):
                self.document_index[identifier.lower()] = {
                    'canonical': canonical,
                    'type': 'list_a',
                    'data': doc_data
                }
            
            # Add variations
            for variation in doc_data.get('variations', []):
                self.document_index[variation.lower()] = {
                    'canonical': canonical,
                    'type': 'list_a',
                    'data': doc_data
                }
        
        # Index List B documents
        for doc_key, doc_data in self.taxonomy['taxonomy'].get('list_b_documents', {}).items():
            canonical = doc_data.get('canonical', doc_key)
            
            for identifier in doc_data.get('identifiers', []):
                self.document_index[identifier.lower()] = {
                    'canonical': canonical,
                    'type': 'list_b',
                    'data': doc_data
                }
            
            for variation in doc_data.get('variations', []):
                self.document_index[variation.lower()] = {
                    'canonical': canonical,
                    'type': 'list_b',
                    'data': doc_data
                }
        
        # Index List C documents
        for doc_key, doc_data in self.taxonomy['taxonomy'].get('list_c_documents', {}).items():
            canonical = doc_data.get('canonical', doc_key)
            
            for identifier in doc_data.get('identifiers', []):
                self.document_index[identifier.lower()] = {
                    'canonical': canonical,
                    'type': 'list_c',
                    'data': doc_data
                }
            
            for variation in doc_data.get('variations', []):
                self.document_index[variation.lower()] = {
                    'canonical': canonical,
                    'type': 'list_c',
                    'data': doc_data
                }
        
        # Form type index
        self.form_index = {}
        
        # Index primary forms
        for form_key, form_data in self.taxonomy['taxonomy']['i9_forms'].get('primary_forms', {}).items():
            canonical = form_data.get('canonical', form_key)
            
            for identifier in form_data.get('identifiers', []):
                self.form_index[identifier.lower()] = {
                    'canonical': canonical,
                    'type': 'primary',
                    'data': form_data
                }
            
            for variation in form_data.get('variations', []):
                self.form_index[variation.lower()] = {
                    'canonical': canonical,
                    'type': 'primary',
                    'data': form_data
                }
        
        # Index supplement forms
        for form_key, form_data in self.taxonomy['taxonomy']['i9_forms'].get('supplement_forms', {}).items():
            canonical = form_data.get('canonical', form_key)
            
            for identifier in form_data.get('identifiers', []):
                self.form_index[identifier.lower()] = {
                    'canonical': canonical,
                    'type': 'supplement',
                    'data': form_data
                }
            
            for variation in form_data.get('variations', []):
                self.form_index[variation.lower()] = {
                    'canonical': canonical,
                    'type': 'supplement',
                    'data': form_data
                }
    
    def match_document(self, document_text: str, threshold: float = 0.75) -> Optional[Dict]:
        """
        Match document text to taxonomy using multi-strategy matching
        
        Returns:
            {
                'canonical': str,
                'type': str (list_a, list_b, list_c),
                'confidence': float,
                'match_strategy': str,
                'data': dict (full taxonomy entry)
            }
        """
        if not document_text:
            return None
        
        text_lower = document_text.lower().strip()
        
        # Strategy 1: Exact match (weight: 1.0)
        if text_lower in self.document_index:
            result = self.document_index[text_lower].copy()
            result['confidence'] = 1.0
            result['match_strategy'] = 'exact_match'
            return result
        
        # Strategy 2: Pattern match for form numbers (weight: 0.9)
        # Look for patterns like I-551, I-766, etc.
        import re
        form_pattern = r'\b(i-?\d{3,4}[a-z]?)\b'
        matches = re.findall(form_pattern, text_lower)
        if matches:
            for match in matches:
                normalized = match.replace('i-', 'i-').replace('i', 'i-')
                if normalized in self.document_index:
                    result = self.document_index[normalized].copy()
                    result['confidence'] = 0.9
                    result['match_strategy'] = 'pattern_match'
                    return result
        
        # Strategy 3: Fuzzy match (weight: 0.85)
        best_match = None
        best_score = 0.0
        
        for key, value in self.document_index.items():
            similarity = SequenceMatcher(None, text_lower, key).ratio()
            
            # Also check if one contains the other
            if text_lower in key or key in text_lower:
                similarity = max(similarity, 0.8)
            
            if similarity > best_score and similarity >= threshold:
                best_score = similarity
                best_match = value.copy()
                best_match['confidence'] = similarity * 0.85  # Apply fuzzy weight
                best_match['match_strategy'] = 'fuzzy_match'
        
        if best_match:
            return best_match
        
        # Strategy 4: Keyword match (weight: 0.75)
        # Check if document text contains key terms
        text_words = set(text_lower.split())
        
        for key, value in self.document_index.items():
            key_words = set(key.split())
            common_words = text_words.intersection(key_words)
            
            if len(common_words) >= 2:  # At least 2 words match
                word_score = len(common_words) / max(len(text_words), len(key_words))
                if word_score > best_score:
                    best_score = word_score
                    best_match = value.copy()
                    best_match['confidence'] = word_score * 0.75  # Apply keyword weight
                    best_match['match_strategy'] = 'keyword_match'
        
        return best_match if best_match else None
    
    def match_form_type(self, form_text: str, threshold: float = 0.75) -> Optional[Dict]:
        """
        Match form type text to taxonomy
        
        Returns:
            {
                'canonical': str,
                'type': str (primary, supplement, section_3),
                'confidence': float,
                'match_strategy': str,
                'data': dict (full taxonomy entry)
            }
        """
        if not form_text:
            return None
        
        text_lower = form_text.lower().strip()
        
        # Exact match
        if text_lower in self.form_index:
            result = self.form_index[text_lower].copy()
            result['confidence'] = 1.0
            result['match_strategy'] = 'exact_match'
            return result
        
        # Fuzzy match
        best_match = None
        best_score = 0.0
        
        for key, value in self.form_index.items():
            similarity = SequenceMatcher(None, text_lower, key).ratio()
            
            if text_lower in key or key in text_lower:
                similarity = max(similarity, 0.8)
            
            if similarity > best_score and similarity >= threshold:
                best_score = similarity
                best_match = value.copy()
                best_match['confidence'] = similarity * 0.85
                best_match['match_strategy'] = 'fuzzy_match'
        
        return best_match if best_match else None
    
    def get_document_by_canonical(self, canonical_name: str) -> Optional[Dict]:
        """Get document data by canonical name"""
        # Search in all document lists
        for list_type in ['list_a_documents', 'list_b_documents', 'list_c_documents']:
            docs = self.taxonomy['taxonomy'].get(list_type, {})
            for doc_key, doc_data in docs.items():
                if doc_data.get('canonical') == canonical_name:
                    return doc_data
        return None
    
    def get_classification_rules(self) -> Dict:
        """Get classification rules from taxonomy"""
        return self.taxonomy.get('metadata', {}).get('classification_rules', {})
    
    def get_match_strategies(self) -> Dict:
        """Get match strategies configuration"""
        return self.match_strategies
    
    def get_confidence_levels(self) -> Dict:
        """Get confidence scoring thresholds"""
        return self.confidence_scoring
    
    def classify_confidence(self, score: float) -> str:
        """
        Classify confidence score into levels
        
        Returns: 'high', 'medium', 'low', or 'uncertain'
        """
        if score >= 0.9:
            return 'high'
        elif score >= 0.7:
            return 'medium'
        elif score >= 0.5:
            return 'low'
        else:
            return 'uncertain'
    
    def get_all_document_variations(self, list_type: Optional[str] = None) -> List[str]:
        """
        Get all document variations for a specific list or all lists
        
        Args:
            list_type: 'A', 'B', 'C', or None for all
        """
        variations = []
        
        lists_to_check = []
        if list_type:
            lists_to_check = [f'list_{list_type.lower()}_documents']
        else:
            lists_to_check = ['list_a_documents', 'list_b_documents', 'list_c_documents']
        
        for list_name in lists_to_check:
            docs = self.taxonomy['taxonomy'].get(list_name, {})
            for doc_data in docs.values():
                variations.extend(doc_data.get('identifiers', []))
                variations.extend(doc_data.get('variations', []))
        
        return list(set(variations))  # Remove duplicates
    
    def has_expiration(self, canonical_name: str) -> bool:
        """Check if a document type has an expiration date"""
        doc_data = self.get_document_by_canonical(canonical_name)
        if doc_data:
            return doc_data.get('expiration') == 'has_expiration_date'
        return False
    
    def get_document_list(self, canonical_name: str) -> Optional[str]:
        """Get which list (A, B, C) a document belongs to"""
        doc_data = self.get_document_by_canonical(canonical_name)
        if doc_data:
            return doc_data.get('list')
        return None


# Singleton instance
_taxonomy_instance = None

def get_taxonomy() -> TaxonomyLoader:
    """Get singleton taxonomy instance"""
    global _taxonomy_instance
    if _taxonomy_instance is None:
        _taxonomy_instance = TaxonomyLoader()
    return _taxonomy_instance
