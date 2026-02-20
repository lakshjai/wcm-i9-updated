#!/usr/bin/env python3
"""
Taxonomy-Guided Prompt Builder for I-9 Document Extraction
"""
import json
from pathlib import Path
from typing import Dict, List

class TaxonomyPromptBuilder:
    """
    Builds intelligent prompts that guide LLM to extract data according to taxonomy
    """
    
    def __init__(self, taxonomy_path: str):
        """Initialize with taxonomy"""
        with open(taxonomy_path) as f:
            self.taxonomy = json.load(f)
    
    def _get_optimized_taxonomy(self, optimization_level: str = 'smart') -> Dict:
        """
        Get optimized taxonomy based on level
        
        Args:
            optimization_level: 'full', 'smart', or 'minimal'
        """
        if optimization_level == 'full':
            return self.taxonomy
        
        elif optimization_level == 'smart':
            # Include essential parts with examples
            return {
                'version': self.taxonomy['version'],
                'forms': self._get_form_summary(),
                'documents': self._get_document_summary(),
                'match_strategies': self.taxonomy['metadata']['match_strategies'],
                'classification_rules': self.taxonomy['metadata']['classification_rules']
            }
        
        else:  # minimal
            return {
                'forms': list(self._get_all_form_canonicals()),
                'documents': list(self._get_all_document_canonicals())
            }
    
    def _get_form_summary(self) -> Dict:
        """Get concise form type summary"""
        forms = {}
        
        # Primary forms
        for form_key, form_data in self.taxonomy['taxonomy']['i9_forms']['primary_forms'].items():
            forms[form_data['canonical']] = {
                'identifiers': form_data['identifiers'][:3],  # Top 3
                'type': 'primary',
                'classification': form_data.get('classification_hints', [])
            }
        
        # Supplement forms
        for form_key, form_data in self.taxonomy['taxonomy']['i9_forms']['supplement_forms'].items():
            forms[form_data['canonical']] = {
                'identifiers': form_data['identifiers'],
                'type': 'supplement',
                'classification': form_data.get('classification_hints', [])
            }
        
        return forms
    
    def _get_document_summary(self) -> Dict:
        """Get concise document summary"""
        documents = {}
        
        # List A documents
        for doc_key, doc_data in self.taxonomy['taxonomy']['list_a_documents'].items():
            documents[doc_data['canonical']] = {
                'identifiers': doc_data['identifiers'][:3],
                'variations': doc_data['variations'][:5],
                'list': 'A',
                'expiration': doc_data.get('expiration')
            }
        
        # List B documents (top 5 most common)
        common_list_b = ['drivers_license', 'us_military_id', 'school_id', 'state_id', 'us_passport']
        for doc_key in common_list_b:
            if doc_key in self.taxonomy['taxonomy']['list_b_documents']:
                doc_data = self.taxonomy['taxonomy']['list_b_documents'][doc_key]
                documents[doc_data['canonical']] = {
                    'identifiers': doc_data['identifiers'][:3],
                    'variations': doc_data['variations'][:5],
                    'list': 'B'
                }
        
        # List C documents
        for doc_key, doc_data in self.taxonomy['taxonomy']['list_c_documents'].items():
            documents[doc_data['canonical']] = {
                'identifiers': doc_data['identifiers'][:3],
                'variations': doc_data['variations'][:5],
                'list': 'C',
                'expiration': doc_data.get('expiration')
            }
        
        return documents
    
    def _get_all_form_canonicals(self) -> List[str]:
        """Get all canonical form names"""
        canonicals = []
        for form_data in self.taxonomy['taxonomy']['i9_forms']['primary_forms'].values():
            canonicals.append(form_data['canonical'])
        for form_data in self.taxonomy['taxonomy']['i9_forms']['supplement_forms'].values():
            canonicals.append(form_data['canonical'])
        return canonicals
    
    def _get_all_document_canonicals(self) -> List[str]:
        """Get all canonical document names"""
        canonicals = []
        for doc_data in self.taxonomy['taxonomy']['list_a_documents'].values():
            canonicals.append(doc_data['canonical'])
        for doc_data in self.taxonomy['taxonomy']['list_b_documents'].values():
            canonicals.append(doc_data['canonical'])
        for doc_data in self.taxonomy['taxonomy']['list_c_documents'].values():
            canonicals.append(doc_data['canonical'])
        return canonicals
    
    def _get_few_shot_examples(self) -> List[Dict]:
        """Get few-shot examples for better extraction"""
        return [
            {
                'input': 'Form I-9 Supplement B, Reverification and Rehire',
                'output': {
                    'canonical': 'i9_supplement_b',
                    'confidence': 1.0,
                    'matched_identifiers': ['Supplement B', 'Reverification and Rehire']
                }
            },
            {
                'input': 'Permanent Resident Card (Green Card)',
                'output': {
                    'canonical': 'permanent_resident_card_i551',
                    'confidence': 1.0,
                    'list': 'A',
                    'matched_identifiers': ['Permanent Resident Card', 'Green Card']
                }
            },
            {
                'input': 'Employment Authorization Document',
                'output': {
                    'canonical': 'employment_authorization_document_i766',
                    'confidence': 1.0,
                    'list': 'A',
                    'matched_identifiers': ['Employment Authorization Document', 'EAD']
                }
            },
            {
                'input': 'Driver\'s License',
                'output': {
                    'canonical': 'drivers_license_or_state_id',
                    'confidence': 1.0,
                    'list': 'B',
                    'matched_identifiers': ['Driver\'s License']
                }
            },
            {
                'input': 'Social Security Card',
                'output': {
                    'canonical': 'social_security_card_unrestricted',
                    'confidence': 1.0,
                    'list': 'C',
                    'matched_identifiers': ['Social Security Card', 'SSN Card']
                }
            }
        ]
    
    def build_extraction_prompt(self, optimization_level: str = 'smart', 
                               include_examples: bool = True) -> str:
        """
        Build the complete extraction prompt with taxonomy
        
        Args:
            optimization_level: 'full', 'smart', or 'minimal'
            include_examples: Whether to include few-shot examples
        """
        taxonomy_data = self._get_optimized_taxonomy(optimization_level)
        
        prompt = f"""# I-9 Document Data Extraction with Taxonomy Normalization

You are an expert I-9 document analyzer. Your task is to extract information from I-9 forms and supporting documents, and normalize all extracted data according to the provided taxonomy.

## Taxonomy Reference (Version {self.taxonomy['version']})

### Form Types
{json.dumps(taxonomy_data.get('forms', {}), indent=2)}

### Document Types
{json.dumps(taxonomy_data.get('documents', {}), indent=2)}

### Match Strategies
{json.dumps(taxonomy_data.get('match_strategies', {}), indent=2)}

### Classification Rules
{json.dumps(taxonomy_data.get('classification_rules', {}), indent=2)}

## Extraction Instructions

1. **Identify Form Type**: Match the page to a canonical form type from the taxonomy
2. **Extract All Fields**: Extract all visible text fields and their values
3. **Normalize Document Names**: Convert document titles to canonical names from taxonomy
4. **Normalize Field Names**: Use consistent field naming (e.g., signature_date, document_title)
5. **Provide Confidence Scores**: Rate your confidence (0.0 to 1.0) for each extraction
6. **Track Variations**: Note which identifiers/variations you matched

## Output Format

Return a JSON object with this structure:

```json
{{
  "page_number": <int>,
  "page_type": {{
    "canonical": "<canonical_form_name>",
    "confidence": <float>,
    "original_text": "<original page title>",
    "matched_identifiers": ["<identifier1>", "<identifier2>"]
  }},
  "fields": {{
    "<field_name>": {{
      "value": "<extracted_value>",
      "confidence": <float>,
      "field_type": "<data_type>"
    }}
  }},
  "documents": [
    {{
      "canonical": "<canonical_document_name>",
      "original_text": "<original document title>",
      "confidence": <float>,
      "list": "<A|B|C>",
      "document_number": "<if present>",
      "expiration_date": "<if present>",
      "matched_identifiers": ["<identifier1>", "<identifier2>"]
    }}
  ]
}}
```
"""

        if include_examples:
            prompt += "\n## Few-Shot Examples\n\n"
            examples = self._get_few_shot_examples()
            for i, example in enumerate(examples, 1):
                prompt += f"### Example {i}\n"
                prompt += f"**Input**: {example['input']}\n"
                prompt += f"**Output**: {json.dumps(example['output'], indent=2)}\n\n"
        
        prompt += """
## Important Rules

1. **Always use canonical names** from the taxonomy, never make up your own
2. **If uncertain**, provide lower confidence score and note the ambiguity
3. **Match flexibly**: Use fuzzy matching for variations (e.g., "Green Card" → "permanent_resident_card_i551")
4. **Extract everything**: Don't skip fields even if they seem unimportant
5. **Preserve original text**: Always keep the original text alongside canonical names
6. **Date format**: Keep dates as extracted (MM/DD/YYYY or other formats)
7. **Empty fields**: If a field is present but empty, note it with value: "N/A"

## Begin Extraction

Analyze the provided image and extract all information according to the taxonomy and instructions above.
"""
        
        return prompt
    
    def build_system_prompt(self) -> str:
        """Build system prompt for the LLM"""
        return """You are an expert I-9 document analyzer with deep knowledge of U.S. immigration and employment verification forms. You excel at:

1. Identifying different I-9 form versions and types (Standard I-9, Supplement B, Section 3)
2. Recognizing all List A, B, and C documents and their variations
3. Extracting data with high accuracy and appropriate confidence scores
4. Normalizing extracted data to canonical taxonomy names
5. Handling edge cases like partial forms, handwritten text, and poor quality scans

Always prioritize accuracy and provide detailed confidence scores for your extractions."""
    
    def get_output_schema(self) -> Dict:
        """Get JSON schema for structured output"""
        return {
            "type": "object",
            "properties": {
                "page_number": {"type": "integer"},
                "page_type": {
                    "type": "object",
                    "properties": {
                        "canonical": {"type": "string"},
                        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                        "original_text": {"type": "string"},
                        "matched_identifiers": {"type": "array", "items": {"type": "string"}}
                    },
                    "required": ["canonical", "confidence"]
                },
                "fields": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "object",
                        "properties": {
                            "value": {"type": "string"},
                            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                            "field_type": {"type": "string"}
                        }
                    }
                },
                "documents": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "canonical": {"type": "string"},
                            "original_text": {"type": "string"},
                            "confidence": {"type": "number"},
                            "list": {"type": "string", "enum": ["A", "B", "C"]},
                            "document_number": {"type": "string"},
                            "expiration_date": {"type": "string"},
                            "matched_identifiers": {"type": "array"}
                        }
                    }
                }
            },
            "required": ["page_number", "page_type", "fields"]
        }
