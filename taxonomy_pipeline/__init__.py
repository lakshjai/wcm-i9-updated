"""
Taxonomy-Based I-9 Pipeline
Complete end-to-end pipeline for I-9 document processing using LLM with taxonomy guidance
"""

__version__ = "1.0.0"
__author__ = "WCM I-9 Team"

from .taxonomy_extractor import TaxonomyExtractor
from .taxonomy_rubric_processor import TaxonomyRubricProcessor
from .taxonomy_prompts import TaxonomyPromptBuilder

__all__ = [
    'TaxonomyExtractor',
    'TaxonomyRubricProcessor',
    'TaxonomyPromptBuilder'
]
