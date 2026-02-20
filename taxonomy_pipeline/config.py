#!/usr/bin/env python3
"""
Configuration for Taxonomy-Based I-9 Pipeline
"""
import os
from pathlib import Path

# API Configuration
LLM_PROVIDER = os.getenv('LLM_PROVIDER', 'gemini')  # 'gemini' or 'anthropic'

# Gemini Configuration (via OpenAI-compatible endpoint - same as main app)
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')  # Same key as main app
OPENAI_BASE_URL = os.getenv('OPENAI_BASE_URL', 'https://api.openai.com/v1')  # Same endpoint as main app
GEMINI_MODEL = os.getenv('GEMINI_MODEL', 'google.gemini-2.5-pro')  # Same model as main app

# Anthropic Configuration (fallback)
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY', '')
ANTHROPIC_MODEL = 'claude-3-5-sonnet-20241022'

# Paths
BASE_DIR = Path(__file__).parent.parent
TAXONOMY_PATH = BASE_DIR / 'docs' / 'i9_taxonomy.json'
INPUT_DIR = BASE_DIR / 'input'
OUTPUT_DIR = Path(__file__).parent / 'output'
CATALOGS_DIR = OUTPUT_DIR / 'catalogs'
CSV_DIR = OUTPUT_DIR / 'csv_results'
AUDIT_DIR = OUTPUT_DIR / 'audit_logs'

# Extraction Settings
CONFIDENCE_THRESHOLD = 0.7
MAX_TOKENS = 8000
TEMPERATURE = 0.1  # Low temperature for consistent extraction

# Taxonomy Settings
USE_FULL_TAXONOMY = True  # Include full taxonomy in prompt
USE_FEW_SHOT_EXAMPLES = True  # Include examples in prompt
TAXONOMY_OPTIMIZATION = 'smart'  # 'full', 'smart', or 'minimal'

# Output Settings
SAVE_INTERMEDIATE_RESULTS = True
VERBOSE_LOGGING = True
