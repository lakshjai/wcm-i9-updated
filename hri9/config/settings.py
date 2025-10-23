#!/usr/bin/env python3
"""
Configuration settings for the I-9 detection system.

This module centralizes all configuration settings including environment variables,
file paths, and processing parameters.
"""

import os
import dotenv
from pathlib import Path

# Load environment variables from .env file
dotenv.load_dotenv()

# Base paths
BASE_DIR = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR.parent, "data")
WORK_DIR = os.path.join(BASE_DIR.parent, "workdir")

# File paths
EXCEL_FILE = os.getenv("EXCEL_FILE", os.path.join(DATA_DIR, "ActiveI9.xlsx"))
OUTPUT_CSV = os.getenv("OUTPUT_CSV", os.path.join(WORK_DIR, "i9_detection_results.csv"))
OUTPUT_DIR = os.getenv("OUTPUT_DIR", os.path.join(WORK_DIR))
I9_EXTRACT_DIR = os.getenv("I9_EXTRACT_DIR", os.path.join(WORK_DIR, "extracted_i9_forms"))
CLEANED_PDF_DIR = os.getenv("CLEANED_PDF_DIR", os.path.join(WORK_DIR, "processed"))
DELETE_FILE_LIST_CSV = os.getenv("DELETE_FILE_LIST_CSV", os.path.join(WORK_DIR, "delete_file_list.csv"))
NETWORK_DRIVE_PATH = os.getenv("NETWORK_DRIVE_PATH", os.path.join(DATA_DIR, "input"))
LOCAL_SAMPLE_PATH = os.getenv("LOCAL_SAMPLE_PATH", os.path.join(DATA_DIR, "input"))

# API configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "google.gemini-2.5-pro")

# API rate limiting and error handling
API_RATE_LIMIT_RPM = int(os.getenv("API_RATE_LIMIT_RPM", "60"))  # Requests per minute
API_RATE_LIMIT_DELAY = float(os.getenv("API_RATE_LIMIT_DELAY", "1.0"))  # Minimum delay between requests
API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "3"))  # Maximum retry attempts
API_RETRY_BASE_DELAY = float(os.getenv("API_RETRY_BASE_DELAY", "2.0"))  # Base delay for exponential backoff
API_CIRCUIT_BREAKER_THRESHOLD = float(os.getenv("API_CIRCUIT_BREAKER_THRESHOLD", "0.5"))  # Errors per minute threshold

# Processing parameters
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "1"))
CONCURRENT_WORKERS = int(os.getenv("CONCURRENT_WORKERS", "1"))
MAX_DOCUMENTS = int(os.getenv("MAX_DOCUMENTS", "0"))  # 0 = process all documents
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))

# Logging configuration
LOG_FILE = os.getenv("LOG_FILE", os.path.join(OUTPUT_DIR, "i9_detection.log"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG")

# Ensure output directories exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(I9_EXTRACT_DIR, exist_ok=True)
os.makedirs(CLEANED_PDF_DIR, exist_ok=True)

# Create logs directory
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# I-9 detection parameters
TOKEN_LIMIT = int(os.getenv("TOKEN_LIMIT", "100000"))  # Token usage limit
API_TIMEOUT = int(os.getenv("API_TIMEOUT", "60"))  # API timeout in seconds

# Network and file access retry settings
NETWORK_MAX_RETRIES = int(os.getenv("NETWORK_MAX_RETRIES", "3"))  # Max retries for network operations
NETWORK_RETRY_DELAY = float(os.getenv("NETWORK_RETRY_DELAY", "1.0"))  # Delay between retries in seconds
FILE_ACCESS_MAX_RETRIES = int(os.getenv("FILE_ACCESS_MAX_RETRIES", "2"))  # Max retries for file operations
FILE_ACCESS_RETRY_DELAY = float(os.getenv("FILE_ACCESS_RETRY_DELAY", "0.5"))  # Delay between file retries

# Catalog system settings
CATALOG_ENABLED = bool(os.getenv("CATALOG_ENABLED", "True"))
CATALOG_CACHE_SIZE = int(os.getenv("CATALOG_CACHE_SIZE", "1000"))
CATALOG_EXPORT_PATH = os.getenv("CATALOG_EXPORT_PATH", os.path.join(WORK_DIR, "catalogs"))
CATALOG_CONFIDENCE_THRESHOLD = float(os.getenv("CATALOG_CONFIDENCE_THRESHOLD", "0.7"))

# Catalog export settings
CATALOG_EXPORT_FORMAT = os.getenv("CATALOG_EXPORT_FORMAT", "json")  # json, jsonl
CATALOG_EXPORT_COMPRESSION = bool(os.getenv("CATALOG_EXPORT_COMPRESSION", "False"))
CATALOG_INCLUDE_PII = bool(os.getenv("CATALOG_INCLUDE_PII", "True"))
CATALOG_AUTO_EXPORT = bool(os.getenv("CATALOG_AUTO_EXPORT", "True"))

# Analysis settings
ENABLE_TEXT_REGIONS = bool(os.getenv("ENABLE_TEXT_REGIONS", "True"))
ENABLE_STRUCTURED_EXTRACTION = bool(os.getenv("ENABLE_STRUCTURED_EXTRACTION", "True"))
MAX_EXTRACTED_FIELDS = int(os.getenv("MAX_EXTRACTED_FIELDS", "50"))

# I-9 specific analysis settings
I9_ONLY_ANALYSIS = os.getenv("I9_ONLY_ANALYSIS", "True").lower() in ("true", "1", "yes")  # Only analyze I-9 forms in detail

# PDF Image Processing Settings (for improved small text extraction)
PDF_DPI = int(os.getenv("PDF_DPI", "300"))  # DPI for PDF to image conversion (higher = better quality)
PDF_IMAGE_SCALE_FACTOR = float(os.getenv("I9_IMAGE_SCALE_FACTOR", "3.0"))  # Additional scaling factor
PDF_IMAGE_FORMAT = os.getenv("PDF_IMAGE_FORMAT", "PNG").upper()  # Image format: PNG, JPEG
PDF_IMAGE_QUALITY = int(os.getenv("PDF_IMAGE_QUALITY", "95"))  # JPEG quality (1-100), ignored for PNG

# Hybrid Text + Vision Extraction (RECOMMENDED for small text)
HYBRID_EXTRACTION_ENABLED = os.getenv("HYBRID_EXTRACTION_ENABLED", "True").lower() in ("true", "1", "yes")
# When enabled, always extracts text first and provides it to LLM along with image
# This ensures small text is captured even if image resolution is limited
SKIP_NON_I9_EXTRACTION = os.getenv("SKIP_NON_I9_EXTRACTION", "True").lower() in ("true", "1", "yes")  # Skip data extraction for non-I9 pages

# Batch processing settings
CATALOG_BATCH_SIZE = int(os.getenv("CATALOG_BATCH_SIZE", "3"))  # Pages per API call
CATALOG_MAX_TOKENS = int(os.getenv("CATALOG_MAX_TOKENS", "16000"))  # Max tokens per batch call

# Business Rules and Validation Settings
BUSINESS_RULES_ENABLED = bool(os.getenv("BUSINESS_RULES_ENABLED", "True"))
ENHANCED_VALIDATION = bool(os.getenv("ENHANCED_VALIDATION", "True"))
STRICT_COMPLIANCE_MODE = bool(os.getenv("STRICT_COMPLIANCE_MODE", "False"))

# Rule Engine Configuration
RULE_ENGINE_MAX_RETRIES = int(os.getenv("RULE_ENGINE_MAX_RETRIES", "3"))
RULE_ENGINE_TIMEOUT_SECONDS = int(os.getenv("RULE_ENGINE_TIMEOUT_SECONDS", "30"))
RULE_EXECUTION_PARALLEL = bool(os.getenv("RULE_EXECUTION_PARALLEL", "False"))

# Validation Thresholds
VALIDATION_SUCCESS_THRESHOLD = float(os.getenv("VALIDATION_SUCCESS_THRESHOLD", "0.8"))  # 80% success rate
CRITICAL_ISSUE_THRESHOLD = int(os.getenv("CRITICAL_ISSUE_THRESHOLD", "0"))  # No critical issues allowed
DATE_TOLERANCE_DAYS = int(os.getenv("DATE_TOLERANCE_DAYS", "30"))  # 30-day tolerance for date matching

# Document Validation Settings
REQUIRE_DOCUMENT_ATTACHMENTS = bool(os.getenv("REQUIRE_DOCUMENT_ATTACHMENTS", "True"))
ALLOW_EXPIRED_DOCUMENTS = bool(os.getenv("ALLOW_EXPIRED_DOCUMENTS", "False"))
DOCUMENT_EXPIRATION_GRACE_DAYS = int(os.getenv("DOCUMENT_EXPIRATION_GRACE_DAYS", "30"))

# Scenario Processing Settings
ENABLE_SCENARIO_1 = bool(os.getenv("ENABLE_SCENARIO_1", "True"))  # Single I-9 form
ENABLE_SCENARIO_2 = bool(os.getenv("ENABLE_SCENARIO_2", "True"))  # Multiple I-9 forms
ENABLE_SCENARIO_3 = bool(os.getenv("ENABLE_SCENARIO_3", "True"))  # I-9 with blank Supplement B
ENABLE_SCENARIO_4 = bool(os.getenv("ENABLE_SCENARIO_4", "True"))  # Filled Supplement B
ENABLE_SCENARIO_5 = bool(os.getenv("ENABLE_SCENARIO_5", "True"))  # Multiple Section 3 forms

# Form Selection Preferences
PRIORITIZE_LATEST_SIGNATURE = bool(os.getenv("PRIORITIZE_LATEST_SIGNATURE", "True"))
PRIORITIZE_SUPPLEMENT_B = bool(os.getenv("PRIORITIZE_SUPPLEMENT_B", "True"))
MIN_FORM_CONFIDENCE_SCORE = float(os.getenv("MIN_FORM_CONFIDENCE_SCORE", "0.6"))

# Alien Work Authorization Settings
REQUIRE_ALIEN_EXPIRATION_MATCH = bool(os.getenv("REQUIRE_ALIEN_EXPIRATION_MATCH", "True"))
ALIEN_EXPIRATION_TOLERANCE_DAYS = int(os.getenv("ALIEN_EXPIRATION_TOLERANCE_DAYS", "7"))  # 7-day tolerance
WARN_EXPIRATION_WITHIN_DAYS = int(os.getenv("WARN_EXPIRATION_WITHIN_DAYS", "90"))  # Warn if expiring within 90 days

# Compliance Timeline Settings (in days)
SECTION_1_MAX_DAYS_AFTER_HIRE = int(os.getenv("SECTION_1_MAX_DAYS_AFTER_HIRE", "1"))  # Section 1 by first day
SECTION_2_MAX_DAYS_AFTER_HIRE = int(os.getenv("SECTION_2_MAX_DAYS_AFTER_HIRE", "3"))  # Section 2 within 3 business days
I9_RETENTION_YEARS = int(os.getenv("I9_RETENTION_YEARS", "3"))  # 3 years retention requirement

# Enhanced Reporting Settings
GENERATE_DETAILED_REPORTS = bool(os.getenv("GENERATE_DETAILED_REPORTS", "True"))
INCLUDE_VALIDATION_DETAILS = bool(os.getenv("INCLUDE_VALIDATION_DETAILS", "True"))
INCLUDE_BUSINESS_RULE_RESULTS = bool(os.getenv("INCLUDE_BUSINESS_RULE_RESULTS", "True"))
EXPORT_RULE_EXECUTION_LOGS = bool(os.getenv("EXPORT_RULE_EXECUTION_LOGS", "False"))

# Performance and Optimization
ENABLE_RULE_CACHING = bool(os.getenv("ENABLE_RULE_CACHING", "True"))
RULE_CACHE_TTL_MINUTES = int(os.getenv("RULE_CACHE_TTL_MINUTES", "60"))  # 1 hour cache TTL
MAX_VALIDATION_THREADS = int(os.getenv("MAX_VALIDATION_THREADS", "2"))  # Limit validation threads

# Ensure directories exist
if CATALOG_ENABLED:
    os.makedirs(CATALOG_EXPORT_PATH, exist_ok=True)

# Create business rules output directory
if BUSINESS_RULES_ENABLED:
    BUSINESS_RULES_OUTPUT_DIR = os.path.join(WORK_DIR, "business_rules_output")
    os.makedirs(BUSINESS_RULES_OUTPUT_DIR, exist_ok=True)
