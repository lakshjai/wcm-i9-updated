#!/usr/bin/env python3
"""
Configuration Management for I-9 Processing System
Loads all configuration from .env file and provides typed access
"""
import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """
    Centralized configuration management for I-9 processing system.
    All configuration is loaded from .env file.
    """
    
    # =============================================================================
    # API CONFIGURATION
    # =============================================================================
    
    OPENAI_API_KEY: str = os.getenv('OPENAI_API_KEY', '')
    OPENAI_BASE_URL: str = os.getenv('OPENAI_BASE_URL', 'https://api.ai.it.cornell.edu/')
    I9_MODEL_NAME: str = os.getenv('I9_MODEL_NAME', 'google.gemini-2.5-pro')
    
    # =============================================================================
    # PROCESSING CONFIGURATION
    # =============================================================================
    
    I9_MAX_WORKERS: int = int(os.getenv('I9_MAX_WORKERS', '4'))
    I9_RATE_LIMIT_DELAY: float = float(os.getenv('I9_RATE_LIMIT_DELAY', '1.0'))
    I9_MAX_RETRIES: int = int(os.getenv('I9_MAX_RETRIES', '3'))
    I9_TIMEOUT_SECONDS: int = int(os.getenv('I9_TIMEOUT_SECONDS', '60'))
    
    # =============================================================================
    # PDF PROCESSING
    # =============================================================================
    
    I9_IMAGE_SCALE_FACTOR: float = float(os.getenv('I9_IMAGE_SCALE_FACTOR', '3.5'))
    I9_MAX_PDF_SIZE_MB: int = int(os.getenv('I9_MAX_PDF_SIZE_MB', '50'))
    
    PDF_DPI: int = int(os.getenv('PDF_DPI', '300'))
    PDF_IMAGE_FORMAT: str = os.getenv('PDF_IMAGE_FORMAT', 'PNG')
    PDF_IMAGE_QUALITY: int = int(os.getenv('PDF_IMAGE_QUALITY', '100'))
    HYBRID_EXTRACTION_ENABLED: bool = os.getenv('HYBRID_EXTRACTION_ENABLED', 'True').lower() == 'true'
    
    # =============================================================================
    # BATCH PROCESSING
    # =============================================================================
    
    CATALOG_BATCH_SIZE: int = int(os.getenv('CATALOG_BATCH_SIZE', '1'))
    
    # =============================================================================
    # DETECTION CONFIGURATION
    # =============================================================================
    
    I9_CONFIDENCE_THRESHOLD: float = float(os.getenv('I9_CONFIDENCE_THRESHOLD', '0.7'))
    I9_ENABLE_FALLBACK: bool = os.getenv('I9_ENABLE_FALLBACK', 'true').lower() == 'true'
    
    # =============================================================================
    # LOGGING CONFIGURATION
    # =============================================================================
    
    I9_LOG_LEVEL: str = os.getenv('I9_LOG_LEVEL', 'INFO')
    I9_LOG_FILE: str = os.getenv('I9_LOG_FILE', 'logs/i9_processor.log')
    
    # =============================================================================
    # DIRECTORY CONFIGURATION
    # =============================================================================
    
    INPUT_FOLDER: Path = Path(os.getenv('INPUT_FOLDER', 'work'))
    OUTPUT_FOLDER: Path = Path(os.getenv('OUTPUT_FOLDER', 'workdir'))
    
    # Subdirectories (relative to OUTPUT_FOLDER)
    CATALOGS_FOLDER: str = os.getenv('CATALOGS_FOLDER', 'catalogs')
    AUDIT_LOGS_FOLDER: str = os.getenv('AUDIT_LOGS_FOLDER', 'audit_logs')
    FIELD_AUDITS_FOLDER: str = os.getenv('FIELD_AUDITS_FOLDER', 'field_level_audits')
    FIELD_AUDIT_TABLES_FOLDER: str = os.getenv('FIELD_AUDIT_TABLES_FOLDER', 'field_audit_tables')
    FIELD_AUDIT_REPORTS_FOLDER: str = os.getenv('FIELD_AUDIT_REPORTS_FOLDER', 'field_audit_reports')
    
    # Full paths
    @property
    def catalogs_path(self) -> Path:
        return self.OUTPUT_FOLDER / self.CATALOGS_FOLDER
    
    @property
    def audit_logs_path(self) -> Path:
        return self.OUTPUT_FOLDER / self.AUDIT_LOGS_FOLDER
    
    @property
    def field_audits_path(self) -> Path:
        return self.OUTPUT_FOLDER / self.FIELD_AUDITS_FOLDER
    
    @property
    def field_audit_tables_path(self) -> Path:
        return self.OUTPUT_FOLDER / self.FIELD_AUDIT_TABLES_FOLDER
    
    @property
    def field_audit_reports_path(self) -> Path:
        return self.OUTPUT_FOLDER / self.FIELD_AUDIT_REPORTS_FOLDER
    
    # =============================================================================
    # CATALOG GENERATION CONFIGURATION
    # =============================================================================
    
    CATALOG_MODE: str = os.getenv('CATALOG_MODE', 'use_existing')
    FORCE_REGENERATE_CATALOGS: bool = os.getenv('FORCE_REGENERATE_CATALOGS', 'false').lower() == 'true'
    
    # =============================================================================
    # PROCESSING PIPELINE CONFIGURATION
    # =============================================================================
    
    GENERATE_CATALOGS: bool = os.getenv('GENERATE_CATALOGS', 'true').lower() == 'true'
    PROCESS_RUBRICS: bool = os.getenv('PROCESS_RUBRICS', 'true').lower() == 'true'
    GENERATE_CSV: bool = os.getenv('GENERATE_CSV', 'true').lower() == 'true'
    GENERATE_FIELD_AUDITS: bool = os.getenv('GENERATE_FIELD_AUDITS', 'true').lower() == 'true'
    GENERATE_SUMMARY: bool = os.getenv('GENERATE_SUMMARY', 'true').lower() == 'true'
    
    # =============================================================================
    # OUTPUT CONFIGURATION
    # =============================================================================
    
    CSV_OUTPUT_FILENAME: str = os.getenv('CSV_OUTPUT_FILENAME', 'rubric_based_results.csv')
    SUMMARY_FILENAME: str = os.getenv('SUMMARY_FILENAME', 'REGENERATION_SUMMARY.txt')
    
    @property
    def csv_output_path(self) -> Path:
        return self.OUTPUT_FOLDER / self.CSV_OUTPUT_FILENAME
    
    @property
    def summary_path(self) -> Path:
        return self.OUTPUT_FOLDER / self.SUMMARY_FILENAME
    
    # =============================================================================
    # VALIDATION & UTILITIES
    # =============================================================================
    
    def validate(self) -> list[str]:
        """
        Validate configuration and return list of errors.
        Returns empty list if configuration is valid.
        """
        errors = []
        
        # Check required API configuration
        if not self.OPENAI_API_KEY:
            errors.append("OPENAI_API_KEY is required")
        
        # Check input folder exists
        if not self.INPUT_FOLDER.exists():
            errors.append(f"INPUT_FOLDER does not exist: {self.INPUT_FOLDER}")
        
        # Check catalog mode is valid
        valid_modes = ['use_existing', 'force_regenerate', 'skip']
        if self.CATALOG_MODE not in valid_modes:
            errors.append(f"CATALOG_MODE must be one of {valid_modes}, got: {self.CATALOG_MODE}")
        
        return errors
    
    def create_directories(self):
        """Create all required output directories if they don't exist."""
        directories = [
            self.OUTPUT_FOLDER,
            self.catalogs_path,
            self.audit_logs_path,
            self.field_audits_path,
            self.field_audit_tables_path,
            self.field_audit_reports_path,
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def print_config(self):
        """Print all effective configuration values."""
        print("=" * 90)
        print("I-9 PROCESSING SYSTEM - EFFECTIVE CONFIGURATION")
        print("=" * 90)
        print()
        
        print("📁 DIRECTORIES:")
        print(f"   Input Folder:              {self.INPUT_FOLDER.absolute()}")
        print(f"   Output Folder:             {self.OUTPUT_FOLDER.absolute()}")
        print(f"   Catalogs:                  {self.catalogs_path.absolute()}")
        print(f"   Audit Logs:                {self.audit_logs_path.absolute()}")
        print(f"   Field Audits:              {self.field_audits_path.absolute()}")
        print()
        
        print("🔧 CATALOG GENERATION:")
        print(f"   Mode:                      {self.CATALOG_MODE}")
        print(f"   Force Regenerate:          {self.FORCE_REGENERATE_CATALOGS}")
        print()
        
        print("⚙️  PROCESSING PIPELINE:")
        print(f"   Generate Catalogs:         {self.GENERATE_CATALOGS}")
        print(f"   Process Rubrics:           {self.PROCESS_RUBRICS}")
        print(f"   Generate CSV:              {self.GENERATE_CSV}")
        print(f"   Generate Field Audits:     {self.GENERATE_FIELD_AUDITS}")
        print(f"   Generate Summary:          {self.GENERATE_SUMMARY}")
        print()
        
        print("🤖 API CONFIGURATION:")
        print(f"   Model:                     {self.I9_MODEL_NAME}")
        print(f"   Base URL:                  {self.OPENAI_BASE_URL}")
        print(f"   API Key:                   {'*' * 20}{self.OPENAI_API_KEY[-4:] if self.OPENAI_API_KEY else 'NOT SET'}")
        print()
        
        print("📊 PROCESSING SETTINGS:")
        print(f"   Max Workers:               {self.I9_MAX_WORKERS}")
        print(f"   Rate Limit Delay:          {self.I9_RATE_LIMIT_DELAY}s")
        print(f"   Max Retries:               {self.I9_MAX_RETRIES}")
        print(f"   Timeout:                   {self.I9_TIMEOUT_SECONDS}s")
        print(f"   Batch Size:                {self.CATALOG_BATCH_SIZE}")
        print()
        
        print("📄 PDF PROCESSING:")
        print(f"   DPI:                       {self.PDF_DPI}")
        print(f"   Image Format:              {self.PDF_IMAGE_FORMAT}")
        print(f"   Image Quality:             {self.PDF_IMAGE_QUALITY}")
        print(f"   Hybrid Extraction:         {self.HYBRID_EXTRACTION_ENABLED}")
        print(f"   Image Scale Factor:        {self.I9_IMAGE_SCALE_FACTOR}")
        print(f"   Max PDF Size:              {self.I9_MAX_PDF_SIZE_MB} MB")
        print()
        
        print("📝 OUTPUT FILES:")
        print(f"   CSV Output:                {self.csv_output_path.absolute()}")
        print(f"   Summary Report:            {self.summary_path.absolute()}")
        print()
        
        print("🔍 DETECTION:")
        print(f"   Confidence Threshold:      {self.I9_CONFIDENCE_THRESHOLD}")
        print(f"   Enable Fallback:           {self.I9_ENABLE_FALLBACK}")
        print()
        
        print("📋 LOGGING:")
        print(f"   Log Level:                 {self.I9_LOG_LEVEL}")
        print(f"   Log File:                  {self.I9_LOG_FILE}")
        print()
        
        print("=" * 90)
        print()


# Create global config instance
config = Config()


# Example usage and validation
if __name__ == "__main__":
    # Print configuration
    config.print_config()
    
    # Validate configuration
    errors = config.validate()
    if errors:
        print("❌ CONFIGURATION ERRORS:")
        for error in errors:
            print(f"   - {error}")
    else:
        print("✅ Configuration is valid!")
