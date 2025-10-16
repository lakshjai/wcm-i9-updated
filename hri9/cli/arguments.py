#!/usr/bin/env python3
"""
Command-line argument parsing for I-9 detection.

This module provides functionality for parsing command-line arguments.
"""

import argparse
from ..config import settings

def parse_arguments():
    """
    Parse command-line arguments for the I-9 detection system.
    
    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description='Cornell HR I-9 Detection and Document Catalog System - '
                   'Automated processing of employee documents with AI-powered analysis, '
                   'I-9 form detection, extraction, and comprehensive document cataloging.',
        epilog='Examples:\n'
               '  %(prog)s --mode detect --catalog-only\n'
               '    Generate document catalogs without I-9 processing\n\n'
               '  %(prog)s --mode all --workers 4 --limit 100\n'
               '    Process 100 documents with full I-9 pipeline using 4 workers\n\n'
               '  %(prog)s --catalog-validate --catalog-export-path ./catalogs\n'
               '    Validate existing catalog files in specified directory\n\n'
               '  %(prog)s --catalog-import ./catalog.json --catalog-stats\n'
               '    Import catalog and generate statistics report',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Mode selection
    parser.add_argument('--mode', type=str, choices=['detect', 'remove', 'extract', 'all'],
                       default='all', 
                       help='Processing mode: detect (find I-9s only), extract (save I-9s to separate files), '
                            'remove (create cleaned PDFs without I-9s), all (extract and remove). '
                            'Catalog generation occurs in all modes when enabled.')
    
    # Concurrency options
    parser.add_argument('--workers', type=int, default=settings.CONCURRENT_WORKERS,
                       help='Number of concurrent workers')
    
    # Processing limits
    parser.add_argument('--limit', type=int, default=settings.MAX_DOCUMENTS,
                       help='Maximum documents to process, 0 for all')
    parser.add_argument('--batch-size', type=int, default=settings.BATCH_SIZE,
                       help='Progress reporting interval')
    
    # Data source options
    parser.add_argument('--use-local', action='store_true', default=False,
                       help='Use local sample data instead of network drive')
    parser.add_argument('--excel-file', type=str, default=settings.EXCEL_FILE,
                       help='Path to Excel file with employee IDs')
    
    # Output options
    parser.add_argument('--output-dir', type=str, default=settings.OUTPUT_DIR,
                       help='Directory for output files')
    parser.add_argument('--extract-dir', type=str, default=settings.I9_EXTRACT_DIR,
                       help='Directory for extracted I-9 forms')
    parser.add_argument('--cleaned-dir', type=str, default=settings.CLEANED_PDF_DIR,
                       help='Directory for cleaned PDFs')
    parser.add_argument('--output-csv', type=str, default=settings.OUTPUT_CSV,
                       help='Path to output CSV file')
    
    # Interactive mode (legacy behavior)
    parser.add_argument('--interactive', action='store_true', default=False,
                       help='Run in interactive mode (legacy behavior)')
    
    # Verbose output
    parser.add_argument('--verbose', action='store_true', default=False,
                       help='Enable verbose output')
    
    # Debug mode
    parser.add_argument('--debug-files', type=str, metavar='PATTERN',
                       help='Debug mode: process only files containing this string (e.g., "1003375")')
    
    # Data extraction options
    parser.add_argument('--extract-all-data', action='store_true', default=False,
                       help='Extract data from all pages, not just I-9 forms')
    
    # Catalog skip option
    parser.add_argument('--skip-existing-catalog', action='store_true', default=True,
                       help='Skip catalog generation if catalog files already exist (default: True)')
    parser.add_argument('--force-catalog-regeneration', action='store_true', default=False,
                       help='Force regeneration of catalog files even if they exist')
    
    # Catalog system options
    catalog_group = parser.add_argument_group('Catalog System Options', 
                                            'Options for document catalog generation and management')
    
    catalog_group.add_argument('--enable-catalog', action='store_true', default=settings.CATALOG_ENABLED,
                              help='Enable document catalog system for comprehensive document analysis')
    catalog_group.add_argument('--disable-catalog', action='store_true', default=False,
                              help='Explicitly disable catalog system (overrides --enable-catalog)')
    catalog_group.add_argument('--catalog-only', action='store_true', default=False,
                              help='Generate catalog only, skip I-9 business logic processing')
    catalog_group.add_argument('--data-only', action='store_true', default=False,
                              help='Data extraction only mode - no PDF manipulation, categorized output files')
    catalog_group.add_argument('--use-existing-catalog', action='store_true', default=False,
                              help='Process using existing catalog files without AI re-extraction')
    catalog_group.add_argument('--catalog-file', type=str, metavar='PATH',
                              help='Specific catalog file to process (use with --use-existing-catalog)')
    
    # Batch processing options
    catalog_group.add_argument('--batch-mode', action='store_true', default=False,
                              help='Enable batch processing mode for multiple files')
    catalog_group.add_argument('--input-folder', type=str, metavar='PATH',
                              help='Process all PDF files in specified folder (requires --batch-mode)')
    
    # Catalog export and validation options
    catalog_group.add_argument('--catalog-export-path', type=str, default=settings.CATALOG_EXPORT_PATH,
                              help='Directory for catalog export files (default: %(default)s)')
    catalog_group.add_argument('--catalog-export-format', type=str, choices=['json', 'csv', 'both'],
                              default='json', help='Export format for catalog data (default: %(default)s)')
    catalog_group.add_argument('--catalog-validate', action='store_true', default=False,
                              help='Validate existing catalog files for data integrity')
    catalog_group.add_argument('--catalog-import', type=str, metavar='PATH',
                              help='Import and validate catalog from specified JSON file')
    
    # Catalog processing options
    catalog_group.add_argument('--catalog-cache-size', type=int, default=settings.CATALOG_CACHE_SIZE,
                              help='Maximum number of documents to cache in memory (default: %(default)s)')
    catalog_group.add_argument('--catalog-confidence-threshold', type=float, 
                              default=settings.CATALOG_CONFIDENCE_THRESHOLD,
                              help='Minimum confidence score for catalog entries (0.0-1.0, default: %(default)s)')
    catalog_group.add_argument('--catalog-include-pii', action='store_true', default=False,
                              help='Include potentially sensitive data in catalog export (use with caution)')
    catalog_group.add_argument('--catalog-text-regions', action='store_true', 
                              default=settings.ENABLE_TEXT_REGIONS,
                              help='Enable text region extraction in catalog analysis')
    catalog_group.add_argument('--catalog-structured-extraction', action='store_true',
                              default=settings.ENABLE_STRUCTURED_EXTRACTION,
                              help='Enable structured data extraction from documents')
    
    # I-9 specific analysis options
    catalog_group.add_argument('--i9-only-analysis', action='store_true', 
                              default=settings.I9_ONLY_ANALYSIS,
                              help='Only perform detailed analysis on I-9 forms, use simplified analysis for other pages')
    catalog_group.add_argument('--skip-non-i9-extraction', action='store_true',
                              default=settings.SKIP_NON_I9_EXTRACTION, 
                              help='Skip detailed data extraction for non-I9 pages (faster processing)')
    
    # Catalog reporting and analysis
    catalog_group.add_argument('--catalog-stats', action='store_true', default=False,
                              help='Generate detailed statistics report for catalog data')
    catalog_group.add_argument('--catalog-report-path', type=str, 
                              help='Path for catalog analysis reports (default: same as export path)')
    
    return parser.parse_args()
