#!/usr/bin/env python3
"""
Complete Taxonomy-Based I-9 Pipeline Runner
PDF → LLM Extraction → Rubric Processing → CSV + Audits
"""
import sys
from pathlib import Path
import logging

from taxonomy_extractor import TaxonomyExtractor
from taxonomy_rubric_processor import TaxonomyRubricProcessor
import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_pipeline(pdf_path: str = None, provider: str = None, force_regenerate: bool = False):
    """
    Run complete pipeline: PDF → Catalog → Results
    
    Args:
        pdf_path: Path to single PDF (optional, processes all in input/ if not provided)
        provider: 'gemini' or 'anthropic' (optional, uses config default)
        force_regenerate: If True, regenerate catalogs even if they exist (default: False)
    """
    logger.info("="*100)
    logger.info("TAXONOMY-BASED I-9 PIPELINE")
    logger.info("="*100)
    
    # Step 1: Extract PDFs
    logger.info("\n📄 STEP 1: PDF EXTRACTION")
    logger.info("-"*100)
    
    extractor = TaxonomyExtractor(provider=provider)
    
    if pdf_path:
        # Single PDF
        pdf_files = [Path(pdf_path)]
    else:
        # All PDFs in input directory (case-insensitive)
        pdf_files_lower = list(config.INPUT_DIR.glob('*.pdf'))
        pdf_files_upper = list(config.INPUT_DIR.glob('*.PDF'))
        pdf_files = sorted(list(set(pdf_files_lower + pdf_files_upper)))
    
    if not pdf_files:
        logger.error("No PDF files found!")
        return
    
    logger.info(f"Found {len(pdf_files)} PDF file(s)")
    
    for pdf_file in pdf_files:
        output_path = config.CATALOGS_DIR / f"{pdf_file.stem}_taxonomy.json"
        try:
            extractor.extract_pdf(str(pdf_file), str(output_path), force_regenerate=force_regenerate)
        except Exception as e:
            logger.error(f"Error processing {pdf_file.name}: {e}")
    
    # Step 2: Process Catalogs
    logger.info("\n📊 STEP 2: RUBRIC PROCESSING")
    logger.info("-"*100)
    
    processor = TaxonomyRubricProcessor()
    results = processor.process_all_catalogs(config.CATALOGS_DIR)
    
    # Step 3: Save Results
    logger.info("\n💾 STEP 3: SAVING RESULTS")
    logger.info("-"*100)
    
    csv_path = config.CSV_DIR / 'taxonomy_results.csv'
    processor.save_csv(csv_path)
    processor.save_audit_logs(config.AUDIT_DIR)
    
    # Summary
    logger.info("\n" + "="*100)
    logger.info("PIPELINE COMPLETE!")
    logger.info("="*100)
    logger.info(f"📁 Catalogs: {config.CATALOGS_DIR}")
    logger.info(f"📊 CSV Results: {csv_path}")
    logger.info(f"📋 Audit Logs: {config.AUDIT_DIR}")
    logger.info(f"\n✓ Processed {len(results)} file(s)")
    
    # Status breakdown
    status_counts = {}
    for result in results:
        status = result.get('status', 'UNKNOWN')
        status_counts[status] = status_counts.get(status, 0) + 1
    
    logger.info("\n📈 Status Breakdown:")
    for status, count in sorted(status_counts.items()):
        pct = (count / len(results)) * 100
        logger.info(f"  • {status}: {count} ({pct:.1f}%)")
    
    logger.info("\n" + "="*100)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Taxonomy-Based I-9 Pipeline')
    parser.add_argument('pdf', nargs='?', help='PDF file to process (optional, processes all if not provided)')
    parser.add_argument('--provider', choices=['gemini', 'anthropic'], help='LLM provider')
    parser.add_argument('--test', action='store_true', help='Test mode (process first PDF only)')
    parser.add_argument('--force', action='store_true', help='Force regenerate catalogs even if they exist')
    
    args = parser.parse_args()
    
    if args.test:
        # Test mode: process first PDF only
        pdf_files_lower = list(config.INPUT_DIR.glob('*.pdf'))
        pdf_files_upper = list(config.INPUT_DIR.glob('*.PDF'))
        pdf_files = sorted(list(set(pdf_files_lower + pdf_files_upper)))
        if pdf_files:
            logger.info("🧪 TEST MODE: Processing first PDF only")
            run_pipeline(str(pdf_files[0]), provider=args.provider, force_regenerate=args.force)
        else:
            logger.error("No PDF files found in input directory")
    else:
        run_pipeline(pdf_path=args.pdf, provider=args.provider, force_regenerate=args.force)


if __name__ == "__main__":
    main()
