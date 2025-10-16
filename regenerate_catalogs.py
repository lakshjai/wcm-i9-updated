#!/usr/bin/env python3
"""
Regenerate all catalog files with improved extraction accuracy.
"""

import os
import sys
from pathlib import Path
from glob import glob

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from hri9.api.gemini_client import GeminiClient
from hri9.catalog.document_catalog import DocumentCatalog
from hri9.catalog.cache import CatalogCache
from hri9.utils.logging_config import logger

def main():
    """Regenerate all catalog files from PDF inputs."""
    
    # Setup
    input_dir = "data/input"
    catalog_output_dir = "workdir/catalogs"
    
    # Create output directory if it doesn't exist
    Path(catalog_output_dir).mkdir(parents=True, exist_ok=True)
    
    # Initialize Gemini client
    logger.info("Initializing Gemini AI client...")
    gemini_client = GeminiClient()
    
    # Initialize catalog system
    logger.info("Initializing document catalog system...")
    catalog_cache = CatalogCache()
    document_catalog = DocumentCatalog(
        gemini_client=gemini_client,
        catalog_cache=catalog_cache,
        catalog_output_dir=catalog_output_dir
    )
    
    # Get all PDF files
    pdf_files = sorted(glob(f"{input_dir}/*.pdf"))
    logger.info(f"Found {len(pdf_files)} PDF files to process")
    
    if not pdf_files:
        logger.error(f"No PDF files found in {input_dir}")
        return 1
    
    # Process each PDF
    success_count = 0
    error_count = 0
    
    for i, pdf_path in enumerate(pdf_files, 1):
        pdf_name = Path(pdf_path).name
        logger.info(f"\n{'='*80}")
        logger.info(f"Processing {i}/{len(pdf_files)}: {pdf_name}")
        logger.info(f"{'='*80}")
        
        try:
            # Generate catalog
            catalog_entry = document_catalog.analyze_document(pdf_path)
            
            # Save catalog to file
            catalog_filename = Path(pdf_path).stem + ".catalog.json"
            catalog_filepath = Path(catalog_output_dir) / catalog_filename
            
            import json
            with open(catalog_filepath, 'w', encoding='utf-8') as f:
                json.dump(catalog_entry.to_dict(), f, indent=2, ensure_ascii=False)
            
            logger.info(f"✅ Successfully generated catalog: {catalog_filename}")
            logger.info(f"   Pages analyzed: {len(catalog_entry.pages)}")
            logger.info(f"   Processing time: {catalog_entry.processing_summary.processing_time_seconds:.2f}s")
            
            success_count += 1
            
        except Exception as e:
            logger.error(f"❌ Error processing {pdf_name}: {e}")
            error_count += 1
            continue
    
    # Summary
    logger.info(f"\n{'='*80}")
    logger.info(f"CATALOG GENERATION COMPLETE")
    logger.info(f"{'='*80}")
    logger.info(f"✅ Successful: {success_count}/{len(pdf_files)}")
    logger.info(f"❌ Errors: {error_count}/{len(pdf_files)}")
    logger.info(f"📂 Catalogs saved to: {catalog_output_dir}")
    logger.info(f"{'='*80}\n")
    
    return 0 if error_count == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
