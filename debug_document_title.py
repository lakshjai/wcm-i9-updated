#!/usr/bin/env python3
"""
Debug script to examine document title extraction on Wu, Qianyi page 19.
"""

import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from hri9.api.gemini_client import GeminiClient
from hri9.core.pdf_processor import PDFProcessor

def main():
    """Debug document title extraction."""
    
    pdf_path = "data/input/Wu, Qianyi 9963.pdf"
    page_num = 19
    
    print("="*80)
    print(f"DEBUGGING DOCUMENT TITLE EXTRACTION")
    print(f"File: {pdf_path}")
    print(f"Page: {page_num}")
    print("="*80)
    
    # Extract text using PDF processor
    print("\n1. EXTRACTING TEXT FROM PDF...")
    text = PDFProcessor.extract_text_from_pdf(pdf_path, page_num - 1)  # 0-indexed
    
    # Find document title section
    print("\n2. SEARCHING FOR DOCUMENT TITLE IN TEXT...")
    lines = text.split('\n')
    for i, line in enumerate(lines):
        if 'document title' in line.lower():
            print(f"\nFound 'Document Title' at line {i}:")
            # Print surrounding lines
            start = max(0, i-2)
            end = min(len(lines), i+5)
            for j in range(start, end):
                marker = ">>>" if j == i else "   "
                print(f"{marker} Line {j}: {lines[j]}")
    
    # Compare with catalog
    print("\n3. CHECKING CATALOG EXTRACTION...")
    catalog_path = "workdir/catalogs/Wu, Qianyi 9963.catalog.json"
    with open(catalog_path) as f:
        catalog = json.load(f)
    
    page19 = [p for p in catalog['pages'] if p['page_number'] == 19][0]
    catalog_title = page19['extracted_values'].get('section_3_document_title')
    list_a_title = page19['extracted_values'].get('list_a_document_title')
    
    print(f"\nSection 3 Document Title: {catalog_title}")
    print(f"List A Document Title: {list_a_title}")
    
    # Check text regions for raw OCR
    print("\n4. RAW OCR TEXT FROM DOCUMENT TITLE REGION:")
    print("-" * 80)
    for region in page19['text_regions']:
        if 'document' in region['region_id'].lower() and 'title' not in region['region_id'].lower():
            print(f"\nRegion: {region['region_id']}")
            print(region['text'][:300])
    print("-" * 80)
    
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)

if __name__ == "__main__":
    main()
