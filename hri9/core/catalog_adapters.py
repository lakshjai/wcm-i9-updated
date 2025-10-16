#!/usr/bin/env python3
"""
Catalog Adapters for HRI9 System

This module provides adapter classes to convert raw catalog JSON data
into the object interfaces expected by the HRI9 processing pipeline.
"""

from typing import Dict, List, Any, Optional
from pathlib import Path
import json

from hri9.core.models import PDFAnalysis
from hri9.utils.logging_config import logger


class PageAnalysisAdapter:
    """Adapter to convert catalog page data to expected page analysis interface"""
    
    def __init__(self, page_data: Dict[str, Any]):
        """Initialize adapter with catalog page data"""
        self.page_number = page_data['page_number']
        self.page_type = page_data.get('page_type', '')
        self.page_subtype = page_data.get('page_subtype', '')
        self.page_title = page_data.get('page_title', '')
        self.extracted_values = page_data.get('extracted_values', {})
        self.confidence_score = page_data.get('confidence_score', 0.0)
        self.text_regions = page_data.get('text_regions', [])
        self.page_metadata = page_data.get('page_metadata', {})


class CatalogEntryAdapter:
    """Adapter to convert catalog data to expected catalog entry interface"""
    
    def __init__(self, catalog_data: Dict[str, Any]):
        """Initialize adapter with full catalog data"""
        self.pages = []
        doc_catalog = catalog_data['document_catalog']
        
        # Convert each page to PageAnalysisAdapter
        for page_data in doc_catalog['pages']:
            page_adapter = PageAnalysisAdapter(page_data)
            self.pages.append(page_adapter)
        
        self.total_pages = doc_catalog['total_pages']
        self.processing_timestamp = doc_catalog.get('processing_timestamp', '')


class CatalogProcessor:
    """Processor for handling existing catalog data without AI re-extraction"""
    
    @staticmethod
    def load_catalog_from_file(catalog_path: str) -> Dict[str, Any]:
        """Load catalog data from JSON file"""
        catalog_file = Path(catalog_path)
        
        if not catalog_file.exists():
            raise FileNotFoundError(f"Catalog file not found: {catalog_path}")
        
        logger.info(f"Loading existing catalog from: {catalog_path}")
        
        with open(catalog_file, 'r') as f:
            catalog_data = json.load(f)
        
        logger.info(f"Loaded catalog with {len(catalog_data['document_catalog']['pages'])} pages")
        return catalog_data
    
    @staticmethod
    def create_pdf_analysis_from_catalog(catalog_data: Dict[str, Any], pdf_path: str) -> PDFAnalysis:
        """Create PDFAnalysis object from existing catalog data"""
        
        doc_catalog = catalog_data['document_catalog']
        
        # Create catalog entry adapter
        catalog_entry = CatalogEntryAdapter(catalog_data)
        
        # Create PDFAnalysis with catalog pages
        pdf_analysis = PDFAnalysis(
            filename=Path(pdf_path).name,
            total_pages=doc_catalog['total_pages'],
            i9_pages=[],
            document_catalog=doc_catalog,
            catalog_data={'catalog_entry': catalog_entry}
        )
        
        # Convert catalog pages to the format expected by the processor
        for page_data in doc_catalog['pages']:
            # Add to i9_pages if it's an I-9 form
            if (page_data.get('page_subtype') == 'i9_form' or 
                'i9' in page_data.get('page_title', '').lower()):
                pdf_analysis.i9_pages.append({
                    'page_number': page_data['page_number'],
                    'extracted_values': page_data.get('extracted_values', {}),
                    'page_title': page_data.get('page_title', ''),
                    'page_type': page_data.get('page_type', ''),
                    'page_subtype': page_data.get('page_subtype', ''),
                    'confidence_score': page_data.get('confidence_score', 0.0),
                    'text_regions': page_data.get('text_regions', []),
                    'page_metadata': page_data.get('page_metadata', {})
                })
        
        logger.info(f"Created PDFAnalysis with {len(pdf_analysis.i9_pages)} I-9 pages from catalog")
        return pdf_analysis
    
    @staticmethod
    def find_catalog_file_for_pdf(pdf_path: str, catalog_dir: str = "workdir/catalogs") -> Optional[str]:
        """Find corresponding catalog file for a PDF"""
        pdf_file = Path(pdf_path)
        catalog_dir_path = Path(catalog_dir)
        
        # Try different catalog file naming patterns
        possible_names = [
            f"{pdf_file.stem}.catalog.json",
            f"{pdf_file.name}.catalog.json"
        ]
        
        for catalog_name in possible_names:
            catalog_path = catalog_dir_path / catalog_name
            if catalog_path.exists():
                return str(catalog_path)
        
        return None
