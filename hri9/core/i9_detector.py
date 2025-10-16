#!/usr/bin/env python3
"""
I-9 form detection module.

This module provides functionality for detecting I-9 forms in PDF documents.
"""

import re
import fitz  # PyMuPDF
from typing import Optional, List, Tuple
from ..utils.logging_config import logger
from ..api.gemini_client import GeminiClient
from ..core.pdf_processor import PDFProcessor

class I9Detector:
    """Class for detecting I-9 forms in PDF documents."""
    
    def __init__(self, api_key=None, catalog_cache=None, error_handler=None):
        """
        Initialize the I-9 detector.
        
        Args:
            api_key (str, optional): API key for Gemini. If None, uses environment variable.
            catalog_cache (CatalogCache, optional): Cache for document catalog data.
            error_handler (CatalogErrorHandler, optional): Error handler for fallback mechanisms.
        """
        self.gemini_client = GeminiClient(api_key=api_key)
        self.catalog_cache = catalog_cache
        self.error_handler = error_handler
        # Page-level cache for Gemini API results to avoid repeated calls per process
        self._api_cache = {}  # key = (pdf_path, page_num_1idx) → (contains_i9, confidence, employee_signature_date)

        self.keyword_patterns = [
            r"form\s+i-9",
            r"employment\s+eligibility\s+verification",
            r"department\s+of\s+homeland\s+security",
            r"uscis",
            r"u\.s\.\s+citizenship\s+and\s+immigration\s+services"
        ]
    
    def detect_i9_by_keywords(self, text):
        """
        Detect I-9 form by searching for keywords in text.
        
        Args:
            text (str): Text content to search.
            
        Returns:
            bool: True if keywords found, False otherwise.
        """
        if not text:
            return False
            
        text_lower = text.lower()
        
        # If either of the two strongest phrases appears, treat as I-9.
        if "form i-9" in text_lower:
            logger.info("I-9 form detected through phrase 'Form I-9'")
            return True
        if "employment eligibility verification" in text_lower:
            logger.info("I-9 form detected through phrase 'Employment Eligibility Verification'")
            return True

        # Otherwise, count keyword pattern matches (relaxed threshold)
        match_count = 0
        for pattern in self.keyword_patterns:
            if re.search(pattern, text_lower):
                match_count += 1

        # If at least two patterns match, consider it an I-9 form (more permissive)
        if match_count >= 2:
            logger.info(f"I-9 form detected with {match_count} keyword matches")
            return True
            
        return False
    
    def _detect_i9_api_cached(self, pdf_path, page_num_1idx, text=""):
        """Call Gemini only once per (pdf,page) and cache the result (uses text+image)."""
        key = (pdf_path, page_num_1idx)
        if key in self._api_cache:
            return self._api_cache[key]

        # Always include an image to maximise detection accuracy
        image_base64 = PDFProcessor.render_page_to_base64(pdf_path, page_num_1idx - 1)
        contains_i9, confidence, employee_signature_date, _ = self.detect_i9_by_api(text=text, image_base64=image_base64)
        self._api_cache[key] = (contains_i9, confidence, employee_signature_date)
        return contains_i9, confidence, employee_signature_date

    def detect_i9_by_api(self, text=None, image_base64=None):
        """
        Detect I-9 form using Gemini API.
        
        Args:
            text (str, optional): Text content to analyze.
            image_base64 (str, optional): Base64-encoded image to analyze.
            
        Returns:
            tuple: (contains_i9, confidence, employee_signature_date, response_text)
        """
        return self.gemini_client.detect_i9_form(text=text, image_base64=image_base64)
    
    def detect_i9_in_page(self, pdf_path, page_num):
        """
        Detect I-9 form in a specific page of a PDF.
        
        Args:
            pdf_path (str): Path to the PDF file.
            page_num (int): Page number to check (0-indexed).
            
        Returns:
            tuple: (contains_i9, confidence, employee_signature_date)
        """
        # Extract text from the page
        text = PDFProcessor.extract_text_from_pdf(pdf_path, page_num)
        
        # First try keyword detection (fast)
        if self.detect_i9_by_keywords(text):
            return True, "HIGH", None

        # Otherwise fall back to cached Gemini detection (uses text+image)
        return self._detect_i9_api_cached(pdf_path, page_num + 1, text)
    
    def detect_i9_pages_from_catalog(self, document_id: str) -> Tuple[List[int], List[int]]:
        """
        Detect I-9 pages using catalog data.
        
        Args:
            document_id: Unique identifier for the document in catalog
            
        Returns:
            tuple: (all_i9_pages, latest_i9_pages) where:
                - all_i9_pages: List of all page numbers (1-indexed) containing I-9 forms
                - latest_i9_pages: List of page numbers for only the latest I-9 form
        """
        if not self.catalog_cache:
            logger.warning("No catalog cache available, falling back to original detection")
            return [], []
        
        catalog = self.catalog_cache.get_document_catalog(document_id)
        if not catalog:
            logger.warning(f"Document {document_id} not found in catalog")
            return [], []
        
        logger.info(f"Using catalog data to detect I-9 pages for document {document_id}")
        
        # Get all I-9 related pages from catalog
        i9_pages = self._identify_i9_pages_from_catalog(catalog.pages)
        
        if not i9_pages:
            logger.info(f"No I-9 pages found in catalog for document {document_id}")
            return [], []
        
        # Identify the latest I-9 form from catalog data
        latest_i9_pages = self._find_latest_i9_form_from_catalog(i9_pages)
        
        all_page_numbers = [page.page_number for page in i9_pages]
        latest_page_numbers = [page.page_number for page in latest_i9_pages]
        
        logger.info(f"Catalog-based detection found {len(all_page_numbers)} I-9 pages: {sorted(all_page_numbers)}")
        logger.info(f"Latest I-9 form pages: {sorted(latest_page_numbers)}")
        
        return sorted(all_page_numbers), sorted(latest_page_numbers)
    
    def detect_i9_pages_from_catalog_entry(self, catalog_entry) -> Tuple[List[int], List[int]]:
        """
        Detect I-9 pages using a catalog entry directly.
        
        Args:
            catalog_entry: DocumentCatalogEntry object
            
        Returns:
            tuple: (all_i9_pages, latest_i9_pages) where:
                - all_i9_pages: List of all page numbers (1-indexed) containing I-9 forms
                - latest_i9_pages: List of page numbers for only the latest I-9 form
        """
        logger.info(f"Using catalog entry directly to detect I-9 pages for document {catalog_entry.document_id}")
        
        # Get all I-9 related pages from catalog
        i9_pages = self._identify_i9_pages_from_catalog(catalog_entry.pages)
        
        if not i9_pages:
            logger.info(f"No I-9 pages found in catalog entry for document {catalog_entry.document_id}")
            return [], []
        
        # Identify the latest I-9 form from catalog data
        latest_i9_pages = self._find_latest_i9_form_from_catalog(i9_pages)
        
        all_page_numbers = [page.page_number for page in i9_pages]
        latest_page_numbers = [page.page_number for page in latest_i9_pages]
        
        logger.info(f"Catalog-based detection found {len(all_page_numbers)} I-9 pages: {sorted(all_page_numbers)}")
        logger.info(f"Latest I-9 form pages: {sorted(latest_page_numbers)}")
        
        return sorted(all_page_numbers), sorted(latest_page_numbers)
    
    def _identify_i9_pages_from_catalog(self, pages: List) -> List:
        """
        Identify I-9 pages from catalog page analyses using pure LLM classification.
        
        Args:
            pages: List of PageAnalysis objects
            
        Returns:
            List of PageAnalysis objects that are I-9 related
        """
        i9_pages = []
        
        for page in pages:
            # CRITICAL FIX: Check for I-9 subtype regardless of page_type
            # The LLM might classify I-9 forms with different page_type but correct subtype
            if page.page_subtype == "i9_form":
                logger.info(f"Page {page.page_number} identified as I-9 form by LLM subtype "
                           f"(page_type: {page.page_type}, confidence: {page.confidence_score:.2f})")
                i9_pages.append(page)
                continue
            
            # Include I-9 related subtypes - also check regardless of page_type
            if page.page_subtype in ["i9_document_list", "i9_instructions"]:
                logger.info(f"Page {page.page_number} identified as I-9 related by LLM subtype "
                           f"({page.page_subtype}, page_type: {page.page_type})")
                i9_pages.append(page)
                continue
            
            # Legacy check: Still include government_form + i9_form for backward compatibility
            if (page.page_type == "government_form" and 
                page.page_subtype == "i9_form"):
                logger.info(f"Page {page.page_number} identified as I-9 form by LLM (legacy path, confidence: {page.confidence_score:.2f})")
                i9_pages.append(page)
                continue
            
            # Legacy check: Include I-9 related subtypes as classified by LLM
            if (page.page_type == "government_form" and 
                page.page_subtype in ["i9_document_list", "i9_instructions"]):
                logger.info(f"Page {page.page_number} identified as I-9 related by LLM (legacy path: {page.page_subtype})")
                i9_pages.append(page)
        
        return i9_pages
    
    def _has_i9_indicators_in_extracted_values(self, extracted_values: dict) -> bool:
        """
        Check if extracted values contain I-9 form indicators.
        
        Args:
            extracted_values: Dictionary of extracted key-value pairs
            
        Returns:
            True if I-9 indicators found
        """
        if not extracted_values:
            return False
        
        # Convert all values to lowercase strings for checking
        all_text = " ".join(str(v).lower() for v in extracted_values.values() if v)
        
        # Check for strong I-9 indicators
        strong_indicators = [
            "form i-9",
            "employment eligibility verification",
            "department of homeland security",
            "uscis"
        ]
        
        for indicator in strong_indicators:
            if indicator in all_text:
                return True
        
        # Check for form version indicators
        if "form_version" in extracted_values:
            version = str(extracted_values["form_version"]).lower()
            if "i-9" in version or "i9" in version:
                return True
        
        return False
    
    def _has_i9_keywords_in_text_regions(self, text_regions: List) -> bool:
        """
        Check if text regions contain I-9 keywords.
        
        Args:
            text_regions: List of TextRegion objects
            
        Returns:
            True if I-9 keywords found
        """
        if not text_regions:
            return False
        
        # Combine all text from regions
        all_text = " ".join(region.text.lower() for region in text_regions if region.text)
        
        # Check for exclusion patterns that indicate this is NOT an I-9 form
        # but rather a document that mentions I-9 requirements
        exclusion_patterns = [
            "prior to beginning work",
            "employment offer",
            "offer letter",
            "must be signed on or before",
            "please contact our personnel office",
            "attached is a list",
            "sincerely",
            "dear "
        ]
        
        for pattern in exclusion_patterns:
            if pattern in all_text:
                logger.debug(f"Excluding page due to pattern: '{pattern}'")
                return False
        
        # Use existing keyword detection logic only if no exclusion patterns found
        return self.detect_i9_by_keywords(all_text)
    
    def _find_latest_i9_form_from_catalog(self, i9_pages: List) -> List:
        """
        Find the latest I-9 form from catalog data.
        
        Args:
            i9_pages: List of PageAnalysis objects that are I-9 related
            
        Returns:
            List of PageAnalysis objects for the latest I-9 form
        """
        if not i9_pages:
            return []
        
        # Separate main form pages from supplemental pages
        main_form_pages = []
        supplemental_pages = []
        
        for page in i9_pages:
            if page.page_subtype == "i9_form":
                main_form_pages.append(page)
            else:
                supplemental_pages.append(page)
        
        if not main_form_pages:
            # If no main form pages, return all pages
            return i9_pages
        
        # Group main form pages by proximity (within 3 pages of each other)
        main_form_pages.sort(key=lambda p: p.page_number)
        clusters = []
        current_cluster = [main_form_pages[0]]
        
        for page in main_form_pages[1:]:
            if page.page_number - current_cluster[-1].page_number <= 3:
                current_cluster.append(page)
            else:
                clusters.append(current_cluster)
                current_cluster = [page]
        
        if current_cluster:
            clusters.append(current_cluster)
        
        # Find the latest cluster based on extracted dates or page position
        latest_cluster = self._select_latest_cluster(clusters)
        
        # Add relevant supplemental pages near the latest cluster
        latest_pages = list(latest_cluster)
        if supplemental_pages:
            cluster_page_range = (
                min(p.page_number for p in latest_cluster),
                max(p.page_number for p in latest_cluster)
            )
            
            for supp_page in supplemental_pages:
                # Only include supplemental pages that are government forms and within 2 pages of the cluster
                # This prevents employment letters that mention I-9 from being included
                if (supp_page.page_type == "government_form" and 
                    cluster_page_range[0] - 2 <= supp_page.page_number <= cluster_page_range[1] + 2):
                    logger.info(f"Including supplemental I-9 page {supp_page.page_number} ({supp_page.page_subtype})")
                    latest_pages.append(supp_page)
                else:
                    logger.info(f"Excluding page {supp_page.page_number} - not a government form or too far from I-9 cluster")
        
        return latest_pages
    
    def _select_latest_cluster(self, clusters: List[List]) -> List:
        """
        Select the latest cluster based on employee signature dates, then extracted dates, then position.
        
        Args:
            clusters: List of clusters, each containing PageAnalysis objects
            
        Returns:
            The cluster representing the latest I-9 form
        """
        if not clusters:
            return []
        
        if len(clusters) == 1:
            return clusters[0]
        
        # First priority: Find the latest based on employee signature dates
        latest_cluster = None
        latest_signature_date = None
        
        for cluster in clusters:
            cluster_signature_date = self._extract_employee_signature_date_from_cluster(cluster)
            if cluster_signature_date:
                if latest_signature_date is None or self._compare_dates(cluster_signature_date, latest_signature_date) > 0:
                    latest_signature_date = cluster_signature_date
                    latest_cluster = cluster
        
        # If we found a signature date-based latest, return it
        if latest_cluster:
            logger.info(f"Selected cluster based on employee signature date: {latest_signature_date}")
            return latest_cluster
        
        # Second priority: Try to find the latest based on other extracted dates
        latest_cluster = None
        latest_date = None
        
        for cluster in clusters:
            cluster_date = self._extract_latest_date_from_cluster(cluster)
            if cluster_date and (latest_date is None or cluster_date > latest_date):
                latest_date = cluster_date
                latest_cluster = cluster
        
        # If we found a date-based latest, return it
        if latest_cluster:
            logger.info(f"Selected cluster based on extracted date: {latest_date}")
            return latest_cluster
        
        # Otherwise, return the cluster with the highest page numbers (last in document)
        logger.info("Selected cluster based on page position (fallback)")
        return max(clusters, key=lambda c: max(p.page_number for p in c))
    
    def _extract_employee_signature_date_from_cluster(self, cluster: List) -> Optional[str]:
        """
        Extract the employee signature date from a cluster of I-9 pages.
        
        Args:
            cluster: List of PageAnalysis objects
            
        Returns:
            Employee signature date string, or None
        """
        for page in cluster:
            # Check extracted values for employee signature date
            if hasattr(page, 'extracted_values') and page.extracted_values:
                # Look for specific employee signature date fields
                for key, value in page.extracted_values.items():
                    if key.lower() in ['employee_signature_date', 'employee_date', 'signature_date'] and value:
                        logger.info(f"Found employee signature date on page {page.page_number}: {value}")
                        return str(value)
        
        return None
    
    def _compare_dates(self, date1: str, date2: str) -> int:
        """
        Compare two date strings.
        
        Args:
            date1: First date string
            date2: Second date string
            
        Returns:
            1 if date1 > date2, -1 if date1 < date2, 0 if equal
        """
        try:
            from datetime import datetime
            
            # Try different date formats
            formats = ['%m/%d/%Y', '%m/%d/%y', '%Y-%m-%d', '%d/%m/%Y', '%d/%m/%y']
            
            parsed_date1 = None
            parsed_date2 = None
            
            for fmt in formats:
                try:
                    if not parsed_date1:
                        parsed_date1 = datetime.strptime(date1, fmt)
                    if not parsed_date2:
                        parsed_date2 = datetime.strptime(date2, fmt)
                    if parsed_date1 and parsed_date2:
                        break
                except ValueError:
                    continue
            
            if parsed_date1 and parsed_date2:
                if parsed_date1 > parsed_date2:
                    return 1
                elif parsed_date1 < parsed_date2:
                    return -1
                else:
                    return 0
            
            # Fallback to string comparison
            return 1 if date1 > date2 else (-1 if date1 < date2 else 0)
            
        except Exception as e:
            logger.warning(f"Error comparing dates {date1} and {date2}: {e}")
            # Fallback to string comparison
            return 1 if date1 > date2 else (-1 if date1 < date2 else 0)
    
    def _extract_latest_date_from_cluster(self, cluster: List) -> Optional[str]:
        """
        Extract the latest date from a cluster of pages.
        
        Args:
            cluster: List of PageAnalysis objects
            
        Returns:
            Latest date string found, or None
        """
        dates = []
        
        for page in cluster:
            # Check extracted values for dates
            for key, value in page.extracted_values.items():
                if "date" in key.lower() and value:
                    dates.append(str(value))
            
            # Check text regions for date patterns
            for region in page.text_regions:
                if region.text:
                    # Simple date pattern matching
                    date_matches = re.findall(r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}', region.text)
                    dates.extend(date_matches)
        
        if not dates:
            return None
        
        # Return the lexicographically latest date (simple heuristic)
        return max(dates)

    def detect_all_i9_pages(self, pdf_path, document_id=None):
        """
        Detect all I-9 form pages in a PDF, including main forms and supplemental pages.
        
        Args:
            pdf_path (str): Path to the PDF file.
            document_id (str, optional): Document ID for catalog lookup.
            
        Returns:
            tuple: (all_i9_pages, latest_i9_pages) where:
                - all_i9_pages: List of all page numbers (1-indexed) containing I-9 forms
                - latest_i9_pages: List of page numbers for only the latest I-9 form
        """
        # Debug logging
        logger.info(f"detect_all_i9_pages called with document_id: {document_id}")
        logger.info(f"catalog_cache available: {self.catalog_cache is not None}")
        
        # Try catalog-based detection first if available
        if self.catalog_cache is not None and document_id:
            try:
                logger.info(f"Attempting catalog-based detection for document_id: {document_id}")
                # Check if document exists in cache
                catalog_entry = self.catalog_cache.get_document_catalog(document_id)
                if catalog_entry:
                    logger.info(f"Found document {document_id} in catalog cache with {len(catalog_entry.pages)} pages")
                else:
                    logger.warning(f"Document {document_id} not found in catalog cache")
                
                catalog_result = self.detect_i9_pages_from_catalog(document_id)
                if catalog_result[0]:  # If we found pages in catalog
                    logger.info(f"Successfully used catalog data for I-9 detection in {pdf_path}")
                    return catalog_result
                else:
                    logger.info(f"No I-9 pages found in catalog for {document_id}, falling back to original detection")
            except Exception as e:
                logger.warning(f"Catalog-based detection failed for {document_id}: {e}, falling back to original detection")
                # Use error handler if available
                if self.error_handler:
                    self.error_handler.handle_cache_error("catalog_detection", document_id, e)
        else:
            if not self.catalog_cache:
                logger.info(f"No catalog cache available, using original detection method for {pdf_path}")
            elif not document_id:
                logger.info(f"No document_id provided, using original detection method for {pdf_path}")
            else:
                logger.info(f"Catalog cache and document_id both available but condition failed - cache: {self.catalog_cache is not None}, document_id: {document_id}")
        
        # Fall back to original detection method
        logger.info(f"Using original detection method for {pdf_path}")
        
        # Use error handler's fallback if available and conditions warrant it
        if self.error_handler and self.error_handler.should_use_fallback("catalog"):
            fallback_detector = self.error_handler.get_fallback_i9_detector()
            return fallback_detector(pdf_path, document_id)
        
        return self._detect_all_i9_pages_original(pdf_path)
    
    def _detect_all_i9_pages_original(self, pdf_path):
        """
        Original I-9 detection method (preserved as fallback).
        
        Args:
            pdf_path (str): Path to the PDF file.
            
        Returns:
            tuple: (all_i9_pages, latest_i9_pages)
        """
        try:
            # Process PDF in blocks for detection
            blocks = PDFProcessor.extract_pdf_blocks(pdf_path, block_size=5)
            if not blocks:
                logger.warning(f"Could not read PDF: {pdf_path}")
                return [], []
            
            # Collect all I-9 forms in the PDF
            i9_pages = []
            i9_form_pages = []  # Pages that are definitely main I-9 form pages
            document_list_pages = []  # Pages that are document list pages
            
            # Open the document for text extraction
            doc = fitz.open(pdf_path)
            total_pages = len(doc)
            
            # First pass: Detect main I-9 form pages using keywords and API
            logger.info(f"First pass: Detecting main I-9 form pages in {pdf_path}")
            for block in blocks:
                for page_num, text in block:
                    # Skip if we've already processed this page
                    if page_num in i9_pages:
                        continue
                        
                    # Check if this page contains I-9 form indicators directly
                    if self.detect_i9_by_keywords(text):
                        logger.info(f"I-9 form detected on page {page_num} through direct text matching")
                        i9_pages.append(page_num)
                        i9_form_pages.append(page_num)
                        continue
                    
                    # Use the Gemini API for more accurate detection
                    # First try with text only if we have text
                    contains_i9, confidence, _ = self._detect_i9_api_cached(pdf_path, page_num, text)
                    
                    if contains_i9 and confidence in ["HIGH", "MEDIUM", "LOW"]:
                        logger.info(f"I-9 form detected on page {page_num} through Gemini API (confidence {confidence})")
                        i9_pages.append(page_num)
                        i9_form_pages.append(page_num)
            
            # Second pass: Check for "Lists of Acceptable Documents" pages and additional I-9 pages
            logger.info(f"Second pass: Detecting document list pages and additional I-9 pages")
            for page_num in range(1, total_pages + 1):
                # Skip pages we've already identified
                if page_num in i9_pages:
                    continue
                    
                # Get the text from the current page
                page_text = doc[page_num - 1].get_text().lower()
                
                # Check for I-9 form title or header (relaxed: either phrase is sufficient)
                if "form i-9" in page_text or "employment eligibility verification" in page_text:
                    logger.info(f"I-9 form header detected on page {page_num}")
                    i9_pages.append(page_num)
                    i9_form_pages.append(page_num)
                    continue
                
                # Enhanced detection for "Lists of Acceptable Documents" page
                is_document_list_page = False
                
                # Primary indicator: explicit title mention
                if "lists of acceptable documents" in page_text or "list of acceptable documents" in page_text:
                    is_document_list_page = True
                    logger.info(f"'Lists of Acceptable Documents' page detected by title on page {page_num}")
                
                # Secondary indicator: presence of all three list sections
                elif ("list a" in page_text and "list b" in page_text and "list c" in page_text):
                    is_document_list_page = True
                    logger.info(f"'Lists of Acceptable Documents' page detected by list sections on page {page_num}")
                
                # Tertiary indicators: specific content from the document list page
                elif any([
                    "documents that establish both identity and employment authorization" in page_text,
                    "documents that establish identity" in page_text and "documents that establish employment authorization" in page_text,
                    "u.s. passport or u.s. passport card" in page_text and "permanent resident card" in page_text,
                    "driver's license" in page_text and "social security account number card" in page_text
                ]):
                    is_document_list_page = True
                    logger.info(f"'Lists of Acceptable Documents' page detected by content on page {page_num}")
                
                # Check if page follows a confirmed I-9 form page (often the second page)
                elif any(p for p in i9_form_pages if abs(page_num - p) <= 2):
                    # Look for additional clues that this is a continuation/document list page
                    if ("employment authorization" in page_text and 
                        ("document" in page_text or "identity" in page_text)):
                        is_document_list_page = True
                        logger.info(f"'Lists of Acceptable Documents' page detected as continuation page {page_num}")
                
                if is_document_list_page:
                    i9_pages.append(page_num)
                    document_list_pages.append(page_num)
            
            # Third pass: Check for pages between confirmed I-9 pages that might be part of the form
            logger.info(f"Third pass: Checking for missed pages between confirmed I-9 pages")
            if len(i9_pages) >= 2:
                sorted_pages = sorted(i9_pages)
                for i in range(len(sorted_pages) - 1):
                    current_page = sorted_pages[i]
                    next_page = sorted_pages[i + 1]
                    
                    # If there's a gap between confirmed I-9 pages, check those pages
                    # Using a larger gap (up to 5 pages) to ensure we don't miss related pages
                    if 1 < next_page - current_page <= 5:
                        for gap_page in range(current_page + 1, next_page):
                            if gap_page not in i9_pages:
                                # Get the text from the gap page
                                page_text = doc[gap_page - 1].get_text().lower()
                                
                                # Check if this page has any I-9 related content
                                if any([
                                    "i-9" in page_text,
                                    "employment eligibility" in page_text,
                                    "employment authorization" in page_text,
                                    "department of homeland security" in page_text,
                                    "uscis" in page_text,
                                    "citizen" in page_text and "alien" in page_text,
                                    "verification" in page_text and "document" in page_text
                                ]):
                                    logger.info(f"Additional I-9 page detected in gap on page {gap_page}")
                                    i9_pages.append(gap_page)
                                    i9_form_pages.append(gap_page)
                                # If the page has very little text, it might be a scanned I-9 form with poor OCR
                                # In this case, we should use image-based detection but with stricter confidence requirements
                                elif len(page_text.strip()) < 100:
                                    logger.info(f"Page {gap_page} has minimal text ({len(page_text.strip())} chars), using image detection")
                                    # Try image-based detection for this page
                                    contains_i9, confidence, _ = self._detect_i9_api_cached(pdf_path, gap_page, "")
                                    # Only accept HIGH confidence for gap pages
                                    if contains_i9 and confidence == "HIGH":
                                            logger.info(f"I-9 form detected on page {gap_page} through image-based detection")
                                            i9_pages.append(gap_page)
                                            i9_form_pages.append(gap_page)
            
            # Fourth pass: Inspect pages directly before and after confirmed I-9 pages
            logger.info(f"Fourth pass: Checking adjacent pages to confirmed I-9 pages")
            candidates = set()
            for p in i9_form_pages:
                if p - 1 >= 1:
                    candidates.add(p - 1)
                if p + 1 <= len(doc):
                    candidates.add(p + 1)
            # Remove already detected pages
            candidates = [c for c in candidates if c not in i9_pages]
            for cand in sorted(candidates):
                page_text = doc[cand - 1].get_text().lower()
                contains_i9 = False
                confidence = "LOW"
                # Heuristic keywords for main or continuation pages
                if any(kw in page_text for kw in [
                    # Main form (employee section)
                    "section 1",
                    "employee information and attestation",
                    "last name (family name)",
                    # Continuation / employer section
                    "employer or authorized representative",
                    "section 2",
                    "review and verification",
                    "certification of employment authorization",
                    "section 3"
                ]):
                    logger.info(f"Adjacent page {cand} has I-9 specific keywords – marking as I-9 page")
                    contains_i9 = True
                else:
                    # Try image-based detection since text may be sparse
                    image_base64 = PDFProcessor.render_page_to_base64(pdf_path, cand - 1)
                    if image_base64:
                        contains_i9, confidence, _ = self._detect_i9_api_cached(pdf_path, cand, page_text)
                if contains_i9 and confidence in ["HIGH", "MEDIUM", "LOW"]:
                    logger.info(f"Adjacent page {cand} added as I-9 page (confidence {confidence})")
                    i9_pages.append(cand)
                    i9_form_pages.append(cand)
            
            # Get total pages in the document before closing
            total_pages = len(doc)
            
            # If no I-9 pages found, return empty lists
            if not i9_pages:
                logger.warning(f"No I-9 pages detected in {pdf_path}")
                doc.close()
                return [], []
            
            # Fourth pass: Final validation of detected pages
            logger.info(f"Fourth pass: Final validation of detected pages")
            
            # No additional processing needed - rely on the detection from previous passes
            
            # Now close the document after we're done with it
            doc.close()
            
            # Log the detection results
            logger.info(f"Detected {len(i9_pages)} total I-9 pages: {sorted(i9_pages)}")
            logger.info(f"Of which {len(i9_form_pages)} are main form pages: {sorted(i9_form_pages)}")
            logger.info(f"And {len(document_list_pages)} are document list pages: {sorted(document_list_pages)}")
            
            # --- Revised logic: group contiguous form pages into clusters and
            #     pick the cluster that appears last in the document (or has the
            #     most recent signature date if available). This guarantees at
            #     most ONE form set is returned. ---
            latest_i9_pages = self._identify_latest_i9_form(pdf_path, sorted(i9_pages))
            
            return sorted(i9_pages), latest_i9_pages
            
        except Exception as e:
            logger.error(f"Error detecting I-9 pages: {e}")
            return [], []
    
    def get_token_usage(self):
        """
        Get current token usage from the Gemini client.
        
        Returns:
            int: Total tokens used.
        """
        return self.gemini_client.get_token_usage()
        
    def _identify_latest_i9_form(self, pdf_path, i9_pages):
        """
        Identify the latest I-9 form based on dates in the document.
        
        Args:
            pdf_path (str): Path to the PDF file
            i9_pages (list): List of page numbers containing I-9 forms
            
        Returns:
            list: List of page numbers for the latest I-9 form
        """
        try:
            if not i9_pages:
                return []
                
            # Open the PDF document
            doc = fitz.open(pdf_path)
            
            # Separate main form pages vs doc-list pages
            main_i9_pages = []
            document_list_pages = []
            
            for page_num in i9_pages:
                page_text = doc[page_num - 1].get_text().lower()
                
                # Check if this is a main I-9 form page or just a document list page
                if ("form i-9" in page_text and "employment eligibility verification" in page_text):
                    main_i9_pages.append(page_num)
                    logger.info(f"Page {page_num} identified as main I-9 form page")
                elif any([
                    "lists of acceptable documents" in page_text,
                    "list of acceptable documents" in page_text,
                    ("list a" in page_text and "list b" in page_text and "list c" in page_text)
                ]):
                    document_list_pages.append(page_num)
                    logger.info(f"Page {page_num} identified as document list page")
                else:
                    # If we can't clearly identify, assume it's part of the main form
                    main_i9_pages.append(page_num)
                    logger.info(f"Page {page_num} assumed to be part of I-9 form")
            
            # Group contiguous main pages into clusters (gap ≤2 pages)
            clusters = []
            for p in sorted(main_i9_pages):
                if not clusters or p - clusters[-1][-1] > 2:
                    clusters.append([p])
                else:
                    clusters[-1].append(p)

            # Prefer cluster with most recent signature date; fallback to last cluster
            latest_cluster = clusters[-1]
            latest_date = None
            date_pattern = r"(0?[1-9]|1[0-2])[\-/](0?[1-9]|[12][0-9]|3[01])[\-/](\d{2,4})"
            for cluster in clusters:
                for page_num in cluster:
                    page_text = doc[page_num - 1].get_text().lower()
                    for match in re.findall(date_pattern, page_text):
                        month, day, year = match
                        if len(year) == 2:
                            year = '20' + year if int(year) < 50 else '19' + year
                        date_value = int(year+month.zfill(2)+day.zfill(2))
                        if latest_date is None or date_value > latest_date:
                            latest_date = date_value
                            latest_cluster = cluster

            # Collect pages: main pages of latest cluster plus doc-list pages within ±2 pages
            latest_form_pages = set(latest_cluster)
            for dl_page in document_list_pages:
                if min(latest_cluster) - 2 <= dl_page <= max(latest_cluster) + 2:
                    latest_form_pages.add(dl_page)

            
            # Add any document list pages that follow the latest form pages
            if document_list_pages and latest_form_pages:
                max_form_page = max(latest_form_pages)
                for doc_page in sorted(document_list_pages):
                    # Include document list pages that are within 3 pages of the last form page
                    if doc_page > max_form_page and doc_page <= max_form_page + 3:
                        latest_form_pages.add(doc_page)
                        logger.info(f"Added document list page {doc_page} to latest form")
            
            # Close the document
            doc.close()
            
            # Return the pages of the latest form, sorted
            return sorted(latest_form_pages)
            
        except Exception as e:
            logger.error(f"Error identifying latest I-9 form: {e}")
            # If there's an error, return all pages as a fallback
            return i9_pages
