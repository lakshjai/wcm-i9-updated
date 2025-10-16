#!/usr/bin/env python3
"""
Page analyzer for comprehensive document cataloging.

This module provides the PageAnalyzer class that uses Gemini AI to perform
detailed analysis of individual PDF pages for document cataloging purposes.
"""

import json
import re
from typing import Dict, List, Optional, Tuple, Any
from ..api.gemini_client import GeminiClient
from ..core.pdf_processor import PDFProcessor
from ..utils.logging_config import logger
from .models import PageAnalysis, PageMetadata, TextRegion, BoundingBox
from .error_handler import CatalogErrorHandler
from .logging import get_catalog_logger


class PageAnalyzer:
    """
    Analyzer for individual PDF pages using Gemini AI.
    
    This class performs comprehensive analysis of document pages including:
    - Page type classification (government_form, identity_document, etc.)
    - Structured data extraction based on document type
    - Text region identification with confidence scoring
    - Metadata collection about page characteristics
    """
    
    # Comprehensive analysis prompt for single page cataloging
    CATALOG_ANALYSIS_PROMPT = """
You are an expert document analyzer specializing in comprehensive document cataloging. 
Analyze this document page and provide a detailed analysis in the following JSON format:

{
  "page_title": "Brief descriptive title of the page",
  "page_type": "government_form|identity_document|employment_record|other",
  "page_subtype": "i9_form|passport|drivers_license|social_security_card|birth_certificate|employment_contract|pay_stub|tax_document|other",
  "confidence_score": 0.0-1.0,
  "extracted_values": {
    // Key-value pairs of structured data found on the page
    // For I-9 forms: form_version, employee_name, hire_date, citizenship_status, etc.
    // For identity docs: document_number, expiration_date, issuing_authority, etc.
    // For employment records: employee_id, position, salary, dates, etc.
  },
  "text_regions": [
    {
      "region_id": "descriptive_name",
      "text": "extracted text content",
      "confidence": 0.0-1.0
    }
  ],
  "page_metadata": {
    "has_handwritten_text": true|false,
    "has_signatures": true|false,
    "image_quality": "high|medium|low",
    "language": "en|es|other",
    "form_version": "version if applicable",
    "security_features": ["watermark", "hologram", "seal", "etc."],
    "text_extraction_method": "text|ocr|hybrid"
  }
}

CRITICAL ACCURACY REQUIREMENTS:

**DATES - EXTREME CARE REQUIRED:**
1. Extract ALL dates in strict MM/DD/YYYY format (e.g., "02/15/2023" NOT "12/15/2023")
2. For HANDWRITTEN dates, examine each digit carefully:
   - "0" vs "1": Look at the shape - "0" is round/oval, "1" is straight/vertical
   - "2" vs "7": "2" has horizontal base, "7" has diagonal stroke
   - Double-check month values are 01-12, days are 01-31
3. If a date is unclear, set confidence to 0.6 or lower and include in metadata
4. NEVER guess dates - if uncertain, use null and note in page_metadata

**DOCUMENT TITLES - VERBATIM EXTRACTION:**
5. Extract document titles EXACTLY as written - do not paraphrase or summarize
6. Include ALL components mentioned (e.g., "EAD issued by DHS form DS-2019 with I-94 and letter from exchange program")
7. Preserve exact wording, capitalization, and punctuation from the original
8. For multi-part documents, list ALL parts separated by "with" or "and"
9. **CRITICAL for handwritten document titles**:
   - Look at the IMAGE carefully, not just OCR text
   - Common handwriting patterns: "issued by DHS" NOT "by itself"
   - Look for "I-94" which is often missed or misread
   - "EAD" = Employment Authorization Document
   - Read each word carefully: "issued", "by", "DHS", "with", "I-94"
   - If text is unclear, examine the image more closely before guessing

**SECTION 2 & 3 HANDLING:**
9. When a page contains BOTH Section 2 and Section 3:
   - Extract Section 2 data (List A/B/C documents) if fields are filled
   - Extract Section 3 data (reverification) if fields are filled
   - Skip sections where ALL fields are empty/blank
   - Use field prefixes: "section_2_" for Section 2, "reverification_" for Section 3

**FIELD VALIDATION:**
10. Only extract fields that have actual values - skip empty/blank fields
11. Use "N/A" only when explicitly written on the form
12. Use null for truly empty fields
13. Validate extracted data makes logical sense (e.g., expiration dates are future dates)

**QUALITY INDICATORS:**
14. Set confidence_score based on actual certainty, not optimism
15. Mark has_handwritten_text=true if ANY handwriting present
16. Note text_extraction_method: "text" for digital PDFs, "ocr" for scanned images
17. Flag low image quality that may affect accuracy

**I-9 SPECIFIC REQUIREMENTS:**
18. For Section 3 reverification, extract: reverification_document_title, reverification_document_number, 
    reverification_expiration_date, reverification_employer_name, reverification_date_signed
19. For Supplement B, use field names: reverification_1_document_title, reverification_1_signature_date, etc.
20. Distinguish between employer_signature_date (Section 2) and reverification_date_signed (Section 3)

Respond ONLY with the JSON object, no additional text.
"""
    
    def __init__(self, gemini_client: GeminiClient, error_handler: Optional[CatalogErrorHandler] = None):
        """
        Initialize the PageAnalyzer.
        
        Args:
            gemini_client (GeminiClient): Configured Gemini API client
            error_handler (CatalogErrorHandler, optional): Error handler for recovery
        """
        self.gemini_client = gemini_client
        self.analysis_cache = {}  # Cache for page analyses within session
        self.error_handler = error_handler or CatalogErrorHandler()
        self.catalog_logger = get_catalog_logger()
        
    def analyze_page(self, pdf_path: str, page_num: int, text: Optional[str] = None, 
                    image_base64: Optional[str] = None) -> PageAnalysis:
        """
        Analyze a single PDF page comprehensively.
        
        Args:
            pdf_path (str): Path to the PDF file
            page_num (int): Page number (1-indexed)
            text (str, optional): Pre-extracted text content
            image_base64 (str, optional): Pre-rendered base64 image
            
        Returns:
            PageAnalysis: Comprehensive analysis results for the page
        """
        import os
        document_id = os.path.basename(pdf_path)
        
        # Check cache first
        cache_key = (pdf_path, page_num)
        if cache_key in self.analysis_cache:
            self.catalog_logger.log_cache_operation(
                "cache_hit", "get", hit=True, cache_size=len(self.analysis_cache)
            )
            logger.debug(f"Using cached analysis for page {page_num} of {pdf_path}")
            return self.analysis_cache[cache_key]
        
        # Log cache miss
        self.catalog_logger.log_cache_operation(
            "cache_miss", "get", hit=False, cache_size=len(self.analysis_cache)
        )
        
        with self.catalog_logger.operation_context(
            "page_analysis",
            document_id=document_id,
            page_number=page_num,
            pdf_path=pdf_path,
            has_text=text is not None,
            has_image=image_base64 is not None
        ) as correlation_id:
            
            analysis_response = None
            try:
                # Extract text if not provided
                if text is None:
                    text = PDFProcessor.extract_text_from_pdf(pdf_path, page_num - 1)  # Convert to 0-indexed
                
                # Check if we should use simplified analysis for non-I9 forms
                from ..config import settings
                if settings.I9_ONLY_ANALYSIS or settings.SKIP_NON_I9_EXTRACTION:
                    # Quick check if this might be an I-9 form using keywords
                    if not self._is_likely_i9_form(text):
                        logger.debug(f"Page {page_num} doesn't appear to be I-9 form, using simplified analysis")
                        page_analysis = self._create_simplified_analysis(page_num, text)
                        
                        # Cache the result
                        self.analysis_cache[cache_key] = page_analysis
                        self.catalog_logger.log_cache_operation(
                            correlation_id, "set", hit=True, cache_size=len(self.analysis_cache)
                        )
                        
                        return page_analysis
                
                # Render image if not provided (only for full analysis)
                if image_base64 is None:
                    image_base64 = PDFProcessor.render_page_to_base64(pdf_path, page_num - 1)  # Convert to 0-indexed
                
                # Perform AI analysis
                analysis_response = self._call_gemini_for_analysis(text, image_base64, correlation_id)
                
                # Parse the response into structured data
                page_analysis = self._parse_analysis_response(analysis_response, page_num)
                
                # Cache the result
                self.analysis_cache[cache_key] = page_analysis
                self.catalog_logger.log_cache_operation(
                    correlation_id, "set", hit=True, cache_size=len(self.analysis_cache)
                )
                
                # Log performance metrics
                self.catalog_logger.log_performance_metrics(
                    correlation_id,
                    {
                        "text_length": len(text) if text else 0,
                        "has_image": image_base64 is not None,
                        "cache_size": len(self.analysis_cache)
                    }
                )
                
                # Log validation results
                self.catalog_logger.log_validation_result(
                    correlation_id,
                    "confidence_check",
                    passed=page_analysis.confidence_score >= 0.5,
                    details={
                        "confidence_score": page_analysis.confidence_score,
                        "page_type": page_analysis.page_type,
                        "page_subtype": page_analysis.page_subtype
                    }
                )
                
                logger.info(f"Analyzed page {page_num}: {page_analysis.page_type}/{page_analysis.page_subtype} "
                           f"(confidence: {page_analysis.confidence_score:.2f})")
                
                return page_analysis
                
            except Exception as e:
                logger.error(f"Error analyzing page {page_num} of {pdf_path}: {e}")
                
                # Check if this is a parsing error (we have a response but couldn't parse it)
                if analysis_response and "parsing" in str(e).lower():
                    return self.error_handler.handle_parsing_error(analysis_response, page_num, e, pdf_path)
                else:
                    # API error or other error - pass the response if available
                    return self.error_handler.handle_api_error(pdf_path, page_num, e, text or "", 0, analysis_response)
    
    # Batch analysis prompt for multiple pages
    BATCH_CATALOG_ANALYSIS_PROMPT = """
You are an expert document analyzer specializing in comprehensive document cataloging. 
Analyze the provided document pages and provide a detailed analysis for each page in the following JSON format:

{
  "pages": [
    {
      "page_number": 1,
      "page_title": "Brief descriptive title of the page",
      "page_type": "government_form|identity_document|employment_record|other",
      "page_subtype": "i9_form|passport|drivers_license|social_security_card|birth_certificate|employment_contract|pay_stub|tax_document|other",
      "confidence_score": 0.0-1.0,
      "extracted_values": {
        // Key-value pairs of structured data found on the page
        // For I-9 forms: form_version, employee_name, hire_date, citizenship_status, etc.
        // For identity docs: document_number, expiration_date, issuing_authority, etc.
        // For employment records: employee_id, position, salary, dates, etc.
      },
      "text_regions": [
        {
          "region_id": "descriptive_name",
          "text": "extracted text content",
          "confidence": 0.0-1.0
        }
      ],
      "page_metadata": {
        "has_handwritten_text": true|false,
        "has_signatures": true|false,
        "image_quality": "high|medium|low",
        "language": "en|es|other",
        "form_version": "version if applicable",
        "security_features": ["watermark", "hologram", "seal", "etc."]
      }
    }
    // ... additional pages
  ]
}

IMPORTANT GUIDELINES:
1. Analyze each page individually and provide separate results for each
2. Maintain the same quality and detail as single-page analysis
3. Use the correct page_number for each page in the batch
4. Focus on accuracy and completeness for each page
5. For I-9 forms, identify the specific version and extract all relevant fields
6. For identity documents, extract key identifying information and validity dates
7. Use high confidence (0.8+) only when you're very certain about classifications
8. Include all visible text regions with their purpose and confidence
9. Extract dates in MM/DD/YYYY format when possible
10. Ensure the JSON is valid and complete for all pages

Respond ONLY with the JSON object containing the "pages" array, no additional text.
"""
    
    def analyze_pages_batch(self, pdf_path: str, page_batch: List[Tuple[int, Optional[str], Optional[str]]]) -> List[PageAnalysis]:
        """
        Analyze multiple PDF pages in a single API call for improved performance.
        
        Args:
            pdf_path (str): Path to the PDF file
            page_batch (List[Tuple[int, str, str]]): List of (page_num, text, image_base64) tuples
            
        Returns:
            List[PageAnalysis]: List of page analyses in the same order as input
        """
        import os
        document_id = os.path.basename(pdf_path)
        
        # Check cache for all pages first
        cached_results = []
        uncached_pages = []
        
        for page_num, text, image_base64 in page_batch:
            cache_key = (pdf_path, page_num)
            if cache_key in self.analysis_cache:
                self.catalog_logger.log_cache_operation(
                    "batch_cache_hit", "get", hit=True, cache_size=len(self.analysis_cache)
                )
                cached_results.append((page_num, self.analysis_cache[cache_key]))
            else:
                uncached_pages.append((page_num, text, image_base64))
        
        # If all pages are cached, return cached results
        if not uncached_pages:
            return [result[1] for result in sorted(cached_results, key=lambda x: x[0])]
        
        # Process uncached pages in batch
        with self.catalog_logger.operation_context(
            "batch_page_analysis",
            document_id=document_id,
            batch_size=len(uncached_pages),
            pdf_path=pdf_path
        ) as correlation_id:
            
            try:
                # Prepare batch data for API call
                batch_data = []
                for page_num, text, image_base64 in uncached_pages:
                    # Extract text if not provided
                    if text is None:
                        text = PDFProcessor.extract_text_from_pdf(pdf_path, page_num - 1)
                    
                    # Render image if not provided
                    if image_base64 is None:
                        image_base64 = PDFProcessor.render_page_to_base64(pdf_path, page_num - 1)
                    
                    batch_data.append({
                        'page_number': page_num,
                        'text': text,
                        'image_base64': image_base64
                    })
                
                # Call API with batch data
                batch_response = self._call_gemini_for_batch_analysis(batch_data, correlation_id)
                
                # Parse batch response
                batch_analyses = self._parse_batch_analysis_response(batch_response, uncached_pages)
                
                # Cache results
                for page_num, analysis in zip([p[0] for p in uncached_pages], batch_analyses):
                    if analysis is not None:
                        cache_key = (pdf_path, page_num)
                        self.analysis_cache[cache_key] = analysis
                        self.catalog_logger.log_cache_operation(
                            correlation_id, "set", hit=True, cache_size=len(self.analysis_cache)
                        )
                
                # Log performance metrics
                self.catalog_logger.log_performance_metrics(
                    correlation_id,
                    {
                        "batch_size": len(uncached_pages),
                        "total_text_length": sum(len(p[1] or "") for p in uncached_pages),
                        "cache_size": len(self.analysis_cache),
                        "pages_processed": len(batch_analyses)
                    }
                )
                
                # Combine cached and new results
                all_results = {}
                for page_num, analysis in cached_results:
                    all_results[page_num] = analysis
                
                for page_num, analysis in zip([p[0] for p in uncached_pages], batch_analyses):
                    if analysis is not None:
                        all_results[page_num] = analysis
                
                # Return results in original order
                return [all_results.get(page_num) for page_num, _, _ in page_batch]
                
            except Exception as e:
                logger.error(f"Error in batch analysis for {pdf_path}: {e}")
                
                # Fallback to individual page analysis
                logger.info("Falling back to individual page analysis")
                fallback_results = []
                for page_num, text, image_base64 in page_batch:
                    try:
                        analysis = self.analyze_page(pdf_path, page_num, text, image_base64)
                        fallback_results.append(analysis)
                    except Exception as page_error:
                        logger.error(f"Error analyzing page {page_num}: {page_error}")
                        fallback_results.append(None)
                
                return fallback_results
    
    def _call_gemini_for_batch_analysis(self, batch_data: List[Dict], correlation_id: str) -> str:
        """
        Call Gemini API for batch page analysis.
        
        Args:
            batch_data (List[Dict]): List of page data dictionaries
            correlation_id (str): Correlation ID for logging
            
        Returns:
            str: Raw response from Gemini API
        """
        # Prepare messages for batch API call
        content_parts = [{"type": "text", "text": f"Analyze these {len(batch_data)} document pages:\n\n"}]
        
        for i, page_data in enumerate(batch_data):
            page_num = page_data['page_number']
            text = page_data['text']
            image_base64 = page_data['image_base64']
            
            content_parts.append({
                "type": "text", 
                "text": f"PAGE {page_num}:\nText content:\n{text}\n\n"
            })
            
            if image_base64:
                content_parts.append({
                    "type": "image_url", 
                    "image_url": {"url": f"data:image/jpeg;base64,{image_base64}"}
                })
        
        messages = [
            {"role": "system", "content": self.BATCH_CATALOG_ANALYSIS_PROMPT},
            {"role": "user", "content": content_parts}
        ]
        
        # Call the API
        try:
            import time
            from ..config import settings
            
            start_time = time.time()
            
            response = self.gemini_client.client.chat.completions.create(
                model=self.gemini_client.model,
                messages=messages,
                temperature=0.1,
                max_tokens=settings.CATALOG_MAX_TOKENS
            )
            
            response_time = time.time() - start_time
            
            # Track usage
            self.gemini_client.call_counter += 1
            tokens_used = 0
            if hasattr(response, 'usage'):
                tokens_used = response.usage.total_tokens
                self.gemini_client.token_usage += tokens_used
                logger.debug(f"Batch token usage: {tokens_used} (total: {self.gemini_client.token_usage})")
            
            # Log API call with metrics
            self.catalog_logger.log_api_call(
                correlation_id,
                model=self.gemini_client.model,
                tokens_used=tokens_used,
                response_time=response_time
            )
            
            response_text = response.choices[0].message.content
            if response_text is None:
                raise ValueError("Received None response from Gemini API")
                
            return response_text.strip()
            
        except Exception as e:
            logger.error(f"Error calling Gemini API for batch analysis: {e}")
            raise
    
    def _parse_batch_analysis_response(self, response: str, page_batch: List[Tuple[int, str, str]]) -> List[PageAnalysis]:
        """
        Parse batch analysis response into individual PageAnalysis objects.
        
        Args:
            response (str): Raw API response
            page_batch (List[Tuple]): Original page batch data
            
        Returns:
            List[PageAnalysis]: List of parsed page analyses
        """
        try:
            import json
            import re
            
            # Clean up response (remove markdown code blocks if present)
            cleaned_response = re.sub(r'```json\s*', '', response)
            cleaned_response = re.sub(r'\s*```', '', cleaned_response)
            cleaned_response = cleaned_response.strip()
            
            # Parse JSON response with robust error handling
            batch_data = self._parse_json_with_recovery(cleaned_response)
            
            if 'pages' not in batch_data:
                raise ValueError("Response missing 'pages' array")
            
            pages_data = batch_data['pages']
            analyses = []
            
            # Log batch processing details for debugging
            original_page_nums = [p[0] for p in page_batch]
            llm_page_nums = [p.get('page_number', 'unknown') for p in pages_data]
            logger.debug(f"Batch processing: Original pages {original_page_nums}, LLM returned pages {llm_page_nums}")
            
            # Validate that we have the expected number of pages
            if len(pages_data) != len(page_batch):
                logger.warning(f"Page count mismatch: Expected {len(page_batch)} pages, LLM returned {len(pages_data)} pages")
            
            # Create PageAnalysis objects for each page, ensuring correct page numbers
            for i, page_data in enumerate(pages_data):
                try:
                    # Get the original page number from the batch (not from LLM response)
                    if i < len(page_batch):
                        original_page_num = page_batch[i][0]  # First element is page_num
                        # Override the page number from LLM with the correct original page number
                        page_data['page_number'] = original_page_num
                        logger.debug(f"Corrected page number: LLM returned {page_data.get('page_number', 'unknown')}, using original {original_page_num}")
                    
                    analysis = self._create_page_analysis_from_dict(page_data)
                    analyses.append(analysis)
                except Exception as e:
                    logger.error(f"Error parsing page {page_data.get('page_number', 'unknown')}: {e}")
                    analyses.append(None)
            
            # Ensure we have the right number of results
            while len(analyses) < len(page_batch):
                analyses.append(None)
            
            return analyses[:len(page_batch)]
            
        except Exception as e:
            logger.error(f"Error parsing batch analysis response: {e}")
            logger.debug(f"Raw response: {response[:500]}...")
            
            # Return None for all pages in batch
            return [None] * len(page_batch)
    
    def _parse_json_with_recovery(self, response: str) -> Dict:
        """Parse JSON with robust error handling and recovery mechanisms"""
        import json
        import re
        
        try:
            # Try normal JSON parsing first
            return json.loads(response)
        except json.JSONDecodeError as e:
            logger.warning(f"Initial JSON parsing failed: {e}")
            logger.debug(f"Problematic response snippet: {response[max(0, e.pos-50):e.pos+50]}")
            
            # Attempt to fix common JSON issues
            fixed_response = self._fix_json_issues(response)
            
            try:
                return json.loads(fixed_response)
            except json.JSONDecodeError as e2:
                logger.error(f"JSON recovery failed: {e2}")
                logger.debug(f"Fixed response snippet: {fixed_response[max(0, e2.pos-50):e2.pos+50]}")
                
                # Last resort: try to extract partial JSON
                return self._extract_partial_json(response)
    
    def _fix_json_issues(self, response: str) -> str:
        """Fix common JSON formatting issues"""
        import re
        
        # Fix unterminated strings by adding closing quotes before newlines or end of response
        fixed = re.sub(r'("(?:[^"\\]|\\.)*?)(?=\n|$)', r'\1"', response)
        
        # Fix missing commas between objects
        fixed = re.sub(r'}\s*{', r'},{', fixed)
        
        # Fix trailing commas
        fixed = re.sub(r',(\s*[}\]])', r'\1', fixed)
        
        # Ensure proper array/object closure
        if fixed.count('{') > fixed.count('}'):
            fixed += '}' * (fixed.count('{') - fixed.count('}'))
        if fixed.count('[') > fixed.count(']'):
            fixed += ']' * (fixed.count('[') - fixed.count(']'))
        
        return fixed
    
    def _extract_partial_json(self, response: str) -> Dict:
        """Extract whatever valid JSON we can from a malformed response"""
        import re
        
        # Try to find a pages array even if the overall JSON is malformed
        pages_match = re.search(r'"pages"\s*:\s*\[(.*?)\]', response, re.DOTALL)
        if pages_match:
            try:
                pages_content = pages_match.group(1)
                # Try to parse just the pages array
                partial_json = f'{{"pages": [{pages_content}]}}'
                return json.loads(partial_json)
            except:
                pass
        
        # If all else fails, return empty structure
        logger.error("Could not extract any valid JSON from response")
        return {"pages": []}
    
    def _create_page_analysis_from_dict(self, page_data: Dict) -> PageAnalysis:
        """
        Create PageAnalysis object from parsed JSON data.
        
        Args:
            page_data (Dict): Parsed page data from API response
            
        Returns:
            PageAnalysis: Created page analysis object
        """
        from .models import PageMetadata, TextRegion, BoundingBox
        
        # Extract page metadata
        metadata_data = page_data.get('page_metadata', {})
        page_metadata = PageMetadata(
            has_handwritten_text=metadata_data.get('has_handwritten_text', False),
            has_signatures=metadata_data.get('has_signatures', False),
            image_quality=metadata_data.get('image_quality', 'medium'),
            language=metadata_data.get('language', 'en'),
            form_version=metadata_data.get('form_version'),
            security_features=metadata_data.get('security_features', []),
            text_extraction_method=metadata_data.get('text_extraction_method', 'text')
        )
        
        # Extract text regions
        text_regions = []
        for region_data in page_data.get('text_regions', []):
            text_region = TextRegion(
                region_id=region_data.get('region_id', ''),
                text=region_data.get('text', ''),
                confidence=region_data.get('confidence', 0.0),
                bounding_box=None  # Not provided in current implementation
            )
            text_regions.append(text_region)
        
        # Create PageAnalysis object with validated page number
        page_number = page_data.get('page_number', 1)
        
        # Ensure page number is valid
        if not isinstance(page_number, int) or page_number < 1:
            logger.warning(f"Invalid page number {page_number}, defaulting to 1")
            page_number = 1
        
        return PageAnalysis(
            page_number=page_number,
            page_title=page_data.get('page_title', ''),
            page_type=page_data.get('page_type', 'other'),
            page_subtype=page_data.get('page_subtype', 'other'),
            confidence_score=page_data.get('confidence_score', 0.0),
            extracted_values=page_data.get('extracted_values', {}),
            text_regions=text_regions,
            page_metadata=page_metadata
        )
    
    def _call_gemini_for_analysis(self, text: str, image_base64: Optional[str], correlation_id: str) -> str:
        """
        Call Gemini API for comprehensive page analysis.
        
        Args:
            text (str): Text content of the page
            image_base64 (str, optional): Base64-encoded image of the page
            
        Returns:
            str: Raw response from Gemini API
        """
        # Prepare messages for API call
        if image_base64 and text:
            # Combined text and image analysis
            messages = [
                {"role": "system", "content": self.CATALOG_ANALYSIS_PROMPT},
                {"role": "user", "content": [
                    {"type": "text", "text": f"Analyze this document page.\n\nText content:\n{text}"},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_base64}"}}
                ]}
            ]
        elif image_base64:
            # Image-only analysis
            messages = [
                {"role": "system", "content": self.CATALOG_ANALYSIS_PROMPT},
                {"role": "user", "content": [
                    {"type": "text", "text": "Analyze this document page."},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_base64}"}}
                ]}
            ]
        else:
            # Text-only analysis
            messages = [
                {"role": "system", "content": self.CATALOG_ANALYSIS_PROMPT},
                {"role": "user", "content": f"Analyze this document page.\n\nText content:\n{text}"}
            ]
        
        # Call the API using the existing client infrastructure
        try:
            import time
            start_time = time.time()
            
            response = self.gemini_client.client.chat.completions.create(
                model=self.gemini_client.model,
                messages=messages,
                temperature=0.1,
                max_tokens=8000
            )
            
            response_time = time.time() - start_time
            
            # Track usage
            self.gemini_client.call_counter += 1
            tokens_used = 0
            if hasattr(response, 'usage'):
                tokens_used = response.usage.total_tokens
                self.gemini_client.token_usage += tokens_used
                logger.debug(f"Token usage: {tokens_used} (total: {self.gemini_client.token_usage})")
            
            # Log API call with metrics
            self.catalog_logger.log_api_call(
                correlation_id,
                model=self.gemini_client.model,
                tokens_used=tokens_used,
                response_time=response_time
            )
            
            response_text = response.choices[0].message.content
            if response_text is None:
                raise ValueError("Received None response from Gemini API")
                
            return response_text.strip()
            
        except Exception as e:
            logger.error(f"Error calling Gemini API for page analysis: {e}")
            # Let the error handler decide whether to retry or fallback
            if self.error_handler.should_use_fallback("api"):
                raise  # Will be caught by analyze_page and handled by error_handler
            raise
    
    def _parse_analysis_response(self, response_text: str, page_num: int) -> PageAnalysis:
        """
        Parse Gemini API response into PageAnalysis object.
        
        Args:
            response_text (str): Raw response from Gemini API
            page_num (int): Page number (1-indexed)
            
        Returns:
            PageAnalysis: Parsed analysis results
        """
        try:
            # Extract JSON from response (handle potential markdown formatting)
            json_match = re.search(r'```json\s*(\{.*?\})\s*```', response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # Try to find JSON object directly
                json_match = re.search(r'(\{.*\})', response_text, re.DOTALL)
                if json_match:
                    json_str = json_match.group(1)
                else:
                    raise ValueError("No JSON object found in response")
            
            # Parse JSON
            data = json.loads(json_str)
            
            # Extract and validate core fields
            page_title = data.get("page_title", "Unknown Document")
            page_type = self._validate_page_type(data.get("page_type", "other"))
            page_subtype = data.get("page_subtype", "other")
            confidence_score = float(data.get("confidence_score", 0.5))
            
            # Ensure confidence is in valid range
            confidence_score = max(0.0, min(1.0, confidence_score))
            
            # Extract structured data
            extracted_values = data.get("extracted_values", {})
            
            # Validate extracted dates for accuracy
            extracted_values = self._validate_extracted_dates(extracted_values, page_num)
            
            # Validate and correct document titles
            extracted_values = self._validate_and_correct_document_titles(extracted_values, page_num)
            
            # Parse text regions
            text_regions = []
            for region_data in data.get("text_regions", []):
                try:
                    region = TextRegion(
                        region_id=region_data.get("region_id", "unknown"),
                        bounding_box=None,  # Gemini doesn't provide bounding boxes yet
                        text=region_data.get("text", ""),
                        confidence=float(region_data.get("confidence", 0.5))
                    )
                    text_regions.append(region)
                except Exception as e:
                    logger.warning(f"Error parsing text region: {e}")
                    continue
            
            # Parse page metadata
            metadata_data = data.get("page_metadata", {})
            page_metadata = PageMetadata(
                has_handwritten_text=metadata_data.get("has_handwritten_text", False),
                has_signatures=metadata_data.get("has_signatures", False),
                image_quality=self._validate_image_quality(metadata_data.get("image_quality", "medium")),
                language=metadata_data.get("language", "en"),
                form_version=metadata_data.get("form_version"),
                security_features=metadata_data.get("security_features", []),
                text_extraction_method="hybrid"  # Since we use both text and image
            )
            
            # Create PageAnalysis object
            return PageAnalysis(
                page_number=page_num,
                page_title=page_title,
                page_type=page_type,
                page_subtype=page_subtype,
                confidence_score=confidence_score,
                extracted_values=extracted_values,
                text_regions=text_regions,
                page_metadata=page_metadata
            )
            
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON response: {e}")
            logger.debug(f"Raw response: {response_text}")
            raise ValueError(f"JSON parsing error: {e}")  # Let analyze_page handle this
        except Exception as e:
            logger.error(f"Error parsing analysis response: {e}")
            raise ValueError(f"Response parsing error: {e}")  # Let analyze_page handle this
    
    def _validate_page_type(self, page_type: str) -> str:
        """
        Validate and normalize page type.
        
        Args:
            page_type (str): Raw page type from API response
            
        Returns:
            str: Validated page type
        """
        valid_types = {"government_form", "identity_document", "employment_record", "other"}
        if page_type in valid_types:
            return page_type
        
        # Try to map common variations
        type_mapping = {
            "government": "government_form",
            "form": "government_form",
            "identity": "identity_document",
            "id": "identity_document",
            "employment": "employment_record",
            "work": "employment_record",
            "job": "employment_record"
        }
        
        for key, mapped_type in type_mapping.items():
            if key in page_type.lower():
                return mapped_type
        
        logger.warning(f"Unknown page type '{page_type}', defaulting to 'other'")
        return "other"
    
    def _validate_image_quality(self, quality: str) -> str:
        """
        Validate and normalize image quality.
        
        Args:
            quality (str): Raw image quality from API response
            
        Returns:
            str: Validated image quality
        """
        valid_qualities = {"high", "medium", "low"}
        if quality in valid_qualities:
            return quality
        
        logger.warning(f"Unknown image quality '{quality}', defaulting to 'medium'")
        return "medium"
    
    def _create_fallback_analysis(self, page_num: int, error_msg: str) -> PageAnalysis:
        """
        Create a fallback PageAnalysis when analysis fails.
        
        Args:
            page_num (int): Page number (1-indexed)
            error_msg (str): Error message describing the failure
            
        Returns:
            PageAnalysis: Fallback analysis with minimal data
        """
        return PageAnalysis(
            page_number=page_num,
            page_title="Analysis Failed",
            page_type="other",
            page_subtype="other",
            confidence_score=0.0,
            extracted_values={"error": error_msg},
            text_regions=[],
            page_metadata=PageMetadata(
                has_handwritten_text=False,
                has_signatures=False,
                image_quality="low",
                language="en",
                text_extraction_method="text"
            )
        )
    
    def classify_page_type(self, text: str, image_base64: Optional[str] = None) -> Tuple[str, str, float]:
        """
        Classify page type using lightweight analysis.
        
        This method provides a faster classification option when full analysis isn't needed.
        
        Args:
            text (str): Text content of the page
            image_base64 (str, optional): Base64-encoded image of the page
            
        Returns:
            Tuple[str, str, float]: (page_type, page_subtype, confidence_score)
        """
        try:
            # Use keyword-based classification for speed
            text_lower = text.lower()
            
            # I-9 form detection
            if any(keyword in text_lower for keyword in [
                "employment eligibility verification", "form i-9", "i-9", "uscis", 
                "department of homeland security", "eligibility verification"
            ]):
                return "government_form", "i9_form", 0.9
            
            # Other government forms
            if any(keyword in text_lower for keyword in [
                "internal revenue service", "irs", "social security", "w-2", "w-4", 
                "1099", "tax", "federal", "state form"
            ]):
                return "government_form", "tax_document", 0.8
            
            # Identity documents
            if any(keyword in text_lower for keyword in [
                "passport", "driver", "license", "identification", "birth certificate",
                "social security card", "green card", "visa"
            ]):
                return "identity_document", "other", 0.8
            
            # Employment records
            if any(keyword in text_lower for keyword in [
                "pay stub", "payroll", "salary", "wage", "employment", "hire", 
                "position", "job title", "employee"
            ]):
                return "employment_record", "other", 0.7
            
            # Default classification
            return "other", "other", 0.3
            
        except Exception as e:
            logger.error(f"Error in page type classification: {e}")
            return "other", "other", 0.0
    
    def extract_structured_data(self, page_analysis: PageAnalysis) -> Dict[str, Any]:
        """
        Extract structured data based on page type.
        
        Args:
            page_analysis (PageAnalysis): Analysis results for the page
            
        Returns:
            Dict[str, Any]: Structured data specific to the document type
        """
        if page_analysis.page_type == "government_form" and page_analysis.page_subtype == "i9_form":
            return self._extract_i9_data(page_analysis)
        elif page_analysis.page_type == "identity_document":
            return self._extract_identity_document_data(page_analysis)
        elif page_analysis.page_type == "employment_record":
            return self._extract_employment_record_data(page_analysis)
        else:
            return page_analysis.extracted_values
    
    def _extract_i9_data(self, page_analysis: PageAnalysis) -> Dict[str, Any]:
        """Extract I-9 specific structured data."""
        i9_data = page_analysis.extracted_values.copy()
        
        # Add I-9 specific processing
        if "form_version" not in i9_data and page_analysis.page_metadata.form_version:
            i9_data["form_version"] = page_analysis.page_metadata.form_version
        
        # Extract dates and normalize format
        for key in ["hire_date", "employee_signature_date", "employer_signature_date"]:
            if key in i9_data:
                i9_data[key] = self._normalize_date(i9_data[key])
        
        return i9_data
    
    def _extract_identity_document_data(self, page_analysis: PageAnalysis) -> Dict[str, Any]:
        """Extract identity document specific structured data."""
        return page_analysis.extracted_values
    
    def _extract_employment_record_data(self, page_analysis: PageAnalysis) -> Dict[str, Any]:
        """Extract employment record specific structured data."""
        return page_analysis.extracted_values
    
    def _normalize_date(self, date_str: str) -> str:
        """
        Normalize date string to MM/DD/YYYY format.
        
        Args:
            date_str (str): Raw date string
            
        Returns:
            str: Normalized date string or original if parsing fails
        """
        if not date_str or not isinstance(date_str, str):
            return date_str
        
        # Try to extract date patterns
        date_patterns = [
            r'(\d{1,2})/(\d{1,2})/(\d{4})',  # MM/DD/YYYY
            r'(\d{1,2})-(\d{1,2})-(\d{4})',  # MM-DD-YYYY
            r'(\d{4})-(\d{1,2})-(\d{1,2})',  # YYYY-MM-DD
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, date_str)
            if match:
                groups = match.groups()
                if len(groups) == 3:
                    if len(groups[0]) == 4:  # YYYY-MM-DD format
                        return f"{groups[1].zfill(2)}/{groups[2].zfill(2)}/{groups[0]}"
                    else:  # MM/DD/YYYY or MM-DD-YYYY format
                        return f"{groups[0].zfill(2)}/{groups[1].zfill(2)}/{groups[2]}"
        
        return date_str
    
    def _is_likely_i9_form(self, text: str) -> bool:
        """
        Quick keyword-based check to determine if a page is likely an I-9 form.
        
        Args:
            text (str): Text content of the page
            
        Returns:
            bool: True if page is likely an I-9 form
        """
        if not text:
            return False
        
        text_lower = text.lower()
        
        # Strong I-9 indicators
        strong_indicators = [
            "employment eligibility verification",
            "form i-9",
            "i-9",
            "uscis",
            "department of homeland security"
        ]
        
        # Check for strong indicators
        for indicator in strong_indicators:
            if indicator in text_lower:
                return True
        
        # Check for combination of weaker indicators
        weak_indicators = [
            "employment",
            "eligibility", 
            "verification",
            "citizen",
            "authorized",
            "work"
        ]
        
        weak_count = sum(1 for indicator in weak_indicators if indicator in text_lower)
        
        # If we have 3+ weak indicators, it might be I-9 related
        return weak_count >= 3
    
    def _create_simplified_analysis(self, page_num: int, text: str) -> PageAnalysis:
        """
        Create a simplified page analysis without detailed AI processing.
        
        Args:
            page_num (int): Page number (1-indexed)
            text (str): Text content of the page
            
        Returns:
            PageAnalysis: Simplified analysis result
        """
        from .models import PageMetadata
        
        # Basic classification using keywords
        page_type, page_subtype, confidence = self._classify_page_by_keywords_simple(text)
        
        return PageAnalysis(
            page_number=page_num,
            page_title=f"Page {page_num} (Simplified Analysis)",
            page_type=page_type,
            page_subtype=page_subtype,
            confidence_score=confidence,
            extracted_values={"analysis_type": "simplified", "reason": "non_i9_form"},
            text_regions=[],  # Skip detailed text region extraction
            page_metadata=PageMetadata(
                has_handwritten_text=False,
                has_signatures=False,
                image_quality="not_analyzed",
                language="en",
                text_extraction_method="text"
            )
        )
    
    def _classify_page_by_keywords_simple(self, text: str) -> Tuple[str, str, float]:
        """
        Classify page type using keyword-based detection (simplified).
        
        Args:
            text (str): Text content of the page
            
        Returns:
            Tuple of (page_type, page_subtype, confidence_score)
        """
        if not text:
            return "other", "other", 0.1
        
        text_lower = text.lower()
        
        # I-9 form detection (should not reach here if _is_likely_i9_form works correctly)
        if any(keyword in text_lower for keyword in [
            "employment eligibility verification", "form i-9", "i-9", "uscis", 
            "department of homeland security"
        ]):
            return "government_form", "i9_form", 0.8
        
        # Other government forms
        if any(keyword in text_lower for keyword in [
            "internal revenue service", "irs", "social security", "w-2", "w-4", 
            "1099", "tax form", "government", "federal"
        ]):
            return "government_form", "tax_document", 0.6
        
        # Identity documents
        if any(keyword in text_lower for keyword in [
            "passport", "driver", "license", "identification", "birth certificate",
            "social security card"
        ]):
            return "identity_document", "other", 0.6
        
        # Employment records
        if any(keyword in text_lower for keyword in [
            "pay stub", "payroll", "salary", "wage", "employment", "hire", 
            "position", "job title", "employee"
        ]):
            return "employment_record", "other", 0.5
        
        return "other", "other", 0.3
    
    def _validate_extracted_dates(self, extracted_values: Dict[str, Any], page_num: int) -> Dict[str, Any]:
        """
        Validate extracted date fields for accuracy and flag potential OCR errors.
        
        Args:
            extracted_values (Dict): Extracted field values from the page
            page_num (int): Page number for logging
            
        Returns:
            Dict: Validated extracted values with warnings logged for suspicious dates
        """
        import re
        from datetime import datetime
        
        # Date field patterns to validate
        date_fields = [
            'employer_signature_date', 'employee_signature_date', 'date_of_birth',
            'reverification_date_signed', 'reverification_signature_date',
            'reverification_1_signature_date', 'reverification_2_signature_date', 'reverification_3_signature_date',
            'date_of_employer_signature', 'date_of_employee_signature',
            'work_auth_expiration_date', 'alien_authorized_to_work_until',
            'reverification_expiration_date', 'reverification_1_expiration_date',
            'list_a_expiration_date', 'list_b_expiration_date', 'list_c_expiration_date',
            'rehire_date', 'first_day_of_employment', 'employee_first_day_of_employment'
        ]
        
        for field in date_fields:
            if field in extracted_values:
                date_value = extracted_values[field]
                
                # Skip null, N/A, or empty values
                if not date_value or date_value in ['N/A', '', None, 'null']:
                    continue
                
                # Validate date format MM/DD/YYYY
                date_pattern = r'^(\d{2})/(\d{2})/(\d{4})$'
                match = re.match(date_pattern, str(date_value))
                
                if match:
                    month, day, year = match.groups()
                    month_int = int(month)
                    day_int = int(day)
                    year_int = int(year)
                    
                    # Validate month range
                    if not (1 <= month_int <= 12):
                        logger.warning(f"Page {page_num}: Invalid month in {field}: {date_value} (month={month_int})")
                        logger.warning(f"  → Possible OCR error: Check if '0' was misread as '1' or vice versa")
                    
                    # Validate day range
                    if not (1 <= day_int <= 31):
                        logger.warning(f"Page {page_num}: Invalid day in {field}: {date_value} (day={day_int})")
                    
                    # Validate year range (reasonable for I-9 forms: 1990-2030)
                    if not (1990 <= year_int <= 2030):
                        logger.warning(f"Page {page_num}: Suspicious year in {field}: {date_value} (year={year_int})")
                    
                    # Try to parse as actual date to catch invalid dates like 02/30/2023
                    try:
                        datetime.strptime(date_value, '%m/%d/%Y')
                    except ValueError as e:
                        logger.warning(f"Page {page_num}: Invalid date in {field}: {date_value} - {e}")
                        logger.warning(f"  → Possible OCR error in date extraction")
                    
                    # Special check for common OCR errors in handwritten dates
                    # Check if month looks suspicious (e.g., 12 when it might be 02)
                    if month == '12' and field in ['reverification_date_signed', 'reverification_signature_date']:
                        logger.warning(f"Page {page_num}: {field} has month=12 (December)")
                        logger.warning(f"  → If this is handwritten, verify '12' is not misread '02' (February)")
                        logger.warning(f"  → Check original document: {date_value}")
                
                else:
                    logger.warning(f"Page {page_num}: Date format error in {field}: {date_value}")
                    logger.warning(f"  → Expected MM/DD/YYYY format")
        
        return extracted_values
    
    def _validate_and_correct_document_titles(self, extracted_values: Dict[str, Any], page_num: int) -> Dict[str, Any]:
        """
        Validate and apply common OCR corrections to document titles.
        
        Args:
            extracted_values (Dict): Extracted field values from the page
            page_num (int): Page number for logging
            
        Returns:
            Dict: Corrected extracted values with warnings logged for suspicious titles
        """
        # Document title fields to validate
        title_fields = [
            'list_a_document_title', 'list_b_document_title', 'list_c_document_title',
            'section_3_document_title', 'reverification_document_title',
            'reverification_1_document_title', 'reverification_2_document_title', 'reverification_3_document_title'
        ]
        
        for field in title_fields:
            if field in extracted_values:
                title = extracted_values[field]
                
                # Skip null, N/A, or empty values
                if not title or title in ['N/A', '', None, 'null']:
                    continue
                
                original_title = title
                corrected = False
                
                # Common OCR error corrections
                corrections = [
                    ("BY ITSELF", "issued by DHS", "Common OCR misread of handwriting"),
                    ("by itself", "issued by DHS", "Common OCR misread of handwriting"),
                    ("EAD CARD", "EAD", "Redundant wording"),
                ]
                
                for wrong, correct, reason in corrections:
                    if wrong in title:
                        title = title.replace(wrong, correct)
                        corrected = True
                        logger.warning(f"Page {page_num}: Corrected {field}")
                        logger.warning(f"  → Changed '{wrong}' to '{correct}' ({reason})")
                        logger.warning(f"  → Original: {original_title}")
                        logger.warning(f"  → Corrected: {title}")
                
                # Check for missing I-94
                if "DS-2019" in title.upper() and "I-94" not in title.upper() and "I94" not in title.upper():
                    logger.warning(f"Page {page_num}: {field} mentions DS-2019 but missing I-94")
                    logger.warning(f"  → Title: {title}")
                    logger.warning(f"  → DS-2019 forms typically come with I-94 - verify if I-94 should be included")
                
                # Update the value if corrected
                if corrected:
                    extracted_values[field] = title
        
        return extracted_values

    def clear_cache(self):
        """Clear the analysis cache."""
        self.analysis_cache.clear()
        logger.debug("Cleared page analysis cache")
    
    def get_cache_stats(self) -> Dict[str, int]:
        """
        Get cache statistics.
        
        Returns:
            Dict[str, int]: Cache statistics
        """
        return {
            "cached_analyses": len(self.analysis_cache),
            "api_calls": self.gemini_client.call_counter,
            "total_tokens": self.gemini_client.token_usage
        }