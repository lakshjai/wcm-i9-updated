#!/usr/bin/env python3
"""
Gemini API client for I-9 detection.

This module provides a wrapper around the OpenAI client for Gemini API access.
"""

import base64
import time
import json
import re
from typing import Dict, Any, Optional, Tuple, TYPE_CHECKING
from openai import OpenAI
from ..utils.logging_config import logger
from ..config import settings

if TYPE_CHECKING:
    from ..catalog.models import PageAnalysis, PageMetadata, TextRegion, BoundingBox


class GeminiClient:
    """Wrapper for Gemini API client with I-9 detection capabilities."""
    
    def __init__(self, api_key=None, base_url=None, model=None):
        """
        Initialize the Gemini API client.
        
        Args:
            api_key (str, optional): API key for Gemini. Defaults to settings.OPENAI_API_KEY.
            base_url (str, optional): Base URL for API. Defaults to settings.OPENAI_BASE_URL.
            model (str, optional): Model to use. Defaults to settings.GEMINI_MODEL.
        """
        self.api_key = api_key or settings.OPENAI_API_KEY
        self.base_url = base_url or settings.OPENAI_BASE_URL
        self.model = model or settings.GEMINI_MODEL
        self.token_usage = 0
        self.call_counter = 0  # track API calls per process
        self.client = self._initialize_client()
        
    def _initialize_client(self):
        """
        Initialize the OpenAI client for Gemini API.
        
        Returns:
            OpenAI: Configured OpenAI client.
        """
        try:
            client = OpenAI(
                api_key=self.api_key,
                base_url=self.base_url
            )
            logger.info(f"Initialized Gemini API client with model: {self.model}")
            return client
        except Exception as e:
            logger.error(f"Error initializing Gemini API client: {e}")
            raise
    
    def encode_image_to_base64(self, image_bytes):
        """
        Encode image bytes to base64 string.
        
        Args:
            image_bytes (bytes): Image bytes to encode.
            
        Returns:
            str: Base64-encoded image string.
        """
        return base64.b64encode(image_bytes).decode('utf-8')
    
    def detect_i9_form(self, text=None, image_base64=None, max_retries=3):
        """
        Detect I-9 form in text and/or image using Gemini API.
        
        Args:
            text (str, optional): Text content to analyze.
            image_base64 (str, optional): Base64-encoded image to analyze.
            max_retries (int, optional): Maximum number of retries on failure. Defaults to 3.
            
        Returns:
            tuple: (contains_i9, confidence, employee_signature_date, response_text)
        """
        if not text and not image_base64:
            logger.error("No text or image provided for I-9 detection")
            return False, "LOW", None, "No content provided"
            
        # Log what we're using for detection
        if text and image_base64:
            logger.info(f"Detecting I-9 using both text ({len(text)} chars) and image")
        elif text:
            logger.info(f"Detecting I-9 using text only ({len(text)} chars)")
        elif image_base64:
            logger.info("Detecting I-9 using image only")
        
        # Prepare system prompt
        system_prompt = """
        You are an expert document analyzer specializing in I-9 Employment Eligibility Verification forms.
        Your task is to determine if the provided document page contains an I-9 form.
        
        Analyze the document carefully and respond in the following structured format:
        
        CONTAINS_I9: [YES/NO]
        CONFIDENCE: [HIGH/MEDIUM/LOW]
        EMPLOYEE_SIGNATURE_DATE: [MM/DD/YYYY or NONE]
        EXPLANATION: [Brief explanation of your determination]
        
        An I-9 form typically contains:
        - Title "Employment Eligibility Verification"
        - Form I-9 designation
        - Sections for employee and employer information
        - Lists of acceptable documents
        - Department of Homeland Security or USCIS branding
        """
        
        # Prepare user message
        user_message = "Please analyze this document page and determine if it contains an I-9 form."
        
        # Prepare messages for API call
        if image_base64 and text:
            # Combined text and image message
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": [
                    {"type": "text", "text": user_message + "\n\nText content:\n" + text},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_base64}"}}
                ]}
            ]
        elif image_base64:
            # Image-only message
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": [
                    {"type": "text", "text": user_message},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_base64}"}}
                ]}
            ]
        else:
            # Text-only message
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message + "\n\nText content:\n" + text}
            ]
        
        # Call API with retry logic and rate limiting
        self.call_counter += 1
        for attempt in range(max_retries):
            try:
                # Apply rate limiting
                from ..utils.rate_limiter import get_rate_limiter
                rate_limiter = get_rate_limiter()
                delay_applied = rate_limiter.acquire()
                
                if delay_applied > 0:
                    logger.debug(f"Rate limiter applied {delay_applied:.2f}s delay")
                
                logger.info(f"Calling Gemini API [ {self.call_counter} ] (attempt {attempt+1}/{max_retries})")
                
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    temperature=0.1,
                    max_tokens=8192
                )
                
                # Extract response text
                response_text = response.choices[0].message.content
                if response_text is None:
                    logger.error("Received None response from Gemini API")
                    return False, "LOW", None, "API returned None response"
                    
                response_text = response_text.strip()
                logger.info(f"Gemini response: {response_text[:200]}...")
                
                # Track token usage
                if hasattr(response, 'usage'):
                    self.token_usage += response.usage.total_tokens
                    logger.debug(f"Token usage: {response.usage.total_tokens} (total: {self.token_usage})")
                
                # Parse structured response
                contains_i9 = 'YES' in response_text and 'CONTAINS_I9: YES' in response_text
                
                # Extract confidence level
                if 'CONFIDENCE: HIGH' in response_text:
                    confidence = 'HIGH'
                elif 'CONFIDENCE: MEDIUM' in response_text:
                    confidence = 'MEDIUM'
                else:
                    confidence = 'LOW'
                
                # Extract employee signature date if present
                employee_signature_date = None
                import re
                date_match = re.search(r'EMPLOYEE_SIGNATURE_DATE:\s*(\d{1,2}/\d{1,2}/\d{4}|NONE)', response_text)
                if date_match and date_match.group(1) != 'NONE':
                    employee_signature_date = date_match.group(1)
                    logger.info(f"Found employee signature date: {employee_signature_date}")
                
                return contains_i9, confidence, employee_signature_date, response_text
                
            except Exception as e:
                error_type = type(e).__name__
                error_msg = str(e)
                
                # Log detailed error information
                logger.error(f"Gemini API error ({error_type}): {error_msg}")
                
                # Check for specific error types and provide guidance
                if "rate_limit" in error_msg.lower() or "429" in error_msg:
                    logger.warning("Rate limit exceeded - consider reducing concurrent workers or adding delays")
                elif "token" in error_msg.lower() or "413" in error_msg:
                    logger.warning("Token limit exceeded - consider reducing batch size or content length")
                elif "authentication" in error_msg.lower() or "401" in error_msg:
                    logger.error("Authentication failed - check API key configuration")
                elif "network" in error_msg.lower() or "connection" in error_msg.lower():
                    logger.warning("Network connectivity issue - check internet connection")
                elif "content_filter" in error_msg.lower() or "safety" in error_msg.lower():
                    logger.warning("Content policy violation - document may contain filtered content")
                
                if attempt < max_retries - 1:
                    # Adaptive retry delay based on error type
                    if "rate_limit" in error_msg.lower():
                        wait_time = min(60, 2 ** (attempt + 2))  # Longer delay for rate limits
                    else:
                        wait_time = 2 ** attempt  # Standard exponential backoff
                    
                    logger.info(f"Retrying in {wait_time} seconds... (attempt {attempt + 2}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Max retries reached after {max_retries} attempts, giving up")
                    return False, "LOW", None, f"API Error ({error_type}): {error_msg}"
    
    def analyze_page_for_catalog(self, page_number: int, text: str = None, image_base64: str = None, max_retries: int = 3) -> 'PageAnalysis':
        """
        Analyze a document page for comprehensive catalog information.
        
        Args:
            page_number (int): Page number being analyzed.
            text (str, optional): Text content to analyze.
            image_base64 (str, optional): Base64-encoded image to analyze.
            max_retries (int, optional): Maximum number of retries on failure. Defaults to 3.
            
        Returns:
            PageAnalysis: Comprehensive analysis results for the page.
        """
        if not text and not image_base64:
            logger.error("No text or image provided for catalog analysis")
            return self._create_error_page_analysis(page_number, "No content provided")
            
        # Log what we're using for analysis
        if text and image_base64:
            logger.info(f"Analyzing page {page_number} using both text ({len(text)} chars) and image")
        elif text:
            logger.info(f"Analyzing page {page_number} using text only ({len(text)} chars)")
        elif image_base64:
            logger.info(f"Analyzing page {page_number} using image only")
        
        # Prepare specialized catalog analysis prompt
        system_prompt = self._get_catalog_analysis_prompt()
        
        # Prepare user message
        user_message = f"Please analyze page {page_number} of this document and provide a comprehensive analysis in the specified JSON format."
        
        # Prepare messages for API call
        messages = self._prepare_catalog_messages(system_prompt, user_message, text, image_base64)
        
        # Call API with retry logic and rate limiting
        self.call_counter += 1
        for attempt in range(max_retries):
            try:
                # Apply rate limiting for catalog analysis
                from ..utils.rate_limiter import get_rate_limiter
                rate_limiter = get_rate_limiter()
                delay_applied = rate_limiter.acquire()
                
                if delay_applied > 0:
                    logger.debug(f"Rate limiter applied {delay_applied:.2f}s delay for catalog analysis")
                
                logger.info(f"Calling Gemini API for catalog analysis [ {self.call_counter} ] (attempt {attempt+1}/{max_retries})")
                
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    temperature=0.1,
                    max_tokens=8192
                )
                
                # Extract response text
                response_text = response.choices[0].message.content
                if response_text is None:
                    logger.error("Received None response from Gemini API")
                    return self._create_error_page_analysis(page_number, "API returned None response")
                    
                response_text = response_text.strip()
                logger.debug(f"Gemini catalog response: {response_text[:300]}...")
                
                # Track token usage
                if hasattr(response, 'usage'):
                    self.token_usage += response.usage.total_tokens
                    logger.debug(f"Token usage: {response.usage.total_tokens} (total: {self.token_usage})")
                
                # Parse structured response
                page_analysis = self._parse_catalog_response(page_number, response_text)
                return page_analysis
                
            except Exception as e:
                error_type = type(e).__name__
                error_msg = str(e)
                
                # Log detailed error information for catalog analysis
                logger.error(f"Gemini catalog API error ({error_type}): {error_msg}")
                
                # Provide specific guidance based on error type
                if "rate_limit" in error_msg.lower() or "429" in error_msg:
                    logger.warning("Rate limit exceeded during catalog analysis - consider reducing batch size")
                elif "token" in error_msg.lower() or "413" in error_msg:
                    logger.warning("Token limit exceeded during catalog analysis - document may be too large")
                elif "timeout" in error_msg.lower():
                    logger.warning("API timeout during catalog analysis - network or processing delay")
                
                if attempt < max_retries - 1:
                    # Adaptive retry delay for catalog analysis
                    if "rate_limit" in error_msg.lower():
                        wait_time = min(120, 2 ** (attempt + 3))  # Even longer delay for catalog rate limits
                    elif "timeout" in error_msg.lower():
                        wait_time = min(30, 2 ** (attempt + 1))  # Moderate delay for timeouts
                    else:
                        wait_time = 2 ** attempt  # Standard exponential backoff
                    
                    logger.info(f"Retrying catalog analysis in {wait_time} seconds... (attempt {attempt + 2}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Max retries reached for catalog analysis after {max_retries} attempts")
                    return self._create_error_page_analysis(page_number, f"Catalog API Error ({error_type}): {error_msg}")
    
    def _get_catalog_analysis_prompt(self) -> str:
        """
        Get the specialized prompt for comprehensive catalog analysis.
        
        Returns:
            str: System prompt for catalog analysis.
        """
        return """You are a precise document analyzer. Extract information EXACTLY as it appears in the document.

CRITICAL RULES:
1. Return ONLY valid JSON - no markdown, no explanations, no code blocks
2. Use null for missing values - NEVER use placeholder text like "N/A", "not present", "if present"
3. Extract dates in MM/DD/YYYY format ONLY if clearly visible - use null if absent
4. Extract text EXACTLY as written - do not paraphrase or interpret
5. Be consistent - same document analyzed twice must produce identical output

REQUIRED JSON STRUCTURE:
{
  "page_title": "EXACT title from document header (e.g., 'Form I-9 Section 1', 'U.S. Passport')",
  "page_type": "government_form|identity_document|employment_record|other",
  "page_subtype": "i9_form|passport|drivers_license|social_security_card|birth_certificate|employment_contract|tax_form|other",
  "confidence_score": 0.95,
  "extracted_values": {
    "first_name": "EXACT first name or null",
    "middle_name": "EXACT middle name/initial or null",
    "last_name": "EXACT last name or null",
    "date_of_birth": "MM/DD/YYYY or null",
    "ssn": "XXX-XX-XXXX format or null",
    "employee_signature_date": "MM/DD/YYYY or null",
    "employer_signature_date": "MM/DD/YYYY or null",
    "signature_date": "MM/DD/YYYY (use if role unclear) or null",
    "form_version": "EXACT version string (e.g., 'Rev. 10/21/2019') or null",
    "section_type": "section_1|section_2|section_3|supplement_b or null",
    "citizenship_status": "EXACT checkbox text selected or null",
    "work_authorization_expiry_date": "MM/DD/YYYY or null",
    "alien_registration_number": "EXACT number or null",
    "list_a_document_title": "EXACT document name or null",
    "list_a_document_number": "EXACT number or null",
    "list_a_expiration_date": "MM/DD/YYYY or null",
    "list_b_document_title": "EXACT document name or null",
    "list_b_document_number": "EXACT number or null",
    "list_c_document_title": "EXACT document name or null",
    "list_c_document_number": "EXACT number or null",
    "reverification_document_title": "EXACT document name or null",
    "reverification_document_number": "EXACT number or null",
    "reverification_expiration_date": "MM/DD/YYYY or null",
    "reverification_signature_date": "MM/DD/YYYY or null",
    "rehire_date": "MM/DD/YYYY or null",
    "first_day_of_employment": "MM/DD/YYYY or null"
  },
  "page_metadata": {
    "has_handwritten_text": true,
    "has_signatures": true,
    "image_quality": "high|medium|low",
    "language": "en",
    "form_version": "EXACT version or null",
    "security_features": [],
    "text_extraction_method": "hybrid"
  }
}

SECTION TYPE IDENTIFICATION (for I-9 forms):
- section_1: Contains "Employee Information and Attestation", citizenship checkboxes, employee signature
- section_2: Contains "Employer or Authorized Representative Review and Verification", List A/B/C documents, employer signature
- section_3: Contains "Reverification and Rehires", new document fields, reverification signature
- supplement_b: Contains "Preparer and/or Translator Certification"

DATE EXTRACTION RULES:
- ONLY extract if date is clearly visible and legible
- Format: MM/DD/YYYY (e.g., "03/15/2023")
- If handwritten and unclear, use null
- Common date fields:
  * employee_signature_date: Date next to employee signature (Section 1)
  * employer_signature_date: Date next to employer/HR signature (Section 2 or 3)
  * reverification_signature_date: Date in Section 3 reverification area
  * first_day_of_employment: Employment start date (Section 2)
  * work_authorization_expiry_date: "Alien Authorized to Work Until" date (Section 1)

DOCUMENT EXTRACTION RULES:
- Extract List A, B, C documents EXACTLY as written
- Include document numbers EXACTLY as they appear
- For Section 3: Extract NEW documents used for reverification
- Use null if document field is blank

CONSISTENCY REQUIREMENTS:
- Same field names must always map to same document locations
- Same document analyzed multiple times must produce identical JSON
- Do not randomize or vary output structure
- Confidence scores should be based on image clarity, not guesswork"""
    
    def _prepare_catalog_messages(self, system_prompt: str, user_message: str, text: str = None, image_base64: str = None) -> list:
        """
        Prepare messages for catalog analysis API call.
        
        Args:
            system_prompt (str): System prompt for analysis.
            user_message (str): User message.
            text (str, optional): Text content.
            image_base64 (str, optional): Base64-encoded image.
            
        Returns:
            list: Formatted messages for API call.
        """
        if image_base64 and text:
            # Combined text and image message
            return [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": [
                    {"type": "text", "text": user_message + "\n\nText content:\n" + text},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_base64}"}}
                ]}
            ]
        elif image_base64:
            # Image-only message
            return [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": [
                    {"type": "text", "text": user_message},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_base64}"}}
                ]}
            ]
        else:
            # Text-only message
            return [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message + "\n\nText content:\n" + text}
            ]
    
    def _parse_catalog_response(self, page_number: int, response_text: str) -> 'PageAnalysis':
        """
        Parse the structured JSON response from catalog analysis.
        
        Args:
            page_number (int): Page number being analyzed.
            response_text (str): Raw response from API.
            
        Returns:
            PageAnalysis: Parsed analysis results.
        """
        try:
            # Extract JSON from response (handle cases where response has extra text)
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if not json_match:
                logger.error("No JSON found in catalog response")
                return self._create_error_page_analysis(page_number, "No JSON in response")
            
            json_str = json_match.group(0)
            data = json.loads(json_str)
            
            # Extract and validate required fields
            page_title = data.get('page_title', 'Unknown Page')
            page_type = data.get('page_type', 'other')
            page_subtype = data.get('page_subtype', 'other')
            confidence_score = float(data.get('confidence_score', 0.5))
            
            # Validate page_type
            valid_types = {"government_form", "identity_document", "employment_record", "other"}
            if page_type not in valid_types:
                logger.warning(f"Invalid page_type '{page_type}', defaulting to 'other'")
                page_type = "other"
            
            # Extract structured data
            extracted_values = data.get('extracted_values', {})
            
            # Import models here to avoid circular imports
            from ..catalog.models import PageAnalysis, PageMetadata, TextRegion
            
            # Parse text regions
            text_regions = []
            for region_data in data.get('text_regions', []):
                try:
                    region = TextRegion(
                        region_id=region_data.get('region_id', 'unknown'),
                        bounding_box=None,  # Not extracting bounding boxes from text analysis
                        text=region_data.get('text', ''),
                        confidence=float(region_data.get('confidence', 0.5))
                    )
                    text_regions.append(region)
                except Exception as e:
                    logger.warning(f"Error parsing text region: {e}")
                    continue
            
            # Parse page metadata
            metadata_data = data.get('page_metadata', {})
            page_metadata = PageMetadata(
                has_handwritten_text=metadata_data.get('has_handwritten_text', False),
                has_signatures=metadata_data.get('has_signatures', False),
                image_quality=metadata_data.get('image_quality', 'medium'),
                language=metadata_data.get('language', 'en'),
                form_version=metadata_data.get('form_version'),
                security_features=metadata_data.get('security_features', []),
                text_extraction_method=metadata_data.get('text_extraction_method', 'text')
            )
            
            # Create PageAnalysis object
            page_analysis = PageAnalysis(
                page_number=page_number,
                page_title=page_title,
                page_type=page_type,
                page_subtype=page_subtype,
                confidence_score=confidence_score,
                extracted_values=extracted_values,
                text_regions=text_regions,
                page_metadata=page_metadata
            )
            
            logger.info(f"Successfully parsed catalog analysis for page {page_number}: {page_type}/{page_subtype} (confidence: {confidence_score:.2f})")
            return page_analysis
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in catalog response: {e}")
            return self._create_error_page_analysis(page_number, f"JSON decode error: {str(e)}")
        except Exception as e:
            logger.error(f"Error parsing catalog response: {e}")
            return self._create_error_page_analysis(page_number, f"Parse error: {str(e)}")
    
    def _create_error_page_analysis(self, page_number: int, error_message: str) -> 'PageAnalysis':
        """
        Create a PageAnalysis object for error cases.
        
        Args:
            page_number (int): Page number.
            error_message (str): Error description.
            
        Returns:
            PageAnalysis: Error analysis result.
        """
        # Import models here to avoid circular imports
        from ..catalog.models import PageAnalysis, PageMetadata
        
        return PageAnalysis(
            page_number=page_number,
            page_title="Analysis Error",
            page_type="other",
            page_subtype="other",
            confidence_score=0.0,
            extracted_values={"error": error_message},
            text_regions=[],
            page_metadata=PageMetadata(
                image_quality="low",
                text_extraction_method="text"
            )
        )
    
    def get_token_usage(self):
        """
        Get current token usage.
        
        Returns:
            int: Total tokens used.
        """
        return self.token_usage
