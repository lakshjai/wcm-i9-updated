#!/usr/bin/env python3
"""
Taxonomy-Based I-9 Data Extractor
Uses LLM (Gemini 2.5 Pro or Claude) with taxonomy guidance
Integrates with existing GeminiClient from main application
"""
import base64
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional
import time
import sys

# Add parent directory to path to import from hri9
sys.path.insert(0, str(Path(__file__).parent.parent))

# PDF processing
try:
    from pdf2image import convert_from_path
    PDF2IMAGE_AVAILABLE = True
except ImportError:
    PDF2IMAGE_AVAILABLE = False
    print("Warning: pdf2image not available. Install with: pip install pdf2image")

# Import existing GeminiClient from main application
try:
    from hri9.api.gemini_client import GeminiClient
    GEMINI_CLIENT_AVAILABLE = True
except ImportError:
    GEMINI_CLIENT_AVAILABLE = False
    print("Warning: Could not import GeminiClient from hri9. Make sure hri9 module is available.")

# Anthropic as fallback
try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False
    print("Warning: Anthropic not available. Install with: pip install anthropic")

from taxonomy_prompts import TaxonomyPromptBuilder
import config

logging.basicConfig(level=logging.INFO if config.VERBOSE_LOGGING else logging.WARNING)
logger = logging.getLogger(__name__)


class TaxonomyExtractor:
    """
    Extracts I-9 data from PDFs using LLM with taxonomy guidance
    """
    
    def __init__(self, provider: str = None):
        """
        Initialize extractor
        
        Args:
            provider: 'gemini' or 'anthropic' (defaults to config.LLM_PROVIDER)
        """
        self.provider = provider or config.LLM_PROVIDER
        self.prompt_builder = TaxonomyPromptBuilder(str(config.TAXONOMY_PATH))
        
        # Initialize LLM client
        if self.provider == 'gemini':
            if not GEMINI_CLIENT_AVAILABLE:
                raise ImportError("GeminiClient not available. Make sure hri9 module is installed.")
            if not config.OPENAI_API_KEY:
                raise ValueError("OPENAI_API_KEY not set in environment (same key used by main app)")
            
            # Use existing GeminiClient from main application (uses OpenAI-compatible endpoint)
            self.gemini_client = GeminiClient(
                api_key=config.OPENAI_API_KEY,
                base_url=config.OPENAI_BASE_URL,
                model=config.GEMINI_MODEL
            )
            logger.info(f"Initialized GeminiClient with model: {config.GEMINI_MODEL} via {config.OPENAI_BASE_URL}")
        
        elif self.provider == 'anthropic':
            if not ANTHROPIC_AVAILABLE:
                raise ImportError("Anthropic not installed")
            if not config.ANTHROPIC_API_KEY:
                raise ValueError("ANTHROPIC_API_KEY not set in environment")
            self.client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
            logger.info(f"Initialized Anthropic {config.ANTHROPIC_MODEL}")
        
        else:
            raise ValueError(f"Unknown provider: {self.provider}")
    
    def pdf_to_images(self, pdf_path: str) -> List[bytes]:
        """
        Convert PDF pages to images
        
        Returns:
            List of image bytes (PNG format)
        """
        if not PDF2IMAGE_AVAILABLE:
            raise ImportError("pdf2image not installed. Install with: pip install pdf2image")
        
        logger.info(f"Converting PDF to images: {pdf_path}")
        images = convert_from_path(pdf_path, dpi=200)
        
        image_bytes_list = []
        for i, image in enumerate(images):
            # Convert PIL Image to bytes
            from io import BytesIO
            buffer = BytesIO()
            image.save(buffer, format='PNG')
            image_bytes_list.append(buffer.getvalue())
            logger.info(f"  Converted page {i+1}/{len(images)}")
        
        return image_bytes_list
    
    def image_to_base64(self, image_bytes: bytes) -> str:
        """Convert image bytes to base64 string"""
        return base64.b64encode(image_bytes).decode('utf-8')
    
    def extract_page_with_gemini(self, image_bytes: bytes, page_number: int) -> Dict:
        """
        Extract data from a single page using existing GeminiClient
        
        Args:
            image_bytes: Image data as bytes
            page_number: Page number (1-indexed)
        
        Returns:
            Extracted data as dict
        """
        logger.info(f"Extracting page {page_number} with GeminiClient...")
        
        # Build taxonomy-guided prompt
        extraction_prompt = self.prompt_builder.build_extraction_prompt(
            optimization_level=config.TAXONOMY_OPTIMIZATION,
            include_examples=config.USE_FEW_SHOT_EXAMPLES
        )
        system_prompt = self.prompt_builder.build_system_prompt()
        
        # Encode image to base64 (GeminiClient expects base64)
        image_base64 = self.gemini_client.encode_image_to_base64(image_bytes)
        
        response_text = None  # Track response for error logging
        
        try:
            # Prepare messages for GeminiClient
            messages = [
                {"role": "system", "content": system_prompt + "\n\n" + extraction_prompt},
                {"role": "user", "content": [
                    {"type": "text", "text": f"Analyze page {page_number} and extract data according to the taxonomy."},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_base64}"}}
                ]}
            ]
            
            # Call existing GeminiClient
            response = self.gemini_client.client.chat.completions.create(
                model=self.gemini_client.model,
                messages=messages,
                temperature=config.TEMPERATURE,
                max_tokens=config.MAX_TOKENS
            )
            
            # Extract response text
            response_text = response.choices[0].message.content
            if not response_text:
                raise ValueError("Empty response from Gemini")
            
            # Log first 500 chars of response for debugging
            logger.debug(f"  Response preview: {response_text[:500]}...")
            
            # Parse JSON from response
            import re
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if not json_match:
                # Save full response to file for debugging
                self._save_error_response(page_number, response_text, "No JSON found")
                raise ValueError("No JSON found in response")
            
            json_str = json_match.group(0)
            
            # Try to parse JSON
            try:
                result = json.loads(json_str)
            except json.JSONDecodeError as json_err:
                # Save problematic JSON to file
                self._save_error_response(page_number, response_text, f"JSON decode error: {json_err}")
                raise
            
            result['page_number'] = page_number
            
            logger.info(f"  ✓ Page {page_number} extracted successfully")
            return result
        
        except Exception as e:
            logger.error(f"  ✗ Error extracting page {page_number}: {e}")
            
            # Print full response to console if available
            if response_text:
                print(f"\n{'='*80}")
                print(f"ERROR ON PAGE {page_number}: {e}")
                print(f"{'='*80}")
                print("FULL LLM RESPONSE:")
                print(response_text)
                print(f"{'='*80}\n")
            
            return {
                'page_number': page_number,
                'error': str(e),
                'page_type': {'canonical': 'unknown', 'confidence': 0.0},
                'fields': {},
                'documents': []
            }
    
    def _save_error_response(self, page_number: int, response_text: str, error_msg: str):
        """Save problematic LLM response to file for debugging"""
        try:
            error_dir = config.OUTPUT_DIR / 'error_responses'
            error_dir.mkdir(parents=True, exist_ok=True)
            
            error_file = error_dir / f"page_{page_number}_error.txt"
            with open(error_file, 'w') as f:
                f.write(f"Error: {error_msg}\n")
                f.write(f"{'='*80}\n")
                f.write("FULL LLM RESPONSE:\n")
                f.write(f"{'='*80}\n")
                f.write(response_text)
            
            logger.error(f"  Saved error response to: {error_file}")
        except Exception as save_err:
            logger.error(f"  Could not save error response: {save_err}")
    
    def extract_page_with_anthropic(self, image_bytes: bytes, page_number: int) -> Dict:
        """
        Extract data from a single page using Claude
        
        Args:
            image_bytes: Image data as bytes
            page_number: Page number (1-indexed)
        
        Returns:
            Extracted data as dict
        """
        logger.info(f"Extracting page {page_number} with Claude...")
        
        # Build prompt
        extraction_prompt = self.prompt_builder.build_extraction_prompt(
            optimization_level=config.TAXONOMY_OPTIMIZATION,
            include_examples=config.USE_FEW_SHOT_EXAMPLES
        )
        system_prompt = self.prompt_builder.build_system_prompt()
        
        # Convert image to base64
        image_base64 = self.image_to_base64(image_bytes)
        
        try:
            # Call Claude with vision
            response = self.client.messages.create(
                model=config.ANTHROPIC_MODEL,
                max_tokens=config.MAX_TOKENS,
                temperature=config.TEMPERATURE,
                system=system_prompt,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": "image/png",
                                    "data": image_base64
                                }
                            },
                            {
                                "type": "text",
                                "text": extraction_prompt
                            }
                        ]
                    }
                ]
            )
            
            # Parse JSON response
            result = json.loads(response.content[0].text)
            result['page_number'] = page_number
            
            logger.info(f"  ✓ Page {page_number} extracted successfully")
            return result
        
        except Exception as e:
            logger.error(f"  ✗ Error extracting page {page_number}: {e}")
            return {
                'page_number': page_number,
                'error': str(e),
                'page_type': {'canonical': 'unknown', 'confidence': 0.0},
                'fields': {},
                'documents': []
            }
    
    def extract_pdf(self, pdf_path: str, output_path: Optional[str] = None, force_regenerate: bool = False) -> Dict:
        """
        Extract data from entire PDF
        
        Args:
            pdf_path: Path to PDF file
            output_path: Optional path to save catalog JSON
            force_regenerate: If True, regenerate catalog even if it exists. Default: False
        
        Returns:
            Complete catalog dict
        """
        pdf_path = Path(pdf_path)
        
        # Check if catalog already exists (unless force_regenerate is True)
        if output_path and not force_regenerate:
            output_file = Path(output_path)
            if output_file.exists():
                logger.info(f"="*80)
                logger.info(f"Catalog already exists: {output_file.name}")
                logger.info(f"Skipping extraction (use --force to regenerate)")
                logger.info(f"="*80)
                
                # Load and return existing catalog
                with open(output_file) as f:
                    return json.load(f)
        
        logger.info(f"="*80)
        logger.info(f"Processing PDF: {pdf_path.name}")
        logger.info(f"="*80)
        
        start_time = time.time()
        
        # Convert PDF to images
        image_bytes_list = self.pdf_to_images(str(pdf_path))
        
        # Extract each page
        pages = []
        for i, image_bytes in enumerate(image_bytes_list, 1):
            if self.provider == 'gemini':
                page_data = self.extract_page_with_gemini(image_bytes, i)
            else:
                page_data = self.extract_page_with_anthropic(image_bytes, i)
            
            pages.append(page_data)
            
            # Rate limiting
            if i < len(image_bytes_list):
                time.sleep(1)  # 1 second between requests
        
        # Build catalog
        catalog = {
            'filename': pdf_path.name,
            'taxonomy_version': self.prompt_builder.taxonomy['version'],
            'extraction_metadata': {
                'llm_provider': self.provider,
                'llm_model': config.GEMINI_MODEL if self.provider == 'gemini' else config.ANTHROPIC_MODEL,
                'extraction_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                'confidence_threshold': config.CONFIDENCE_THRESHOLD,
                'processing_time_seconds': round(time.time() - start_time, 2),
                'total_pages': len(pages)
            },
            'pages': pages
        }
        
        # Save catalog
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w') as f:
                json.dump(catalog, f, indent=2)
            logger.info(f"\n✓ Catalog saved: {output_path}")
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Extraction Complete!")
        logger.info(f"  Total pages: {len(pages)}")
        logger.info(f"  Processing time: {catalog['extraction_metadata']['processing_time_seconds']}s")
        logger.info(f"  Provider: {self.provider}")
        logger.info(f"{'='*80}\n")
        
        return catalog


def main():
    """Test the extractor"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python taxonomy_extractor.py <pdf_file>")
        sys.exit(1)
    
    pdf_path = sys.argv[1]
    
    # Initialize extractor
    extractor = TaxonomyExtractor()
    
    # Extract
    output_path = config.CATALOGS_DIR / f"{Path(pdf_path).stem}_taxonomy.json"
    catalog = extractor.extract_pdf(pdf_path, output_path)
    
    print(f"\n✓ Extraction complete!")
    print(f"  Catalog: {output_path}")


if __name__ == "__main__":
    main()
