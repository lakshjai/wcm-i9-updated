#!/usr/bin/env python3
"""
PDF processing utilities for I-9 detection.

This module provides functionality for extracting text and images from PDFs.
"""

import io
import fitz  # PyMuPDF
import pdfplumber
from functools import lru_cache
from ..utils.logging_config import logger

class PDFProcessor:
    """Class for processing PDF files and extracting content.
    Simple per-process in-memory caches are used to avoid re-reading pages that we already
    decoded during the same run (detect→extract→remove pipeline). This drastically cuts
    down on IO and OCR/Gemini calls while remaining safe because each worker process
    has its own memory space.
    """
    # Caches keyed by (pdf_path, page_num)
    _text_cache = {}
    _image_cache = {}
    
    @staticmethod
    def extract_text_from_pdf(pdf_path, page_num=None, max_retries=None, retry_delay=None):
        """
        Extract text from a PDF file using pdfplumber with retry logic.
        
        Args:
            pdf_path (str): Path to the PDF file.
            page_num (int, optional): Specific page number to extract (0-indexed). 
                                     If None, extracts all pages.
            max_retries (int, optional): Maximum number of retry attempts for network issues.
                                       Defaults to settings.FILE_ACCESS_MAX_RETRIES.
            retry_delay (float, optional): Delay between retries in seconds.
                                         Defaults to settings.FILE_ACCESS_RETRY_DELAY.
            
        Returns:
            str: Extracted text content.
        """
        import time
        from ..config import settings
        
        # Use settings defaults if not provided
        max_retries = max_retries if max_retries is not None else settings.FILE_ACCESS_MAX_RETRIES
        retry_delay = retry_delay if retry_delay is not None else settings.FILE_ACCESS_RETRY_DELAY
        
        # Caching single page extract
        cache_key = (pdf_path, page_num)
        if cache_key in PDFProcessor._text_cache:
            return PDFProcessor._text_cache[cache_key]

        for attempt in range(max_retries + 1):
            try:
                with pdfplumber.open(pdf_path) as pdf:
                    if page_num is not None:
                        if 0 <= page_num < len(pdf.pages):
                            page = pdf.pages[page_num]
                            text = page.extract_text() or ""
                            PDFProcessor._text_cache[cache_key] = text
                            return text
                        else:
                            logger.warning(f"Page {page_num} out of range for PDF with {len(pdf.pages)} pages")
                            return ""
                    else:
                        # Extract text from all pages
                        texts = []
                        for idx, page in enumerate(pdf.pages):
                            page_cache_key = (pdf_path, idx)
                            if page_cache_key in PDFProcessor._text_cache:
                                texts.append(PDFProcessor._text_cache[page_cache_key])
                            else:
                                page_text = page.extract_text() or ""
                                PDFProcessor._text_cache[page_cache_key] = page_text
                                texts.append(page_text)
                        full_text = "\n\n".join(texts)
                        PDFProcessor._text_cache[cache_key] = full_text
                        return full_text
                        
            except (FileNotFoundError, OSError, PermissionError) as e:
                if attempt < max_retries:
                    logger.warning(f"PDF access failed, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries + 1}): {e}")
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.error(f"Error extracting text from PDF {pdf_path} after {max_retries + 1} attempts: {e}")
                    return ""
            except Exception as e:
                logger.error(f"Error extracting text from PDF {pdf_path}: {e}")
                return ""
        
        return ""
    
    @staticmethod
    def extract_pdf_blocks(pdf_path, block_size=5):
        """
        Split a PDF into blocks of pages for efficient processing.
        
        Args:
            pdf_path (str): Path to the PDF file.
            block_size (int): Number of pages per block.
            
        Returns:
            list: List of blocks, where each block is a list of (page_num, text) tuples.
        """
        try:
            doc = fitz.open(pdf_path)
            blocks = []
            current_block = []
            
            for page_index in range(len(doc)):
                page = doc.load_page(page_index)
                text = page.get_text("text") or ""
                current_block.append((page_index + 1, text))  # 1-indexed page numbers
                
                if (page_index + 1) % block_size == 0:
                    blocks.append(current_block)
                    current_block = []
            
            # Append any remaining pages
            if current_block:
                blocks.append(current_block)
                
            logger.info(f"Split PDF into {len(blocks)} block(s) of up to {block_size} pages each")
            doc.close()
            return blocks
        except Exception as e:
            logger.error(f"Error reading PDF blocks: {e}")
            return []
    
    @staticmethod
    def render_page_to_image(pdf_path, page_num, dpi=None, max_retries=None, retry_delay=None):
        """
        Render a PDF page to an image using PyMuPDF with retry logic.
        
        Args:
            pdf_path (str): Path to the PDF file.
            page_num (int): Page number to render (0-indexed).
            dpi (int, optional): DPI for rendering. Defaults to settings.PDF_DPI.
            max_retries (int, optional): Maximum number of retry attempts for network issues.
                                       Defaults to settings.FILE_ACCESS_MAX_RETRIES.
            retry_delay (float, optional): Delay between retries in seconds.
                                         Defaults to settings.FILE_ACCESS_RETRY_DELAY.
            
        Returns:
            bytes: Image bytes in PNG format.
        """
        import time
        from ..config import settings
        
        # Use settings defaults if not provided
        dpi = dpi if dpi is not None else settings.PDF_DPI
        max_retries = max_retries if max_retries is not None else settings.FILE_ACCESS_MAX_RETRIES
        retry_delay = retry_delay if retry_delay is not None else settings.FILE_ACCESS_RETRY_DELAY
        
        cache_key = (pdf_path, page_num)
        if cache_key in PDFProcessor._image_cache:
            return PDFProcessor._image_cache[cache_key]

        for attempt in range(max_retries + 1):
            doc = None
            try:
                doc = fitz.open(pdf_path)
                if 0 <= page_num < len(doc):
                    page = doc[page_num]
                    
                    # Calculate zoom factor based on DPI (default is 72 dpi)
                    # Apply additional scale factor for improved small text extraction
                    base_zoom = dpi / 72
                    scale_factor = settings.PDF_IMAGE_SCALE_FACTOR
                    zoom = base_zoom * scale_factor
                    
                    # Smart image size limiting to avoid API errors
                    # Gemini has ~20MB limit, aim for max 10MB to be safe
                    MAX_IMAGE_SIZE_MB = 10
                    MAX_PIXELS = 16_000_000  # ~4000x4000 pixels
                    
                    # Get page dimensions
                    page_rect = page.rect
                    page_width = page_rect.width
                    page_height = page_rect.height
                    
                    # Calculate resulting image dimensions
                    img_width = int(page_width * zoom)
                    img_height = int(page_height * zoom)
                    total_pixels = img_width * img_height
                    
                    # If image would be too large, reduce zoom
                    if total_pixels > MAX_PIXELS:
                        reduction_factor = (MAX_PIXELS / total_pixels) ** 0.5
                        zoom = zoom * reduction_factor
                        img_width = int(page_width * zoom)
                        img_height = int(page_height * zoom)
                        logger.warning(f"Reduced zoom from {base_zoom * scale_factor:.2f} to {zoom:.2f} to avoid oversized image ({img_width}x{img_height})")
                    
                    matrix = fitz.Matrix(zoom, zoom)
                    
                    # Render page to pixmap with high quality settings
                    pixmap = page.get_pixmap(matrix=matrix, alpha=False)
                    
                    # Convert pixmap to image bytes (PNG format for lossless quality)
                    img_bytes = pixmap.tobytes("png")
                    img_size_mb = len(img_bytes) / (1024 * 1024)
                    
                    logger.debug(f"Rendered page {page_num} at {dpi} DPI with {scale_factor}x scale (effective: {zoom*72:.0f} DPI, size: {img_size_mb:.2f}MB, {img_width}x{img_height}px)")
                    
                    # If still too large, warn but proceed
                    if img_size_mb > MAX_IMAGE_SIZE_MB:
                        logger.warning(f"Image size {img_size_mb:.2f}MB exceeds {MAX_IMAGE_SIZE_MB}MB - may cause API errors")
                    
                    PDFProcessor._image_cache[cache_key] = img_bytes
                    doc.close()
                    return img_bytes
                else:
                    logger.warning(f"Page {page_num} out of range for PDF with {len(doc)} pages")
                    doc.close()
                    return None
                    
            except (FileNotFoundError, OSError, PermissionError) as e:
                if doc:
                    doc.close()
                if attempt < max_retries:
                    logger.warning(f"PDF rendering failed, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries + 1}): {e}")
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.error(f"Error rendering PDF page to image after {max_retries + 1} attempts: {e}")
                    return None
            except Exception as e:
                if doc:
                    doc.close()
                logger.error(f"Error rendering PDF page to image: {e}")
                return None
        
        return None
    
    @staticmethod
    def render_page_to_base64(pdf_path, page_num, dpi=None):
        """
        Render a PDF page to a base64-encoded image.
        
        Args:
            pdf_path (str): Path to the PDF file.
            page_num (int): Page number to render (0-indexed).
            dpi (int, optional): DPI for rendering. Defaults to settings.PDF_DPI.
            
        Returns:
            str: Base64-encoded image string.
        """
        import base64
        from ..config import settings
        
        # Use settings default if not provided
        dpi = dpi if dpi is not None else settings.PDF_DPI
        
        img_bytes = PDFProcessor.render_page_to_image(pdf_path, page_num, dpi)
        if img_bytes:
            return base64.b64encode(img_bytes).decode('utf-8')
        return None
    
    @staticmethod
    def get_pdf_page_count(pdf_path):
        """
        Get the number of pages in a PDF file.
        
        Args:
            pdf_path (str): Path to the PDF file.
            
        Returns:
            int: Number of pages, or 0 if error.
        """
        try:
            doc = fitz.open(pdf_path)
            page_count = len(doc)
            doc.close()
            return page_count
        except Exception as e:
            logger.error(f"Error getting PDF page count: {e}")
            return 0
