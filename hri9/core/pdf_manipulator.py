#!/usr/bin/env python3
"""
PDF manipulation utilities for I-9 detection.

This module provides functionality for extracting and removing I-9 pages from PDFs.
"""

import os
import fitz  # PyMuPDF
from ..utils.logging_config import logger

class PDFManipulator:
    """Class for manipulating PDF files (extracting and removing pages)."""
    
    @staticmethod
    def extract_pages(pdf_path, page_numbers, output_path):
        """
        Extract specific pages from a PDF and save them as a new PDF.
        
        Args:
            pdf_path (str): Path to the source PDF file.
            page_numbers (list): List of page numbers to extract (1-indexed).
            output_path (str): Path to save the extracted pages as a new PDF.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            if not page_numbers:
                logger.warning(f"No pages to extract from {pdf_path}")
                return False
            
            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Use PyMuPDF (fitz) to extract pages
            doc = fitz.open(pdf_path)
            new_doc = fitz.open()
            
            # Convert 1-indexed page numbers to 0-indexed and filter valid pages
            pages_to_extract = [page_num - 1 for page_num in page_numbers if 0 < page_num <= len(doc)]
            
            # Extract pages
            for page_num in sorted(pages_to_extract):
                new_doc.insert_pdf(doc, from_page=page_num, to_page=page_num)
            
            # Save the extracted pages as a new PDF
            new_doc.save(output_path)
            new_doc.close()
            doc.close()
            
            logger.info(f"Successfully extracted {len(pages_to_extract)} pages to {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error extracting pages from {pdf_path}: {e}")
            return False
    
    @staticmethod
    def remove_pages(pdf_path, page_numbers, output_path):
        """
        Remove specific pages from a PDF and save the result as a new PDF.
        
        Args:
            pdf_path (str): Path to the source PDF file.
            page_numbers (list): List of page numbers to remove (1-indexed).
            output_path (str): Path to save the modified PDF.
            
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            # Debug information
            logger.info(f"Removing pages from PDF: {pdf_path}")
            logger.info(f"Pages to remove: {page_numbers}")
            logger.info(f"Output path: {output_path}")
            
            # Open the PDF with PyMuPDF
            doc = fitz.open(pdf_path)
            
            if not page_numbers:
                logger.warning(f"No pages provided for removal")
                return False
            
            # Convert to 0-indexed page numbers for PyMuPDF
            pages_to_remove = [page_num - 1 for page_num in page_numbers if 0 < page_num <= len(doc)]
            remove_set = set(pages_to_remove)

            # Detailed page-level logging before any mutation
            total_pages = len(doc)
            logger.info("===== Page-level removal decision =====")
            for idx in range(total_pages):
                page_num_1idx = idx + 1
                will_remove = idx in remove_set
                logger.info(
                    f"Page {page_num_1idx:>3}: {'I-9 page' if will_remove else 'Kept'} | Action: {'DELETE' if will_remove else 'KEEP'}"
                )
            logger.info("=======================================")
            
            # Sort in descending order to avoid index shifting when deleting pages
            pages_to_remove.sort(reverse=True)
            
            # Debug information
            original_pages = total_pages
            logger.info(f"Original PDF has {original_pages} pages")
            
            # Remove the pages
            for page_idx in pages_to_remove:
                logger.info(f"Removing page {page_idx+1}")
                doc.delete_page(page_idx)
            
            # Debug information
            after_removal_pages = len(doc)
            logger.info(f"After removal, PDF has {after_removal_pages} pages")
            
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Save the modified PDF
            logger.info(f"Saving modified PDF to: {output_path}")
            doc.save(output_path)
            doc.close()
            
            # Verify the file was saved
            if os.path.exists(output_path):
                logger.info(f"Successfully saved modified PDF to {output_path} (size: {os.path.getsize(output_path)} bytes)")
                return True
            else:
                logger.error(f"Failed to save modified PDF to {output_path} - file does not exist after save operation")
                return False
                
        except Exception as e:
            logger.error(f"Error removing pages from {pdf_path}: {e}")
            return False
