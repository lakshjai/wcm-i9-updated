#!/usr/bin/env python3
"""
Catalog export and import functionality with PII sanitization.

This module provides comprehensive export/import capabilities for document catalogs,
including data sanitization for PII protection, configurable export formats,
and robust file management.
"""

import json
import os
import re
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Set
from dataclasses import asdict

from ..utils.logging_config import logger
from .models import DocumentCatalogEntry, PageAnalysis, TextRegion
from .cache import CatalogCache


class PIISanitizer:
    """Handles PII detection and sanitization for catalog exports."""
    
    def __init__(self):
        """Initialize PII sanitizer with detection patterns."""
        # Common PII patterns
        self.ssn_pattern = re.compile(r'\b\d{3}-?\d{2}-?\d{4}\b')
        self.phone_pattern = re.compile(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b')
        self.email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        self.address_pattern = re.compile(r'\b\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd)\b', re.IGNORECASE)
        self.date_pattern = re.compile(r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b')
        
        # Fields that commonly contain PII
        self.pii_fields = {
            'social_security_number', 'ssn', 'employee_id', 'employee_number',
            'full_name', 'first_name', 'last_name', 'name', 'address',
            'phone_number', 'phone', 'email', 'date_of_birth', 'dob',
            'signature', 'alien_number', 'uscis_number', 'passport_number',
            'drivers_license', 'state_id', 'birth_certificate'
        }
        
        # Replacement tokens
        self.replacements = {
            'ssn': '[SSN_REDACTED]',
            'phone': '[PHONE_REDACTED]',
            'email': '[EMAIL_REDACTED]',
            'address': '[ADDRESS_REDACTED]',
            'date': '[DATE_REDACTED]',
            'name': '[NAME_REDACTED]',
            'id': '[ID_REDACTED]'
        }
    
    def sanitize_text(self, text: str, include_pii: bool = False) -> str:
        """
        Sanitize text content by removing or masking PII.
        
        Args:
            text: Text content to sanitize
            include_pii: If True, keep PII; if False, redact it
            
        Returns:
            Sanitized text content
        """
        if include_pii or not text:
            return text
        
        sanitized = text
        
        # Replace PII patterns
        sanitized = self.ssn_pattern.sub(self.replacements['ssn'], sanitized)
        sanitized = self.phone_pattern.sub(self.replacements['phone'], sanitized)
        sanitized = self.email_pattern.sub(self.replacements['email'], sanitized)
        sanitized = self.address_pattern.sub(self.replacements['address'], sanitized)
        sanitized = self.date_pattern.sub(self.replacements['date'], sanitized)
        
        return sanitized
    
    def sanitize_extracted_values(self, extracted_values: Dict[str, Any], include_pii: bool = False) -> Dict[str, Any]:
        """
        Sanitize extracted field values.
        
        Args:
            extracted_values: Dictionary of extracted field values
            include_pii: If True, keep PII; if False, redact it
            
        Returns:
            Sanitized extracted values dictionary
        """
        if include_pii:
            return extracted_values
        
        sanitized = {}
        
        for key, value in extracted_values.items():
            key_lower = key.lower()
            
            # Check if field name suggests PII
            is_pii_field = any(pii_field in key_lower for pii_field in self.pii_fields)
            
            if is_pii_field:
                # Replace with appropriate redaction token
                if 'ssn' in key_lower or 'social' in key_lower:
                    sanitized[key] = self.replacements['ssn']
                elif 'phone' in key_lower:
                    sanitized[key] = self.replacements['phone']
                elif 'email' in key_lower:
                    sanitized[key] = self.replacements['email']
                elif 'address' in key_lower:
                    sanitized[key] = self.replacements['address']
                elif 'name' in key_lower:
                    sanitized[key] = self.replacements['name']
                elif any(id_field in key_lower for id_field in ['id', 'number']):
                    sanitized[key] = self.replacements['id']
                else:
                    sanitized[key] = '[PII_REDACTED]'
            else:
                # Sanitize text content if it's a string
                if isinstance(value, str):
                    sanitized[key] = self.sanitize_text(value, include_pii=False)
                else:
                    sanitized[key] = value
        
        return sanitized
    
    def sanitize_file_path(self, file_path: str, include_pii: bool = False) -> str:
        """
        Sanitize file paths to remove PII from directory/file names.
        
        Args:
            file_path: Original file path
            include_pii: If True, keep PII; if False, redact it
            
        Returns:
            Sanitized file path
        """
        if include_pii:
            return file_path
        
        # Replace the filename with a hash to preserve uniqueness without PII
        path_obj = Path(file_path)
        filename_hash = hashlib.md5(path_obj.name.encode()).hexdigest()[:8]
        
        # Keep directory structure but sanitize directory names that might contain PII
        parts = path_obj.parts
        sanitized_parts = []
        
        for part in parts:
            # If part looks like it contains names (has spaces and mixed case), sanitize it
            if ' ' in part and any(c.isupper() for c in part) and any(c.islower() for c in part):
                part_hash = hashlib.md5(part.encode()).hexdigest()[:8]
                sanitized_parts.append(f"[DIR_{part_hash}]")
            else:
                sanitized_parts.append(part)
        
        # Reconstruct path with sanitized filename
        if len(sanitized_parts) > 1:
            return str(Path(*sanitized_parts[:-1]) / f"[FILE_{filename_hash}]{path_obj.suffix}")
        else:
            return f"[FILE_{filename_hash}]{path_obj.suffix}"


class CatalogExporter:
    """Handles export of catalog data with configurable sanitization and formats."""
    
    def __init__(self, sanitizer: Optional[PIISanitizer] = None):
        """
        Initialize catalog exporter.
        
        Args:
            sanitizer: PII sanitizer instance
        """
        self.sanitizer = sanitizer or PIISanitizer()
    
    def export_catalog_entry(self, catalog_entry: DocumentCatalogEntry, 
                           include_pii: bool = False) -> Dict[str, Any]:
        """
        Export a single catalog entry with optional PII sanitization.
        
        Args:
            catalog_entry: Document catalog entry to export
            include_pii: Whether to include PII in export
            
        Returns:
            Sanitized catalog entry dictionary
        """
        # Convert to dictionary
        catalog_dict = catalog_entry.to_dict()
        
        if include_pii:
            return catalog_dict
        
        # Sanitize document metadata
        if 'document_metadata' in catalog_dict:
            metadata = catalog_dict['document_metadata']
            if 'file_path' in metadata:
                metadata['file_path'] = self.sanitizer.sanitize_file_path(
                    metadata['file_path'], include_pii=False
                )
        
        # Sanitize document name
        if 'document_name' in catalog_dict:
            catalog_dict['document_name'] = self.sanitizer.sanitize_file_path(
                catalog_dict['document_name'], include_pii=False
            )
        
        # Sanitize page data
        if 'pages' in catalog_dict:
            for page in catalog_dict['pages']:
                # Sanitize page title
                if 'page_title' in page:
                    page['page_title'] = self.sanitizer.sanitize_text(
                        page['page_title'], include_pii=False
                    )
                
                # Sanitize extracted values
                if 'extracted_values' in page:
                    page['extracted_values'] = self.sanitizer.sanitize_extracted_values(
                        page['extracted_values'], include_pii=False
                    )
                
                # Sanitize text regions
                if 'text_regions' in page:
                    for region in page['text_regions']:
                        if 'text' in region:
                            # Check if text looks like PII (names, etc.)
                            text = region['text']
                            if any(pii_field in region.get('region_id', '').lower() for pii_field in self.sanitizer.pii_fields):
                                region['text'] = '[PII_REDACTED]'
                            else:
                                region['text'] = self.sanitizer.sanitize_text(text, include_pii=False)
        
        return catalog_dict
    
    def export_multiple_catalogs(self, catalog_entries: List[DocumentCatalogEntry],
                               output_path: str, include_pii: bool = False,
                               export_format: str = 'json',
                               compression: bool = False) -> bool:
        """
        Export multiple catalog entries to a file.
        
        Args:
            catalog_entries: List of catalog entries to export
            output_path: Output file path
            include_pii: Whether to include PII in export
            export_format: Export format ('json', 'jsonl')
            compression: Whether to compress the output
            
        Returns:
            True if export successful, False otherwise
        """
        try:
            # Ensure output directory exists
            output_dir = os.path.dirname(output_path)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
            
            # Prepare export data
            export_data = {
                "export_metadata": {
                    "export_timestamp": datetime.now().isoformat(),
                    "export_format": export_format,
                    "total_documents": len(catalog_entries),
                    "include_pii": include_pii,
                    "compression": compression,
                    "exporter_version": "1.0"
                },
                "documents": []
            }
            
            # Export each catalog entry
            for entry in catalog_entries:
                sanitized_entry = self.export_catalog_entry(entry, include_pii=include_pii)
                export_data["documents"].append(sanitized_entry)
            
            # Write to file based on format
            if export_format == 'json':
                self._write_json_export(export_data, output_path, compression)
            elif export_format == 'jsonl':
                self._write_jsonl_export(export_data, output_path, compression)
            else:
                raise ValueError(f"Unsupported export format: {export_format}")
            
            logger.info(f"Exported {len(catalog_entries)} catalog entries to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting catalogs to {output_path}: {e}")
            return False
    
    def export_from_cache(self, catalog_cache: CatalogCache, output_path: str,
                         document_ids: Optional[List[str]] = None,
                         include_pii: bool = False, export_format: str = 'json',
                         compression: bool = False) -> bool:
        """
        Export catalog entries from cache.
        
        Args:
            catalog_cache: Catalog cache instance
            output_path: Output file path
            document_ids: Specific document IDs to export (None for all)
            include_pii: Whether to include PII in export
            export_format: Export format ('json', 'jsonl')
            compression: Whether to compress the output
            
        Returns:
            True if export successful, False otherwise
        """
        try:
            # Get document IDs to export
            if document_ids is None:
                document_ids = list(catalog_cache.get_cached_document_ids())
            
            # Collect catalog entries
            catalog_entries = []
            for doc_id in document_ids:
                entry = catalog_cache.get_document_catalog(doc_id)
                if entry:
                    catalog_entries.append(entry)
                else:
                    logger.warning(f"Document {doc_id} not found in cache")
            
            return self.export_multiple_catalogs(
                catalog_entries, output_path, include_pii, export_format, compression
            )
            
        except Exception as e:
            logger.error(f"Error exporting from cache to {output_path}: {e}")
            return False
    
    def _write_json_export(self, export_data: Dict[str, Any], output_path: str, 
                          compression: bool = False) -> None:
        """Write export data as JSON file."""
        if compression:
            import gzip
            with gzip.open(f"{output_path}.gz", 'wt', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)
        else:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)
    
    def _write_jsonl_export(self, export_data: Dict[str, Any], output_path: str,
                           compression: bool = False) -> None:
        """Write export data as JSONL file (one JSON object per line)."""
        if compression:
            import gzip
            file_handle = gzip.open(f"{output_path}.gz", 'wt', encoding='utf-8')
        else:
            file_handle = open(output_path, 'w', encoding='utf-8')
        
        try:
            # Write metadata as first line
            metadata_line = {
                "type": "export_metadata",
                "data": export_data["export_metadata"]
            }
            file_handle.write(json.dumps(metadata_line, ensure_ascii=False) + '\n')
            
            # Write each document as a separate line
            for document in export_data["documents"]:
                document_line = {
                    "type": "document_catalog",
                    "data": document
                }
                file_handle.write(json.dumps(document_line, ensure_ascii=False) + '\n')
        finally:
            file_handle.close()


class CatalogImporter:
    """Handles import of catalog data from various formats."""
    
    def __init__(self):
        """Initialize catalog importer."""
        pass
    
    def import_catalog_file(self, input_path: str, catalog_cache: Optional[CatalogCache] = None) -> Dict[str, Any]:
        """
        Import catalog data from file.
        
        Args:
            input_path: Path to input file
            catalog_cache: Optional cache to store imported entries
            
        Returns:
            Dictionary with import results
        """
        try:
            # Determine file format
            if input_path.endswith('.gz'):
                import gzip
                file_opener = lambda p: gzip.open(p, 'rt', encoding='utf-8')
                actual_path = input_path[:-3]  # Remove .gz extension
            else:
                file_opener = lambda p: open(p, 'r', encoding='utf-8')
                actual_path = input_path
            
            if actual_path.endswith('.json'):
                return self._import_json_file(input_path, file_opener, catalog_cache)
            elif actual_path.endswith('.jsonl'):
                return self._import_jsonl_file(input_path, file_opener, catalog_cache)
            else:
                raise ValueError(f"Unsupported file format: {input_path}")
                
        except Exception as e:
            logger.error(f"Error importing catalog from {input_path}: {e}")
            return {
                'success': False,
                'error': str(e),
                'imported_count': 0,
                'failed_count': 0,
                'import_metadata': {}
            }
    
    def _import_json_file(self, input_path: str, file_opener, 
                         catalog_cache: Optional[CatalogCache] = None) -> Dict[str, Any]:
        """Import from JSON file."""
        with file_opener(input_path) as f:
            catalog_data = json.load(f)
        
        return self._process_import_data(catalog_data, catalog_cache)
    
    def _import_jsonl_file(self, input_path: str, file_opener,
                          catalog_cache: Optional[CatalogCache] = None) -> Dict[str, Any]:
        """Import from JSONL file."""
        import_metadata = {}
        documents = []
        
        with file_opener(input_path) as f:
            for line_num, line in enumerate(f, 1):
                try:
                    line_data = json.loads(line.strip())
                    
                    if line_data.get('type') == 'export_metadata':
                        import_metadata = line_data.get('data', {})
                    elif line_data.get('type') == 'document_catalog':
                        documents.append(line_data.get('data', {}))
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON on line {line_num}: {e}")
                    continue
        
        catalog_data = {
            'export_metadata': import_metadata,
            'documents': documents
        }
        
        return self._process_import_data(catalog_data, catalog_cache)
    
    def _process_import_data(self, catalog_data: Dict[str, Any],
                           catalog_cache: Optional[CatalogCache] = None) -> Dict[str, Any]:
        """Process imported catalog data."""
        imported_count = 0
        failed_count = 0
        errors = []
        
        # Handle both list and dict formats for documents
        documents = catalog_data.get('documents', [])
        if isinstance(documents, dict):
            # Convert dict to list of documents
            documents = list(documents.values())
        
        for doc_data in documents:
            try:
                # Reconstruct DocumentCatalogEntry
                catalog_entry = DocumentCatalogEntry.from_dict(doc_data)
                
                # Store in cache if provided
                if catalog_cache is not None:
                    catalog_cache.store_document_catalog(catalog_entry.document_id, catalog_entry)
                
                imported_count += 1
                
            except Exception as e:
                failed_count += 1
                error_msg = f"Failed to import document {doc_data.get('document_id', 'unknown')}: {e}"
                errors.append(error_msg)
                logger.warning(error_msg)

        
        return {
            'success': failed_count == 0,
            'imported_count': imported_count,
            'failed_count': failed_count,
            'errors': errors,
            'import_metadata': catalog_data.get('export_metadata', {})
        }


# Convenience functions for backward compatibility
def sanitize_for_export(catalog_dict: Dict[str, Any], include_pii: bool = False) -> Dict[str, Any]:
    """
    Sanitize catalog dictionary for export (backward compatibility function).
    
    Args:
        catalog_dict: Catalog dictionary to sanitize
        include_pii: Whether to include PII
        
    Returns:
        Sanitized catalog dictionary
    """
    # Create a temporary catalog entry to use the exporter
    try:
        catalog_entry = DocumentCatalogEntry.from_dict(catalog_dict)
        exporter = CatalogExporter()
        return exporter.export_catalog_entry(catalog_entry, include_pii=include_pii)
    except Exception as e:
        logger.warning(f"Error sanitizing catalog dict: {e}")
        return catalog_dict


def export_catalog_to_file(catalog_entries: Union[List[DocumentCatalogEntry], CatalogCache],
                          output_path: str, include_pii: bool = False,
                          export_format: str = 'json', compression: bool = False,
                          document_ids: Optional[List[str]] = None) -> bool:
    """
    Export catalog entries to file (convenience function).
    
    Args:
        catalog_entries: List of catalog entries or CatalogCache instance
        output_path: Output file path
        include_pii: Whether to include PII
        export_format: Export format ('json', 'jsonl')
        compression: Whether to compress output
        document_ids: Specific document IDs to export (for cache input)
        
    Returns:
        True if export successful, False otherwise
    """
    exporter = CatalogExporter()
    
    if isinstance(catalog_entries, CatalogCache):
        return exporter.export_from_cache(
            catalog_entries, output_path, document_ids, include_pii, export_format, compression
        )
    else:
        return exporter.export_multiple_catalogs(
            catalog_entries, output_path, include_pii, export_format, compression
        )


def import_catalog_from_file(input_path: str, catalog_cache: Optional[CatalogCache] = None) -> Dict[str, Any]:
    """
    Import catalog from file (convenience function).
    
    Args:
        input_path: Input file path
        catalog_cache: Optional cache to store imported entries
        
    Returns:
        Import results dictionary
    """
    importer = CatalogImporter()
    return importer.import_catalog_file(input_path, catalog_cache)