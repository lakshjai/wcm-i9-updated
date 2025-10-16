#!/usr/bin/env python3
"""
Catalog validation and analysis utilities.

This module provides functionality for validating catalog files,
generating statistics, and performing quality assurance checks.
"""

import json
import os
import glob
from typing import Dict, List, Any, Optional
from pathlib import Path

from ..utils.logging_config import logger
from .models import DocumentCatalogEntry, PageAnalysis, ProcessingSummary


class CatalogValidator:
    """Validator for document catalog files and data integrity."""
    
    def __init__(self, confidence_threshold: float = 0.7):
        """Initialize the catalog validator.
        
        Args:
            confidence_threshold: Minimum confidence score for high-quality analysis
        """
        self.confidence_threshold = confidence_threshold
        self.required_fields = {
            'document_id', 'document_name', 'total_pages', 'processing_timestamp',
            'document_metadata', 'pages', 'document_classification', 'processing_summary'
        }
        self.required_page_fields = {
            'page_number', 'page_title', 'page_type', 'page_subtype',
            'confidence_score', 'extracted_values', 'text_regions', 'page_metadata'
        }
    
    def find_catalog_files(self, directory: str) -> List[str]:
        """
        Find all catalog files in the specified directory.
        
        Args:
            directory: Directory to search for catalog files
            
        Returns:
            List of catalog file paths
        """
        if not os.path.exists(directory):
            logger.warning(f"Directory does not exist: {directory}")
            return []
        
        # Look for JSON catalog files
        json_pattern = os.path.join(directory, "*catalog*.json")
        json_files = glob.glob(json_pattern)
        
        # Look for CSV catalog files
        csv_pattern = os.path.join(directory, "*catalog*.csv")
        csv_files = glob.glob(csv_pattern)
        
        all_files = json_files + csv_files
        logger.info(f"Found {len(all_files)} catalog files in {directory}")
        
        return sorted(all_files)
    
    def validate_catalog_file(self, file_path: str) -> Dict[str, Any]:
        """
        Validate a single catalog file.
        
        Args:
            file_path: Path to the catalog file
            
        Returns:
            Dictionary with validation results
        """
        result = {
            'file_path': file_path,
            'valid': False,
            'error': None,
            'warnings': [],
            'statistics': {}
        }
        
        try:
            if not os.path.exists(file_path):
                result['error'] = "File does not exist"
                return result
            
            # Determine file type and validate accordingly
            if file_path.endswith('.json'):
                return self._validate_json_catalog(file_path, result)
            elif file_path.endswith('.csv'):
                return self._validate_csv_catalog(file_path, result)
            else:
                result['error'] = "Unsupported file format"
                return result
                
        except Exception as e:
            result['error'] = f"Validation error: {str(e)}"
            return result
    
    def _validate_json_catalog(self, file_path: str, result: Dict[str, Any]) -> Dict[str, Any]:
        """Validate JSON catalog file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                catalog_data = json.load(f)
            
            # Validate top-level structure
            if not isinstance(catalog_data, dict):
                result['error'] = "Catalog data must be a dictionary"
                return result
            
            if 'documents' not in catalog_data:
                result['error'] = "Missing 'documents' field in catalog"
                return result
            
            documents = catalog_data['documents']
            if not isinstance(documents, list):
                result['error'] = "'documents' field must be a list"
                return result
            
            # Validate each document
            document_count = 0
            page_count = 0
            validation_errors = []
            
            for i, doc in enumerate(documents):
                doc_validation = self._validate_document_entry(doc, i)
                if not doc_validation['valid']:
                    validation_errors.extend(doc_validation['errors'])
                else:
                    document_count += 1
                    page_count += len(doc.get('pages', []))
            
            # Set validation result
            if validation_errors:
                result['error'] = f"Document validation errors: {'; '.join(validation_errors[:5])}"
                if len(validation_errors) > 5:
                    result['error'] += f" (and {len(validation_errors) - 5} more)"
            else:
                result['valid'] = True
            
            # Add statistics
            result['statistics'] = {
                'document_count': document_count,
                'page_count': page_count,
                'file_size_bytes': os.path.getsize(file_path),
                'validation_errors': len(validation_errors)
            }
            
            return result
            
        except json.JSONDecodeError as e:
            result['error'] = f"Invalid JSON format: {str(e)}"
            return result
        except Exception as e:
            result['error'] = f"JSON validation error: {str(e)}"
            return result
    
    def _validate_csv_catalog(self, file_path: str, result: Dict[str, Any]) -> Dict[str, Any]:
        """Validate CSV catalog file."""
        try:
            import csv
            
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                # Check for required columns
                required_columns = {'document_id', 'document_name', 'page_number', 'page_type'}
                if not required_columns.issubset(set(reader.fieldnames or [])):
                    missing = required_columns - set(reader.fieldnames or [])
                    result['error'] = f"Missing required columns: {', '.join(missing)}"
                    return result
                
                # Count rows and validate basic structure
                row_count = 0
                document_ids = set()
                
                for row in reader:
                    row_count += 1
                    if row.get('document_id'):
                        document_ids.add(row['document_id'])
                
                result['valid'] = True
                result['statistics'] = {
                    'row_count': row_count,
                    'document_count': len(document_ids),
                    'file_size_bytes': os.path.getsize(file_path)
                }
                
                return result
                
        except Exception as e:
            result['error'] = f"CSV validation error: {str(e)}"
            return result
    
    def _validate_document_entry(self, document: Dict[str, Any], index: int) -> Dict[str, Any]:
        """Validate a single document entry."""
        result = {'valid': True, 'errors': []}
        
        # Check required fields
        missing_fields = self.required_fields - set(document.keys())
        if missing_fields:
            result['errors'].append(f"Document {index}: Missing fields {missing_fields}")
            result['valid'] = False
        
        # Validate pages if present
        if 'pages' in document:
            pages = document['pages']
            if not isinstance(pages, list):
                result['errors'].append(f"Document {index}: 'pages' must be a list")
                result['valid'] = False
            else:
                for j, page in enumerate(pages):
                    page_validation = self._validate_page_entry(page, index, j)
                    if not page_validation['valid']:
                        result['errors'].extend(page_validation['errors'])
                        result['valid'] = False
        
        return result
    
    def _validate_page_entry(self, page: Dict[str, Any], doc_index: int, page_index: int) -> Dict[str, Any]:
        """Validate a single page entry."""
        result = {'valid': True, 'errors': []}
        
        # Check required fields
        missing_fields = self.required_page_fields - set(page.keys())
        if missing_fields:
            result['errors'].append(f"Document {doc_index}, Page {page_index}: Missing fields {missing_fields}")
            result['valid'] = False
        
        # Validate confidence score
        if 'confidence_score' in page:
            confidence = page['confidence_score']
            if not isinstance(confidence, (int, float)) or not (0.0 <= confidence <= 1.0):
                result['errors'].append(f"Document {doc_index}, Page {page_index}: Invalid confidence score")
                result['valid'] = False
        
        return result
    
    def load_catalog_file(self, file_path: str) -> Dict[str, Any]:
        """
        Load catalog data from file.
        
        Args:
            file_path: Path to the catalog file
            
        Returns:
            Catalog data dictionary
        """
        if file_path.endswith('.json'):
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            raise ValueError(f"Unsupported file format for loading: {file_path}")
    
    def generate_validation_stats(self, validation_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate summary statistics from validation results.
        
        Args:
            validation_results: List of validation result dictionaries
            
        Returns:
            Summary statistics dictionary
        """
        total_files = len(validation_results)
        valid_files = sum(1 for r in validation_results if r['valid'])
        
        total_documents = sum(r['statistics'].get('document_count', 0) for r in validation_results)
        total_pages = sum(r['statistics'].get('page_count', 0) for r in validation_results)
        total_size = sum(r['statistics'].get('file_size_bytes', 0) for r in validation_results)
        
        return {
            'total_files': total_files,
            'valid_files': valid_files,
            'invalid_files': total_files - valid_files,
            'validation_success_rate': (valid_files / total_files * 100) if total_files > 0 else 0,
            'total_documents': total_documents,
            'total_pages': total_pages,
            'total_size_mb': total_size / (1024 * 1024),
            'average_documents_per_file': total_documents / total_files if total_files > 0 else 0,
            'average_pages_per_document': total_pages / total_documents if total_documents > 0 else 0
        }
    
    def generate_catalog_statistics(self, catalog_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate statistics for a loaded catalog.
        
        Args:
            catalog_data: Loaded catalog data
            
        Returns:
            Statistics dictionary
        """
        documents = catalog_data.get('documents', [])
        
        # Basic counts
        document_count = len(documents)
        total_pages = sum(len(doc.get('pages', [])) for doc in documents)
        
        # Page type distribution
        page_types = {}
        confidence_scores = []
        
        for doc in documents:
            for page in doc.get('pages', []):
                page_type = page.get('page_type', 'unknown')
                page_types[page_type] = page_types.get(page_type, 0) + 1
                
                confidence = page.get('confidence_score')
                if isinstance(confidence, (int, float)):
                    confidence_scores.append(confidence)
        
        # Calculate confidence statistics
        avg_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0
        min_confidence = min(confidence_scores) if confidence_scores else 0
        max_confidence = max(confidence_scores) if confidence_scores else 0
        
        return {
            'document_count': document_count,
            'total_pages': total_pages,
            'average_pages_per_document': total_pages / document_count if document_count > 0 else 0,
            'page_type_distribution': page_types,
            'confidence_statistics': {
                'average': avg_confidence,
                'minimum': min_confidence,
                'maximum': max_confidence,
                'total_scores': len(confidence_scores)
            }
        }
    
    def validate_confidence_scores(self, catalog_entry: DocumentCatalogEntry) -> Dict[str, Any]:
        """
        Validate confidence scores and flag documents for manual review.
        
        Args:
            catalog_entry: Document catalog entry to validate
            
        Returns:
            Dictionary with confidence validation results
        """
        confidence_scores = [page.confidence_score for page in catalog_entry.pages]
        
        if not confidence_scores:
            return {
                'requires_manual_review': True,
                'reason': 'No pages analyzed',
                'low_confidence_pages': [],
                'high_confidence_pages': [],
                'average_confidence': 0.0
            }
        
        low_confidence_pages = [
            page.page_number for page in catalog_entry.pages 
            if page.confidence_score < self.confidence_threshold
        ]
        
        high_confidence_pages = [
            page.page_number for page in catalog_entry.pages 
            if page.confidence_score >= self.confidence_threshold
        ]
        
        average_confidence = sum(confidence_scores) / len(confidence_scores)
        
        # Flag for manual review if more than 50% of pages are low confidence
        # or if any critical document types (I-9 forms) have low confidence
        requires_manual_review = False
        reason = None
        
        if len(low_confidence_pages) > len(confidence_scores) / 2:
            requires_manual_review = True
            reason = f"More than 50% of pages ({len(low_confidence_pages)}/{len(confidence_scores)}) have low confidence"
        
        # Check for low confidence I-9 forms specifically
        i9_pages = catalog_entry.get_pages_by_subtype('i9_form')
        low_confidence_i9_pages = [
            page.page_number for page in i9_pages 
            if page.confidence_score < self.confidence_threshold
        ]
        
        if low_confidence_i9_pages:
            requires_manual_review = True
            reason = f"I-9 form pages {low_confidence_i9_pages} have low confidence scores"
        
        return {
            'requires_manual_review': requires_manual_review,
            'reason': reason,
            'low_confidence_pages': low_confidence_pages,
            'high_confidence_pages': high_confidence_pages,
            'average_confidence': average_confidence,
            'low_confidence_i9_pages': low_confidence_i9_pages
        }
    
    def validate_catalog_completeness(self, catalog_entry: DocumentCatalogEntry) -> Dict[str, Any]:
        """
        Check catalog completeness and data integrity.
        
        Args:
            catalog_entry: Document catalog entry to validate
            
        Returns:
            Dictionary with completeness validation results
        """
        issues = []
        warnings = []
        
        # Check if all pages are analyzed
        expected_pages = set(range(1, catalog_entry.total_pages + 1))
        analyzed_pages = set(page.page_number for page in catalog_entry.pages)
        missing_pages = expected_pages - analyzed_pages
        
        if missing_pages:
            issues.append(f"Missing analysis for pages: {sorted(missing_pages)}")
        
        # Check for duplicate page numbers
        page_numbers = [page.page_number for page in catalog_entry.pages]
        duplicates = set([x for x in page_numbers if page_numbers.count(x) > 1])
        if duplicates:
            issues.append(f"Duplicate page numbers found: {sorted(duplicates)}")
        
        # Check for pages with empty or minimal content
        empty_pages = []
        for page in catalog_entry.pages:
            if not page.page_title.strip() or page.page_title.lower() in ['untitled', 'blank', 'empty']:
                empty_pages.append(page.page_number)
        
        if empty_pages:
            warnings.append(f"Pages with minimal content: {empty_pages}")
        
        # Check for inconsistent page types
        page_type_counts = {}
        for page in catalog_entry.pages:
            page_type_counts[page.page_type] = page_type_counts.get(page.page_type, 0) + 1
        
        # Check document classification consistency
        classification = catalog_entry.document_classification
        i9_pages = catalog_entry.get_pages_by_subtype('i9_form')
        
        if len(i9_pages) != classification.i9_form_count:
            issues.append(f"I-9 form count mismatch: found {len(i9_pages)} pages but classification says {classification.i9_form_count}")
        
        # Check processing summary consistency
        summary = catalog_entry.processing_summary
        if summary.total_pages_analyzed != len(catalog_entry.pages):
            issues.append(f"Processing summary mismatch: analyzed {summary.total_pages_analyzed} but have {len(catalog_entry.pages)} page entries")
        
        return {
            'is_complete': len(issues) == 0,
            'issues': issues,
            'warnings': warnings,
            'completeness_score': 1.0 - (len(missing_pages) / catalog_entry.total_pages) if catalog_entry.total_pages > 0 else 0.0,
            'missing_pages': sorted(missing_pages),
            'duplicate_pages': sorted(duplicates),
            'empty_pages': empty_pages,
            'page_type_distribution': page_type_counts
        }
    
    def generate_processing_summary_statistics(self, catalog_entry: DocumentCatalogEntry) -> Dict[str, Any]:
        """
        Generate detailed statistics for processing summary validation.
        
        Args:
            catalog_entry: Document catalog entry to analyze
            
        Returns:
            Dictionary with processing summary statistics
        """
        pages = catalog_entry.pages
        
        if not pages:
            return {
                'field_extraction_stats': {},
                'confidence_distribution': {},
                'page_type_stats': {},
                'text_region_stats': {},
                'quality_metrics': {}
            }
        
        # Field extraction statistics
        field_extraction_stats = {}
        total_extracted_fields = 0
        
        for page in pages:
            extracted_count = len(page.extracted_values)
            total_extracted_fields += extracted_count
            page_type = page.page_type
            
            if page_type not in field_extraction_stats:
                field_extraction_stats[page_type] = {
                    'total_fields': 0,
                    'page_count': 0,
                    'avg_fields_per_page': 0
                }
            
            field_extraction_stats[page_type]['total_fields'] += extracted_count
            field_extraction_stats[page_type]['page_count'] += 1
        
        # Calculate averages
        for page_type in field_extraction_stats:
            stats = field_extraction_stats[page_type]
            stats['avg_fields_per_page'] = stats['total_fields'] / stats['page_count']
        
        # Confidence score distribution
        confidence_scores = [page.confidence_score for page in pages]
        confidence_distribution = {
            'high_confidence': len([s for s in confidence_scores if s >= 0.8]),
            'medium_confidence': len([s for s in confidence_scores if 0.5 <= s < 0.8]),
            'low_confidence': len([s for s in confidence_scores if s < 0.5]),
            'average': sum(confidence_scores) / len(confidence_scores),
            'minimum': min(confidence_scores),
            'maximum': max(confidence_scores)
        }
        
        # Page type statistics
        page_type_stats = {}
        for page in pages:
            page_type = page.page_type
            if page_type not in page_type_stats:
                page_type_stats[page_type] = {'count': 0, 'subtypes': {}}
            
            page_type_stats[page_type]['count'] += 1
            subtype = page.page_subtype
            if subtype not in page_type_stats[page_type]['subtypes']:
                page_type_stats[page_type]['subtypes'][subtype] = 0
            page_type_stats[page_type]['subtypes'][subtype] += 1
        
        # Text region statistics
        total_text_regions = sum(len(page.text_regions) for page in pages)
        text_region_stats = {
            'total_regions': total_text_regions,
            'avg_regions_per_page': total_text_regions / len(pages),
            'pages_with_regions': len([p for p in pages if p.text_regions])
        }
        
        # Quality metrics
        quality_metrics = {
            'pages_with_handwritten_text': len([p for p in pages if p.page_metadata.has_handwritten_text]),
            'pages_with_signatures': len([p for p in pages if p.page_metadata.has_signatures]),
            'high_quality_images': len([p for p in pages if p.page_metadata.image_quality == 'high']),
            'low_quality_images': len([p for p in pages if p.page_metadata.image_quality == 'low']),
            'ocr_pages': len([p for p in pages if p.page_metadata.text_extraction_method == 'ocr'])
        }
        
        return {
            'field_extraction_stats': field_extraction_stats,
            'confidence_distribution': confidence_distribution,
            'page_type_stats': page_type_stats,
            'text_region_stats': text_region_stats,
            'quality_metrics': quality_metrics,
            'total_extracted_fields': total_extracted_fields
        }
    
    def flag_for_manual_review(self, catalog_entry: DocumentCatalogEntry) -> Dict[str, Any]:
        """
        Determine if a document requires manual review and generate flagging report.
        
        Args:
            catalog_entry: Document catalog entry to evaluate
            
        Returns:
            Dictionary with manual review flagging results
        """
        flags = []
        priority = 'low'
        
        # Check confidence scores
        confidence_validation = self.validate_confidence_scores(catalog_entry)
        if confidence_validation['requires_manual_review']:
            flags.append({
                'type': 'low_confidence',
                'description': confidence_validation['reason'],
                'affected_pages': confidence_validation['low_confidence_pages'],
                'priority': 'high' if confidence_validation['low_confidence_i9_pages'] else 'medium'
            })
            if confidence_validation['low_confidence_i9_pages']:
                priority = 'high'
            elif priority == 'low':
                priority = 'medium'
        
        # Check completeness
        completeness_validation = self.validate_catalog_completeness(catalog_entry)
        if not completeness_validation['is_complete']:
            flags.append({
                'type': 'incomplete_analysis',
                'description': f"Data integrity issues: {'; '.join(completeness_validation['issues'])}",
                'affected_pages': completeness_validation['missing_pages'],
                'priority': 'high'
            })
            priority = 'high'
        
        # Check for processing errors
        if catalog_entry.processing_summary.error_count > 0:
            flags.append({
                'type': 'processing_errors',
                'description': f"{catalog_entry.processing_summary.error_count} processing errors occurred",
                'affected_pages': [],
                'priority': 'medium'
            })
            if priority == 'low':
                priority = 'medium'
        
        # Check for handwritten content that might need verification
        handwritten_pages = [
            page.page_number for page in catalog_entry.pages 
            if page.page_metadata.has_handwritten_text
        ]
        if handwritten_pages:
            flags.append({
                'type': 'handwritten_content',
                'description': f"Pages contain handwritten text that may need verification",
                'affected_pages': handwritten_pages,
                'priority': 'low'
            })
        
        # Check for security features that might indicate important documents
        security_feature_pages = []
        for page in catalog_entry.pages:
            if page.page_metadata.security_features:
                security_feature_pages.append(page.page_number)
        
        if security_feature_pages:
            flags.append({
                'type': 'security_features',
                'description': f"Pages contain security features (watermarks, holograms, etc.)",
                'affected_pages': security_feature_pages,
                'priority': 'medium'
            })
            if priority == 'low':
                priority = 'medium'
        
        return {
            'requires_manual_review': len(flags) > 0,
            'priority': priority,
            'flags': flags,
            'flag_count': len(flags),
            'summary': f"Document flagged for {priority} priority manual review with {len(flags)} issues" if flags else "No manual review required"
        }


def validate_document_catalog_entry(entry: Dict[str, Any]) -> bool:
    """
    Validate a document catalog entry for completeness and correctness.
    
    Args:
        entry: Document catalog entry dictionary
        
    Returns:
        bool: True if valid, False otherwise
    """
    validator = CatalogValidator()
    result = validator._validate_document_entry(entry, 0)
    return result['valid']


def is_valid_catalog_entry(entry: Dict[str, Any]) -> bool:
    """
    Check if a catalog entry is valid.
    
    Args:
        entry: Catalog entry dictionary
        
    Returns:
        bool: True if valid, False otherwise
    """
    return validate_document_catalog_entry(entry)


def validate_confidence_threshold(catalog_entry: DocumentCatalogEntry, threshold: float = 0.7) -> bool:
    """
    Check if document meets confidence threshold requirements.
    
    Args:
        catalog_entry: Document catalog entry to validate
        threshold: Minimum confidence threshold
        
    Returns:
        bool: True if document meets threshold, False otherwise
    """
    validator = CatalogValidator(confidence_threshold=threshold)
    confidence_validation = validator.validate_confidence_scores(catalog_entry)
    return not confidence_validation['requires_manual_review']


def generate_quality_assurance_report(catalog_entries: List[DocumentCatalogEntry], 
                                     confidence_threshold: float = 0.7) -> Dict[str, Any]:
    """
    Generate comprehensive quality assurance report for multiple catalog entries.
    
    Args:
        catalog_entries: List of document catalog entries
        confidence_threshold: Minimum confidence threshold for validation
        
    Returns:
        Dictionary with comprehensive QA report
    """
    validator = CatalogValidator(confidence_threshold=confidence_threshold)
    
    total_documents = len(catalog_entries)
    documents_requiring_review = 0
    high_priority_reviews = 0
    total_pages = 0
    total_errors = 0
    
    confidence_scores = []
    page_type_distribution = {}
    flag_type_counts = {}
    
    document_reports = []
    
    for entry in catalog_entries:
        # Generate individual document report
        confidence_validation = validator.validate_confidence_scores(entry)
        completeness_validation = validator.validate_catalog_completeness(entry)
        manual_review_flags = validator.flag_for_manual_review(entry)
        processing_stats = validator.generate_processing_summary_statistics(entry)
        
        document_report = {
            'document_id': entry.document_id,
            'document_name': entry.document_name,
            'requires_manual_review': manual_review_flags['requires_manual_review'],
            'priority': manual_review_flags['priority'],
            'flag_count': manual_review_flags['flag_count'],
            'confidence_validation': confidence_validation,
            'completeness_validation': completeness_validation,
            'processing_stats': processing_stats
        }
        
        document_reports.append(document_report)
        
        # Aggregate statistics
        if manual_review_flags['requires_manual_review']:
            documents_requiring_review += 1
            if manual_review_flags['priority'] == 'high':
                high_priority_reviews += 1
        
        total_pages += entry.total_pages
        total_errors += entry.processing_summary.error_count
        
        # Collect confidence scores
        for page in entry.pages:
            confidence_scores.append(page.confidence_score)
        
        # Aggregate page types
        for page in entry.pages:
            page_type = page.page_type
            page_type_distribution[page_type] = page_type_distribution.get(page_type, 0) + 1
        
        # Count flag types
        for flag in manual_review_flags['flags']:
            flag_type = flag['type']
            flag_type_counts[flag_type] = flag_type_counts.get(flag_type, 0) + 1
    
    # Calculate overall statistics
    overall_stats = {
        'total_documents': total_documents,
        'total_pages': total_pages,
        'documents_requiring_review': documents_requiring_review,
        'high_priority_reviews': high_priority_reviews,
        'review_rate': (documents_requiring_review / total_documents * 100) if total_documents > 0 else 0,
        'high_priority_rate': (high_priority_reviews / total_documents * 100) if total_documents > 0 else 0,
        'total_processing_errors': total_errors,
        'average_confidence': sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0,
        'confidence_threshold': confidence_threshold,
        'page_type_distribution': page_type_distribution,
        'flag_type_distribution': flag_type_counts
    }
    
    return {
        'overall_statistics': overall_stats,
        'document_reports': document_reports,
        'summary': {
            'quality_score': (total_documents - documents_requiring_review) / total_documents * 100 if total_documents > 0 else 0,
            'recommendation': _generate_qa_recommendation(overall_stats),
            'next_actions': _generate_next_actions(overall_stats, flag_type_counts)
        }
    }


def _generate_qa_recommendation(stats: Dict[str, Any]) -> str:
    """Generate QA recommendation based on statistics."""
    review_rate = stats['review_rate']
    high_priority_rate = stats['high_priority_rate']
    avg_confidence = stats['average_confidence']
    
    if high_priority_rate > 20:
        return "CRITICAL: High number of documents require immediate manual review"
    elif review_rate > 50:
        return "WARNING: More than half of documents require manual review"
    elif avg_confidence < 0.6:
        return "CAUTION: Overall confidence scores are below acceptable threshold"
    elif review_rate < 10:
        return "EXCELLENT: Processing quality is very high with minimal manual review needed"
    else:
        return "GOOD: Processing quality is acceptable with normal manual review requirements"


def _generate_next_actions(stats: Dict[str, Any], flag_counts: Dict[str, int]) -> List[str]:
    """Generate recommended next actions based on QA results."""
    actions = []
    
    if stats['high_priority_reviews'] > 0:
        actions.append(f"Review {stats['high_priority_reviews']} high-priority documents immediately")
    
    if flag_counts.get('low_confidence', 0) > 0:
        actions.append("Investigate low confidence scores - consider reprocessing or model tuning")
    
    if flag_counts.get('incomplete_analysis', 0) > 0:
        actions.append("Address incomplete analysis issues - check for processing failures")
    
    if flag_counts.get('processing_errors', 0) > 0:
        actions.append("Review processing error logs and fix underlying issues")
    
    if stats['review_rate'] > 30:
        actions.append("Consider adjusting confidence thresholds or improving processing pipeline")
    
    if not actions:
        actions.append("Continue monitoring - processing quality is acceptable")
    
    return actions


def sanitize_for_export(data: Dict[str, Any], include_pii: bool = False) -> Dict[str, Any]:
    """
    Sanitize catalog data for export, optionally removing PII.
    
    Args:
        data: Catalog data dictionary
        include_pii: Whether to include potentially sensitive information
        
    Returns:
        Sanitized data dictionary
    """
    if include_pii:
        return data
    
    # Create a deep copy to avoid modifying original data
    import copy
    sanitized = copy.deepcopy(data)
    
    # Remove or mask potentially sensitive fields
    sensitive_fields = ['ssn', 'social_security', 'tax_id', 'employee_id', 'personal_id']
    
    def _sanitize_dict(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if any(sensitive in key.lower() for sensitive in sensitive_fields):
                    obj[key] = "[REDACTED]"
                elif isinstance(value, (dict, list)):
                    _sanitize_dict(value)
        elif isinstance(obj, list):
            for item in obj:
                _sanitize_dict(item)
    
    _sanitize_dict(sanitized)
    return sanitized