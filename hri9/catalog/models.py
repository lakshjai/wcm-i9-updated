"""
Core data models for the Document Catalog System.

This module defines the dataclasses and type definitions used throughout
the catalog system for document analysis and metadata storage.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import json


@dataclass
class BoundingBox:
    """Represents a rectangular region on a document page."""
    x: float
    y: float
    width: float
    height: float
    
    def to_dict(self) -> Dict[str, float]:
        """Convert to dictionary for JSON serialization."""
        return {
            'x': self.x,
            'y': self.y,
            'width': self.width,
            'height': self.height
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, float]) -> 'BoundingBox':
        """Create from dictionary for JSON deserialization."""
        return cls(
            x=data['x'],
            y=data['y'],
            width=data['width'],
            height=data['height']
        )


@dataclass
class TextRegion:
    """Represents a text region identified on a document page."""
    region_id: str
    bounding_box: Optional[BoundingBox]
    text: str
    confidence: float
    
    def __post_init__(self):
        """Validate confidence score range."""
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"Confidence must be between 0.0 and 1.0, got {self.confidence}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'region_id': self.region_id,
            'bounding_box': self.bounding_box.to_dict() if self.bounding_box else None,
            'text': self.text,
            'confidence': self.confidence
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TextRegion':
        """Create from dictionary for JSON deserialization."""
        bounding_box = None
        if data.get('bounding_box'):
            bounding_box = BoundingBox.from_dict(data['bounding_box'])
        
        return cls(
            region_id=data['region_id'],
            bounding_box=bounding_box,
            text=data['text'],
            confidence=data['confidence']
        )


@dataclass
class PageMetadata:
    """Metadata about a document page."""
    has_handwritten_text: bool = False
    has_signatures: bool = False
    image_quality: str = "medium"  # high, medium, low
    language: str = "en"
    form_version: Optional[str] = None
    security_features: List[str] = field(default_factory=list)
    text_extraction_method: str = "text"  # text, ocr, hybrid
    
    def __post_init__(self):
        """Validate metadata values."""
        valid_qualities = {"high", "medium", "low"}
        if self.image_quality not in valid_qualities:
            raise ValueError(f"Image quality must be one of {valid_qualities}")
        
        valid_methods = {"text", "ocr", "hybrid"}
        if self.text_extraction_method not in valid_methods:
            raise ValueError(f"Text extraction method must be one of {valid_methods}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'has_handwritten_text': self.has_handwritten_text,
            'has_signatures': self.has_signatures,
            'image_quality': self.image_quality,
            'language': self.language,
            'form_version': self.form_version,
            'security_features': self.security_features,
            'text_extraction_method': self.text_extraction_method
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PageMetadata':
        """Create from dictionary for JSON deserialization."""
        return cls(
            has_handwritten_text=data.get('has_handwritten_text', False),
            has_signatures=data.get('has_signatures', False),
            image_quality=data.get('image_quality', 'medium'),
            language=data.get('language', 'en'),
            form_version=data.get('form_version'),
            security_features=data.get('security_features', []),
            text_extraction_method=data.get('text_extraction_method', 'text')
        )


@dataclass
class PageAnalysis:
    """Complete analysis results for a single document page."""
    page_number: int
    page_title: str
    page_type: str  # government_form, identity_document, employment_record, other
    page_subtype: str  # i9_form, passport, drivers_license, etc.
    confidence_score: float
    extracted_values: Dict[str, Any] = field(default_factory=dict)
    text_regions: List[TextRegion] = field(default_factory=list)
    page_metadata: PageMetadata = field(default_factory=PageMetadata)
    
    def __post_init__(self):
        """Validate page analysis data."""
        if self.page_number < 1:
            raise ValueError(f"Page number must be >= 1, got {self.page_number}")
        
        if not 0.0 <= self.confidence_score <= 1.0:
            raise ValueError(f"Confidence score must be between 0.0 and 1.0, got {self.confidence_score}")
        
        valid_types = {"government_form", "identity_document", "employment_record", "other"}
        if self.page_type not in valid_types:
            raise ValueError(f"Page type must be one of {valid_types}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'page_number': self.page_number,
            'page_title': self.page_title,
            'page_type': self.page_type,
            'page_subtype': self.page_subtype,
            'confidence_score': self.confidence_score,
            'extracted_values': self.extracted_values,
            'text_regions': [region.to_dict() for region in self.text_regions],
            'page_metadata': self.page_metadata.to_dict()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PageAnalysis':
        """Create from dictionary for JSON deserialization."""
        text_regions = [
            TextRegion.from_dict(region_data) 
            for region_data in data.get('text_regions', [])
        ]
        
        page_metadata = PageMetadata.from_dict(data.get('page_metadata', {}))
        
        return cls(
            page_number=data['page_number'],
            page_title=data['page_title'],
            page_type=data['page_type'],
            page_subtype=data['page_subtype'],
            confidence_score=data['confidence_score'],
            extracted_values=data.get('extracted_values', {}),
            text_regions=text_regions,
            page_metadata=page_metadata
        )


@dataclass
class DocumentMetadata:
    """Metadata about the document file."""
    file_size: int
    creation_date: Optional[datetime] = None
    modification_date: Optional[datetime] = None
    file_path: str = ""
    
    def __post_init__(self):
        """Validate document metadata."""
        if self.file_size < 0:
            raise ValueError(f"File size must be >= 0, got {self.file_size}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'file_size': self.file_size,
            'creation_date': self.creation_date.isoformat() if self.creation_date else None,
            'modification_date': self.modification_date.isoformat() if self.modification_date else None,
            'file_path': self.file_path
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DocumentMetadata':
        """Create from dictionary for JSON deserialization."""
        creation_date = None
        if data.get('creation_date'):
            creation_date = datetime.fromisoformat(data['creation_date'])
        
        modification_date = None
        if data.get('modification_date'):
            modification_date = datetime.fromisoformat(data['modification_date'])
        
        return cls(
            file_size=data['file_size'],
            creation_date=creation_date,
            modification_date=modification_date,
            file_path=data.get('file_path', '')
        )


@dataclass
class DocumentClassification:
    """Overall classification of the document."""
    primary_document_type: str
    contains_government_forms: bool = False
    contains_identity_documents: bool = False
    contains_employment_records: bool = False
    i9_form_count: int = 0
    latest_i9_page: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'primary_document_type': self.primary_document_type,
            'contains_government_forms': self.contains_government_forms,
            'contains_identity_documents': self.contains_identity_documents,
            'contains_employment_records': self.contains_employment_records,
            'i9_form_count': self.i9_form_count,
            'latest_i9_page': self.latest_i9_page
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DocumentClassification':
        """Create from dictionary for JSON deserialization."""
        return cls(
            primary_document_type=data['primary_document_type'],
            contains_government_forms=data.get('contains_government_forms', False),
            contains_identity_documents=data.get('contains_identity_documents', False),
            contains_employment_records=data.get('contains_employment_records', False),
            i9_form_count=data.get('i9_form_count', 0),
            latest_i9_page=data.get('latest_i9_page')
        )


@dataclass
class ProcessingSummary:
    """Summary of document processing results."""
    total_pages_analyzed: int
    api_calls_made: int
    processing_time_seconds: float
    high_confidence_pages: int = 0
    low_confidence_pages: int = 0
    manual_review_required: bool = False
    error_count: int = 0
    errors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'total_pages_analyzed': self.total_pages_analyzed,
            'api_calls_made': self.api_calls_made,
            'processing_time_seconds': self.processing_time_seconds,
            'high_confidence_pages': self.high_confidence_pages,
            'low_confidence_pages': self.low_confidence_pages,
            'manual_review_required': self.manual_review_required,
            'error_count': self.error_count,
            'errors': self.errors
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ProcessingSummary':
        """Create from dictionary for JSON deserialization."""
        return cls(
            total_pages_analyzed=data['total_pages_analyzed'],
            api_calls_made=data['api_calls_made'],
            processing_time_seconds=data['processing_time_seconds'],
            high_confidence_pages=data.get('high_confidence_pages', 0),
            low_confidence_pages=data.get('low_confidence_pages', 0),
            manual_review_required=data.get('manual_review_required', False),
            error_count=data.get('error_count', 0),
            errors=data.get('errors', [])
        )


@dataclass
class DocumentCatalogEntry:
    """Complete catalog entry for a processed document."""
    document_id: str
    document_name: str
    total_pages: int
    processing_timestamp: str
    document_metadata: DocumentMetadata
    pages: List[PageAnalysis] = field(default_factory=list)
    document_classification: DocumentClassification = field(default_factory=lambda: DocumentClassification("unknown"))
    processing_summary: ProcessingSummary = field(default_factory=lambda: ProcessingSummary(0, 0, 0.0))
    
    def __post_init__(self):
        """Validate document catalog entry."""
        if self.total_pages < 0:
            raise ValueError(f"Total pages must be >= 0, got {self.total_pages}")
        
        if len(self.pages) > self.total_pages:
            raise ValueError(f"Number of page analyses ({len(self.pages)}) exceeds total pages ({self.total_pages})")
    
    def get_page_analysis(self, page_number: int) -> Optional[PageAnalysis]:
        """Get analysis for a specific page number."""
        for page in self.pages:
            if page.page_number == page_number:
                return page
        return None
    
    def get_pages_by_type(self, page_type: str) -> List[PageAnalysis]:
        """Get all pages of a specific type."""
        return [page for page in self.pages if page.page_type == page_type]
    
    def get_pages_by_subtype(self, page_subtype: str) -> List[PageAnalysis]:
        """Get all pages of a specific subtype."""
        return [page for page in self.pages if page.page_subtype == page_subtype]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'document_id': self.document_id,
            'document_name': self.document_name,
            'total_pages': self.total_pages,
            'processing_timestamp': self.processing_timestamp,
            'document_metadata': self.document_metadata.to_dict(),
            'pages': [page.to_dict() for page in self.pages],
            'document_classification': self.document_classification.to_dict(),
            'processing_summary': self.processing_summary.to_dict()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DocumentCatalogEntry':
        """Create from dictionary for JSON deserialization."""
        document_metadata = DocumentMetadata.from_dict(data['document_metadata'])
        pages = [PageAnalysis.from_dict(page_data) for page_data in data.get('pages', [])]
        document_classification = DocumentClassification.from_dict(data.get('document_classification', {}))
        processing_summary = ProcessingSummary.from_dict(data.get('processing_summary', {}))
        
        return cls(
            document_id=data['document_id'],
            document_name=data['document_name'],
            total_pages=data['total_pages'],
            processing_timestamp=data['processing_timestamp'],
            document_metadata=document_metadata,
            pages=pages,
            document_classification=document_classification,
            processing_summary=processing_summary
        )
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'DocumentCatalogEntry':
        """Create from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)


# Type aliases for better code readability
CatalogDict = Dict[str, DocumentCatalogEntry]
PageDict = Dict[int, PageAnalysis]