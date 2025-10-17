#!/usr/bin/env python3
"""
Field-Level Audit Logger - Tracks each CSV field with source, rule, and confidence
"""
import json
from typing import Dict, Any, Optional
from datetime import datetime

class FieldLevelAuditLogger:
    """
    Logs detailed information for each field in the CSV output
    """
    
    def __init__(self, filename: str):
        self.filename = filename
        self.audit = {
            'filename': filename,
            'processing_timestamp': datetime.now().isoformat(),
            'fields': {}
        }
    
    def log_field(self, 
                  field_name: str, 
                  value: Any, 
                  source: str, 
                  rule_applied: str, 
                  confidence_score: float,
                  additional_info: Dict = None):
        """
        Log a single field with complete audit information
        
        Args:
            field_name: Name of the CSV column
            value: The actual value extracted
            source: Where the value came from (page number, field name, etc.)
            rule_applied: The rule or logic used to select this value
            confidence_score: Confidence level (0.0 to 1.0)
            additional_info: Any additional context
        """
        self.audit['fields'][field_name] = {
            'value': str(value) if value is not None else 'N/A',
            'source': source,
            'rule_applied': rule_applied,
            'confidence_score': confidence_score,
            'additional_info': additional_info or {}
        }
    
    def save(self, output_path: str):
        """Save audit to JSON file"""
        with open(output_path, 'w') as f:
            json.dump(self.audit, f, indent=2)
    
    def get_audit(self) -> Dict:
        """Get the complete audit"""
        return self.audit
    
    def generate_csv_row(self) -> Dict:
        """Generate a CSV-friendly row with audit columns"""
        row = {}
        for field_name, field_data in self.audit['fields'].items():
            # Original value
            row[field_name] = field_data['value']
            # Audit columns
            row[f"{field_name}_source"] = field_data['source']
            row[f"{field_name}_rule"] = field_data['rule_applied']
            row[f"{field_name}_confidence"] = field_data['confidence_score']
        return row
