#!/usr/bin/env python3
"""
Rubric Audit Logger - Detailed decision tracking for rubric processing
"""
import json
from typing import Dict, List, Any
from datetime import datetime

class RubricAuditLogger:
    """
    Logs detailed decision-making process for each rubric processing step
    """
    
    def __init__(self, filename: str):
        self.filename = filename
        self.audit_log = {
            'filename': filename,
            'processing_timestamp': datetime.now().isoformat(),
            'buckets': {},
            'business_fields': {},
            'status_determination': {},
            'decisions': []
        }
    
    def log_decision(self, category: str, decision: str, reason: str, data: Dict = None):
        """Log a decision with category, decision made, and reasoning"""
        entry = {
            'category': category,
            'decision': decision,
            'reason': reason,
            'timestamp': datetime.now().isoformat()
        }
        if data:
            entry['data'] = data
        self.audit_log['decisions'].append(entry)
    
    def log_bucket_1_personal_data(self, score: int, data: Dict, reasons: List[str]):
        """Log Bucket 1 (Personal Data) processing"""
        self.audit_log['buckets']['bucket_1_personal_data'] = {
            'score': score,
            'max_score': 25,
            'data': data,
            'reasons': reasons,
            'fields_extracted': {
                'first_name': data.get('first_name', 'NOT FOUND'),
                'middle_name': data.get('middle_name', 'NOT FOUND'),
                'last_name': data.get('last_name', 'NOT FOUND'),
                'date_of_birth': data.get('date_of_birth', 'NOT FOUND'),
                'ssn': data.get('ssn', 'NOT FOUND'),
                'citizenship_status': data.get('citizenship_status', 'NOT FOUND')
            }
        }
        
        # Log individual field decisions
        for field, value in self.audit_log['buckets']['bucket_1_personal_data']['fields_extracted'].items():
            if value and value not in ['NOT FOUND', 'N/A', '', None]:
                self.log_decision('Bucket 1', f'{field} extracted', f'Found value: {value}')
            else:
                self.log_decision('Bucket 1', f'{field} NOT extracted', f'Value: {value}')
    
    def log_bucket_2_i9_detection(self, score: int, data: Dict, reasons: List[str]):
        """Log Bucket 2 (I-9 Detection) processing"""
        self.audit_log['buckets']['bucket_2_i9_detection'] = {
            'score': score,
            'max_score': 20,
            'data': data,
            'reasons': reasons,
            'i9_sets_found': data.get('i9_sets_found', {}),
            'section_counts': {
                'section_1': data.get('section_1_count', 0),
                'section_2': data.get('section_2_count', 0),
                'section_3': data.get('section_3_count', 0),
                'supplement_b': data.get('supplement_b_count', 0)
            }
        }
        
        self.log_decision('Bucket 2', f'I-9 Sets Detected', 
                         f"Found {len(data.get('i9_sets_found', {}))} I-9 set(s)",
                         {'sets': list(data.get('i9_sets_found', {}).keys())})
    
    def log_bucket_3_business_rules(self, score: int, data: Dict, reasons: List[str]):
        """Log Bucket 3 (Business Rules) processing"""
        self.audit_log['buckets']['bucket_3_business_rules'] = {
            'score': score,
            'max_score': 25,
            'data': data,
            'reasons': reasons,
            'form_type_selected': data.get('selected_form_type', 'UNKNOWN'),
            'form_validation': {
                'is_valid': data.get('selected_form_validity', 'Unknown'),
                'decision_basis': data.get('form_type_decision_basis', ''),
                'source_page': data.get('form_type_source_page', '')
            },
            'priority_hierarchy': {
                'supplement_b_found': data.get('supplement_b_pages', []) != [],
                'section_3_found': data.get('section_3_pages', []) != [],
                'standard_i9_found': data.get('standard_i9_pages', []) != []
            }
        }
        
        self.log_decision('Bucket 3', f"Form Type: {data.get('selected_form_type', 'UNKNOWN')}", 
                         data.get('form_type_decision_basis', 'No basis provided'),
                         {'source_page': data.get('form_type_source_page', 'Unknown')})
    
    def log_bucket_4_work_authorization(self, score: int, data: Dict, reasons: List[str]):
        """Log Bucket 4 (Work Authorization) processing"""
        self.audit_log['buckets']['bucket_4_work_authorization'] = {
            'score': score,
            'max_score': 15,
            'data': data,
            'reasons': reasons,
            'work_auth_expiry': data.get('work_auth_expiry', 'NOT FOUND'),
            'work_auth_source': data.get('work_auth_source', 'NOT FOUND')
        }
        
        if data.get('work_auth_expiry'):
            self.log_decision('Bucket 4', 'Work Authorization Expiry Found', 
                             f"Date: {data.get('work_auth_expiry')}, Source: {data.get('work_auth_source')}")
        else:
            self.log_decision('Bucket 4', 'Work Authorization Expiry NOT Found', 
                             'No work authorization expiry date extracted')
    
    def log_bucket_5_document_tracking(self, score: int, data: Dict, reasons: List[str]):
        """Log Bucket 5 (Document Tracking) processing"""
        self.audit_log['buckets']['bucket_5_document_tracking'] = {
            'score': score,
            'max_score': 15,
            'data': data,
            'reasons': reasons,
            'documents_in_primary_set': data.get('documents', []),
            'document_count': data.get('document_count', 0),
            'supporting_documents_count': data.get('supporting_docs_count', 0),
            'attachment_status': data.get('attachment_status', 'UNKNOWN')
        }
        
        self.log_decision('Bucket 5', f"Documents in Primary Set: {data.get('document_count', 0)}", 
                         f"Documents: {data.get('documents', [])}",
                         {'supporting_docs': data.get('supporting_docs_count', 0)})
    
    def log_business_fields(self, business_fields: Dict):
        """Log all business fields extracted"""
        self.audit_log['business_fields'] = {
            'employee_info': {
                'first_name': business_fields.get('employee_first_name', ''),
                'middle_name': business_fields.get('employee_middle_name', ''),
                'last_name': business_fields.get('employee_last_name', ''),
                'date_of_birth': business_fields.get('employee_date_of_birth', ''),
                'ssn': business_fields.get('employee_ssn', ''),
                'citizenship_status': business_fields.get('citizenship_status', '')
            },
            'form_info': {
                'form_type_detected': business_fields.get('form_type_detected', ''),
                'form_type_decision_basis': business_fields.get('form_type_decision_basis', ''),
                'form_type_source_page': business_fields.get('form_type_source_page', ''),
                'primary_i9_set_type': business_fields.get('primary_i9_set_type', ''),
                'primary_i9_set_signature_date': business_fields.get('primary_i9_set_signature_date', ''),
                'primary_i9_set_pages': business_fields.get('primary_i9_set_pages', '')
            },
            'work_authorization': {
                'work_authorization_expiry_date': business_fields.get('work_authorization_expiry_date', ''),
                'work_authorization_source': business_fields.get('work_authorization_source', '')
            },
            'documents': {
                'documents_in_primary_set': business_fields.get('documents_in_primary_set', ''),
                'document_count_in_primary_set': business_fields.get('document_count_in_primary_set', 0),
                'supporting_documents_count': business_fields.get('supporting_documents_count', 0),
                'document_expiry_dates': business_fields.get('document_expiry_dates', ''),
                'expiry_date_matches': business_fields.get('expiry_date_matches', '')
            },
            'support_documents': {
                'matching_support_documents_attached': business_fields.get('matching_support_documents_attached', ''),
                'matching_support_documents_not_attached': business_fields.get('matching_support_documents_not_attached', '')
            },
            'signature_dates': {
                'employee_signature_date': business_fields.get('employee_signature_date', ''),
                'employer_signature_date': business_fields.get('employer_signature_date', '')
            }
        }
    
    def log_status_determination(self, status: str, citizenship: str, criteria_met: Dict, reasoning: str):
        """Log the final status determination with detailed reasoning"""
        self.audit_log['status_determination'] = {
            'final_status': status,
            'citizenship_type': citizenship,
            'is_us_citizen': 'us citizen' in citizenship.lower(),
            'criteria_checked': criteria_met,
            'reasoning': reasoning,
            'decision_tree': self._build_decision_tree(citizenship, criteria_met)
        }
        
        self.log_decision('Status Determination', f"Final Status: {status}", reasoning, 
                         {'citizenship': citizenship, 'criteria': criteria_met})
    
    def _build_decision_tree(self, citizenship: str, criteria_met: Dict) -> Dict:
        """Build a decision tree showing the logic flow"""
        is_us_citizen = 'us citizen' in citizenship.lower()
        
        tree = {
            'step_1_citizenship': {
                'check': 'Determine citizenship type',
                'result': 'US Citizen' if is_us_citizen else 'Non-Citizen',
                'value': citizenship
            }
        }
        
        if is_us_citizen:
            tree['step_2_us_citizen_criteria'] = {
                'check': 'US Citizen requires 4 criteria',
                'criteria': {
                    '1_first_name': criteria_met.get('has_first_name', False),
                    '2_last_name': criteria_met.get('has_last_name', False),
                    '3_date_of_birth': criteria_met.get('has_dob', False),
                    '4_all_docs_attached': criteria_met.get('all_docs_attached', False)
                },
                'all_met': all([
                    criteria_met.get('has_first_name', False),
                    criteria_met.get('has_last_name', False),
                    criteria_met.get('has_dob', False),
                    criteria_met.get('all_docs_attached', False)
                ])
            }
        else:
            tree['step_2_non_citizen_criteria'] = {
                'check': 'Non-Citizen requires 5 criteria',
                'criteria': {
                    '1_first_name': criteria_met.get('has_first_name', False),
                    '2_last_name': criteria_met.get('has_last_name', False),
                    '3_date_of_birth': criteria_met.get('has_dob', False),
                    '4_expiry_match': criteria_met.get('has_expiry_match', False),
                    '5_all_docs_attached': criteria_met.get('all_docs_attached', False)
                },
                'all_met': all([
                    criteria_met.get('has_first_name', False),
                    criteria_met.get('has_last_name', False),
                    criteria_met.get('has_dob', False),
                    criteria_met.get('has_expiry_match', False),
                    criteria_met.get('all_docs_attached', False)
                ])
            }
        
        return tree
    
    def add_summary(self, total_score: int, max_score: int = 100):
        """Add summary information"""
        self.audit_log['summary'] = {
            'total_score': total_score,
            'max_score': max_score,
            'percentage': f"{(total_score/max_score)*100:.1f}%",
            'final_status': self.audit_log.get('status_determination', {}).get('final_status', 'UNKNOWN'),
            'total_decisions_logged': len(self.audit_log['decisions'])
        }
    
    def save(self, output_path: str):
        """Save audit log to JSON file"""
        with open(output_path, 'w') as f:
            json.dump(self.audit_log, f, indent=2)
    
    def get_log(self) -> Dict:
        """Get the complete audit log"""
        return self.audit_log
