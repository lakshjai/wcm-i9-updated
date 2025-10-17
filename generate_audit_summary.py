#!/usr/bin/env python3
"""
Generate a comprehensive summary of all audit logs
"""
import json
from pathlib import Path
import pandas as pd

def main():
    audit_dir = Path("workdir/audit_logs")
    audit_files = sorted(audit_dir.glob("*_audit.json"))
    
    print("="*100)
    print("COMPREHENSIVE AUDIT SUMMARY - ALL FILES")
    print("="*100)
    
    for audit_file in audit_files:
        with open(audit_file) as f:
            audit = json.load(f)
        
        print(f"\n{'='*100}")
        print(f"FILE: {audit['filename']}")
        print(f"{'='*100}")
        
        # Summary
        summary = audit.get('summary', {})
        print(f"\n📊 SUMMARY:")
        print(f"   Total Score: {summary.get('total_score', 'N/A')}/{summary.get('max_score', 100)}")
        print(f"   Percentage: {summary.get('percentage', 'N/A')}")
        print(f"   Final Status: {summary.get('final_status', 'N/A')}")
        
        # Employee Info
        emp_info = audit.get('business_fields', {}).get('employee_info', {})
        print(f"\n👤 EMPLOYEE INFORMATION:")
        print(f"   Name: {emp_info.get('first_name', '')} {emp_info.get('middle_name', '')} {emp_info.get('last_name', '')}")
        print(f"   DOB: {emp_info.get('date_of_birth', 'N/A')}")
        print(f"   SSN: {emp_info.get('ssn', 'N/A')}")
        print(f"   Citizenship: {emp_info.get('citizenship_status', 'N/A')}")
        
        # Bucket Scores
        print(f"\n📈 BUCKET SCORES:")
        buckets = audit.get('buckets', {})
        for bucket_name, bucket_data in buckets.items():
            score = bucket_data.get('score', 0)
            max_score = bucket_data.get('max_score', 0)
            print(f"   {bucket_name}: {score}/{max_score}")
            reasons = bucket_data.get('reasons', [])
            if reasons:
                for reason in reasons:
                    if reason:
                        print(f"      - {reason}")
        
        # Form Type Decision
        form_info = audit.get('business_fields', {}).get('form_info', {})
        print(f"\n📝 FORM TYPE DECISION:")
        print(f"   Detected: {form_info.get('form_type_detected', 'N/A')}")
        print(f"   Decision Basis: {form_info.get('form_type_decision_basis', 'N/A')}")
        print(f"   Source Page: {form_info.get('form_type_source_page', 'N/A')}")
        print(f"   Primary Set Type: {form_info.get('primary_i9_set_type', 'N/A')}")
        print(f"   Primary Set Date: {form_info.get('primary_i9_set_signature_date', 'N/A')}")
        print(f"   Primary Set Pages: {form_info.get('primary_i9_set_pages', 'N/A')}")
        
        # Documents
        docs = audit.get('business_fields', {}).get('documents', {})
        print(f"\n📄 DOCUMENTS:")
        print(f"   In Primary Set: {docs.get('documents_in_primary_set', 'N/A')}")
        print(f"   Count: {docs.get('document_count_in_primary_set', 0)}")
        print(f"   Supporting Docs: {docs.get('supporting_documents_count', 0)}")
        print(f"   Document Expiry: {docs.get('document_expiry_dates', 'N/A')}")
        print(f"   Expiry Match: {docs.get('expiry_date_matches', 'N/A')}")
        
        # Support Documents
        support = audit.get('business_fields', {}).get('support_documents', {})
        attached = support.get('matching_support_documents_attached', '')
        not_attached = support.get('matching_support_documents_not_attached', '')
        
        print(f"\n📎 SUPPORT DOCUMENTS:")
        if attached:
            print(f"   ✅ Attached: {attached[:100]}...")
        else:
            print(f"   ✅ Attached: All documents attached")
        
        if not_attached:
            print(f"   ❌ Not Attached: {not_attached}")
        else:
            print(f"   ✅ All documents have supporting files")
        
        # Status Determination
        status_det = audit.get('status_determination', {})
        print(f"\n🎯 STATUS DETERMINATION:")
        print(f"   Final Status: {status_det.get('final_status', 'N/A')}")
        print(f"   Citizenship Type: {status_det.get('citizenship_type', 'N/A')}")
        print(f"   Is US Citizen: {status_det.get('is_us_citizen', 'N/A')}")
        
        # Criteria Checked
        criteria = status_det.get('criteria_checked', {})
        print(f"\n   Criteria Checked:")
        print(f"      Required Count: {criteria.get('required_criteria_count', 'N/A')}")
        print(f"      ✓ First Name: {criteria.get('has_first_name', False)} ({criteria.get('first_name_value', 'N/A')})")
        print(f"      ✓ Last Name: {criteria.get('has_last_name', False)} ({criteria.get('last_name_value', 'N/A')})")
        print(f"      ✓ Date of Birth: {criteria.get('has_dob', False)} ({criteria.get('dob_value', 'N/A')})")
        
        if not status_det.get('is_us_citizen', False):
            print(f"      ✓ Expiry Match: {criteria.get('has_expiry_match', False)}")
            print(f"         Detail: {criteria.get('expiry_match_detail', 'N/A')}")
        
        print(f"      ✓ All Docs Attached: {criteria.get('all_docs_attached', False)}")
        if not criteria.get('all_docs_attached', False):
            print(f"         Missing: {criteria.get('docs_not_attached', 'N/A')}")
        
        # Reasoning
        reasoning = status_det.get('reasoning', '')
        if reasoning:
            print(f"\n   📋 Detailed Reasoning:")
            for reason in reasoning.split(' | '):
                print(f"      {reason}")
        
        # Decision Tree
        decision_tree = status_det.get('decision_tree', {})
        if decision_tree:
            print(f"\n   🌳 Decision Tree:")
            for step_name, step_data in decision_tree.items():
                print(f"      {step_name}:")
                print(f"         Check: {step_data.get('check', 'N/A')}")
                if 'result' in step_data:
                    print(f"         Result: {step_data.get('result', 'N/A')}")
                if 'criteria' in step_data:
                    print(f"         Criteria Met:")
                    for crit_name, crit_value in step_data.get('criteria', {}).items():
                        print(f"            {crit_name}: {crit_value}")
                    print(f"         All Met: {step_data.get('all_met', 'N/A')}")
        
        # Key Decisions
        decisions = audit.get('decisions', [])
        if decisions:
            print(f"\n   📝 Key Decisions Logged: {len(decisions)}")
            # Show first 5 decisions
            for i, decision in enumerate(decisions[:5]):
                print(f"      {i+1}. [{decision.get('category', 'N/A')}] {decision.get('decision', 'N/A')}")
                print(f"         Reason: {decision.get('reason', 'N/A')}")
    
    print("\n" + "="*100)
    print("AUDIT SUMMARY COMPLETE")
    print("="*100)
    print(f"\nTotal Files Audited: {len(audit_files)}")
    print(f"Audit Logs Location: {audit_dir}")
    print("\nEach audit log contains:")
    print("  - Complete bucket-by-bucket scoring breakdown")
    print("  - Detailed field extraction results")
    print("  - Form type detection reasoning")
    print("  - Document tracking and validation")
    print("  - Status determination with full criteria check")
    print("  - Decision tree showing logic flow")
    print("  - All individual decisions made during processing")

if __name__ == "__main__":
    main()
