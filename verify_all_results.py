#!/usr/bin/env python3
"""
Comprehensive verification of rubric-based results against requirements
"""
import pandas as pd
import json

def main():
    # Load results
    df = pd.read_csv('workdir/rubric_based_results.csv')
    
    print("="*100)
    print("COMPREHENSIVE VERIFICATION OF ALL RESULTS")
    print("="*100)
    
    # Check 1: Column Alignment
    print("\n" + "="*100)
    print("CHECK 1: COLUMN ALIGNMENT WITH REQUIREMENTS")
    print("="*100)
    
    required_cols = pd.read_csv('rubric_based_results_description.csv', nrows=1).columns.tolist()
    actual_cols = df.columns.tolist()
    
    if required_cols == actual_cols:
        print("✅ ALL COLUMNS MATCH REQUIREMENTS (43 columns)")
    else:
        print("❌ COLUMN MISMATCH")
        missing = set(required_cols) - set(actual_cols)
        extra = set(actual_cols) - set(required_cols)
        if missing:
            print(f"  Missing: {missing}")
        if extra:
            print(f"  Extra: {extra}")
    
    # Check 2: Form Type Detection
    print("\n" + "="*100)
    print("CHECK 2: FORM TYPE DETECTION")
    print("="*100)
    
    for idx, row in df.iterrows():
        print(f"\n📄 {row['filename']}")
        print(f"   Form Type: {row['form_type_detected']}")
        print(f"   Decision Basis: {row['form_type_decision_basis']}")
        print(f"   Source Page: {row['form_type_source_page']}")
    
    # Check 3: Document Extraction by Form Type
    print("\n" + "="*100)
    print("CHECK 3: DOCUMENT EXTRACTION (BY FORM TYPE)")
    print("="*100)
    
    for idx, row in df.iterrows():
        print(f"\n📄 {row['filename']}")
        print(f"   Form Type: {row['form_type_detected']}")
        print(f"   Documents: {row['documents_in_primary_set']}")
        print(f"   Count: {row['document_count_in_primary_set']}")
        
        # Verify Section 3 cases have Section 3 documents only
        if row['form_type_detected'] == 're-verification':
            docs = str(row['documents_in_primary_set'])
            if 'List A' in docs or 'List B' in docs or 'List C' in docs:
                print("   ⚠️  WARNING: Section 3 case has Section 2 document references")
            else:
                print("   ✅ Correctly extracting Section 3 documents only")
    
    # Check 4: Work Authorization Matching
    print("\n" + "="*100)
    print("CHECK 4: WORK AUTHORIZATION DATE MATCHING")
    print("="*100)
    
    for idx, row in df.iterrows():
        print(f"\n📄 {row['filename']}")
        print(f"   Work Auth Expiry: {row['work_authorization_expiry_date']}")
        print(f"   Document Expiry Dates: {row['document_expiry_dates']}")
        print(f"   Match Status: {row['expiry_date_matches']}")
        
        # Check if exact match
        if 'MATCH:' in str(row['expiry_date_matches']):
            print("   ✅ Exact date match found")
        elif row['work_authorization_expiry_date'] and row['document_expiry_dates']:
            print("   ⚠️  No exact match")
    
    # Check 5: Supporting Documents
    print("\n" + "="*100)
    print("CHECK 5: SUPPORTING DOCUMENTS VALIDATION")
    print("="*100)
    
    for idx, row in df.iterrows():
        print(f"\n📄 {row['filename']}")
        print(f"   Documents in Primary Set: {row['document_count_in_primary_set']}")
        print(f"   Supporting Docs Count: {row['supporting_documents_count']}")
        
        attached = str(row['matching_support_documents_attached'])
        not_attached = str(row['matching_support_documents_not_attached'])
        
        if attached and attached != 'nan' and attached != '':
            print(f"   ✅ Attached: {attached[:100]}...")
        if not_attached and not_attached != 'nan' and not_attached != '':
            print(f"   ⚠️  Not Attached: {not_attached}")
    
    # Check 6: Status Classification
    print("\n" + "="*100)
    print("CHECK 6: STATUS CLASSIFICATION")
    print("="*100)
    
    status_counts = df['status'].value_counts()
    print(f"\nStatus Distribution:")
    for status, count in status_counts.items():
        print(f"   {status}: {count}")
    
    # Check 7: Scoring Summary
    print("\n" + "="*100)
    print("CHECK 7: SCORING SUMMARY")
    print("="*100)
    
    for idx, row in df.iterrows():
        print(f"\n📄 {row['filename']}")
        print(f"   Total Score: {row['total_score']}")
        print(f"   Bucket 1 (Personal Data): {row['bucket_1_personal_data_score']}")
        print(f"   Bucket 2 (I-9 Detection): {row['bucket_2_i9_detection_score']}")
        print(f"   Bucket 3 (Business Rules): {row['bucket_3_business_rules_score']}")
        print(f"   Bucket 4 (Work Auth): {row['bucket_4_work_authorization_score']}")
        print(f"   Bucket 5 (Doc Tracking): {row['bucket_5_document_tracking_score']}")
    
    # Check 8: Special Case - Wu, Qianyi
    print("\n" + "="*100)
    print("CHECK 8: SPECIAL VERIFICATION - WU, QIANYI")
    print("="*100)
    
    wu = df[df['filename'].str.contains('Wu', case=False, na=False)]
    if not wu.empty:
        wu = wu.iloc[0]
        print(f"\n✅ Wu, Qianyi Verification:")
        print(f"   Form Type: {wu['form_type_detected']} (Expected: re-verification)")
        print(f"   Documents: {wu['documents_in_primary_set']}")
        print(f"   Document Count: {wu['document_count_in_primary_set']} (Expected: 1)")
        print(f"   Work Auth Expiry: {wu['work_authorization_expiry_date']}")
        print(f"   Document Expiry: {wu['document_expiry_dates']}")
        print(f"   Match: {wu['expiry_date_matches']}")
        
        # Verify requirements
        checks = []
        checks.append(("Form Type = re-verification", wu['form_type_detected'] == 're-verification'))
        checks.append(("Only 1 document extracted", wu['document_count_in_primary_set'] == 1))
        checks.append(("Document is Section 3 doc", 'EAD' in str(wu['documents_in_primary_set'])))
        checks.append(("Exact date match", 'MATCH:' in str(wu['expiry_date_matches'])))
        
        print(f"\n   Requirement Checks:")
        for check_name, passed in checks:
            status = "✅" if passed else "❌"
            print(f"   {status} {check_name}")
    
    # Summary
    print("\n" + "="*100)
    print("VERIFICATION COMPLETE")
    print("="*100)
    print(f"\n✅ Total Files Processed: {len(df)}")
    print(f"✅ Complete Success: {len(df[df['status'] == 'COMPLETE_SUCCESS'])}")
    print(f"⚠️  Partial Success: {len(df[df['status'] == 'PARTIAL_SUCCESS'])}")
    
    # Check for any errors
    errors = df[df['status'] == 'ERROR']
    if not errors.empty:
        print(f"❌ Errors: {len(errors)}")
    
    print("\n" + "="*100)

if __name__ == "__main__":
    main()
