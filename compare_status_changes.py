#!/usr/bin/env python3
"""
Compare status changes before and after implementing new citizenship-based criteria
"""
import pandas as pd

# Old expected statuses (before the change)
old_statuses = {
    'Delusa, Taylor 8781': 'COMPLETE_SUCCESS',
    'Wu, Qianyi 9963': 'COMPLETE_SUCCESS',
    'Balder_Pauline_0540': 'COMPLETE_SUCCESS',
    'Chulsoo_Lee_2109': 'COMPLETE_SUCCESS',
    'Abdelmassih, Mark 3842': 'PARTIAL_SUCCESS',
    'De Lima, Renan 8143': 'PARTIAL_SUCCESS',
    'Stilling, Joan 2466': 'PARTIAL_SUCCESS',
    'Abdelmalek_Ehab_3388': 'COMPLETE_SUCCESS',
    'Debek, Sonia 6684': 'COMPLETE_SUCCESS',
    'ABDELMASSIH, MARK FORM I-9 3842': 'PARTIAL_SUCCESS'
}

# Load new results
df = pd.read_csv('workdir/rubric_based_results.csv')

print("="*100)
print("STATUS COMPARISON - BEFORE vs AFTER NEW CITIZENSHIP-BASED CRITERIA")
print("="*100)

print("\n" + "="*100)
print("DETAILED COMPARISON")
print("="*100)

status_changes = []

for idx, row in df.iterrows():
    filename = row['filename']
    new_status = row['status']
    
    # Find matching old status
    old_status = None
    for key in old_statuses:
        if key in filename:
            old_status = old_statuses[key]
            break
    
    if old_status is None:
        old_status = 'UNKNOWN'
    
    status_changed = old_status != new_status
    
    print(f"\n📄 {filename}")
    print(f"   Citizenship: {row['citizenship_status']}")
    print(f"   Old Status: {old_status}")
    print(f"   New Status: {new_status}")
    
    if status_changed:
        print(f"   ⚠️  STATUS CHANGED!")
    else:
        print(f"   ✅ Status unchanged")
    
    # Show criteria details
    print(f"\n   Criteria Check:")
    print(f"   - First Name: {row['employee_first_name']}")
    print(f"   - Last Name: {row['employee_last_name']}")
    print(f"   - Date of Birth: {row['employee_date_of_birth']}")
    
    # Check citizenship type
    citizenship = str(row['citizenship_status']).lower()
    is_us_citizen = 'us citizen' in citizenship
    
    if is_us_citizen:
        print(f"   - US Citizen: YES")
        print(f"   - All Docs Attached: {row['matching_support_documents_not_attached']}")
        if str(row['matching_support_documents_not_attached']) in ['', 'nan']:
            print(f"     ✅ All documents attached")
        else:
            print(f"     ❌ Missing: {row['matching_support_documents_not_attached']}")
    else:
        print(f"   - US Citizen: NO (Non-Citizen)")
        print(f"   - Expiry Match: {row['expiry_date_matches']}")
        if 'MATCH:' in str(row['expiry_date_matches']):
            print(f"     ✅ Expiry matches")
        else:
            print(f"     ❌ Expiry does not match")
        print(f"   - All Docs Attached: {row['matching_support_documents_not_attached']}")
        if str(row['matching_support_documents_not_attached']) in ['', 'nan']:
            print(f"     ✅ All documents attached")
        else:
            print(f"     ❌ Missing: {row['matching_support_documents_not_attached']}")
    
    status_changes.append({
        'filename': filename,
        'citizenship': row['citizenship_status'],
        'old_status': old_status,
        'new_status': new_status,
        'changed': status_changed
    })

print("\n" + "="*100)
print("SUMMARY STATISTICS")
print("="*100)

# Count changes
changes_df = pd.DataFrame(status_changes)
total_changes = changes_df['changed'].sum()
complete_to_partial = len(changes_df[(changes_df['old_status'] == 'COMPLETE_SUCCESS') & 
                                      (changes_df['new_status'] == 'PARTIAL_SUCCESS')])
partial_to_complete = len(changes_df[(changes_df['old_status'] == 'PARTIAL_SUCCESS') & 
                                      (changes_df['new_status'] == 'COMPLETE_SUCCESS')])

print(f"\nTotal Files: {len(df)}")
print(f"Status Changes: {total_changes}")
print(f"  - COMPLETE → PARTIAL: {complete_to_partial}")
print(f"  - PARTIAL → COMPLETE: {partial_to_complete}")

print(f"\nOld Status Distribution:")
old_complete = sum(1 for s in old_statuses.values() if s == 'COMPLETE_SUCCESS')
old_partial = sum(1 for s in old_statuses.values() if s == 'PARTIAL_SUCCESS')
print(f"  - COMPLETE_SUCCESS: {old_complete} ({old_complete/len(old_statuses)*100:.0f}%)")
print(f"  - PARTIAL_SUCCESS: {old_partial} ({old_partial/len(old_statuses)*100:.0f}%)")

print(f"\nNew Status Distribution:")
new_complete = len(df[df['status'] == 'COMPLETE_SUCCESS'])
new_partial = len(df[df['status'] == 'PARTIAL_SUCCESS'])
print(f"  - COMPLETE_SUCCESS: {new_complete} ({new_complete/len(df)*100:.0f}%)")
print(f"  - PARTIAL_SUCCESS: {new_partial} ({new_partial/len(df)*100:.0f}%)")

print("\n" + "="*100)
print("CITIZENSHIP BREAKDOWN")
print("="*100)

us_citizens = df[df['citizenship_status'].str.contains('US Citizen', case=False, na=False)]
non_citizens = df[~df['citizenship_status'].str.contains('US Citizen', case=False, na=False)]

print(f"\nUS Citizens: {len(us_citizens)}")
for idx, row in us_citizens.iterrows():
    print(f"  - {row['filename']}: {row['status']}")

print(f"\nNon-Citizens: {len(non_citizens)}")
for idx, row in non_citizens.iterrows():
    print(f"  - {row['filename']}: {row['status']}")

print("\n" + "="*100)
