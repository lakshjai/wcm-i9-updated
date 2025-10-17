#!/usr/bin/env python3
import json

with open('workdir/catalogs/Wu, Qianyi 9963.catalog.json') as f:
    data = json.load(f)

print("="*80)
print("SEARCHING FOR SECTION 3 DOCUMENTS IN WU, QIANYI")
print("="*80)

for page in data['pages']:
    page_num = page['page_number']
    page_title = page['page_title']
    ev = page['extracted_values']
    
    # Check for Section 3 document titles
    section3_docs = []
    for k, v in ev.items():
        if 'section_3' in k.lower() and 'document' in k.lower() and 'title' in k.lower():
            if v and v not in ['N/A', '', None]:
                section3_docs.append((k, v))
        elif 'reverification' in k.lower() and 'document' in k.lower() and 'title' in k.lower():
            if v and v not in ['N/A', '', '', None]:
                section3_docs.append((k, v))
    
    if section3_docs:
        print(f"\n📄 Page {page_num}: {page_title}")
        for field, value in section3_docs:
            print(f"   {field}: {value}")
        
        # Also show signature dates
        for k, v in ev.items():
            if 'signature' in k.lower() and 'date' in k.lower() and v and v not in ['N/A', '']:
                print(f"   {k}: {v}")

print("\n" + "="*80)
print("CHECKING LIST A DOCUMENTS (Section 2)")
print("="*80)

for page in data['pages']:
    page_num = page['page_number']
    page_title = page['page_title']
    ev = page['extracted_values']
    
    # Check for List A documents
    list_a_docs = []
    for k, v in ev.items():
        if 'list_a' in k.lower() and 'document' in k.lower() and 'title' in k.lower():
            if v and v not in ['N/A', '', None]:
                list_a_docs.append((k, v))
    
    if list_a_docs:
        print(f"\n📄 Page {page_num}: {page_title}")
        for field, value in list_a_docs:
            print(f"   {field}: {value}")
