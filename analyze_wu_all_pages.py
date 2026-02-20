#!/usr/bin/env python3
import json

with open('workdir/catalogs/Wu, Qianyi 9963.catalog.json') as f:
    data = json.load(f)

print("="*80)
print("ALL PAGES IN WU, QIANYI CATALOG")
print("="*80)

for page in data['pages']:
    page_num = page['page_number']
    page_title = page['page_title']
    ev = page['extracted_values']
    
    print(f"\n{'='*80}")
    print(f"PAGE {page_num}: {page_title}")
    print('='*80)
    
    # Show ALL document-related fields
    doc_fields = {k: v for k, v in ev.items() if 'document' in k.lower() and v and v not in ['N/A', '']}
    if doc_fields:
        print("\nDocument Fields:")
        for k, v in sorted(doc_fields.items()):
            print(f"  {k}: {v}")
    
    # Show signature dates
    sig_fields = {k: v for k, v in ev.items() if 'signature' in k.lower() and 'date' in k.lower() and v and v not in ['N/A', '']}
    if sig_fields:
        print("\nSignature Dates:")
        for k, v in sorted(sig_fields.items()):
            print(f"  {k}: {v}")
