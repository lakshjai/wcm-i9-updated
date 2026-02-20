#!/usr/bin/env python3
import json

# Load catalog
with open('workdir/catalogs/Wu, Qianyi 9963.catalog.json') as f:
    data = json.load(f)

# Find page 19
page19 = [p for p in data['pages'] if p['page_number'] == 19][0]

print("="*80)
print("WU, QIANYI - PAGE 19 ANALYSIS")
print("="*80)
print(f"\nPage Title: {page19['page_title']}")
print(f"Page Type: {page19['page_type']}")

print("\n" + "="*80)
print("DOCUMENT TITLES EXTRACTED:")
print("="*80)

for k, v in sorted(page19['extracted_values'].items()):
    if 'document' in k.lower() and 'title' in k.lower() and v:
        print(f"\n{k}:")
        print(f"  → {v}")

print("\n" + "="*80)
print("ALL SECTION 3 FIELDS:")
print("="*80)

for k, v in sorted(page19['extracted_values'].items()):
    if 'section_3' in k.lower() and v and v != 'N/A':
        print(f"{k}: {v}")

print("\n" + "="*80)
print("ALL LIST A/B/C FIELDS:")
print("="*80)

for k, v in sorted(page19['extracted_values'].items()):
    if ('list_a' in k.lower() or 'list_b' in k.lower() or 'list_c' in k.lower()) and v and v != 'N/A':
        print(f"{k}: {v}")
