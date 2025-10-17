#!/usr/bin/env python3
"""
One-Shot Regeneration Script
Regenerates all outputs from existing catalog files:
- Rubric-based CSV results
- Audit logs (JSON)
- Field-level audits (JSON)
- Audit tables (CSV)
- Audit reports (TXT)
"""
import json
import csv
from pathlib import Path
from rubric_processor import I9RubricProcessor
from generate_field_audit import analyze_field_extraction
import pandas as pd

def main():
    print("="*100)
    print("🚀 ONE-SHOT REGENERATION - FROM EXISTING CATALOGS")
    print("="*100)
    
    # Setup paths
    catalog_dir = Path('workdir/catalogs')
    output_dir = Path('workdir')
    
    # Create output directories
    (output_dir / 'audit_logs').mkdir(parents=True, exist_ok=True)
    (output_dir / 'field_level_audits').mkdir(parents=True, exist_ok=True)
    (output_dir / 'field_audit_tables').mkdir(parents=True, exist_ok=True)
    (output_dir / 'field_audit_reports').mkdir(parents=True, exist_ok=True)
    
    # Initialize processor
    processor = I9RubricProcessor()
    
    # Get all catalog files
    catalog_files = sorted(catalog_dir.glob('*.catalog.json'))
    
    if not catalog_files:
        print("❌ No catalog files found in workdir/catalogs/")
        return
    
    print(f"\n📁 Found {len(catalog_files)} catalog files")
    print(f"📂 Output directory: {output_dir}")
    
    # Process all catalogs
    all_results = []
    
    print("\n" + "="*100)
    print("STEP 1: APPLYING RUBRIC & GENERATING AUDIT LOGS")
    print("="*100)
    
    for i, catalog_path in enumerate(catalog_files, 1):
        filename = catalog_path.stem.replace('.catalog', '')
        print(f"\n[{i}/{len(catalog_files)}] Processing: {filename}")
        
        try:
            # Process with rubric
            result = processor.process_catalog_file(str(catalog_path))
            all_results.append(result)
            
            print(f"  ✅ Rubric applied: {result.get('status')} ({result.get('total_score')}/100)")
            print(f"  ✅ Form type: {result.get('form_type_detected')}")
            print(f"  ✅ Audit log saved: workdir/audit_logs/{filename}_audit.json")
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
            continue
    
    # Save CSV results
    print("\n" + "="*100)
    print("STEP 2: GENERATING CSV RESULTS")
    print("="*100)
    
    csv_path = output_dir / 'rubric_based_results.csv'
    
    if all_results:
        # Get all field names from first result
        fieldnames = list(all_results[0].keys())
        
        with open(csv_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_results)
        
        print(f"✅ CSV saved: {csv_path}")
        print(f"   Rows: {len(all_results)}")
        print(f"   Columns: {len(fieldnames)}")
    
    # Generate field-level audits
    print("\n" + "="*100)
    print("STEP 3: GENERATING FIELD-LEVEL AUDITS")
    print("="*100)
    
    df = pd.DataFrame(all_results)
    
    for i, (idx, row) in enumerate(df.iterrows(), 1):
        filename = row['filename']
        print(f"\n[{i}/{len(df)}] Analyzing: {filename}")
        
        # Find matching catalog
        catalog_files_match = list(catalog_dir.glob(f"{filename}*.json"))
        if not catalog_files_match:
            print(f"  ⚠️  Catalog not found")
            continue
        
        catalog_path = catalog_files_match[0]
        
        try:
            # Generate field audit
            audit = analyze_field_extraction(str(catalog_path), row.to_dict())
            
            # Save JSON audit
            json_output = output_dir / 'field_level_audits' / f"{filename}_field_audit.json"
            with open(json_output, 'w') as f:
                json.dump(audit, f, indent=2)
            
            print(f"  ✅ Field audit saved: {json_output}")
            
            # Generate CSV table
            csv_output = output_dir / 'field_audit_tables' / f"{filename}_audit_table.csv"
            with open(csv_output, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Field Name', 'Value', 'Source', 'Rule Applied', 'Confidence Score'])
                
                for field_name, field_data in sorted(audit['fields'].items()):
                    writer.writerow([
                        field_name,
                        field_data['value'],
                        field_data['source'],
                        field_data['rule_applied'],
                        f"{field_data['confidence_score']:.2f}"
                    ])
            
            print(f"  ✅ CSV table saved: {csv_output}")
            
            # Generate text report
            txt_output = output_dir / 'field_audit_reports' / f"{filename}_audit_report.txt"
            lines = []
            lines.append("="*120)
            lines.append(f"FIELD-LEVEL AUDIT: {filename}")
            lines.append("="*120)
            lines.append("")
            lines.append(f"{'Field Name':<40} | {'Value':<30} | {'Confidence':<10}")
            lines.append("-"*120)
            
            for field_name, field_data in sorted(audit['fields'].items()):
                value = field_data['value'][:28] + '..' if len(field_data['value']) > 30 else field_data['value']
                confidence = f"{field_data['confidence_score']:.2f}"
                lines.append(f"{field_name:<40} | {value:<30} | {confidence:<10}")
            
            lines.append("")
            lines.append("="*120)
            lines.append("DETAILED FIELD INFORMATION")
            lines.append("="*120)
            lines.append("")
            
            for field_name, field_data in sorted(audit['fields'].items()):
                lines.append(f"\n{'='*120}")
                lines.append(f"FIELD: {field_name}")
                lines.append(f"{'='*120}")
                lines.append(f"Value:            {field_data['value']}")
                lines.append(f"Source:           {field_data['source']}")
                lines.append(f"Rule Applied:     {field_data['rule_applied']}")
                lines.append(f"Confidence Score: {field_data['confidence_score']:.2f}")
            
            with open(txt_output, 'w') as f:
                f.write('\n'.join(lines))
            
            print(f"  ✅ Text report saved: {txt_output}")
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
            continue
    
    # Generate summary
    print("\n" + "="*100)
    print("STEP 4: GENERATING SUMMARY")
    print("="*100)
    
    summary_lines = []
    summary_lines.append("="*100)
    summary_lines.append("REGENERATION SUMMARY")
    summary_lines.append("="*100)
    summary_lines.append("")
    summary_lines.append(f"📊 Files Processed: {len(all_results)}")
    summary_lines.append("")
    summary_lines.append("📁 Generated Files:")
    summary_lines.append(f"  • CSV Results: workdir/rubric_based_results.csv")
    summary_lines.append(f"  • Audit Logs (JSON): workdir/audit_logs/ ({len(all_results)} files)")
    summary_lines.append(f"  • Field Audits (JSON): workdir/field_level_audits/ ({len(all_results)} files)")
    summary_lines.append(f"  • Audit Tables (CSV): workdir/field_audit_tables/ ({len(all_results)} files)")
    summary_lines.append(f"  • Audit Reports (TXT): workdir/field_audit_reports/ ({len(all_results)} files)")
    summary_lines.append("")
    summary_lines.append("📈 Results Breakdown:")
    
    # Count statuses
    status_counts = {}
    form_type_counts = {}
    
    for result in all_results:
        status = result.get('status', 'UNKNOWN')
        form_type = result.get('form_type_detected', 'UNKNOWN')
        
        status_counts[status] = status_counts.get(status, 0) + 1
        form_type_counts[form_type] = form_type_counts.get(form_type, 0) + 1
    
    summary_lines.append("")
    summary_lines.append("  Status Distribution:")
    for status, count in sorted(status_counts.items()):
        pct = (count / len(all_results)) * 100
        summary_lines.append(f"    • {status}: {count} ({pct:.1f}%)")
    
    summary_lines.append("")
    summary_lines.append("  Form Type Distribution:")
    for form_type, count in sorted(form_type_counts.items()):
        pct = (count / len(all_results)) * 100
        summary_lines.append(f"    • {form_type}: {count} ({pct:.1f}%)")
    
    summary_lines.append("")
    summary_lines.append("="*100)
    summary_lines.append("✅ REGENERATION COMPLETE!")
    summary_lines.append("="*100)
    
    summary_text = '\n'.join(summary_lines)
    print("\n" + summary_text)
    
    # Save summary
    summary_path = output_dir / 'REGENERATION_SUMMARY.txt'
    with open(summary_path, 'w') as f:
        f.write(summary_text)
    
    print(f"\n📄 Summary saved: {summary_path}")
    
    print("\n" + "="*100)
    print("🎉 ALL FILES REGENERATED SUCCESSFULLY!")
    print("="*100)
    print("\nYou can now review:")
    print("  • workdir/rubric_based_results.csv - Main results")
    print("  • workdir/audit_logs/ - Detailed audit logs")
    print("  • workdir/field_level_audits/ - Field-by-field audits")
    print("  • workdir/field_audit_tables/ - Excel-friendly tables")
    print("  • workdir/field_audit_reports/ - Human-readable reports")

if __name__ == "__main__":
    main()
