#!/usr/bin/env python3
"""
Generate human-readable table format for field-level audits
"""
import json
from pathlib import Path
import csv

def generate_audit_table(audit_file: Path, output_file: Path):
    """Generate a table format for the audit"""
    with open(audit_file) as f:
        audit = json.load(f)
    
    filename = audit['filename']
    
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Header
        writer.writerow(['Field Name', 'Value', 'Source', 'Rule Applied', 'Confidence Score'])
        
        # Write each field
        for field_name, field_data in sorted(audit['fields'].items()):
            writer.writerow([
                field_name,
                field_data['value'],
                field_data['source'],
                field_data['rule_applied'],
                f"{field_data['confidence_score']:.2f}"
            ])

def generate_text_report(audit_file: Path) -> str:
    """Generate a text report for the audit"""
    with open(audit_file) as f:
        audit = json.load(f)
    
    filename = audit['filename']
    
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
    
    return '\n'.join(lines)

def main():
    audit_dir = Path('workdir/field_level_audits')
    output_dir = Path('workdir/field_audit_tables')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    text_output_dir = Path('workdir/field_audit_reports')
    text_output_dir.mkdir(parents=True, exist_ok=True)
    
    print("="*100)
    print("GENERATING FIELD AUDIT TABLES AND REPORTS")
    print("="*100)
    
    audit_files = sorted(audit_dir.glob('*_field_audit.json'))
    
    for audit_file in audit_files:
        filename = audit_file.stem.replace('_field_audit', '')
        
        # Generate CSV table
        csv_output = output_dir / f"{filename}_audit_table.csv"
        generate_audit_table(audit_file, csv_output)
        print(f"\n✅ CSV Table: {csv_output}")
        
        # Generate text report
        text_output = text_output_dir / f"{filename}_audit_report.txt"
        report = generate_text_report(audit_file)
        with open(text_output, 'w') as f:
            f.write(report)
        print(f"✅ Text Report: {text_output}")
    
    print("\n" + "="*100)
    print("GENERATION COMPLETE")
    print("="*100)
    print(f"\nGenerated {len(audit_files)} audit tables and reports")
    print(f"\nCSV Tables: {output_dir}")
    print(f"Text Reports: {text_output_dir}")
    print("\nEach file shows for every CSV field:")
    print("  1. Field Name")
    print("  2. Value")
    print("  3. Source (where it came from)")
    print("  4. Rule Applied (why it was selected)")
    print("  5. Confidence Score (0.00 to 1.00)")

if __name__ == "__main__":
    main()
