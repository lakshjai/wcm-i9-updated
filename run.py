#!/usr/bin/env python3
"""
I-9 Processing System - Main Entry Point
Single script to run the entire I-9 processing pipeline
All configuration is controlled via .env file
"""
import sys
import time
from pathlib import Path
from typing import List, Dict, Any
import json

from config import config
from rubric_processor import I9RubricProcessor


def print_banner():
    """Print startup banner."""
    print()
    print("╔" + "═" * 88 + "╗")
    print("║" + " " * 88 + "║")
    print("║" + "  I-9 PROCESSING SYSTEM".center(88) + "║")
    print("║" + "  Automated I-9 Form Analysis & Compliance Verification".center(88) + "║")
    print("║" + " " * 88 + "║")
    print("╚" + "═" * 88 + "╝")
    print()


def validate_configuration() -> bool:
    """
    Validate configuration and return True if valid.
    Prints errors and returns False if invalid.
    """
    errors = config.validate()
    if errors:
        print("❌ CONFIGURATION ERRORS:")
        print()
        for error in errors:
            print(f"   • {error}")
        print()
        print("Please fix the errors in your .env file and try again.")
        return False
    return True


def setup_directories():
    """Create all required output directories."""
    print("📁 Setting up directories...")
    config.create_directories()
    print(f"   ✓ Created: {config.OUTPUT_FOLDER}")
    print(f"   ✓ Created: {config.catalogs_path}")
    print(f"   ✓ Created: {config.audit_logs_path}")
    print(f"   ✓ Created: {config.field_audits_path}")
    print(f"   ✓ Created: {config.field_audit_tables_path}")
    print(f"   ✓ Created: {config.field_audit_reports_path}")
    print()


def get_pdf_files() -> List[Path]:
    """Get list of PDF files from input folder."""
    pdf_files = list(config.INPUT_FOLDER.glob("*.pdf"))
    return sorted(pdf_files)


def get_catalog_files() -> List[Path]:
    """Get list of existing catalog files."""
    catalog_files = list(config.catalogs_path.glob("*.catalog.json"))
    return sorted(catalog_files)


def should_generate_catalog(pdf_file: Path) -> bool:
    """
    Determine if catalog should be generated for a PDF file.
    Based on CATALOG_MODE configuration.
    """
    catalog_name = pdf_file.stem + ".catalog.json"
    catalog_path = config.catalogs_path / catalog_name
    
    if config.CATALOG_MODE == "force_regenerate":
        return True
    elif config.CATALOG_MODE == "skip":
        return False
    elif config.CATALOG_MODE == "use_existing":
        return not catalog_path.exists()
    else:
        # Default to use_existing
        return not catalog_path.exists()


def generate_catalogs(pdf_files: List[Path]) -> int:
    """
    Generate catalog files from PDFs.
    Returns number of catalogs generated.
    """
    if not config.GENERATE_CATALOGS:
        print("⏭️  Skipping catalog generation (GENERATE_CATALOGS=false)")
        print()
        return 0
    
    print("=" * 90)
    print("STEP 1: CATALOG GENERATION")
    print("=" * 90)
    print()
    
    if config.CATALOG_MODE == "skip":
        print("⏭️  Skipping catalog generation (CATALOG_MODE=skip)")
        print("   Using existing catalogs only")
        print()
        return 0
    
    # Determine which PDFs need catalog generation
    pdfs_to_process = []
    for pdf_file in pdf_files:
        if should_generate_catalog(pdf_file):
            pdfs_to_process.append(pdf_file)
    
    if not pdfs_to_process:
        print("✓ All catalogs already exist, no generation needed")
        print()
        return 0
    
    print(f"📄 Found {len(pdf_files)} PDF files")
    print(f"🔄 Need to generate {len(pdfs_to_process)} catalogs")
    print()
    
    if config.CATALOG_MODE == "force_regenerate":
        print("⚠️  FORCE REGENERATE mode - regenerating ALL catalogs")
        print()
    
    # Run catalog generation as subprocess
    import subprocess
    
    print("🔄 Running catalog generation...")
    print()
    
    try:
        result = subprocess.run(
            [sys.executable, "regenerate_catalogs.py"],
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True
        )
        
        # Print output
        if result.stdout:
            print(result.stdout)
        
        if result.returncode != 0:
            print("⚠️  Catalog generation encountered errors")
            if result.stderr:
                print(result.stderr)
        
        # Count generated catalogs
        generated_count = len(list(config.catalogs_path.glob("*.catalog.json")))
        
    except Exception as e:
        print(f"❌ Error running catalog generation: {e}")
        generated_count = 0
    
    print()
    print(f"✅ Generated {generated_count} catalog(s)")
    print()
    
    return generated_count


def process_rubrics(catalog_files: List[Path]) -> List[Dict[str, Any]]:
    """
    Process rubrics for all catalog files.
    Returns list of results.
    """
    if not config.PROCESS_RUBRICS:
        print("⏭️  Skipping rubric processing (PROCESS_RUBRICS=false)")
        print()
        return []
    
    print("=" * 90)
    print("STEP 2: RUBRIC PROCESSING")
    print("=" * 90)
    print()
    
    if not catalog_files:
        print("❌ No catalog files found to process")
        print()
        return []
    
    print(f"📊 Processing {len(catalog_files)} catalog files...")
    print()
    
    processor = I9RubricProcessor()
    results = []
    
    for i, catalog_file in enumerate(catalog_files, 1):
        filename = catalog_file.stem.replace('.catalog', '')
        print(f"[{i}/{len(catalog_files)}] Processing: {filename}")
        
        try:
            result = processor.process_catalog_file(str(catalog_file))
            results.append(result)
            
            status = result.get('status', 'UNKNOWN')
            form_type = result.get('form_type_detected', 'N/A')
            score = result.get('total_score', 0)
            
            print(f"  ✅ {status} - Form: {form_type} - Score: {score}/100")
            
            # Save audit log
            audit_filename = f"{filename}_audit.json"
            audit_path = config.audit_logs_path / audit_filename
            with open(audit_path, 'w') as f:
                json.dump(result, f, indent=2)
            
        except Exception as e:
            print(f"  ❌ Error: {str(e)}")
            results.append({
                'filename': filename,
                'status': 'ERROR',
                'error': str(e)
            })
    
    print()
    print(f"✅ Processed {len(results)} files")
    print()
    
    return results


def generate_csv(results: List[Dict[str, Any]]):
    """Generate CSV output from results."""
    if not config.GENERATE_CSV:
        print("⏭️  Skipping CSV generation (GENERATE_CSV=false)")
        print()
        return
    
    print("=" * 90)
    print("STEP 3: CSV GENERATION")
    print("=" * 90)
    print()
    
    if not results:
        print("❌ No results to generate CSV")
        print()
        return
    
    import csv
    
    # Get all possible fields from results
    all_fields = set()
    for result in results:
        all_fields.update(result.keys())
    
    # Define field order (common fields first)
    priority_fields = [
        'filename', 'status', 'employee_first_name', 'employee_middle_name', 
        'employee_last_name', 'employee_date_of_birth', 'employee_ssn',
        'citizenship_status', 'form_type_detected', 'form_type_decision_basis',
        'form_type_source_page', 'total_score'
    ]
    
    # Remaining fields in alphabetical order
    remaining_fields = sorted(all_fields - set(priority_fields))
    fieldnames = [f for f in priority_fields if f in all_fields] + remaining_fields
    
    csv_path = config.csv_output_path
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    print(f"✅ CSV saved: {csv_path}")
    print(f"   Rows: {len(results)}")
    print(f"   Columns: {len(fieldnames)}")
    print()


def generate_field_audits(catalog_files: List[Path]):
    """Generate field-level audits for all catalogs."""
    if not config.GENERATE_FIELD_AUDITS:
        print("⏭️  Skipping field audit generation (GENERATE_FIELD_AUDITS=false)")
        print()
        return
    
    print("=" * 90)
    print("STEP 4: FIELD-LEVEL AUDITS")
    print("=" * 90)
    print()
    
    if not catalog_files:
        print("❌ No catalog files found for field audits")
        print()
        return
    
    # Import here to avoid loading if not needed
    try:
        from generate_field_audit import analyze_field_extraction
        from generate_field_audit_table import save_audit_as_csv, save_audit_as_text
    except ImportError as e:
        print(f"⚠️  Field audit modules not available: {e}")
        print("   Skipping field audit generation")
        print()
        return
    
    # Load CSV results to get field values
    csv_path = config.csv_output_path
    if not csv_path.exists():
        print("⚠️  CSV file not found, cannot generate field audits")
        print("   Run with GENERATE_CSV=true first")
        print()
        return
    
    import csv as csv_module
    with open(csv_path, 'r') as f:
        csv_reader = csv_module.DictReader(f)
        csv_rows = {row['filename']: row for row in csv_reader}
    
    for i, catalog_file in enumerate(catalog_files, 1):
        filename = catalog_file.stem.replace('.catalog', '')
        print(f"[{i}/{len(catalog_files)}] Analyzing: {filename}")
        
        try:
            # Get corresponding CSV row
            csv_row = csv_rows.get(filename, {})
            if not csv_row:
                print(f"  ⚠️  No CSV data found for {filename}")
                continue
            
            # Generate audit
            audit_result = analyze_field_extraction(str(catalog_file), csv_row)
            
            # Save JSON audit
            audit_json_path = config.field_audits_path / f"{filename}_field_audit.json"
            with open(audit_json_path, 'w') as f:
                json.dump(audit_result, f, indent=2)
            
            # Save CSV table
            csv_table_path = config.field_audit_tables_path / f"{filename}_audit_table.csv"
            save_audit_as_csv(audit_result, str(csv_table_path))
            
            # Save text report
            report_path = config.field_audit_reports_path / f"{filename}_audit_report.txt"
            save_audit_as_text(audit_result, str(report_path))
            
            print(f"  ✅ Saved: JSON, CSV, and text report")
            
        except Exception as e:
            print(f"  ❌ Error: {str(e)}")
            import traceback
            traceback.print_exc()
    
    print()
    print(f"✅ Generated field audits for {len(catalog_files)} files")
    print()


def generate_summary(results: List[Dict[str, Any]], catalog_count: int):
    """Generate summary report."""
    if not config.GENERATE_SUMMARY:
        print("⏭️  Skipping summary generation (GENERATE_SUMMARY=false)")
        print()
        return
    
    print("=" * 90)
    print("STEP 5: SUMMARY GENERATION")
    print("=" * 90)
    print()
    
    # Calculate statistics
    total_files = len(results)
    
    status_counts = {}
    form_type_counts = {}
    scores = []
    
    for result in results:
        status = result.get('status', 'UNKNOWN')
        form_type = result.get('form_type_detected', 'unknown')
        score = result.get('total_score', 0)
        
        status_counts[status] = status_counts.get(status, 0) + 1
        form_type_counts[form_type] = form_type_counts.get(form_type, 0) + 1
        
        try:
            scores.append(int(score))
        except:
            pass
    
    avg_score = sum(scores) / len(scores) if scores else 0
    
    # Generate summary text
    summary_lines = []
    summary_lines.append("=" * 90)
    summary_lines.append("PROCESSING SUMMARY")
    summary_lines.append("=" * 90)
    summary_lines.append("")
    summary_lines.append(f"📊 Files Processed: {total_files}")
    summary_lines.append(f"📁 Catalogs Generated: {catalog_count}")
    summary_lines.append("")
    
    summary_lines.append("📈 Results Breakdown:")
    summary_lines.append("")
    summary_lines.append("  Status Distribution:")
    for status, count in sorted(status_counts.items()):
        pct = (count / total_files * 100) if total_files > 0 else 0
        summary_lines.append(f"    • {status}: {count} ({pct:.1f}%)")
    summary_lines.append("")
    
    summary_lines.append("  Form Type Distribution:")
    for form_type, count in sorted(form_type_counts.items()):
        pct = (count / total_files * 100) if total_files > 0 else 0
        summary_lines.append(f"    • {form_type}: {count} ({pct:.1f}%)")
    summary_lines.append("")
    
    if scores:
        summary_lines.append(f"  Average Score: {avg_score:.1f}/100")
        summary_lines.append(f"  Min Score: {min(scores)}/100")
        summary_lines.append(f"  Max Score: {max(scores)}/100")
        summary_lines.append("")
    
    summary_lines.append("=" * 90)
    summary_lines.append("GENERATED FILES")
    summary_lines.append("=" * 90)
    summary_lines.append("")
    summary_lines.append(f"📄 Main Results:")
    summary_lines.append(f"  • {config.csv_output_path}")
    summary_lines.append("")
    summary_lines.append(f"📁 Detailed Outputs:")
    summary_lines.append(f"  • Catalogs: {config.catalogs_path}/ ({len(list(config.catalogs_path.glob('*.json')))} files)")
    summary_lines.append(f"  • Audit Logs: {config.audit_logs_path}/ ({len(list(config.audit_logs_path.glob('*.json')))} files)")
    summary_lines.append(f"  • Field Audits: {config.field_audits_path}/ ({len(list(config.field_audits_path.glob('*.json')))} files)")
    summary_lines.append(f"  • Audit Tables: {config.field_audit_tables_path}/ ({len(list(config.field_audit_tables_path.glob('*.csv')))} files)")
    summary_lines.append(f"  • Audit Reports: {config.field_audit_reports_path}/ ({len(list(config.field_audit_reports_path.glob('*.txt')))} files)")
    summary_lines.append("")
    summary_lines.append("=" * 90)
    
    summary_text = "\n".join(summary_lines)
    
    # Print to console
    print(summary_text)
    print()
    
    # Save to file
    summary_path = config.summary_path
    with open(summary_path, 'w') as f:
        f.write(summary_text)
    
    print(f"📄 Summary saved: {summary_path}")
    print()


def main():
    """Main entry point."""
    start_time = time.time()
    
    # Print banner
    print_banner()
    
    # Print configuration
    config.print_config()
    
    # Validate configuration
    if not validate_configuration():
        sys.exit(1)
    
    print("✅ Configuration validated successfully")
    print()
    
    # Setup directories
    setup_directories()
    
    # Get PDF files
    pdf_files = get_pdf_files()
    print(f"📄 Found {len(pdf_files)} PDF file(s) in {config.INPUT_FOLDER}")
    print()
    
    # Step 1: Generate catalogs
    catalog_count = generate_catalogs(pdf_files)
    
    # Get catalog files (after generation)
    catalog_files = get_catalog_files()
    print(f"📚 Found {len(catalog_files)} catalog file(s)")
    print()
    
    if not catalog_files:
        print("❌ No catalog files available. Cannot proceed.")
        print("   Please check:")
        print("   1. PDF files exist in INPUT_FOLDER")
        print("   2. CATALOG_MODE is not set to 'skip'")
        print("   3. Catalog generation completed successfully")
        sys.exit(1)
    
    # Step 2: Process rubrics
    results = process_rubrics(catalog_files)
    
    # Step 3: Generate CSV
    generate_csv(results)
    
    # Step 4: Generate field audits
    generate_field_audits(catalog_files)
    
    # Step 5: Generate summary
    generate_summary(results, catalog_count)
    
    # Final summary
    elapsed_time = time.time() - start_time
    print("=" * 90)
    print("✅ PROCESSING COMPLETE!")
    print("=" * 90)
    print()
    print(f"⏱️  Total time: {elapsed_time:.1f} seconds")
    print()
    print("📊 Review your results:")
    print(f"   • Main CSV: {config.csv_output_path}")
    print(f"   • Summary: {config.summary_path}")
    print(f"   • All outputs: {config.OUTPUT_FOLDER}/")
    print()
    print("=" * 90)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print()
        print()
        print("⚠️  Processing interrupted by user")
        sys.exit(130)
    except Exception as e:
        print()
        print("=" * 90)
        print("❌ FATAL ERROR")
        print("=" * 90)
        print()
        print(f"Error: {str(e)}")
        print()
        import traceback
        traceback.print_exc()
        sys.exit(1)
