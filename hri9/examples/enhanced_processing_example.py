#!/usr/bin/env python3
"""
Enhanced I-9 Processing Example

This script demonstrates how to use the enhanced I-9 processor with
business rules and validation for processing I-9 documents.
"""

import sys
import os
from pathlib import Path

# Add the parent directory to the path so we can import hri9 modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from hri9.core.enhanced_processor import EnhancedI9Processor, ProcessingPipeline
from hri9.api.gemini_client import GeminiClient
from hri9.catalog.cache import CatalogCache
from hri9.utils.logging_config import logger


def example_single_document_processing():
    """Example of processing a single I-9 document with enhanced rules"""
    
    print("=== Enhanced I-9 Processing Example ===\n")
    
    # Initialize the enhanced processor
    print("1. Initializing Enhanced I-9 Processor...")
    try:
        gemini_client = GeminiClient()
        catalog_cache = CatalogCache(max_documents=100)
        
        processor = EnhancedI9Processor(
            gemini_client=gemini_client,
            catalog_cache=catalog_cache
        )
        
        print("✓ Enhanced processor initialized successfully")
        
        # Display processor statistics
        stats = processor.get_processing_statistics()
        print(f"✓ Rule engine: {stats['rule_engine_stats']['total_rules_registered']} rules registered")
        print(f"✓ Validation framework: {stats['validation_framework_stats']['field_validators']} field validators")
        
    except Exception as e:
        print(f"✗ Failed to initialize processor: {e}")
        return
    
    # Example PDF path (you would replace this with an actual PDF path)
    pdf_path = Path("/path/to/your/i9_document.pdf")
    
    if not pdf_path.exists():
        print(f"\n2. Example PDF Processing (simulated - {pdf_path.name} not found)")
        print("   In a real scenario, you would provide a valid PDF path:")
        print(f"   processor.process_pdf(Path('/actual/path/to/document.pdf'), 'John Doe')")
        
        # Show what the processing result would look like
        print("\n   Expected Processing Result Structure:")
        print("   - status: ProcessingStatus (COMPLETE_SUCCESS, PARTIAL_SUCCESS, etc.)")
        print("   - primary_i9_data: I9FormData with extracted information")
        print("   - scenario_results: List of scenario processing results")
        print("   - validation_success_rate: Percentage of validations passed")
        print("   - critical_issues: Number of critical validation failures")
        print("   - form_type_selected: Type of I-9 form selected")
        print("   - selection_reason: Reason for form selection")
        print("   - notes: Additional processing notes")
        
        return
    
    print(f"\n2. Processing PDF: {pdf_path.name}")
    
    try:
        # Process the document
        result = processor.process_pdf(pdf_path, "John Doe")
        
        print(f"✓ Processing completed with status: {result.status.value}")
        print(f"✓ Validation success rate: {result.validation_success_rate:.1f}%")
        
        if result.primary_i9_data:
            form = result.primary_i9_data
            print(f"✓ Primary form found: {form.form_type.value} on page {form.page_number}")
            print(f"  - Employee: {form.first_name} {form.last_name}")
            print(f"  - Citizenship: {form.citizenship_status.value}")
            print(f"  - Signature present: {form.employee_signature_present}")
            
            if form.is_non_citizen():
                print(f"  - Work authorization until: {form.authorized_to_work_until}")
        
        # Display scenario results
        if result.scenario_results:
            print(f"\n✓ Scenario Processing Results:")
            for scenario in result.scenario_results:
                print(f"  - {scenario.scenario_name}: {scenario.status.value}")
                if scenario.validation_results:
                    passed = sum(1 for v in scenario.validation_results if v.is_valid)
                    total = len(scenario.validation_results)
                    print(f"    Validations: {passed}/{total} passed")
        
        # Display critical issues if any
        if result.critical_issues > 0:
            print(f"\n⚠ Critical Issues Found: {result.critical_issues}")
        
        if result.notes:
            print(f"\nNotes: {result.notes}")
            
    except Exception as e:
        print(f"✗ Processing failed: {e}")


def example_batch_processing():
    """Example of batch processing multiple I-9 documents"""
    
    print("\n=== Batch Processing Example ===\n")
    
    # Initialize processor and pipeline
    try:
        processor = EnhancedI9Processor()
        pipeline = ProcessingPipeline(processor)
        
        print("✓ Processing pipeline initialized")
        
    except Exception as e:
        print(f"✗ Failed to initialize pipeline: {e}")
        return
    
    # Example PDF paths (you would replace these with actual paths)
    pdf_paths = [
        Path("/path/to/employee1_documents.pdf"),
        Path("/path/to/employee2_documents.pdf"),
        Path("/path/to/employee3_documents.pdf")
    ]
    
    employee_names = ["Alice Johnson", "Bob Smith", "Carol Davis"]
    
    print("Processing batch of documents (simulated)...")
    
    # In a real scenario, you would call:
    # results = pipeline.process_batch(pdf_paths, employee_names)
    
    print("Expected batch processing workflow:")
    print("1. Each PDF is processed through the enhanced pipeline")
    print("2. Business rules and scenarios are applied")
    print("3. Comprehensive validation is performed")
    print("4. Results are collected and statistics generated")
    
    # Show expected statistics
    print("\nExpected Batch Statistics:")
    print("- total_processed: Number of documents processed")
    print("- successful: Documents with COMPLETE_SUCCESS status")
    print("- partial_success: Documents with validation issues but processable")
    print("- errors: Documents that failed processing")
    print("- no_i9_found: Documents without I-9 forms")
    print("- success_rate: Percentage of successful processing")
    print("- error_rate: Percentage of processing errors")


def example_business_rules_scenarios():
    """Example showing the 5 business rule scenarios"""
    
    print("\n=== Business Rules Scenarios ===\n")
    
    scenarios = {
        "Scenario 1": {
            "description": "Single I-9 form processing",
            "rules": [
                "Extract all details from Section 1 (name, DOB, citizenship)",
                "Get supporting documents from Section 2", 
                "Check alien expiration date matches supporting documents",
                "Verify supporting documents are attached in PDF"
            ]
        },
        "Scenario 2": {
            "description": "Multiple I-9 forms - select latest by signature date",
            "rules": [
                "Identify all I-9 Section 1 forms",
                "Select form with latest employee signature date",
                "Apply Scenario 1 rules to selected form"
            ]
        },
        "Scenario 3": {
            "description": "I-9 with blank Supplement B",
            "rules": [
                "Detect I-9 Section 1 & 2 with blank Supplement B",
                "Use Section 1 and 2 data (ignore blank Supplement B)",
                "Apply standard I-9 validation rules"
            ]
        },
        "Scenario 4": {
            "description": "Filled Supplement B form processing",
            "rules": [
                "Process filled Supplement B form",
                "Get supporting documents from Supplement B",
                "Compare alien expiration with Supplement B documents",
                "Verify Supplement B documents are attached"
            ]
        },
        "Scenario 5": {
            "description": "Multiple Section 3 forms with Section 1 dependency",
            "rules": [
                "Select latest Section 3 by employee signature date",
                "Verify Section 1 exists before Section 3 (page order)",
                "Get primary details from Section 1",
                "Compare alien date with Section 3 document dates",
                "Verify Section 3 documents are attached",
                "Log error if Section 1 not found before Section 3"
            ]
        }
    }
    
    for scenario_id, info in scenarios.items():
        print(f"{scenario_id}: {info['description']}")
        for rule in info['rules']:
            print(f"  • {rule}")
        print()


def example_validation_framework():
    """Example showing validation framework capabilities"""
    
    print("\n=== Validation Framework Example ===\n")
    
    validation_types = {
        "Field Validation": [
            "Required fields (first name, last name, DOB)",
            "Date format validation (MM/DD/YYYY)",
            "SSN format validation (if provided)",
            "Citizenship status validation"
        ],
        "Cross-Field Validation": [
            "Citizenship status vs work authorization consistency",
            "Signature presence vs signature date consistency",
            "Employee age validation (must be 16+)"
        ],
        "Document Validation": [
            "Document type validation",
            "Document number format validation",
            "Expiration date validation",
            "List A/B+C combination rules"
        ],
        "Attachment Validation": [
            "Verify listed documents are in PDF",
            "Document number matching",
            "Document type consistency"
        ],
        "Compliance Validation": [
            "I-9 completion timeline requirements",
            "Work authorization expiration checks",
            "Document retention requirements",
            "Regulatory compliance verification"
        ],
        "Date Validation": [
            "Date format and reasonableness",
            "Date relationship validation",
            "Document expiration validation",
            "Timeline compliance checks"
        ]
    }
    
    for category, validations in validation_types.items():
        print(f"{category}:")
        for validation in validations:
            print(f"  • {validation}")
        print()


if __name__ == "__main__":
    # Run all examples
    example_single_document_processing()
    example_batch_processing()
    example_business_rules_scenarios()
    example_validation_framework()
    
    print("\n=== Usage Instructions ===")
    print("\nTo use the enhanced I-9 processor in your own code:")
    print("\n1. Import the required modules:")
    print("   from hri9.core.enhanced_processor import EnhancedI9Processor")
    print("   from pathlib import Path")
    print("\n2. Initialize the processor:")
    print("   processor = EnhancedI9Processor()")
    print("\n3. Process a document:")
    print("   result = processor.process_pdf(Path('document.pdf'), 'Employee Name')")
    print("\n4. Check the results:")
    print("   print(f'Status: {result.status.value}')")
    print("   print(f'Success Rate: {result.validation_success_rate:.1f}%')")
    print("\nFor batch processing, use the ProcessingPipeline class.")
    print("\nConfiguration can be customized through environment variables")
    print("or by modifying hri9/config/settings.py")
