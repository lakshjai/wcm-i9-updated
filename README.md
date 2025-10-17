# Enhanced I-9 Document Processing System v3.0

A comprehensive, modular Python system for processing I-9 Employment Eligibility Verification forms from PDF documents with AI-powered detection, advanced business rules, and comprehensive validation.

## 🔍 How It Works

The system uses a **two-stage processing pipeline**:

### **Stage 1: Catalog Generation (AI Vision + OCR)**
- **Converts PDF pages to images** (300 DPI by default)
- **Sends images to Gemini 2.5 Pro Vision AI** for analysis
- **Gemini performs OCR and data extraction** from the images
- **Generates catalog.json files** with structured data for each PDF
- **Enhanced prompts** for accurate handwritten text recognition

### **Stage 2: Rubric Processing (Business Rules)**
- **Reads catalog.json files** (no AI calls needed)
- **Applies business rules** for I-9 form validation
- **Generates CSV reports** with compliance scoring
- **400x faster** than re-extraction (uses cached catalogs)

This architecture allows you to:
- ✅ Run extraction once, process multiple times
- ✅ Tune business rules without re-extracting
- ✅ Debug extraction vs. business logic separately

## 🎯 Key Features

### **Enhanced Processing Pipeline**
- **AI-Powered Document Analysis**: Advanced Gemini 2.5 Pro integration for comprehensive document understanding
- **Modular Architecture**: Clean separation of concerns with 40+ specialized modules
- **Advanced Business Rules Engine**: 5 configurable business rule scenarios with validation framework
- **Smart Form Classification**: Intelligent detection and classification of I-9 form types
- **Document Cataloging**: Comprehensive analysis and cataloging of all document content

### **Business Rule Scenarios**
1. **Single I-9 Form Processing**: Standard form processing with document matching
2. **Multiple I-9 Forms**: Latest signature date selection with validation
3. **I-9 with Blank Supplement B**: Section 1 and 2 data extraction
4. **Filled Supplement B Processing**: Advanced supplement form handling
5. **Multiple Section 3 Forms**: Section 1 dependency validation

### **Advanced Validation Framework**
- **Field Validators**: 5 specialized field validation rules
- **Cross-Field Validators**: 2 advanced cross-field validation rules
- **Document Validators**: Comprehensive attachment and document validation
- **Compliance Validators**: Date matching and expiration validation
- **Configurable Thresholds**: Environment-based configuration for all validation rules

### **Enterprise Features**
- **Concurrent Processing**: Configurable parallel processing with thread safety
- **Comprehensive Logging**: Multi-level logging with performance metrics
- **Error Recovery**: Graceful degradation with detailed error reporting
- **Export Capabilities**: JSON, CSV, and enhanced reporting formats
- **Caching System**: Intelligent caching for improved performance

## 🚀 Quick Start

### **⚡ Two-Stage Processing (Recommended)**

The system works in two stages - catalog generation and rubric processing:

#### **Option 1: Run Both Stages Together (Full Pipeline)**
```bash
# Generate catalogs AND process with rubric in one command
python run_processing.py process

# Or manually:
python regenerate_catalogs.py  # Stage 1: Generate catalogs
python rubric_processor.py      # Stage 2: Apply business rules
```

#### **Option 2: Run Catalog Generation Only**
```bash
# Generate catalog.json files from PDFs (uses AI Vision + OCR)
python regenerate_catalogs.py

# Output: workdir/catalogs/*.catalog.json
# Time: ~1-2 minutes per PDF (depends on page count)
```

#### **Option 3: Run Rubric Processing Only**
```bash
# Process existing catalogs with business rules (no AI calls)
python rubric_processor.py

# Input: workdir/catalogs/*.catalog.json
# Output: workdir/rubric_based_results.csv
# Time: ~1 second per catalog (400x faster!)
```

#### **Option 4: Reprocess Using Existing Catalogs**
```bash
# Skip catalog generation, use existing catalogs
python run_processing.py reprocess

# Perfect for:
# - Tuning business rules
# - Testing different validation thresholds
# - Quick iterations without AI costs
```

### **⚡ Legacy Quick Start**

Use the included quick-start script for common tasks:

```bash
# Test the system with limited files
python run_processing.py validate

# Clean previous results
python run_processing.py clean
```

### Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd wcm-i9-updated
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Configure environment**
```bash
cp .env.example .env
# Edit .env with your API credentials and preferences
```

4. **Prepare your data**
```bash
# Place PDF files directly in data/input/ folder
cp your_pdfs/*.pdf data/input/

# The system will process all PDFs in this directory
```

## 🔧 Technical Details

### **PDF Processing Pipeline**

The system uses a sophisticated image-based processing approach:

1. **PDF to Image Conversion**
   - Each PDF page is rendered as a high-resolution image (300 DPI)
   - Uses `pdf2image` library with `poppler` backend
   - Converts to JPEG format for optimal AI processing

2. **Image Encoding**
   - Images are base64-encoded for API transmission
   - Sent to Gemini 2.5 Pro Vision API
   - Both image and extracted text are provided for best accuracy

3. **AI Vision + OCR**
   - Gemini performs OCR on the image
   - Vision model reads handwritten text
   - Structured data extraction with field validation
   - Enhanced prompts for date and document title accuracy

4. **Catalog Generation**
   - Structured JSON output per PDF
   - Includes extracted values, confidence scores, metadata
   - Cached for fast reprocessing

### **Why Image-Based Processing?**

✅ **Better OCR Accuracy**: Vision AI handles handwritten text better than traditional OCR  
✅ **Layout Understanding**: AI understands form structure and context  
✅ **Signature Detection**: Can identify and validate signatures  
✅ **Quality Assessment**: Detects image quality issues  
✅ **Flexible Extraction**: Adapts to different I-9 form versions  

### **Performance Characteristics**

| Stage | Time per PDF | AI Calls | Cost |
|-------|-------------|----------|------|
| **Catalog Generation** | 1-2 min | 1 per 3 pages | $$ |
| **Rubric Processing** | <1 second | 0 | Free |
| **Reprocessing** | <1 second | 0 | Free |

💡 **Tip**: Generate catalogs once, then iterate on business rules for free!

## 📖 Common Workflows

### **Workflow 1: First-Time Processing**
```bash
# Step 1: Place PDFs in input folder
cp *.pdf data/input/

# Step 2: Generate catalogs (AI Vision + OCR)
python regenerate_catalogs.py

# Step 3: Process with business rules
python rubric_processor.py

# Result: workdir/rubric_based_results.csv
```

### **Workflow 2: Tuning Business Rules**
```bash
# Already have catalogs? Just reprocess!
# Edit rubric_processor.py to adjust rules

python rubric_processor.py  # Instant results!

# No AI calls, no waiting, no cost
```

### **Workflow 3: Debugging Extraction Issues**
```bash
# Check specific page extraction
python debug_document_title.py

# Regenerate single file
rm "workdir/catalogs/Employee_Name.catalog.json"
python regenerate_catalogs.py

# Process again
python rubric_processor.py
```

### **Workflow 4: Production Batch Processing**
```bash
# Process 100s of files
python regenerate_catalogs.py  # Takes time, run once

# Generate reports multiple times
python rubric_processor.py     # Instant, run anytime
```

## Usage Guide

The HRI9 system supports multiple processing modes to handle different use cases. All commands use `hri9/main.py` as the entry point.

### **🚀 Quick Start**

```bash
# Data extraction only (recommended for most use cases)
python hri9/main.py --data-only --limit 5 --use-local --workers 2

# Traditional I-9 processing with PDF manipulation
python hri9/main.py --mode all --limit 5 --use-local --enable-catalog
```

### **📋 Processing Modes**

#### **1. Data-Only Mode (Recommended)**
Pure data extraction without PDF manipulation. Creates categorized output files.

```bash
# Basic data extraction
python hri9/main.py --data-only --limit 10 --use-local --workers 4

# Process specific employees
python hri9/main.py --data-only --employee-ids 9963 1234 5678 --use-local

# Process all available documents
python hri9/main.py --data-only --use-local --workers 8
```

**Output Files:**
- `i9_success_YYYYMMDD_HHMMSS.csv` - Successfully processed documents
- `i9_partial_YYYYMMDD_HHMMSS.csv` - Partially processed documents  
- `i9_errors_YYYYMMDD_HHMMSS.csv` - Failed processing with error details

#### **2. Traditional Processing Modes**
Full I-9 processing pipeline with PDF manipulation capabilities.

```bash
# I-9 Detection Only
python hri9/main.py --mode detect --limit 10 --use-local --enable-catalog

# Extract I-9 Forms Only
python hri9/main.py --mode extract --limit 10 --use-local --enable-catalog

# Remove I-9 Pages Only
python hri9/main.py --mode remove --limit 10 --use-local --enable-catalog

# Full Pipeline (detect, extract, and remove)
python hri9/main.py --mode all --limit 10 --use-local --enable-catalog
```

#### **3. Catalog-Only Mode**
Document analysis and cataloging without I-9 business logic.

```bash
# Generate document catalogs only
python hri9/main.py --catalog-only --limit 10 --use-local --catalog-include-pii

# Export catalog data in different formats
python hri9/main.py --catalog-only --limit 10 --use-local --catalog-export-format json
python hri9/main.py --catalog-only --limit 10 --use-local --catalog-export-format csv
```

#### **4. Existing Catalog Processing (NEW)**
Process documents using existing catalog files **WITHOUT AI re-extraction** for ultra-fast processing.

```bash
# Process specific catalog file
python hri9/main.py --use-existing-catalog --catalog-file "workdir/catalogs/Employee_Name.catalog.json"

# Process all catalog files in directory
python hri9/main.py --use-existing-catalog --catalog-export-path "workdir/catalogs"

# With verbose logging for debugging
python hri9/main.py --use-existing-catalog --catalog-file "path/to/catalog.json" --verbose
```

**Benefits:**
- ⚡ **400x faster** - No AI re-extraction required
- ✅ **Validation logic testing** - Perfect for debugging business rules
- 🔄 **Reprocessing** - Apply updated business rules to existing data
- 🎯 **Targeted processing** - Process specific documents quickly

### **⚙️ Configuration Options**

#### **Data Source Options**
```bash
--use-local              # Use local sample data (data/input/)
--excel-file PATH        # Custom Excel file with employee IDs
--employee-ids ID1 ID2   # Process specific employee IDs
```

#### **Performance Options**
```bash
--workers N              # Number of concurrent workers (default: 4)
--limit N                # Limit number of documents to process
--batch-size N           # Batch size for processing (default: 10)
```

#### **Output Options**
```bash
--output-dir PATH        # Directory for output files
--extract-dir PATH       # Directory for extracted I-9 forms
--cleaned-dir PATH       # Directory for cleaned PDFs
--output-csv PATH        # Path to output CSV file
```

#### **Catalog System Options**
```bash
--enable-catalog         # Enable document catalog system
--disable-catalog        # Explicitly disable catalog system
--catalog-include-pii    # Include PII in catalog exports
--catalog-export-format FORMAT  # Export format: json, csv, both
--catalog-export-path PATH       # Directory for catalog exports

# NEW: Existing Catalog Processing Options
--use-existing-catalog   # Process using existing catalog files (no AI re-extraction)
--catalog-file PATH      # Specific catalog file to process
```

#### **Advanced Options**
```bash
--verbose                # Enable verbose logging
--debug-files PATTERN    # Process only files matching pattern
--extract-all-data       # Extract data from all pages
--force-catalog-regeneration  # Force regeneration of existing catalogs
```

### **📊 Example Workflows**

#### **Workflow 1: Data Analysis Project**
For data extraction and analysis without modifying original PDFs:

```bash
# Step 1: Process documents with data extraction only
python hri9/main.py --data-only --limit 100 --use-local --workers 6

# Step 2: Review categorized results
ls workdir/categorized_results/

# Step 3: Process remaining documents if needed
python hri9/main.py --data-only --use-local --workers 8
```

#### **Workflow 2: Document Processing Pipeline**
For organizations needing I-9 extraction and document cleanup:

```bash
# Step 1: Detect and analyze I-9 forms
python hri9/main.py --mode detect --limit 50 --use-local --enable-catalog

# Step 2: Extract I-9 forms to separate files
python hri9/main.py --mode extract --limit 50 --use-local --enable-catalog

# Step 3: Remove I-9 pages from original documents
python hri9/main.py --mode remove --limit 50 --use-local --enable-catalog

# Alternative: Do all steps at once
python hri9/main.py --mode all --limit 50 --use-local --enable-catalog
```

#### **Workflow 3: Document Cataloging**
For comprehensive document analysis and cataloging:

```bash
# Generate detailed document catalogs
python hri9/main.py --catalog-only --use-local --catalog-include-pii \
  --catalog-export-format both --workers 4

# Validate existing catalog files
python hri9/main.py --catalog-validate --catalog-export-path ./catalogs

# Import and analyze catalog data
python hri9/main.py --catalog-import ./catalog.json --catalog-stats
```

#### **Workflow 4: Complete Processing Pipeline (RECOMMENDED)**
For processing all files in input directory with output to specific directory:

```bash
# Step 1: Clean previous results (optional)
rm -rf workdir/catalogs/* workdir/categorized_results/*

# Step 2: Process all files in input directory with data extraction
python hri9/main.py --data-only --use-local --workers 4 \
  --output-dir "workdir/results" \
  --catalog-export-path "workdir/catalogs"

# Step 3: Verify catalog generation
ls workdir/catalogs/

# Step 4: Reprocess using existing catalogs (ultra-fast validation)
python hri9/main.py --use-existing-catalog \
  --catalog-export-path "workdir/catalogs" --verbose

# Step 5: Review results
ls workdir/categorized_results/
```

**This workflow:**
- 🗂️ **Processes all PDF files** in the `input/` directory
- 📊 **Generates comprehensive catalogs** in `workdir/catalogs/`
- ⚡ **Validates business logic** using existing catalogs (400x faster)
- 📈 **Creates categorized CSV reports** in `workdir/categorized_results/`

### **📁 Output Files and Directory Structure**

The system generates various output files depending on the processing mode:

#### **Data-Only Mode Output**
```
workdir/categorized_results/
├── i9_success_20251007_143022.csv    # Successfully processed documents
├── i9_partial_20251007_143022.csv    # Partially processed documents
└── i9_errors_20251007_143022.csv     # Failed processing with errors
```

#### **Traditional Mode Output**
```
workdir/
├── i9_detection_results.csv          # Main processing results
├── extracted_i9_forms/               # Extracted I-9 forms
│   └── Employee_Name_i9_forms.pdf
├── processed/                        # Cleaned PDFs (I-9s removed)
│   └── Employee_Name.pdf
├── catalogs/                         # Document catalog exports
│   ├── document_catalog_20251007.json
│   └── document_catalog_20251007.csv
└── business_rules_output/            # Business rules reports
    ├── business_rules_9963_20251007.json
    └── business_rules_9963_20251007.txt
```

#### **CSV Output Fields**
The output CSV files contain comprehensive data including:
- **Basic Info**: Employee ID, PDF file name, processing status
- **I-9 Data**: First name, last name, citizenship status, signature dates
- **Document Info**: Supporting documents, expiration matches, validation status
- **Processing Metrics**: Pages cataloged, API calls, processing time
- **Business Rules**: Validation results, critical issues, scenario outcomes

### **🏗️ System Architecture**

#### **Modular Components**
- **`/core/`** - Business logic (enhanced processor, form classification, set grouping)
- **`/api/`** - External service integration (Gemini AI client)
- **`/catalog/`** - Document cataloging system with caching and error handling
- **`/rules/`** - Business rules engine and I-9-specific validation rules
- **`/validation/`** - Comprehensive validation framework
- **`/utils/`** - Shared utilities (logging, reporting, concurrency)
- **`/data/`** - Data access layer and file management
- **`/cli/`** - Command-line interface and argument parsing

#### **Processing Pipeline**
1. **Document Cataloging** - AI-powered page analysis and classification
2. **I-9 Set Grouping** - Logical grouping of related I-9 pages
3. **Form Selection** - Latest form selection based on signature dates
4. **Business Rules** - 5 scenario-based rule processing
5. **Validation** - Comprehensive field and cross-field validation
6. **Reporting** - Categorized output generation

### **🔧 Troubleshooting**

#### **Common Issues**

**1. "No PDF found for employee" Error**
```bash
# Check data directory structure
ls -la data/input/[EMPLOYEE_ID]/

# Verify Excel file has correct employee IDs
python -c "import pandas as pd; print(pd.read_excel('data/ActiveI9.xlsx')['Employee ID'].tolist())"
```

**2. "Failed to initialize processing components" Error**
```bash
# Check API credentials
echo $GEMINI_API_KEY

# Verify dependencies
pip install -r requirements.txt
```

**3. "No valid I-9 sets found" Warning**
```bash
# Run with verbose logging to see detailed analysis
python hri9/main.py --data-only --limit 1 --use-local --verbose

# Check catalog output for page classifications
cat workdir/catalogs/document_catalog_*.json | jq '.pages[].page_classification'
```

**4. Memory Issues with Large Documents**
```bash
# Reduce concurrent workers
python hri9/main.py --data-only --workers 2 --use-local

# Process in smaller batches
python hri9/main.py --data-only --limit 10 --use-local
```

#### **Performance Optimization**

**For Large Document Sets:**
```bash
# Use optimal worker count (CPU cores - 1)
python hri9/main.py --data-only --workers 7 --use-local

# Enable catalog caching for repeated runs
python hri9/main.py --data-only --use-local --skip-existing-catalog
```

**For Network Processing:**
```bash
# Use network drive with health checks
python hri9/main.py --mode detect --workers 4 --limit 100

# Enable comprehensive logging for debugging
python hri9/main.py --mode detect --verbose --debug-files "1234"
```

### **📊 Monitoring and Logging**

#### **Log Levels**
```bash
# Standard logging
python hri9/main.py --data-only --use-local

# Verbose logging for debugging
python hri9/main.py --data-only --use-local --verbose

# Debug specific files
python hri9/main.py --data-only --use-local --debug-files "9963"
```

#### **Progress Monitoring**
The system provides real-time progress updates:
```
Progress: 45/100 (45.0%) | Success: 38 | Rate: 2.34 docs/sec | ETA: 23.5s
```

#### **Performance Metrics**
- **Processing Rate**: Documents per second
- **Success Rate**: Percentage of successful processing
- **API Usage**: Gemini API calls and token consumption
- **Cache Hit Rate**: Catalog cache efficiency

#### **Method 2: Programmatic Usage**

```python
from pathlib import Path
from hri9.core.enhanced_processor import EnhancedI9Processor
from hri9.api.gemini_client import GeminiClient
from hri9.catalog.cache import CatalogCache

# Initialize the enhanced processor
gemini_client = GeminiClient()
catalog_cache = CatalogCache(max_documents=1000)
processor = EnhancedI9Processor(gemini_client, catalog_cache)

# Process a single document
pdf_path = Path("path/to/document.pdf")
result = processor.process_pdf(pdf_path, "Employee Name")

# Access results
print(f"Status: {result.status.value}")
print(f"Validation Success Rate: {result.validation_success_rate:.1f}%")
print(f"Critical Issues: {result.critical_issues}")

if result.primary_i9_data:
    form = result.primary_i9_data
    print(f"Form Type: {form.form_type.value}")
    print(f"Page Number: {form.page_number}")
```

### **🎯 Key Features and Benefits**

#### **Data-Only Mode Advantages**
- **✅ Non-Destructive**: Original PDFs remain completely untouched
- **✅ Categorized Output**: Automatic SUCCESS/PARTIAL/ERROR file separation
- **✅ Comprehensive Data**: Rich CSV output with 50+ fields per document
- **✅ Business Rules**: Advanced validation with 5 scenario-based rules
- **✅ Fast Processing**: Optimized for data extraction without PDF manipulation
- **✅ Research Friendly**: Perfect for data analysis and compliance projects

#### **Enhanced Processing Features**
- **🤖 AI-Powered**: Gemini 2.5 Pro for intelligent document analysis
- **📊 Comprehensive Cataloging**: Page-by-page classification and metadata
- **🔍 Smart I-9 Detection**: Advanced form recognition and grouping
- **⚡ Concurrent Processing**: Multi-worker support for high throughput
- **🎯 Business Rules Engine**: Configurable validation scenarios
- **📈 Real-time Monitoring**: Progress tracking and performance metrics
- **🔄 Caching System**: Intelligent caching for repeated processing
- **📋 Multiple Output Formats**: CSV, JSON, and structured reports

#### **System Requirements**
- **Python**: 3.8+ (3.10+ recommended)
- **Memory**: 4GB+ RAM (8GB+ for large document sets)
- **Storage**: 1GB+ free space for outputs and caching
- **API**: Google Gemini API key required
- **Dependencies**: See `requirements.txt` for complete list

#### **Supported Document Types**
- **I-9 Forms**: All versions and sections (1, 2, 3, Supplement B)
- **Supporting Documents**: Passports, driver's licenses, work authorization
- **Mixed Documents**: Employee files with multiple document types
- **Scanned PDFs**: OCR and image-based documents
- **Multi-page Files**: Complex documents with 20+ pages

## 🏗️ Enhanced Architecture

### **Modular HRI9 System Structure**

```
hri9/
├── core/                    # Core business logic
│   ├── enhanced_processor.py   # Main processing orchestrator with business rules
│   ├── models.py              # Data models and enums
│   ├── form_classifier.py     # Intelligent form classification
│   ├── i9_detector.py         # I-9 form detection logic
│   └── pdf_manipulator.py     # PDF manipulation utilities
├── rules/                   # Business rules engine
│   ├── rule_engine.py         # Rule execution framework
│   ├── scenario_processor.py  # 5 business rule scenarios
│   └── i9_rules.py           # I-9 specific validation rules
├── validation/              # Comprehensive validation framework
│   ├── validators.py          # Field and cross-field validators
│   ├── document_validators.py # Document attachment validators
│   └── compliance_validators.py # Date and compliance validators
├── catalog/                 # Advanced document cataloging
│   ├── document_catalog.py    # AI-powered document analysis
│   ├── cache.py              # Intelligent caching system
│   ├── export.py             # Multi-format export capabilities
│   └── validation.py         # Catalog validation and QA
├── api/                     # External service integration
│   └── gemini_client.py      # Gemini AI client with retry logic
├── utils/                   # Shared utilities
│   ├── logging_config.py     # Advanced logging configuration
│   ├── concurrency.py        # Thread-safe processing utilities
│   ├── reporting.py          # Standard reporting
│   └── enhanced_reporting.py # Advanced reporting with business rules
├── config/                  # Configuration management
│   └── settings.py           # Environment-based settings (40+ options)
├── cli/                     # Command-line interface
│   └── arguments.py          # Comprehensive CLI argument parsing
└── data/                    # Data access layer
    ├── excel_reader.py       # Excel file processing
    └── file_manager.py       # File system management
```

## 📋 Command Line Options

### **Processing Modes**
- `--mode {detect,remove,extract,all}`: Processing mode selection
  - **detect**: Find I-9 forms only
  - **extract**: Save I-9s to separate files  
  - **remove**: Create cleaned PDFs without I-9s
  - **all**: Extract and remove (full pipeline)

### **Performance & Concurrency**
- `--workers WORKERS`: Number of concurrent workers (default: 4)
- `--limit LIMIT`: Maximum documents to process (0 for all)
- `--batch-size BATCH_SIZE`: Progress reporting interval (default: 10)

### **Data Source**
- `--use-local`: Use local sample data instead of network drive
- `--excel-file EXCEL_FILE`: Path to Excel file with employee IDs
- `--debug-files PATTERN`: Debug mode - process only files containing pattern

### **Catalog System Options**
- `--enable-catalog`: Enable comprehensive document analysis
- `--disable-catalog`: Explicitly disable catalog system
- `--catalog-only`: Generate catalog only, skip I-9 processing
- `--catalog-export-format {json,csv,both}`: Export format
- `--catalog-confidence-threshold THRESHOLD`: Minimum confidence (0.0-1.0)
- `--catalog-include-pii`: Include sensitive data in exports
- `--catalog-cache-size SIZE`: Maximum cached documents

### **Output Configuration**
- `--output-dir OUTPUT_DIR`: Directory for output files
- `--extract-dir EXTRACT_DIR`: Directory for extracted I-9 forms
- `--cleaned-dir CLEANED_DIR`: Directory for cleaned PDFs
- `--output-csv OUTPUT_CSV`: Path to output CSV file

## 🔧 Advanced Usage Examples

### **Production Processing**
```bash
# Process 1000 documents with 8 workers, full pipeline
python hri9/main.py \
  --mode all \
  --workers 8 \
  --limit 1000 \
  --enable-catalog \
  --catalog-export-format both \
  --catalog-confidence-threshold 0.8

# High-confidence catalog generation only
python hri9/main.py \
  --catalog-only \
  --workers 4 \
  --catalog-confidence-threshold 0.9 \
  --catalog-include-pii \
  --catalog-export-format json
```

### **Development & Testing**
```bash
# Debug specific employee
python hri9/main.py \
  --mode detect \
  --debug-files "1003375" \
  --verbose \
  --enable-catalog

# Test with small batch
python hri9/main.py \
  --mode all \
  --limit 5 \
  --workers 1 \
  --use-local \
  --enable-catalog
```

### **Quality Assurance**
```bash
# Validate existing catalogs
python hri9/main.py \
  --catalog-validate \
  --catalog-export-path ./catalogs

# Generate detailed statistics
python hri9/main.py \
  --catalog-stats \
  --catalog-report-path ./reports
```

## ⚙️ Configuration

### **Environment Variables (.env)**

```bash
# API Configuration
GOOGLE_API_KEY=your_gemini_api_key_here
GEMINI_MODEL=gemini-2.5-pro

# Processing Configuration
CONCURRENT_WORKERS=4
MAX_DOCUMENTS=0
BATCH_SIZE=10

# Business Rules Configuration
ENABLE_BUSINESS_RULES=true
ENABLE_ADVANCED_VALIDATION=true
VALIDATION_CONFIDENCE_THRESHOLD=0.7
DATE_MATCHING_TOLERANCE_DAYS=30

# Catalog Configuration
CATALOG_CACHE_SIZE=1000
CATALOG_CONFIDENCE_THRESHOLD=0.7
ENABLE_TEXT_REGIONS=true
ENABLE_STRUCTURED_EXTRACTION=true

# Output Configuration
WORK_DIR=./workdir
I9_EXTRACT_DIR=./workdir/extracted_i9_forms
CLEANED_PDF_DIR=./workdir/processed
CATALOG_EXPORT_DIR=./workdir/catalogs

# Logging Configuration
LOG_LEVEL=INFO
LOG_FILE=./workdir/i9_detection.log
ENABLE_PERFORMANCE_LOGGING=true
```

### **Directory Structure Setup**

```bash
# Required directory structure
wcm-i9-updated/
├── data/
│   ├── ActiveI9.xlsx          # Excel file with Employee ID column
│   └── input/                 # Employee document directories
│       ├── [EMPLOYEE_ID_1]/   # e.g., 1003375/
│       │   └── document.pdf   # Employee's PDF document
│       └── [EMPLOYEE_ID_2]/   # e.g., 9963/
│           └── document.pdf   # Employee's PDF document
├── workdir/                   # Output directory (auto-created)
│   ├── extracted_i9_forms/    # Extracted I-9 forms
│   ├── processed/             # Cleaned PDFs (I-9s removed)
│   ├── catalogs/              # Document catalog exports
│   └── business_rules_output/ # Business rule results
└── logs/                      # Log files (auto-created)
```

## 🔍 Output Files

### **CSV Results (Enhanced)**
- **File**: `workdir/i9_detection_results.csv`
- **Columns**: Employee ID, PDF File Name, I-9 Forms Found, Business Rules Status, Validation Success Rate, Critical Issues, Form Type Selected, etc.

### **Catalog Exports**
- **JSON**: `workdir/catalogs/document_catalog_[timestamp].json`
- **CSV**: `workdir/catalogs/document_catalog_[timestamp].csv`
- **Content**: Comprehensive document analysis, extracted fields, confidence scores

### **Business Rules Reports**
- **File**: `workdir/business_rules_output/`
- **Content**: Detailed business rule execution results, validation reports, scenario analysis

## 🚨 Troubleshooting

### **Common Issues**

#### **"No I-9 forms detected"**
```bash
# Check if catalog system is working
python hri9/main.py --catalog-only --limit 1 --use-local --verbose

# Debug form detection
python debug_form_detection.py
```

#### **"ImportError: attempted relative import"**
```bash
# Use absolute path execution
cd /path/to/wcm-i9-updated
python hri9/main.py [options]

# Or use module execution
python -m hri9.main [options]
```

#### **"Excel file not found"**
```bash
# Create the required Excel file
mkdir -p data
echo "Employee ID" > data/ActiveI9.csv
echo "9963" >> data/ActiveI9.csv
python -c "import pandas as pd; pd.read_csv('data/ActiveI9.csv').to_excel('data/ActiveI9.xlsx', index=False)"
```

#### **"API rate limiting"**
```bash
# Reduce workers and add delays
python hri9/main.py --workers 1 --batch-size 5 [other options]
```

### **Debug Scripts**

```bash
# Test enhanced processor components
python debug_enhanced_processor.py

# Debug form detection issues  
python debug_form_detection.py

# Examine catalog structure
python debug_catalog_structure.py
```

### **Performance Optimization**

```bash
# For large datasets (1000+ documents)
python hri9/main.py \
  --mode detect \
  --workers 8 \
  --batch-size 50 \
  --catalog-cache-size 2000 \
  --enable-catalog

# For high accuracy (slower but more thorough)
python hri9/main.py \
  --mode all \
  --workers 2 \
  --catalog-confidence-threshold 0.9 \
  --enable-catalog \
  --catalog-text-regions \
  --catalog-structured-extraction
```

## 📊 Business Rules & Validation

### **Validation Rules (6 Total)**
1. **Employee Name Validation**: Ensures employee name is present and valid
2. **Signature Date Validation**: Validates signature date format and logic
3. **Citizenship Status Validation**: Verifies citizenship status selection
4. **Document Attachment Validation**: Checks for required supporting documents
5. **Date Consistency Validation**: Cross-validates dates across form sections
6. **Completeness Validation**: Ensures all required fields are filled

### **Business Rule Scenarios**
- **Scenario 1**: Single I-9 form with standard processing
- **Scenario 2**: Multiple I-9 forms requiring latest selection
- **Scenario 3**: I-9 with blank Supplement B handling
- **Scenario 4**: Filled Supplement B form processing  
- **Scenario 5**: Multiple Section 3 forms with dependencies

### **Success Metrics**
- **Validation Success Rate**: Percentage of passed validation rules
- **Critical Issues Count**: Number of critical validation failures
- **Processing Status**: COMPLETE_SUCCESS, PARTIAL_SUCCESS, ERROR, NO_I9_FOUND
