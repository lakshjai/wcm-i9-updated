# I-9 Processing Rubric Specification

## Overview
This rubric provides a systematic, score-based approach to evaluate I-9 form processing quality. It replaces complex heuristics with clear decision trees and quantifiable metrics.

## Scoring System (Total: 100 points + bonus)

### BUCKET 1: PERSONAL DATA EXTRACTION (25 points)
**Purpose**: Evaluate quality of personal information extraction from catalog data

| Field | Points | Criteria |
|-------|--------|----------|
| First Name | 5 | Found in Section 1 pages |
| Last Name | 5 | Found in Section 1 pages |
| Middle Name/Initial | 3 | Found in Section 1 pages |
| Date of Birth | 5 | Found in Section 1 pages |
| SSN | 4 | Found in Section 1 pages |
| Citizenship Status | 3 | Found and properly mapped |

**Field Variations Checked**:
- Names: `first_name`, `employee_first_name`, `section_1_first_name`
- DOB: `date_of_birth`, `employee_date_of_birth`, `employee_dob`, `birth_date`
- SSN: `ssn`, `employee_ssn`, `us_social_security_number`
- Citizenship: `citizenship_status`, `employee_citizenship_status`

**Quality Thresholds**:
- 23-25 points: Excellent personal data
- 18-22 points: Good personal data
- 12-17 points: Partial personal data
- <12 points: Poor personal data

### BUCKET 2: I-9 FORM DETECTION & CLASSIFICATION (20 points + bonus)
**Purpose**: Evaluate I-9 form detection and proper classification

| Component | Points | Criteria |
|-----------|--------|----------|
| I-9 Forms Detected | 8 | At least one I-9 form found |
| Section 1 Pages | 4 | Section 1 pages identified |
| Section 2 Pages | 4 | Section 2 pages identified |
| Section 3/Supplement B | 4 | Advanced form types found |

**Bonus Points (Form Type Priority)**:
- Supplement B: +5 bonus points
- Section 3: +3 bonus points
- Standard I-9: +1 bonus point

**Detection Logic**:
- I-9 forms: Title contains "i-9", "form i-9", or "employment eligibility"
- Section classification: Based on title patterns and field presence
- Field-based detection: Checks for section-specific extracted values

### BUCKET 3: BUSINESS RULES HIERARCHY APPLICATION (25 points)
**Purpose**: Evaluate adherence to business rules priority hierarchy

| Component | Points | Criteria |
|-----------|--------|----------|
| Correct Form Type Selected | 10 | Follows priority: Supplement B > Section 3 > Standard I-9 |
| Latest Signature Selected | 8 | Within selected type, chooses latest employer signature |
| Proper Section 1 Selection | 4 | Uses Section 1 from selected form type |
| Proper Document Selection | 3 | Uses documents from selected form type |

**Priority Hierarchy**:
1. **Supplement B** (highest priority)
2. **Section 3/Reverification** (medium priority)
3. **Standard I-9** (lowest priority)

**Signature Date Fields Checked**:
- `employer_signature_date`
- `reverification_signature_date`
- `reverification_date_signed`
- `employer_signature_date_reverification`

### BUCKET 4: WORK AUTHORIZATION & EXPIRY MATCHING (15 points)
**Purpose**: Evaluate work authorization extraction and expiry date matching

| Component | Points | Criteria |
|-----------|--------|----------|
| Work Auth Expiry Found | 8 | Work authorization expiry date extracted |
| Document Expiry Found | 4 | Document expiry date extracted |
| Expiry Dates Match | 3 | Work auth and document expiry dates match |

**Work Authorization Fields**:
- `work_auth_expiration_date`
- `work_until_date`
- `work_authorization_expiration_date`
- `alien_authorized_to_work_until`
- `alien_authorized_to_work_until_date`
- `alien_expiration_date`

**Document Expiry Sources** (by form type):
- Supplement B: `reverification_1_expiration_date`
- Section 3: `reverification_expiration_date`, `section_3_expiration_date`
- Section 2: `list_a_expiration_date`, `list_b_expiration_date`, `list_c_expiration_date`

### BUCKET 5: DOCUMENT TRACKING & VERIFICATION (15 points)
**Purpose**: Evaluate document extraction and tracking quality

| Component | Points | Criteria |
|-----------|--------|----------|
| Documents Listed Correctly | 6 | Documents extracted from appropriate sections |
| Document Numbers Extracted | 3 | Document numbers paired with titles |
| Supporting Documents Found | 3 | Non-I-9 pages identified |
| Document Attachment Status | 3 | Attachment verification |

**Document Fields Checked**:
- `reverification_document_title`
- `section_3_document_title`
- `list_a_document_title`, `list_b_document_title`, `list_c_document_title`
- `list_a_document_title_1`, `list_a_document_title_2`, `list_a_document_title_3`
- `additional_information_document_title_2`, `additional_information_document_title_3`

## Status Determination Logic

### Overall Status Calculation
```
Total Score = Bucket1 + Bucket2 + Bucket3 + Bucket4 + Bucket5 + Bonus Points

Critical Requirements:
- I-9 Forms Detected: Bucket2 ≥ 10 points
- Personal Data Present: Bucket1 ≥ 12 points  
- Business Rules Followed: Bucket3 ≥ 15 points
```

### Status Thresholds
| Status | Score Range | Additional Requirements |
|--------|-------------|------------------------|
| **COMPLETE_SUCCESS** | 85+ points | All critical requirements met |
| **PARTIAL_SUCCESS** | 60-84 points | - |
| **ERROR** | 40-59 points | - |
| **NO_I9_FOUND** | <40 points OR missing I-9 forms | - |

## Decision Tree Flow

```
1. EXTRACT PERSONAL DATA (Bucket 1)
   └── Score personal information quality

2. DETECT I-9 FORMS (Bucket 2)
   ├── No I-9 found → NO_I9_FOUND
   └── I-9 found → Continue

3. APPLY BUSINESS RULES HIERARCHY (Bucket 3)
   ├── Find Supplement B → Priority 1
   ├── Find Section 3 → Priority 2
   └── Find Standard I-9 → Priority 3

4. SELECT LATEST WITHIN TYPE (Bucket 3)
   └── Choose latest employer signature date

5. EXTRACT WORK AUTHORIZATION (Bucket 4)
   └── From Section 1 of selected form

6. EXTRACT & MATCH DOCUMENTS (Bucket 5)
   └── From corresponding sections of selected form

7. CALCULATE FINAL SCORE & STATUS
   └── Apply scoring criteria and thresholds
```

## Implementation Notes

### Field Extraction Strategy
- **Multiple Field Variations**: Each data point checks multiple possible field names
- **Priority Order**: Fields are checked in priority order (most common first)
- **Data Validation**: Values must not be 'N/A', empty string, or None
- **Type Conversion**: All extracted values converted to strings for consistency

### Date Handling
- **Format Expected**: MM/DD/YYYY
- **Comparison Logic**: Uses `datetime.strptime()` for proper date comparison
- **Fallback**: Invalid dates treated as minimum date for comparison

### Error Handling
- **Graceful Degradation**: Processing continues even if individual buckets fail
- **Error Logging**: Detailed logging for debugging
- **Partial Results**: Returns partial scores even on errors

### Output Format
The rubric processor generates a CSV with the following columns:
- Basic info: `filename`, `status`, `total_score`
- Bucket scores: `bucket_1_personal_data_score`, etc.
- Extracted data: `first_name`, `last_name`, etc.
- Analysis results: `form_type_detected`, `expiry_match_details`, etc.

## Usage

```bash
cd /Users/dhanalakshmijayakumar/WCMAI/wcm-i9-updated
python rubric_processor.py
```

This will:
1. Process all `.catalog.json` files in `workdir/catalogs/`
2. Apply the rubric scoring system
3. Generate `workdir/rubric_based_results.csv`
4. Display processing summary and status counts
