# Wu, Qianyi - Before & After Comparison

## 🎯 The Problem You Identified

You correctly pointed out that for Wu, Qianyi's **Section 3 (re-verification)** case, the documents should only show the Section 3 document, not documents from multiple pages including Section 2 documents.

**Your Question**:
> "For the employee Wu, Qianyi, the documents in the primary set should only have that section 3 document listed, but it is pulling document from multiple pages. When you check it out, is that correct? Because there is no foreign passport with the form I-4, I-94 and DS forms. Am I missing something? Is that correct?"

**Answer**: You were 100% correct! The system was incorrectly extracting documents from multiple Section 3 pages, including a bad extraction.

---

## 📊 Before Fix

### **Documents Extracted**
```
Documents in Primary Set: Document Number (#607451605AZ) | EAD from DHS with Photo (#607421605A2)
Document Count: 2
```

### **The Issues**
1. ❌ **Extracting from Page 6** (06/08/2021): "Document Number" - This is a BAD extraction (field label instead of value)
2. ❌ **Extracting from Page 12** (02/15/2023): "EAD from DHS with Photo" - This is correct, but...
3. ❌ **Should only extract from the LATEST Section 3 page** (Page 12), not all Section 3 pages

### **Why This Happened**
The code was extracting documents from **ALL** pages that matched the form type criteria, not just the latest one.

```python
# OLD CODE - Extracted from ALL Section 3 pages
for page in pages:
    if 'section 3' in page_title or 'reverification' in page_title:
        should_extract = True
        # Extracted documents from this page
```

---

## ✅ After Fix

### **Documents Extracted**
```
Documents in Primary Set: EAD from DHS with Photo (#607421605A2)
Document Count: 1
```

### **What Changed**
1. ✅ **Only extracting from Page 12** (02/15/2023) - The LATEST Section 3 page
2. ✅ **Ignoring Page 6** (06/08/2021) - Older Section 3 page with bad extraction
3. ✅ **Correct document**: "EAD from DHS with Photo" from the latest reverification

### **How We Fixed It**
```python
# NEW CODE - Extract ONLY from the LATEST Section 3 page
if form_type == 'reverification_section_3' or form_type == 're-verification':
    # Find all Section 3 pages with signature dates
    section3_pages = []
    for page in pages:
        if 'section 3' in page_title or 'reverification' in page_title:
            # Get signature date from this page
            sig_dates = [employer_signature_date, reverification_signature_date, ...]
            latest_sig = max(sig_dates)
            section3_pages.append({'page': page, 'signature_date': latest_sig})
    
    # Select ONLY the page with the latest signature date
    if section3_pages:
        latest_section3 = max(section3_pages, key=lambda x: parse_date(x['signature_date']))
        pages_to_extract = [latest_section3['page']]  # ONLY ONE PAGE!
```

---

## 📋 Complete Comparison

| Aspect | Before | After |
|--------|--------|-------|
| **Documents** | "Document Number" + "EAD from DHS with Photo" | "EAD from DHS with Photo" |
| **Document Count** | 2 | 1 ✅ |
| **Pages Used** | Page 6 + Page 12 | Page 12 only ✅ |
| **Form Type** | re-verification ✅ | re-verification ✅ |
| **Work Auth Expiry** | 03/06/2025 ✅ | 03/06/2025 ✅ |
| **Document Expiry** | 03/06/2025 ✅ | 03/06/2025 ✅ |
| **Exact Match** | MATCH ✅ | MATCH ✅ |
| **Alignment with Requirements** | ❌ Partial | ✅ Complete |

---

## 🔍 Wu, Qianyi's PDF Structure

The PDF has **multiple I-9 forms** across different time periods:

### **Page 2** (06/01/2021) - Original Hire
- **Section 2 Documents**:
  - FOREIGN PASSPORT (China, #E03935174)
  - FORM I-94 (#607421605A2)
  - DS-2019 (#N0031723170)

### **Page 6** (06/08/2021) - First Reverification
- **Section 3 Document**: "Document Number" ❌ (bad extraction)
- **List A Document**: FOREIGN PASSPORT WITH A FORM I-94 AND DS-2019

### **Page 12** (02/15/2023) - Latest Reverification ✅
- **Section 3 Document**: "EAD from DHS with Photo" (#607421605A2) ✅
- **This is the correct document to extract!**

### **Page 19** (12/15/2023) - Another Section 2
- **Section 2 Document**: EAD ISSUED BY DHS, FORM I94/I94A AND A LETTER...

---

## ✅ Verification Against Requirements

From `rubric_based_results_description.csv`, Column W definition:
> "if the pdf file have latest valid section 3 document extract the document title listed under this section 3"

**Before**: ❌ Extracting from multiple Section 3 pages  
**After**: ✅ Extracting ONLY from the LATEST Section 3 page

---

## 🎉 Final Result

**Wu, Qianyi now shows**:
- ✅ Form Type: re-verification
- ✅ Documents: EAD from DHS with Photo (#607421605A2)
- ✅ Document Count: 1
- ✅ Source: Page 12 (latest Section 3, dated 02/15/2023)
- ✅ Work Auth Match: MATCH: 03/06/2025
- ✅ Total Score: 93/100

**All requirements met!** ✅

---

## 📝 Key Takeaway

Your observation was **100% correct**. The system was incorrectly pulling documents from multiple Section 3 pages. The fix ensures that:

1. **For Section 3 (re-verification)**: Extract ONLY from the LATEST Section 3 page
2. **For Supplement B (re-hire)**: Extract ONLY from the LATEST Supplement B page
3. **For Standard I-9 (new hire)**: Extract from all Section 2 pages (as multiple Section 2 pages may exist)

This aligns perfectly with the requirements in `rubric_based_results_description.csv`.
