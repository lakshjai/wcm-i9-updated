# Quick Start Guide - I-9 Processing System

## 🚀 TL;DR - Get Started in 3 Commands

```bash
# 1. Put PDFs in input folder
cp your_files/*.pdf data/input/

# 2. Generate catalogs (AI extraction)
python regenerate_catalogs.py

# 3. Process with business rules
python rubric_processor.py
```

**Result**: `workdir/rubric_based_results.csv` with all I-9 data and compliance scores!

---

## 📋 How the System Works

### Two-Stage Pipeline

```
┌─────────────────────────────────────────────────────────────┐
│  Stage 1: CATALOG GENERATION (Slow, Run Once)              │
├─────────────────────────────────────────────────────────────┤
│  PDF → Images (300 DPI) → Gemini Vision AI → catalog.json  │
│  • Uses OCR for text extraction                             │
│  • AI reads handwritten text                                │
│  • Time: 1-2 min per PDF                                    │
│  • Cost: API calls required                                 │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  Stage 2: RUBRIC PROCESSING (Fast, Run Anytime)            │
├─────────────────────────────────────────────────────────────┤
│  catalog.json → Business Rules → CSV Report                │
│  • No AI calls needed                                       │
│  • Pure Python logic                                        │
│  • Time: <1 second per file                                 │
│  • Cost: FREE                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 🎯 Common Use Cases

### Use Case 1: First Time Processing
**Scenario**: You have new PDFs and need to extract data

```bash
# Step 1: Add PDFs
cp new_employees/*.pdf data/input/

# Step 2: Generate catalogs (takes time)
python regenerate_catalogs.py
# ⏱️  Estimated: 10 PDFs × 2 min = 20 minutes

# Step 3: Generate report (instant)
python rubric_processor.py
# ⏱️  Estimated: <1 second

# Output: workdir/rubric_based_results.csv
```

---

### Use Case 2: Tuning Business Rules
**Scenario**: You want to adjust validation thresholds or scoring

```bash
# Edit business rules in rubric_processor.py
# (No need to regenerate catalogs!)

# Just reprocess
python rubric_processor.py
# ⏱️  Instant results!

# Try different rules, run again
python rubric_processor.py
# ⏱️  Still instant!
```

**Why it's fast**: Catalogs are already generated, no AI calls needed!

---

### Use Case 3: Fixing Extraction Issues
**Scenario**: One PDF has wrong data (e.g., wrong date extracted)

```bash
# Step 1: Delete bad catalog
rm "workdir/catalogs/Employee_Name.catalog.json"

# Step 2: Regenerate just that one
python regenerate_catalogs.py
# ⏱️  Only processes missing catalogs

# Step 3: Reprocess all
python rubric_processor.py
# ⏱️  Instant
```

---

### Use Case 4: Batch Processing
**Scenario**: Process 100+ employee files

```bash
# Step 1: Copy all PDFs
cp /network/drive/employees/*.pdf data/input/
# 150 PDFs copied

# Step 2: Generate catalogs (one-time cost)
python regenerate_catalogs.py
# ⏱️  150 PDFs × 2 min = ~5 hours
# 💡 Run overnight or in background

# Step 3: Generate reports (anytime)
python rubric_processor.py
# ⏱️  <1 minute for all 150 files

# Step 4: Share results
cp workdir/rubric_based_results.csv /shared/reports/
```

---

## 🔧 Advanced Options

### Regenerate Specific Files Only

```bash
# Delete catalogs you want to regenerate
rm workdir/catalogs/Wu*.catalog.json
rm workdir/catalogs/Balder*.catalog.json

# Run regeneration (only processes missing ones)
python regenerate_catalogs.py
```

### Debug Specific Page Extraction

```bash
# Edit debug_document_title.py to change PDF and page number
python debug_document_title.py

# Shows:
# - Raw OCR text
# - Extracted values
# - Catalog comparison
```

### Check Extraction Quality

```bash
# View catalog file directly
cat "workdir/catalogs/Employee_Name.catalog.json" | jq .

# Check specific page
cat "workdir/catalogs/Employee_Name.catalog.json" | jq '.pages[18]'
```

---

## 📊 Understanding the Output

### Catalog Files (`workdir/catalogs/*.catalog.json`)

```json
{
  "document_id": "Wu, Qianyi 9963.pdf",
  "total_pages": 24,
  "pages": [
    {
      "page_number": 19,
      "page_title": "Form I-9 Section 2 & 3",
      "extracted_values": {
        "section_3_signature_date": "02/15/2023",
        "section_3_document_title": "EAD issued by DHS...",
        "list_a_expiration_date": "03/06/2025"
      }
    }
  ]
}
```

### CSV Report (`workdir/rubric_based_results.csv`)

| employee_id | form_type | status | bucket_3_score | employer_signature_date |
|-------------|-----------|--------|----------------|------------------------|
| Wu_Qianyi_9963 | re-verification | COMPLETE_SUCCESS | 25/25 | 02/15/2023 |
| Balder_Pauline_0540 | new hire | COMPLETE_SUCCESS | 25/25 | 09/03/2020 |

---

## ❓ FAQ

### Q: Do I need to regenerate catalogs every time?
**A**: No! Only regenerate when:
- You have new PDFs
- Extraction was wrong (bad OCR)
- You improved the extraction prompts

### Q: Can I process just one file?
**A**: Yes! Just delete its catalog and run `regenerate_catalogs.py`

### Q: How much does it cost?
**A**: 
- Catalog generation: ~$0.01-0.05 per PDF (Gemini API)
- Rubric processing: FREE (no API calls)

### Q: Can I run this on a server?
**A**: Yes! Both scripts are command-line tools:
```bash
nohup python regenerate_catalogs.py > catalog.log 2>&1 &
python rubric_processor.py
```

### Q: What if extraction is wrong?
**A**: 
1. Check the catalog.json file to see what was extracted
2. Improve prompts in `hri9/catalog/page_analyzer.py`
3. Delete catalog and regenerate
4. Or manually edit the catalog.json file

### Q: Can I customize business rules?
**A**: Yes! Edit `rubric_processor.py`:
- Scoring thresholds
- Validation rules
- Field requirements
- Form type detection logic

---

## 🎓 Pro Tips

1. **Generate catalogs once, iterate on rules many times**
   - Saves time and money
   - Faster development cycle

2. **Keep catalogs in version control**
   - Track extraction changes over time
   - Compare before/after improvements

3. **Use debug scripts for troubleshooting**
   - `debug_document_title.py` for extraction issues
   - Check logs for validation warnings

4. **Batch process overnight**
   - Catalog generation is slow
   - Run large batches when you're not waiting

5. **Manual corrections are OK**
   - Edit catalog.json files directly if needed
   - Rerun rubric processor instantly

---

## 📞 Need Help?

- Check `README.md` for detailed documentation
- Review `I9_RUBRIC_SPECIFICATION.md` for business rules
- Look at logs in `catalog_regeneration.log`
- Examine catalog.json files for extraction details
