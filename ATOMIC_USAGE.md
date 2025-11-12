# Atomic Pipeline Usage Guide

This guide shows how to run and debug individual pipeline stages independently.

## Philosophy

**Don't run the full pipeline blindly.** Each stage is atomic and can be:
- Run independently
- Tested on specific files
- Inspected for quality
- Tuned and re-run

## Atomic Stages

```
1. Extract  → intermediate/extracted/
2. Clean    → intermediate/cleaned/
3. Chunk    → intermediate/chunks/
4. Filter   → intermediate/chunks/ (filtered)
5. Shard    → dataset/shards/
6. Export   → dataset/annotation_ready/
```

---

## Stage 1: Text Extraction

### Run extraction
```bash
# Extract all files
python scripts/extract_text.py

# Extract a single file
python scripts/extract_text.py --file raw_data/pdf/document.pdf
```

### Inspect results
```bash
# See what was extracted
python scripts/inspect_pipeline.py --stage extracted

# Manually check a file
cat intermediate/extracted/text/document.txt
cat intermediate/extracted/text/document.json
```

### Test on specific folder
```bash
# Create test folder
mkdir -p raw_data/test_pdf
cp raw_data/pdf/problematic.pdf raw_data/test_pdf/

# Extract just that folder (modify script or use single file mode)
python scripts/extract_text.py --file raw_data/test_pdf/problematic.pdf
```

---

## Stage 2: Text Cleaning

### Run cleaning
```bash
# Clean all extracted files
python scripts/clean_text.py
```

### Inspect results
```bash
# See cleaning statistics
python scripts/inspect_pipeline.py --stage cleaned

# Compare before/after
echo "BEFORE:"
head -20 intermediate/extracted/text/document.txt
echo "AFTER:"
head -20 intermediate/cleaned/text/document.txt
```

### Check specific metrics
```bash
# See what was removed
cat intermediate/cleaned/text/document.json | python -m json.tool
```

### Tune cleaning rules

Edit `config/pipeline_config.yaml`:
```yaml
cleaning:
  remove_urls: true              # Toggle this
  min_line_length: 10            # Adjust this
  languages: ["en"]              # Change this
```

Then re-run:
```bash
# Clean again with new settings
rm -rf intermediate/cleaned/
python scripts/clean_text.py
python scripts/inspect_pipeline.py --stage cleaned
```

---

## Stage 3: Chunking

### Run chunking
```bash
python scripts/chunk_text.py
```

### Inspect results
```bash
# See chunk statistics
python scripts/inspect_pipeline.py --stage chunks

# Count chunks created
find intermediate/chunks -name "*.json" | wc -l

# Read a specific chunk
cat intermediate/chunks/text/document/CHUNK-UUID.json | python -m json.tool
```

### Tune chunking

Edit `config/pipeline_config.yaml`:
```yaml
chunking:
  method: "sentence"      # Try: sentence, paragraph, fixed
  min_chunk_size: 500     # Adjust minimum
  max_chunk_size: 2000    # Adjust maximum
  overlap: 100            # Tune overlap
```

Re-run:
```bash
rm -rf intermediate/chunks/
python scripts/chunk_text.py
python scripts/inspect_pipeline.py --stage chunks
```

---

## Stage 4: Deduplication & Filtering

### Run filtering
```bash
python scripts/dedup_filter.py
```

### Inspect results
```bash
# Before filtering
find intermediate/chunks -name "*.json" | wc -l

# Run filter
python scripts/dedup_filter.py

# After filtering
find intermediate/chunks -name "*.json" | wc -l

# See what was removed (check logs)
```

### Tune quality filters

Edit `config/pipeline_config.yaml`:
```yaml
quality:
  min_words: 20                    # Minimum words
  min_unique_words_ratio: 0.3      # Vocabulary diversity
  max_repetition_ratio: 0.5        # Repetition threshold
  min_alpha_ratio: 0.7             # Alphabetic character ratio
```

### Test filtering on sample data
```bash
# Backup chunks first
cp -r intermediate/chunks intermediate/chunks_backup

# Run with different settings
python scripts/dedup_filter.py

# Compare
echo "Before: $(find intermediate/chunks_backup -name '*.json' | wc -l)"
echo "After: $(find intermediate/chunks -name '*.json' | wc -l)"

# Restore if needed
rm -rf intermediate/chunks
mv intermediate/chunks_backup intermediate/chunks
```

---

## Stage 5: Shard Creation

### Run sharding
```bash
python scripts/create_shards.py
```

### Inspect results
```bash
# See shards created
python scripts/inspect_pipeline.py --stage shards

# List shards
ls -lh dataset/shards/

# Read a shard
python -c "import pandas as pd; df = pd.read_parquet('dataset/shards/part-0000.parquet'); print(df.head())"
```

### Tune shard size

Edit `config/pipeline_config.yaml`:
```yaml
sharding:
  max_shard_size_mb: 500     # Adjust shard size
  compression: "snappy"       # Try: snappy, gzip, brotli
```

---

## Stage 6: Annotation Export

### Run export
```bash
python scripts/export_annotation.py
```

### Inspect results
```bash
# See exported files
python scripts/inspect_pipeline.py --stage annotation

# Check JSONL content
head -1 dataset/annotation_ready/finance.jsonl | python -m json.tool

# Count chunks per niche
wc -l dataset/annotation_ready/*.jsonl
```

### Tune niche categorisation

Edit `config/pipeline_config.yaml`:
```yaml
annotation:
  niches:
    - name: "finance"
      keywords: ["financial", "investment", "trading"]  # Add more keywords
```

Re-run:
```bash
rm -rf dataset/annotation_ready/
python scripts/export_annotation.py
```

---

## Inspection & Debugging

### Inspect any stage
```bash
# Inspect everything
python scripts/inspect_pipeline.py

# Inspect specific stage
python scripts/inspect_pipeline.py --stage raw
python scripts/inspect_pipeline.py --stage extracted
python scripts/inspect_pipeline.py --stage cleaned
python scripts/inspect_pipeline.py --stage chunks
python scripts/inspect_pipeline.py --stage shards
python scripts/inspect_pipeline.py --stage annotation
```

### Compare before/after
```bash
# Create comparison script
cat > compare.sh << 'EOF'
#!/bin/bash
echo "Raw files: $(find raw_data -type f | wc -l)"
echo "Extracted: $(find intermediate/extracted -name '*.txt' | wc -l)"
echo "Cleaned: $(find intermediate/cleaned -name '*.txt' | wc -l)"
echo "Chunks: $(find intermediate/chunks -name '*.json' | wc -l)"
echo "Shards: $(find dataset/shards -name '*.parquet' | wc -l)"
echo "JSONL: $(find dataset/annotation_ready -name '*.jsonl' | wc -l)"
EOF

chmod +x compare.sh
./compare.sh
```

---

## Testing on Subset

### Test on single file
```bash
# Extract
python scripts/extract_text.py --file raw_data/pdf/test.pdf

# Clean (processes all extracted)
python scripts/clean_text.py

# Chunk
python scripts/chunk_text.py

# Filter
python scripts/dedup_filter.py

# Inspect
python scripts/inspect_pipeline.py --stage chunks
```

### Test on folder subset
```bash
# Create test folder
mkdir -p raw_data/pdf_test
cp raw_data/pdf/doc1.pdf raw_data/pdf/doc2.pdf raw_data/pdf_test/

# Temporarily rename original folder
mv raw_data/pdf raw_data/pdf_full
mv raw_data/pdf_test raw_data/pdf

# Run pipeline stages
python scripts/extract_text.py
python scripts/clean_text.py
python scripts/chunk_text.py

# Restore
mv raw_data/pdf raw_data/pdf_test
mv raw_data/pdf_full raw_data/pdf
```

---

## Iterative Tuning Workflow

1. **Test on small sample**
   ```bash
   # Use 2-3 files only
   mkdir raw_data/test
   cp raw_data/pdf/sample.pdf raw_data/test/
   ```

2. **Run one stage at a time**
   ```bash
   python scripts/extract_text.py
   python scripts/inspect_pipeline.py --stage extracted
   ```

3. **Tune settings** in `config/pipeline_config.yaml`

4. **Clean and re-run**
   ```bash
   rm -rf intermediate/cleaned/
   python scripts/clean_text.py
   python scripts/inspect_pipeline.py --stage cleaned
   ```

5. **Validate quality**
   ```bash
   # Check specific output
   cat intermediate/cleaned/text/sample.txt
   cat intermediate/cleaned/text/sample.json
   ```

6. **Scale up when satisfied**
   ```bash
   # Now run on full data
   python scripts/extract_text.py
   python scripts/clean_text.py
   python scripts/chunk_text.py
   ```

---

## Quick Commands

```bash
# See what you have at each stage
./compare.sh

# Inspect everything
python scripts/inspect_pipeline.py

# Clean a stage and re-run
rm -rf intermediate/cleaned/
python scripts/clean_text.py

# Test one file
python scripts/extract_text.py --file raw_data/pdf/test.pdf

# Check quality of chunks
python scripts/inspect_pipeline.py --stage chunks
```

---

## Summary

**Don't use:** `python workflows/main_pipeline.py` blindly

**Do use:**
1. Run stages individually
2. Inspect after each stage
3. Tune config based on inspection
4. Test on small samples
5. Scale up when satisfied

**Each stage is atomic and independent.**

