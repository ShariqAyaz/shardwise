# Configuration Guide - Flexible Data Processing

This guide explains how to configure ShardWise to handle YOUR data, based on real-world requirements.

## Your Data Profile (from shard_00241.parquet)

```
✓ 53,248 training examples
✓ Average: 769 words (~1023 tokens)
✓ Range: 43 - 82,419 words
✓ Distribution:
  • 24.7% are 0-256 words
  • 29.1% are 257-512 words  
  • 28.3% are 513-1024 words
  • 17.8% are 1025+ words
✓ Schema: Minimal (text only)
✓ No overlap between chunks
```

## Configuration Philosophy

**FLEXIBLE by default** - The config accepts wide ranges and lets YOU decide limits based on your data.

## 1️⃣ Chunking Configuration

### Current Settings (Permissive)

```yaml
chunking:
  method: "fixed"
  min_chunk_size: 50       # Accept chunks as small as 50 words
  max_chunk_size: 100000   # Accept chunks up to 100K words
  overlap: 0               # No overlap
  enforce_limits: false    # Don't reject, just accept all
```

### How to Adjust

**To match your data exactly:**
```yaml
min_chunk_size: 43        # Your minimum
max_chunk_size: 82419     # Your maximum
overlap: 0                # No overlap (like your data)
```

**To filter outliers:**
```yaml
min_chunk_size: 100       # Filter very short chunks
max_chunk_size: 5000      # Filter extremely long chunks
enforce_limits: true      # Reject chunks outside limits
```

**To add overlap:**
```yaml
overlap: 100              # 100-word overlap for context
```

## 2️⃣ Schema Configuration (What to Store)

### Current Setting (Minimal)

```yaml
sharding:
  schema:
    active: "minimal"     # Store text only
```

This matches your shard_00241.parquet (text-only).

### Available Options

**Minimal (Text Only)**
```yaml
active: "minimal"
# Stores: text
# File size: Smallest
# Use case: Pure training data
```

**Basic (Text + ID)**
```yaml
active: "basic"
# Stores: id, text
# File size: Small
# Use case: Tracking, deduplication
```

**Standard (Text + Metadata)**
```yaml
active: "standard"
# Stores: id, text, source_file, language, word_count
# File size: Medium
# Use case: Analysis, filtering, traceability
```

**Full (Everything)**
```yaml
active: "full"
# Stores: All 16 columns
# File size: Largest
# Use case: Comprehensive analysis, quality metrics
```

**Custom (Your Choice)**
```yaml
active: "custom"
custom: ["text", "word_count", "source_file"]
# Stores: Only what you specify
# Use case: Specific requirements
```

## 3️⃣ Row Limits Per Shard

### Size-Based (Default)

```yaml
max_shard_size_mb: 500
max_rows_per_shard: null    # Auto-calculate based on size
```

Shards will be ~500MB each, rows vary based on content.

### Row-Based (Fixed Count)

```yaml
max_rows_per_shard: 10000   # Exactly 10K rows per shard
```

For your data (769 words avg):
- 10,000 rows ≈ 7.7M words ≈ ~40MB
- 50,000 rows ≈ 38.5M words ≈ ~200MB

## 4️⃣ Quality Filtering

### Permissive (Current - Accept All)

```yaml
quality:
  min_words: 20
  max_words: 100000
  min_unique_words_ratio: 0.0    # Accept all
  max_repetition_ratio: 1.0      # Accept all
  min_alpha_ratio: 0.0           # Accept all
  strict_filtering: false        # Warn only, don't reject
```

### Strict (Enforce Quality)

```yaml
quality:
  min_words: 100                 # Filter very short
  max_words: 10000               # Filter very long
  min_unique_words_ratio: 0.3    # Require vocabulary diversity
  max_repetition_ratio: 0.5      # Reject highly repetitive
  min_alpha_ratio: 0.7           # Require mostly text
  strict_filtering: true         # Actually reject bad chunks
```

## 5️⃣ Common Configuration Scenarios

### Scenario 1: Match Your Existing Data

**Goal:** Process data like shard_00241.parquet

```yaml
chunking:
  min_chunk_size: 50
  max_chunk_size: 100000
  overlap: 0
  enforce_limits: false

sharding:
  max_rows_per_shard: null
  schema:
    active: "minimal"

quality:
  min_words: 20
  max_words: 100000
  strict_filtering: false
```

**Result:** Accepts all chunk sizes, text-only output, permissive quality.

### Scenario 2: Standardized Training Data

**Goal:** Consistent chunk sizes for training

```yaml
chunking:
  min_chunk_size: 256
  max_chunk_size: 1024
  overlap: 100
  enforce_limits: true

sharding:
  max_rows_per_shard: 10000
  schema:
    active: "basic"

quality:
  min_words: 200
  max_words: 1200
  strict_filtering: true
```

**Result:** Uniform 256-1024 word chunks, predictable shards, quality enforced.

### Scenario 3: Analysis-Ready Dataset

**Goal:** Rich metadata for analysis

```yaml
chunking:
  min_chunk_size: 100
  max_chunk_size: 5000
  overlap: 0

sharding:
  max_rows_per_shard: 5000
  schema:
    active: "full"

quality:
  min_words: 100
  calculate_readability: true
  strict_filtering: false
```

**Result:** Complete metadata, quality scores, smaller manageable shards.

### Scenario 4: Production LLM Training

**Goal:** Optimized for GPT-style training

```yaml
chunking:
  min_chunk_size: 512
  max_chunk_size: 1024
  overlap: 100
  enforce_limits: true

sharding:
  max_shard_size_mb: 500
  max_rows_per_shard: null
  schema:
    active: "minimal"
  compression: "snappy"

quality:
  min_words: 400
  max_words: 1200
  min_unique_words_ratio: 0.3
  strict_filtering: true
```

**Result:** Production-ready, high-quality, optimized chunks.

## 6️⃣ Quick Reference: Config Values

### Chunk Sizes

| Setting | Description | Your Data | Typical |
|---------|-------------|-----------|---------|
| min_chunk_size | Minimum words | 50 | 100-512 |
| max_chunk_size | Maximum words | 100000 | 512-2048 |
| overlap | Overlap words | 0 | 0-200 |

### Row Limits

| Setting | Effect | Example |
|---------|--------|---------|
| null | Size-based | Auto-calculate |
| 1000 | Fixed | 1000 rows/shard |
| 10000 | Fixed | 10K rows/shard |
| 50000 | Fixed | 50K rows/shard |

### Quality Thresholds

| Metric | Permissive | Moderate | Strict |
|--------|------------|----------|--------|
| min_words | 0-50 | 100-200 | 256-512 |
| max_words | 50K-100K | 5K-10K | 1K-2K |
| unique_ratio | 0.0 | 0.2 | 0.3+ |
| repetition_ratio | 1.0 | 0.7 | 0.5 |
| alpha_ratio | 0.0 | 0.5 | 0.7+ |

## 7️⃣ How to Apply Changes

1. **Edit config:**
   ```bash
   nano config/pipeline_config.yaml
   ```

2. **Test on sample:**
   ```bash
   # Process a few files to test
   python scripts/chunk_text.py
   ```

3. **Run full pipeline:**
   ```bash
   python workflows/main_pipeline.py
   ```

4. **Verify output:**
   ```bash
   python3 analyze_shard.py dataset/shards/part-0000.parquet
   ```

## 8️⃣ Best Practices

✅ **DO:**
- Start permissive, tighten gradually
- Analyze your existing data first
- Test config changes on small batches
- Match chunk sizes to your model's context window
- Use minimal schema for pure training

❌ **DON'T:**
- Set limits tighter than your source data
- Over-filter without analyzing impact
- Use full schema unless you need it
- Forget to test after config changes

## Summary

The config is now **FLEXIBLE** - it accepts your real-world data (43-82K words, text-only) by default. Adjust limits based on YOUR needs:

- **Chunk sizes:** Set min/max to match your data
- **Schema:** Choose what columns to store
- **Row limits:** Control shard sizes
- **Quality:** Decide filtering strictness

All limits are **configurable** and **optional** - you control what gets stored and how!

