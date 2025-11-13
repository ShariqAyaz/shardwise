# Parquet Schema Configuration Guide

This guide explains how to control Parquet schema (columns) and row limits in ShardWise.

## Configuration Location

Edit `config/pipeline_config.yaml` in the `sharding` section:

```yaml
sharding:
  format: "parquet"
  max_shard_size_mb: 500
  max_rows_per_shard: null  # Row-based limit (optional)
  compression: "snappy"
  
  schema:
    active: "standard"  # Choose schema preset
    # ... schema definitions
```

## 1️⃣ Control Columns (Schema)

### Built-in Schema Presets

**Minimal** - Just the data:
```yaml
schema:
  active: "minimal"
```
Columns: `text`

**Basic** - Data + ID:
```yaml
schema:
  active: "basic"
```
Columns: `id`, `text`

**Standard** (default) - Data + key metadata:
```yaml
schema:
  active: "standard"
```
Columns: `id`, `text`, `source_file`, `language`, `word_count`

**Full** - All available columns:
```yaml
schema:
  active: "full"
```
Columns: All 16 columns including quality metrics

### Custom Schema

Define your own column selection:

```yaml
schema:
  active: "custom"
  custom: ["id", "text", "word_count", "language"]
```

### Available Columns

| Column | Description | Type |
|--------|-------------|------|
| `text` | Chunk text content | string |
| `id` | Unique chunk identifier | string |
| `source_file` | Original file path | string |
| `filename` | Original filename | string |
| `file_type` | File type (pdf, html, text) | string |
| `language` | Detected language | string |
| `chunk_index` | Position in document | int |
| `word_count` | Number of words | int |
| `char_count` | Number of characters | int |
| `chunking_method` | Chunking strategy used | string |
| `chunking_timestamp` | When chunked | string |
| `unique_words` | Unique word count | int |
| `unique_ratio` | Vocabulary diversity | float |
| `repetition_ratio` | Repetition score | float |
| `alpha_ratio` | Alphabetic character ratio | float |
| `readability_score` | Flesch reading ease | float |

## 2️⃣ Control Rows Per Shard

### Size-Based (Default)

Shards created based on file size:

```yaml
sharding:
  max_shard_size_mb: 500
  max_rows_per_shard: null
```

The system calculates how many rows fit in 500MB.

### Row-Based (Fixed Row Limit)

Set a specific number of rows per shard:

```yaml
sharding:
  max_shard_size_mb: 500
  max_rows_per_shard: 1000  # Exactly 1000 rows per shard
```

This creates shards with exactly 1000 rows each (regardless of size).

## 3️⃣ Usage Examples

### Example 1: Minimal Schema for Training

Just text data for pre-training:

```yaml
sharding:
  max_rows_per_shard: 10000
  schema:
    active: "minimal"
```

Result: Large shards with only text column.

### Example 2: Basic Schema with Row Limit

ID + text, 5000 rows per shard:

```yaml
sharding:
  max_rows_per_shard: 5000
  schema:
    active: "basic"
```

Result: Manageable shards with ID and text.

### Example 3: Custom Schema for Analysis

Specific columns for analysis:

```yaml
sharding:
  max_rows_per_shard: 2000
  schema:
    active: "custom"
    custom: ["text", "language", "word_count", "readability_score"]
```

Result: Analysis-ready shards with selected metrics.

### Example 4: Full Schema, Size-Based

Everything, optimised by size:

```yaml
sharding:
  max_shard_size_mb: 100
  max_rows_per_shard: null
  schema:
    active: "full"
```

Result: Smaller shards (100MB) with all columns.

## 4️⃣ How to Apply Changes

1. **Edit configuration:**
   ```bash
   nano config/pipeline_config.yaml
   ```

2. **Delete old shards:**
   ```bash
   rm -rf dataset/shards/*.parquet
   ```

3. **Re-run sharding:**
   ```bash
   # Full pipeline
   python workflows/main_pipeline.py
   
   # Or just sharding step
   python scripts/create_shards.py
   ```

4. **View new schema:**
   ```bash
   python scripts/view_parquet.py
   ```

## 5️⃣ Best Practices

### For Training/Pre-training:
```yaml
schema:
  active: "minimal"  # Just text
max_rows_per_shard: 10000  # Large batches
```

### For Fine-tuning:
```yaml
schema:
  active: "basic"  # ID + text
max_rows_per_shard: 5000
```

### For Analysis:
```yaml
schema:
  active: "standard"  # Key metadata
max_rows_per_shard: 2000
```

### For Debugging:
```yaml
schema:
  active: "full"  # Everything
max_rows_per_shard: 100  # Small shards
```

## 6️⃣ Tips

- **Smaller schemas** = smaller files, faster loading
- **Row limits** make shards predictable and easy to manage
- **Minimal schema** is best for pure text training
- **Full schema** is useful for data exploration and quality analysis
- You can change schema anytime—just re-run the sharding step

## 7️⃣ Verify Your Schema

After running the pipeline:

```bash
# View schema and data
python scripts/view_parquet.py

# Check what columns exist
python -c "import pandas as pd; df = pd.read_parquet('dataset/shards/part-0000.parquet'); print(df.columns.tolist())"

# Check row count
python -c "import pandas as pd; df = pd.read_parquet('dataset/shards/part-0000.parquet'); print(f'Rows: {len(df)}')"
```

## Summary

✅ Control schema via `schema.active` in config
✅ Use presets: minimal, basic, standard, full
✅ Define custom column lists
✅ Set row limits with `max_rows_per_shard`
✅ Re-run sharding to apply changes

