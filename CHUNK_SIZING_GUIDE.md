# Chunk (Atomic Blob) Sizing Guide

## What is a Chunk?

A **chunk** (or atomic blob) is a single text unit stored in your Parquet files. Each row in the Parquet shard contains one chunk.

## Standard Blob Sizes

### Recommended Configurations

| Use Case | Words | Tokens* | Config |
|----------|-------|---------|--------|
| **Embeddings** | 256-384 | 340-512 | Small, focused |
| **Fine-tuning** | 384-512 | 512-680 | Standard |
| **General Training** | 512-1024 | 680-1360 | Medium |
| **Long Context** | 1024-2048 | 1360-2720 | Large |
| **Document Understanding** | 2048+ | 2720+ | Extra Large |

*Approximate: 1 word ≈ 1.33 tokens (English average)

## Current Configuration

Edit `config/pipeline_config.yaml`:

```yaml
chunking:
  method: "fixed"  # Use fixed for blob/atomic chunks
  min_chunk_size: 256   # Minimum words per chunk
  max_chunk_size: 512   # Maximum words per chunk
  overlap: 50           # Words to overlap between chunks
  preserve_sentence_boundaries: false  # false for fixed blobs
```

## Size Presets

### Small Blobs (256-512 words)
**Best for:**
- Embedding generation
- Semantic search
- Fine-tuning on specific tasks
- Question-answering systems

**Configuration:**
```yaml
chunking:
  method: "fixed"
  min_chunk_size: 256
  max_chunk_size: 512
  overlap: 50
```

### Medium Blobs (512-1024 words) ⭐ **RECOMMENDED DEFAULT**
**Best for:**
- General pre-training
- Retrieval-augmented generation (RAG)
- Most LLM training tasks
- Balanced context and specificity

**Configuration:**
```yaml
chunking:
  method: "fixed"
  min_chunk_size: 512
  max_chunk_size: 1024
  overlap: 100
```

### Large Blobs (1024-2048 words)
**Best for:**
- Long document understanding
- Context-heavy tasks
- Summarization
- Models with large context windows

**Configuration:**
```yaml
chunking:
  method: "fixed"
  min_chunk_size: 1024
  max_chunk_size: 2048
  overlap: 150
```

## Token Count Reference

| Words | Tokens (approx) | Model Context Used |
|-------|-----------------|-------------------|
| 256 | 340 | 8.5% of 4K context |
| 512 | 680 | 17% of 4K context |
| 1024 | 1,360 | 34% of 4K context |
| 2048 | 2,720 | 68% of 4K context |

## How Overlap Works

**Overlap** preserves context between chunks:

```
Chunk 1: [words 1-512] + overlap [words 463-512]
Chunk 2: [words 463-512] + [words 513-1024] + overlap [words 975-1024]
Chunk 3: [words 975-1024] + [words 1025-1536]
```

**Typical overlap:**
- Small chunks: 25-50 words (10-20%)
- Medium chunks: 50-100 words (10-15%)
- Large chunks: 100-200 words (10-15%)

## Real-World Examples

### Example 1: GPT-3 Style Training
```yaml
chunking:
  method: "fixed"
  min_chunk_size: 512
  max_chunk_size: 1024
  overlap: 100
```
Result: ~680-1360 tokens per chunk

### Example 2: BERT/Embedding Models
```yaml
chunking:
  method: "fixed"
  min_chunk_size: 256
  max_chunk_size: 384
  overlap: 50
```
Result: ~340-512 tokens per chunk (fits BERT's 512 token limit)

### Example 3: Long Context Models (GPT-4, Claude)
```yaml
chunking:
  method: "fixed"
  min_chunk_size: 1024
  max_chunk_size: 2048
  overlap: 150
```
Result: ~1360-2720 tokens per chunk

## How to Calculate Your Chunk Size

1. **Know your model's context window:**
   - BERT: 512 tokens
   - GPT-3: 4,096 tokens
   - GPT-4: 8,192-32,768 tokens
   - Claude 3: 200,000 tokens

2. **Target 15-30% of context window:**
   ```
   chunk_size = (context_window * 0.2) / 1.33
   ```
   
   Examples:
   - GPT-3 (4K): (4096 * 0.2) / 1.33 = ~615 words
   - GPT-4 (8K): (8192 * 0.2) / 1.33 = ~1230 words

3. **Round to standard sizes:**
   - 615 words → use 512-1024 (medium)
   - 1230 words → use 1024-2048 (large)

## Checking Your Chunk Sizes

After running the pipeline:

```bash
# View word counts in Parquet
python -c "
import pandas as pd
df = pd.read_parquet('dataset/shards/part-0000.parquet')
print(f'Min words: {df[\"word_count\"].min()}')
print(f'Max words: {df[\"word_count\"].max()}')
print(f'Avg words: {df[\"word_count\"].mean():.0f}')
"
```

## Quick Reference

**Most Common Setup (General Purpose):**
```yaml
chunking:
  method: "fixed"
  min_chunk_size: 512
  max_chunk_size: 1024
  overlap: 100
```

**Why these numbers?**
- 512-1024 words = ~680-1360 tokens
- Fits well in most model contexts
- Good balance of specificity and context
- Industry standard for training data

## Tips

✅ **DO:**
- Use fixed sizes for consistent training
- Set overlap at 10-15% of chunk size
- Test with your specific model's context window
- Start with 512-1024 words (standard)

❌ **DON'T:**
- Make chunks too small (<128 words) - loses context
- Make chunks too large (>4096 words) - hard to process
- Use zero overlap - loses cross-chunk context
- Exceed your model's context window

## Current Pipeline Default

The pipeline is now configured with:
- **512 words** per chunk (standard atomic blob)
- **50 words** overlap
- **Fixed method** (blob-style, no sentence boundaries)

This creates ~680-token chunks, suitable for most LLM training tasks.

To change, edit `config/pipeline_config.yaml` and re-run the pipeline.

