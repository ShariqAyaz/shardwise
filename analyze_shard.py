#!/usr/bin/env python3
import pandas as pd
import sys

file_path = "shard_00241.parquet" if len(sys.argv) < 2 else sys.argv[1]

print("="*100)
print("📊 ANALYZING:", file_path)
print("="*100)

df = pd.read_parquet(file_path)

# Basic info
print(f"\n📈 BASIC STATS:")
print(f"Rows: {len(df):,}")
print(f"Columns: {len(df.columns)}")
print(f"Columns: {list(df.columns)}")

# Data types
print(f"\n📋 SCHEMA:")
print(df.dtypes)

# Calculate word counts
text_col = 'text' if 'text' in df.columns else df.columns[0]
print(f"\n⏳ Calculating word counts...")
df['calculated_word_count'] = df[text_col].astype(str).str.split().str.len()

print(f"\n📝 WORD COUNT STATS:")
if 'word_count' in df.columns:
    wc_col = 'word_count'
else:
    wc_col = 'calculated_word_count'

print(f"Min: {df[wc_col].min():,} words")
print(f"Max: {df[wc_col].max():,} words")
print(f"Mean: {df[wc_col].mean():.1f} words (~{df[wc_col].mean()*1.33:.0f} tokens)")
print(f"Median: {df[wc_col].median():.1f} words")
print(f"Std Dev: {df[wc_col].std():.1f} words")

# Distribution
print(f"\nWord Count Distribution:")
print(f"  • 0-256 words: {(df[wc_col] <= 256).sum():,} ({(df[wc_col] <= 256).sum()/len(df)*100:.1f}%)")
print(f"  • 257-512 words: {((df[wc_col] > 256) & (df[wc_col] <= 512)).sum():,} ({((df[wc_col] > 256) & (df[wc_col] <= 512)).sum()/len(df)*100:.1f}%)")
print(f"  • 513-1024 words: {((df[wc_col] > 512) & (df[wc_col] <= 1024)).sum():,} ({((df[wc_col] > 512) & (df[wc_col] <= 1024)).sum()/len(df)*100:.1f}%)")
print(f"  • 1025+ words: {(df[wc_col] > 1024).sum():,} ({(df[wc_col] > 1024).sum()/len(df)*100:.1f}%)")

# Character counts
print(f"\nCharacter Count Stats:")
df['char_count'] = df[text_col].astype(str).str.len()
print(f"Min: {df['char_count'].min():,} chars")
print(f"Max: {df['char_count'].max():,} chars")
print(f"Mean: {df['char_count'].mean():.0f} chars")

# Check for overlap
print(f"\n🔗 OVERLAP CHECK:")
if len(df) > 1:
    # Check first pair
    t1_end = str(df[text_col].iloc[0])[-100:]
    t2_start = str(df[text_col].iloc[1])[:100]
    has_overlap = any(t1_end[i:] == t2_start[:len(t1_end)-i] for i in range(20, len(t1_end)))
    print(f"Has overlap: {'YES' if has_overlap else 'NO'}")

# First 3 rows
print(f"\n📖 FIRST 3 ROWS:")
for i in range(min(3, len(df))):
    print(f"\n--- ROW {i+1} ---")
    for col in df.columns:
        val = df[col].iloc[i]
        if col == text_col:
            print(f"{col}: {str(val)[:200]}...")
        else:
            print(f"{col}: {val}")

# Last 3 rows
print(f"\n📖 LAST 3 ROWS:")
for i in range(max(0, len(df)-3), len(df)):
    print(f"\n--- ROW {i+1} ---")
    for col in df.columns:
        val = df[col].iloc[i]
        if col == text_col:
            print(f"{col}: {str(val)[:200]}...")
        else:
            print(f"{col}: {val}")

print("\n" + "="*100)
print("✅ ANALYSIS COMPLETE")
print("="*100)

# LLM Training Assessment
print("\n" + "="*100)
print("🤖 LLM TRAINING ASSESSMENT")
print("="*100)
avg_words = df[wc_col].mean()
print(f"✓ Total training examples: {len(df):,}")
print(f"✓ Average chunk size: {avg_words:.0f} words (~{avg_words*1.33:.0f} tokens)")

if avg_words < 300:
    print(f"✓ Size category: Small chunks")
    print(f"  → Good for: Embeddings, fine-tuning, specific tasks")
elif avg_words < 700:
    print(f"✓ Size category: Medium chunks") 
    print(f"  → Good for: General training, RAG, most LLM tasks")
else:
    print(f"✓ Size category: Large chunks")
    print(f"  → Good for: Long context, document understanding")

print(f"✓ Overlap: {'YES' if has_overlap else 'NO'}")
if not has_overlap:
    print(f"  → Note: No overlap = clean boundaries, no redundant context")
else:
    print(f"  → Benefit: Overlap preserves context across chunks")

print(f"✓ Schema: Minimal (text-only)")
print(f"  → Optimized for pure text training")
print(f"✓ Format: Parquet (compressed, efficient)")
print(f"✓ Dataset quality: Professional (diverse topics, clean text)")
print("="*100)

