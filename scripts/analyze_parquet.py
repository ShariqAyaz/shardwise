"""
Comprehensive Parquet File Analyzer
Analyzes structure, content, and overlap in parquet files
"""

import sys
from pathlib import Path
import pandas as pd
import json


def detect_overlap(df, text_column='text'):
    """Detect if chunks have overlap by comparing consecutive rows"""
    if text_column not in df.columns or len(df) < 2:
        return None
    
    overlaps = []
    total_compared = 0
    
    # Sample pairs to check (first few, middle, last few)
    indices_to_check = []
    
    # First 5 pairs
    indices_to_check.extend(range(min(5, len(df) - 1)))
    
    # Middle pairs
    mid = len(df) // 2
    indices_to_check.extend(range(max(0, mid - 2), min(len(df) - 1, mid + 3)))
    
    # Last 5 pairs
    indices_to_check.extend(range(max(0, len(df) - 6), len(df) - 1))
    
    indices_to_check = sorted(set(indices_to_check))
    
    for i in indices_to_check:
        if i >= len(df) - 1:
            continue
            
        text1 = str(df[text_column].iloc[i])
        text2 = str(df[text_column].iloc[i + 1])
        
        if not text1 or not text2:
            continue
        
        # Get last 200 chars of text1 and first 200 chars of text2
        end_text1 = text1[-200:]
        start_text2 = text2[:200]
        
        # Check for overlap
        max_overlap = 0
        for length in range(min(len(end_text1), len(start_text2)), 19, -1):  # Min 20 chars
            if end_text1[-length:] == start_text2[:length]:
                max_overlap = length
                break
        
        if max_overlap > 0:
            overlaps.append({
                'row_pair': (i, i + 1),
                'overlap_chars': max_overlap,
                'overlap_words': len(end_text1[-max_overlap:].split())
            })
        
        total_compared += 1
    
    if overlaps:
        avg_overlap_chars = sum(o['overlap_chars'] for o in overlaps) / len(overlaps)
        avg_overlap_words = sum(o['overlap_words'] for o in overlaps) / len(overlaps)
        return {
            'has_overlap': True,
            'overlapping_pairs': len(overlaps),
            'total_compared': total_compared,
            'overlap_rate': len(overlaps) / total_compared if total_compared > 0 else 0,
            'avg_overlap_chars': avg_overlap_chars,
            'avg_overlap_words': avg_overlap_words,
            'examples': overlaps[:3]
        }
    else:
        return {
            'has_overlap': False,
            'total_compared': total_compared,
            'overlap_rate': 0
        }


def analyze_parquet(file_path: str):
    """Comprehensive analysis of parquet file"""
    path = Path(file_path)
    
    if not path.exists():
        print(f"❌ File not found: {file_path}")
        return
    
    print("=" * 100)
    print(f"📊 COMPREHENSIVE PARQUET ANALYSIS")
    print("=" * 100)
    print(f"File: {path.name}")
    print(f"Path: {path}")
    print(f"Size: {path.stat().st_size / (1024*1024):.2f} MB")
    print()
    
    # Load parquet
    print("⏳ Loading parquet file...")
    df = pd.read_parquet(file_path)
    print(f"✅ Loaded successfully\n")
    
    # Basic stats
    print("=" * 100)
    print("📈 BASIC STATISTICS")
    print("=" * 100)
    print(f"Total Rows: {len(df):,}")
    print(f"Total Columns: {len(df.columns)}")
    print(f"Memory Usage: {df.memory_usage(deep=True).sum() / (1024*1024):.2f} MB")
    print()
    
    # Schema
    print("=" * 100)
    print("📋 SCHEMA")
    print("=" * 100)
    print(f"Columns: {', '.join(df.columns)}")
    print()
    print("Data Types:")
    for col, dtype in df.dtypes.items():
        non_null = df[col].count()
        null_pct = (len(df) - non_null) / len(df) * 100
        print(f"  • {col:25s} {str(dtype):15s} (non-null: {non_null:,}, null: {null_pct:.1f}%)")
    print()
    
    # Text column stats
    text_col = None
    for col in ['text', 'content', 'data']:
        if col in df.columns:
            text_col = col
            break
    
    if text_col:
        print("=" * 100)
        print(f"📝 TEXT COLUMN ANALYSIS ('{text_col}')")
        print("=" * 100)
        
        # Word counts
        if 'word_count' in df.columns:
            print(f"Word Count Statistics:")
            print(f"  • Min: {df['word_count'].min():,} words")
            print(f"  • Max: {df['word_count'].max():,} words")
            print(f"  • Mean: {df['word_count'].mean():.0f} words")
            print(f"  • Median: {df['word_count'].median():.0f} words")
            print(f"  • Std Dev: {df['word_count'].std():.0f} words")
        else:
            # Calculate word counts
            word_counts = df[text_col].str.split().str.len()
            print(f"Word Count Statistics (calculated):")
            print(f"  • Min: {word_counts.min():,} words")
            print(f"  • Max: {word_counts.max():,} words")
            print(f"  • Mean: {word_counts.mean():.0f} words")
            print(f"  • Median: {word_counts.median():.0f} words")
        
        # Character counts
        char_counts = df[text_col].str.len()
        print(f"\nCharacter Count Statistics:")
        print(f"  • Min: {char_counts.min():,} chars")
        print(f"  • Max: {char_counts.max():,} chars")
        print(f"  • Mean: {char_counts.mean():.0f} chars")
        print()
    
    # Additional metadata columns
    metadata_cols = [col for col in df.columns if col not in [text_col, 'id']]
    if metadata_cols:
        print("=" * 100)
        print("🏷️  METADATA COLUMNS")
        print("=" * 100)
        for col in metadata_cols:
            if df[col].dtype == 'object':
                unique = df[col].nunique()
                print(f"\n{col}:")
                print(f"  • Unique values: {unique}")
                if unique <= 20:
                    value_counts = df[col].value_counts().head(10)
                    for val, count in value_counts.items():
                        print(f"    - {val}: {count} ({count/len(df)*100:.1f}%)")
            elif df[col].dtype in ['int64', 'float64']:
                print(f"\n{col}:")
                print(f"  • Min: {df[col].min()}")
                print(f"  • Max: {df[col].max()}")
                print(f"  • Mean: {df[col].mean():.2f}")
        print()
    
    # Overlap detection
    if text_col:
        print("=" * 100)
        print("🔗 OVERLAP DETECTION")
        print("=" * 100)
        print("Analyzing consecutive rows for text overlap...")
        overlap_info = detect_overlap(df, text_col)
        
        if overlap_info:
            if overlap_info['has_overlap']:
                print(f"✅ OVERLAP DETECTED")
                print(f"  • Overlapping pairs: {overlap_info['overlapping_pairs']} out of {overlap_info['total_compared']} checked")
                print(f"  • Overlap rate: {overlap_info['overlap_rate']*100:.1f}%")
                print(f"  • Average overlap: {overlap_info['avg_overlap_chars']:.0f} characters (~{overlap_info['avg_overlap_words']:.0f} words)")
                print(f"\n  Example overlaps:")
                for ex in overlap_info.get('examples', [])[:2]:
                    print(f"    - Rows {ex['row_pair'][0]}-{ex['row_pair'][1]}: {ex['overlap_chars']} chars (~{ex['overlap_words']} words)")
            else:
                print(f"❌ NO OVERLAP DETECTED")
                print(f"  • Checked {overlap_info['total_compared']} consecutive pairs")
                print(f"  • No overlapping text found between chunks")
        print()
    
    # First 3 rows
    print("=" * 100)
    print("📖 FIRST 3 ROWS")
    print("=" * 100)
    for i in range(min(3, len(df))):
        print(f"\n{'─' * 100}")
        print(f"ROW {i + 1}:")
        print(f"{'─' * 100}")
        for col in df.columns:
            value = df[col].iloc[i]
            if col == text_col and value:
                # Show first 300 chars of text
                text_preview = str(value)[:300] + ("..." if len(str(value)) > 300 else "")
                print(f"{col}: {text_preview}")
            else:
                print(f"{col}: {value}")
    
    # Last 3 rows
    print("\n" + "=" * 100)
    print("📖 LAST 3 ROWS")
    print("=" * 100)
    for i in range(max(0, len(df) - 3), len(df)):
        print(f"\n{'─' * 100}")
        print(f"ROW {i + 1} (index {i}):")
        print(f"{'─' * 100}")
        for col in df.columns:
            value = df[col].iloc[i]
            if col == text_col and value:
                # Show first 300 chars of text
                text_preview = str(value)[:300] + ("..." if len(str(value)) > 300 else "")
                print(f"{col}: {text_preview}")
            else:
                print(f"{col}: {value}")
    
    print("\n" + "=" * 100)
    print("✅ ANALYSIS COMPLETE")
    print("=" * 100)
    
    # Summary for LLM training
    print("\n" + "=" * 100)
    print("🤖 LLM TRAINING SUITABILITY")
    print("=" * 100)
    
    if text_col and 'word_count' in df.columns:
        avg_words = df['word_count'].mean()
        print(f"• Average chunk size: {avg_words:.0f} words (~{avg_words * 1.33:.0f} tokens)")
        
        if avg_words < 256:
            print(f"• Size category: Small chunks (good for: embeddings, fine-tuning)")
        elif avg_words < 768:
            print(f"• Size category: Medium chunks (good for: general training, RAG)")
        else:
            print(f"• Size category: Large chunks (good for: long context, document understanding)")
    
    if overlap_info and overlap_info['has_overlap']:
        print(f"• Has overlap: Yes (~{overlap_info['avg_overlap_words']:.0f} words)")
        print(f"• Overlap benefit: Preserves context between chunks, reduces boundary issues")
    else:
        print(f"• Has overlap: No")
        print(f"• Note: No overlap may cause context loss at chunk boundaries")
    
    print(f"• Total training examples: {len(df):,}")
    print(f"• File optimized: ✅ Parquet format with compression")
    
    return df


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Analyze parquet file comprehensively")
    parser.add_argument('file', help='Path to parquet file')
    
    args = parser.parse_args()
    
    analyze_parquet(args.file)


if __name__ == '__main__':
    main()

