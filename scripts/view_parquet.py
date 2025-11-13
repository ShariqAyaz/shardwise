"""
Parquet Viewer Utility
Quick script to view and inspect Parquet shard contents
"""

import sys
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("Error: pandas is required. Install with: pip install pandas pyarrow")
    sys.exit(1)


def view_parquet(file_path: str, num_rows: int = None):
    """View parquet file contents"""
    path = Path(file_path)
    
    if not path.exists():
        print(f"Error: File not found: {file_path}")
        return
    
    print(f"Reading: {file_path}\n")
    
    # Read parquet file
    df = pd.read_parquet(file_path)
    
    # Basic info
    print("=" * 80)
    print("PARQUET FILE SUMMARY")
    print("=" * 80)
    print(f"Total rows: {len(df)}")
    print(f"Total columns: {len(df.columns)}")
    print(f"\nColumns: {', '.join(df.columns)}")
    print("\n" + "=" * 80)
    print("SCHEMA")
    print("=" * 80)
    print(df.dtypes)
    
    # Show data
    print("\n" + "=" * 80)
    print(f"DATA (showing {num_rows or 'all'} rows)")
    print("=" * 80)
    
    if num_rows:
        print(df.head(num_rows).to_string())
    else:
        print(df.to_string())
    
    # Show sample text
    if 'text' in df.columns:
        print("\n" + "=" * 80)
        print("SAMPLE TEXT CONTENT (first row)")
        print("=" * 80)
        print(df['text'].iloc[0][:500] + "..." if len(df['text'].iloc[0]) > 500 else df['text'].iloc[0])
    
    # Statistics
    print("\n" + "=" * 80)
    print("STATISTICS")
    print("=" * 80)
    if 'word_count' in df.columns:
        print(f"Word count - Min: {df['word_count'].min()}, Max: {df['word_count'].max()}, Avg: {df['word_count'].mean():.2f}")
    if 'char_count' in df.columns:
        print(f"Char count - Min: {df['char_count'].min()}, Max: {df['char_count'].max()}, Avg: {df['char_count'].mean():.2f}")
    if 'language' in df.columns:
        print(f"\nLanguages: {df['language'].value_counts().to_dict()}")
    if 'file_type' in df.columns:
        print(f"File types: {df['file_type'].value_counts().to_dict()}")
    
    return df


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="View Parquet shard contents")
    parser.add_argument(
        'file',
        nargs='?',
        default='dataset/shards/part-0000.parquet',
        help='Path to parquet file (default: dataset/shards/part-0000.parquet)'
    )
    parser.add_argument(
        '-n', '--rows',
        type=int,
        help='Number of rows to display (default: all)'
    )
    parser.add_argument(
        '-c', '--columns',
        nargs='+',
        help='Specific columns to display'
    )
    parser.add_argument(
        '--list',
        action='store_true',
        help='List all parquet files in dataset/shards/'
    )
    
    args = parser.parse_args()
    
    if args.list:
        shards_dir = Path('dataset/shards')
        if shards_dir.exists():
            parquet_files = list(shards_dir.glob('*.parquet'))
            if parquet_files:
                print("Available Parquet files:")
                for f in sorted(parquet_files):
                    size = f.stat().st_size / 1024  # KB
                    print(f"  {f.name} ({size:.2f} KB)")
            else:
                print("No parquet files found in dataset/shards/")
        else:
            print("dataset/shards/ directory not found")
        return
    
    df = view_parquet(args.file, args.rows)
    
    if df is not None and args.columns:
        print("\n" + "=" * 80)
        print(f"SELECTED COLUMNS: {', '.join(args.columns)}")
        print("=" * 80)
        print(df[args.columns].to_string())


if __name__ == '__main__':
    main()

