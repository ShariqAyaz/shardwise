"""
Pipeline Inspector
Provides visibility into each stage of the pipeline with detailed statistics
"""

import json
import os
from pathlib import Path
from typing import Dict, List
import yaml


class PipelineInspector:
    """Inspect intermediate outputs at each pipeline stage"""
    
    def __init__(self, config_path: str = "config/pipeline_config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.base_path = Path('.')
        self.raw_data = self.base_path / self.config['paths']['raw_data']
        self.intermediate = self.base_path / self.config['paths']['intermediate']
        self.dataset = self.base_path / self.config['paths']['dataset']
    
    def inspect_raw_data(self) -> Dict:
        """Inspect raw input data"""
        print("\n" + "="*80)
        print("📁 RAW DATA INSPECTION")
        print("="*80)
        
        stats = {}
        for file_type in ['pdf', 'html', 'text']:
            path = self.raw_data / file_type
            if path.exists():
                files = list(path.glob('*'))
                stats[file_type] = len(files)
                print(f"\n{file_type.upper()}:")
                print(f"  Location: {path}")
                print(f"  Files: {len(files)}")
                
                if files:
                    print(f"  Examples:")
                    for f in files[:5]:
                        size = f.stat().st_size / 1024
                        print(f"    - {f.name} ({size:.1f} KB)")
            else:
                stats[file_type] = 0
                print(f"\n{file_type.upper()}: No directory")
        
        total = sum(stats.values())
        print(f"\n📊 Total files: {total}")
        return stats
    
    def inspect_extracted(self) -> Dict:
        """Inspect extracted text"""
        print("\n" + "="*80)
        print("📄 EXTRACTED TEXT INSPECTION")
        print("="*80)
        
        extracted_path = self.intermediate / 'extracted'
        
        if not extracted_path.exists():
            print("❌ No extracted files found. Run extraction first:")
            print("   python scripts/extract_text.py")
            return {}
        
        text_files = list(extracted_path.rglob('*.txt'))
        json_files = list(extracted_path.rglob('*.json'))
        
        print(f"\nLocation: {extracted_path}")
        print(f"Text files: {len(text_files)}")
        print(f"Metadata files: {len(json_files)}")
        
        if text_files:
            print(f"\nSample extractions:")
            for txt_file in text_files[:3]:
                json_file = txt_file.with_suffix('.json')
                
                # Read metadata
                if json_file.exists():
                    with open(json_file, 'r') as f:
                        metadata = json.load(f)
                    
                    # Read text sample
                    with open(txt_file, 'r', encoding='utf-8') as f:
                        text = f.read()
                    
                    print(f"\n  📄 {txt_file.name}")
                    print(f"     Source: {metadata.get('filename')}")
                    print(f"     Type: {metadata.get('file_type')}")
                    print(f"     Length: {metadata.get('text_length')} chars")
                    print(f"     Preview: {text[:100]}...")
        
        return {'text_files': len(text_files), 'metadata_files': len(json_files)}
    
    def inspect_cleaned(self) -> Dict:
        """Inspect cleaned text"""
        print("\n" + "="*80)
        print("🧹 CLEANED TEXT INSPECTION")
        print("="*80)
        
        cleaned_path = self.intermediate / 'cleaned'
        
        if not cleaned_path.exists():
            print("❌ No cleaned files found. Run cleaning first:")
            print("   python scripts/clean_text.py")
            return {}
        
        text_files = list(cleaned_path.rglob('*.txt'))
        
        print(f"\nLocation: {cleaned_path}")
        print(f"Cleaned files: {len(text_files)}")
        
        if text_files:
            print(f"\nQuality metrics:")
            for txt_file in text_files[:3]:
                json_file = txt_file.with_suffix('.json')
                
                if json_file.exists():
                    with open(json_file, 'r') as f:
                        metadata = json.load(f)
                    
                    with open(txt_file, 'r', encoding='utf-8') as f:
                        text = f.read()
                    
                    print(f"\n  📄 {txt_file.name}")
                    print(f"     Language: {metadata.get('language', 'unknown')}")
                    print(f"     Original length: {metadata.get('original_length', 0)} chars")
                    print(f"     Cleaned length: {metadata.get('cleaned_length', 0)} chars")
                    print(f"     Reduction: {metadata.get('reduction_ratio', 0)*100:.1f}%")
                    print(f"     Word count: {len(text.split())} words")
        
        return {'cleaned_files': len(text_files)}
    
    def inspect_chunks(self) -> Dict:
        """Inspect text chunks"""
        print("\n" + "="*80)
        print("✂️  CHUNKS INSPECTION")
        print("="*80)
        
        chunks_path = self.intermediate / 'chunks'
        
        if not chunks_path.exists():
            print("❌ No chunks found. Run chunking first:")
            print("   python scripts/chunk_text.py")
            return {}
        
        chunk_files = list(chunks_path.rglob('*.json'))
        
        print(f"\nLocation: {chunks_path}")
        print(f"Total chunks: {len(chunk_files)}")
        
        if chunk_files:
            # Analyze chunks
            word_counts = []
            languages = {}
            sources = {}
            
            print(f"\nAnalyzing {len(chunk_files)} chunks...")
            
            for chunk_file in chunk_files:
                with open(chunk_file, 'r', encoding='utf-8') as f:
                    chunk = json.load(f)
                
                metadata = chunk.get('metadata', {})
                word_counts.append(metadata.get('word_count', 0))
                
                lang = metadata.get('language', 'unknown')
                languages[lang] = languages.get(lang, 0) + 1
                
                source = metadata.get('filename', 'unknown')
                sources[source] = sources.get(source, 0) + 1
            
            # Statistics
            avg_words = sum(word_counts) / len(word_counts) if word_counts else 0
            min_words = min(word_counts) if word_counts else 0
            max_words = max(word_counts) if word_counts else 0
            
            print(f"\n📊 Statistics:")
            print(f"   Average chunk size: {avg_words:.0f} words")
            print(f"   Min: {min_words} words")
            print(f"   Max: {max_words} words")
            
            print(f"\n🌍 Languages:")
            for lang, count in sorted(languages.items()):
                print(f"   {lang}: {count} chunks")
            
            print(f"\n📚 Source files:")
            for source, count in sorted(sources.items()):
                print(f"   {source}: {count} chunks")
            
            # Show sample chunk
            if chunk_files:
                print(f"\n📝 Sample chunk:")
                with open(chunk_files[0], 'r', encoding='utf-8') as f:
                    sample = json.load(f)
                print(f"   ID: {sample.get('id')}")
                print(f"   Text preview: {sample.get('text', '')[:150]}...")
        
        return {'total_chunks': len(chunk_files)}
    
    def inspect_shards(self) -> Dict:
        """Inspect Parquet shards"""
        print("\n" + "="*80)
        print("💾 SHARDS INSPECTION")
        print("="*80)
        
        shards_path = self.dataset / 'shards'
        
        if not shards_path.exists():
            print("❌ No shards found. Run shard creation first:")
            print("   python scripts/create_shards.py")
            return {}
        
        shard_files = list(shards_path.glob('*.parquet'))
        
        print(f"\nLocation: {shards_path}")
        print(f"Shards: {len(shard_files)}")
        
        if shard_files:
            total_size = 0
            for shard in shard_files:
                size_mb = shard.stat().st_size / (1024 * 1024)
                total_size += size_mb
                print(f"   {shard.name}: {size_mb:.2f} MB")
            
            print(f"\n📊 Total size: {total_size:.2f} MB")
            
            # Try to read schema
            try:
                import pandas as pd
                df = pd.read_parquet(shard_files[0])
                print(f"\n📋 Schema:")
                print(f"   Rows: {len(df)}")
                print(f"   Columns: {list(df.columns)}")
                print(f"\n   Sample data:")
                print(df[['id', 'language', 'word_count']].head(3).to_string(index=False))
            except Exception as e:
                print(f"\n⚠️  Could not read shard: {e}")
        
        return {'shard_files': len(shard_files)}
    
    def inspect_annotation_ready(self) -> Dict:
        """Inspect annotation-ready JSONL files"""
        print("\n" + "="*80)
        print("🏷️  ANNOTATION-READY FILES INSPECTION")
        print("="*80)
        
        annotation_path = self.dataset / 'annotation_ready'
        
        if not annotation_path.exists():
            print("❌ No annotation files found. Run export first:")
            print("   python scripts/export_annotation.py")
            return {}
        
        jsonl_files = list(annotation_path.glob('*.jsonl'))
        
        print(f"\nLocation: {annotation_path}")
        print(f"JSONL files: {len(jsonl_files)}")
        
        stats = {}
        for jsonl_file in jsonl_files:
            niche = jsonl_file.stem
            
            # Count lines
            with open(jsonl_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            stats[niche] = len(lines)
            print(f"\n   📄 {niche}.jsonl: {len(lines)} chunks")
            
            # Show sample
            if lines:
                sample = json.loads(lines[0])
                print(f"      Sample ID: {sample.get('id', 'N/A')}")
                print(f"      Text length: {len(sample.get('text', ''))} chars")
                print(f"      Preview: {sample.get('text', '')[:100]}...")
        
        return stats
    
    def inspect_all(self):
        """Run complete pipeline inspection"""
        print("\n" + "█"*80)
        print("🔍 SHARDWISE PIPELINE INSPECTION")
        print("█"*80)
        
        self.inspect_raw_data()
        self.inspect_extracted()
        self.inspect_cleaned()
        self.inspect_chunks()
        self.inspect_shards()
        self.inspect_annotation_ready()
        
        print("\n" + "="*80)
        print("✅ INSPECTION COMPLETE")
        print("="*80)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Inspect pipeline stages")
    parser.add_argument(
        '--stage',
        choices=['raw', 'extracted', 'cleaned', 'chunks', 'shards', 'annotation', 'all'],
        default='all',
        help='Stage to inspect'
    )
    parser.add_argument(
        '--config',
        default='config/pipeline_config.yaml',
        help='Config file path'
    )
    
    args = parser.parse_args()
    
    inspector = PipelineInspector(args.config)
    
    if args.stage == 'all':
        inspector.inspect_all()
    elif args.stage == 'raw':
        inspector.inspect_raw_data()
    elif args.stage == 'extracted':
        inspector.inspect_extracted()
    elif args.stage == 'cleaned':
        inspector.inspect_cleaned()
    elif args.stage == 'chunks':
        inspector.inspect_chunks()
    elif args.stage == 'shards':
        inspector.inspect_shards()
    elif args.stage == 'annotation':
        inspector.inspect_annotation_ready()


if __name__ == '__main__':
    main()

