"""
Sharding Module
Creates Parquet shards from filtered chunks
"""

import os
import json
import logging
from pathlib import Path
from typing import List, Dict
from datetime import datetime

import yaml
from tqdm import tqdm

try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pd = None
    pa = None
    pq = None


class ShardCreator:
    """Creates Parquet shards from chunks"""
    
    def __init__(self, config_path: str = "config/pipeline_config.yaml"):
        """Initialise the shard creator with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.input_path = Path(self.config['paths']['intermediate']) / 'chunks'
        self.output_path = Path(self.config['paths']['dataset']) / 'shards'
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        self.sharding_config = self.config['sharding']
        
        # Set up logging
        logging.basicConfig(
            level=self.config['logging']['level'],
            format=self.config['logging']['format']
        )
        self.logger = logging.getLogger(__name__)
        
        if not pd or not pa or not pq:
            raise ImportError("pandas and pyarrow are required for sharding")
    
    def load_chunk(self, chunk_file: Path) -> Dict:
        """Load a single chunk file"""
        try:
            with open(chunk_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading chunk {chunk_file}: {e}")
            return None
    
    def load_all_chunks(self) -> List[Dict]:
        """Load all filtered chunks"""
        chunk_files = list(self.input_path.rglob('*.json'))
        
        if not chunk_files:
            self.logger.warning("No chunk files found")
            return []
        
        self.logger.info(f"Loading {len(chunk_files)} chunks")
        
        chunks = []
        for chunk_file in tqdm(chunk_files, desc="Loading chunks"):
            chunk = self.load_chunk(chunk_file)
            if chunk:
                chunks.append(chunk)
        
        return chunks
    
    def chunks_to_dataframe(self, chunks: List[Dict]) -> pd.DataFrame:
        """Convert chunks to a pandas DataFrame"""
        records = []
        
        for chunk in chunks:
            metadata = chunk.get('metadata', {})
            quality_metrics = metadata.get('quality_metrics', {})
            
            record = {
                'id': chunk.get('id'),
                'text': chunk.get('text'),
                'source_file': metadata.get('source_file'),
                'filename': metadata.get('filename'),
                'file_type': metadata.get('file_type'),
                'language': metadata.get('language'),
                'chunk_index': metadata.get('chunk_index'),
                'word_count': metadata.get('word_count'),
                'char_count': metadata.get('char_count'),
                'chunking_method': metadata.get('chunking_method'),
                'chunking_timestamp': metadata.get('chunking_timestamp'),
            }
            
            # Add quality metrics if available
            if quality_metrics:
                record.update({
                    'unique_words': quality_metrics.get('unique_words'),
                    'unique_ratio': quality_metrics.get('unique_ratio'),
                    'repetition_ratio': quality_metrics.get('repetition_ratio'),
                    'alpha_ratio': quality_metrics.get('alpha_ratio'),
                    'readability_score': quality_metrics.get('readability_score'),
                })
            
            records.append(record)
        
        return pd.DataFrame(records)
    
    def save_shard(self, df: pd.DataFrame, shard_idx: int):
        """Save a DataFrame as a Parquet shard"""
        shard_filename = f"part-{shard_idx:04d}.parquet"
        shard_path = self.output_path / shard_filename
        
        compression = self.sharding_config.get('compression', 'snappy')
        
        df.to_parquet(
            shard_path,
            engine='pyarrow',
            compression=compression,
            index=False
        )
        
        # Calculate size
        size_mb = shard_path.stat().st_size / (1024 * 1024)
        self.logger.info(f"Saved shard {shard_filename} ({size_mb:.2f} MB, {len(df)} chunks)")
        
        return size_mb
    
    def create_shards(self):
        """Create Parquet shards from all chunks"""
        # Load all chunks
        chunks = self.load_all_chunks()
        
        if not chunks:
            self.logger.warning("No chunks to shard")
            return
        
        self.logger.info(f"Creating shards from {len(chunks)} chunks")
        
        # Convert to DataFrame
        df = self.chunks_to_dataframe(chunks)
        
        # Calculate shard sizes
        max_shard_size_mb = self.sharding_config['max_shard_size_mb']
        
        # Estimate rows per shard based on average row size
        # First create a small sample to estimate size
        sample_size = min(100, len(df))
        sample_df = df.head(sample_size)
        
        # Save temporary sample to estimate size
        temp_path = self.output_path / 'temp_sample.parquet'
        sample_df.to_parquet(temp_path, engine='pyarrow', index=False)
        sample_size_mb = temp_path.stat().st_size / (1024 * 1024)
        temp_path.unlink()
        
        # Calculate approximate rows per shard
        avg_row_size_mb = sample_size_mb / sample_size
        rows_per_shard = int(max_shard_size_mb / avg_row_size_mb)
        
        # Ensure at least 100 rows per shard
        rows_per_shard = max(100, rows_per_shard)
        
        self.logger.info(f"Creating shards with approximately {rows_per_shard} rows each")
        
        # Create shards
        total_shards = (len(df) + rows_per_shard - 1) // rows_per_shard
        shard_idx = 0
        
        for start_idx in range(0, len(df), rows_per_shard):
            end_idx = min(start_idx + rows_per_shard, len(df))
            shard_df = df.iloc[start_idx:end_idx]
            
            self.save_shard(shard_df, shard_idx)
            shard_idx += 1
        
        self.logger.info(f"Created {shard_idx} shards")
        
        # Save metadata
        metadata = {
            'total_chunks': len(chunks),
            'total_shards': shard_idx,
            'compression': self.sharding_config.get('compression'),
            'creation_timestamp': datetime.now().isoformat(),
            'schema': {col: str(dtype) for col, dtype in df.dtypes.items()}
        }
        
        metadata_path = self.output_path / 'shards_metadata.json'
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
        
        self.logger.info(f"Saved metadata to {metadata_path}")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Create Parquet shards from filtered chunks")
    parser.add_argument(
        '--config',
        default='config/pipeline_config.yaml',
        help='Path to configuration file'
    )
    
    args = parser.parse_args()
    
    shard_creator = ShardCreator(args.config)
    shard_creator.create_shards()


if __name__ == '__main__':
    main()

