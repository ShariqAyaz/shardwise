"""
Deduplication and Quality Filtering Module
Removes duplicate and low-quality chunks
"""

import os
import json
import hashlib
import logging
import re
from pathlib import Path
from typing import Dict, List, Set
from datetime import datetime
from collections import defaultdict

import yaml
from tqdm import tqdm

try:
    from datasketch import MinHash, MinHashLSH
except ImportError:
    MinHash = None
    MinHashLSH = None

try:
    import textstat
except ImportError:
    textstat = None


class DedupFilter:
    """Handles deduplication and quality filtering of chunks"""
    
    def __init__(self, config_path: str = "config/pipeline_config.yaml"):
        """Initialise the deduplication filter with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.input_path = Path(self.config['paths']['intermediate']) / 'chunks'
        self.output_path = Path(self.config['paths']['intermediate']) / 'chunks'  # In-place filtering
        
        self.dedup_config = self.config['deduplication']
        self.quality_config = self.config['quality']
        
        # Set up logging
        logging.basicConfig(
            level=self.config['logging']['level'],
            format=self.config['logging']['format']
        )
        self.logger = logging.getLogger(__name__)
        
        # Track seen hashes
        self.seen_hashes: Set[str] = set()
        
        # MinHash LSH for near-duplicate detection
        if MinHashLSH and self.dedup_config['near_dedup']:
            self.lsh = MinHashLSH(
                threshold=self.dedup_config['minhash']['threshold'],
                num_perm=self.dedup_config['minhash']['num_perm']
            )
        else:
            self.lsh = None
    
    def compute_hash(self, text: str) -> str:
        """Compute SHA256 hash of text"""
        return hashlib.sha256(text.encode('utf-8')).hexdigest()
    
    def create_minhash(self, text: str) -> 'MinHash':
        """Create MinHash signature for text"""
        if not MinHash:
            return None
        
        # Tokenise text into words
        words = text.lower().split()
        
        # Create MinHash
        m = MinHash(num_perm=self.dedup_config['minhash']['num_perm'])
        for word in words:
            m.update(word.encode('utf-8'))
        
        return m
    
    def is_exact_duplicate(self, text: str) -> bool:
        """Check if text is an exact duplicate"""
        text_hash = self.compute_hash(text)
        
        if text_hash in self.seen_hashes:
            return True
        
        self.seen_hashes.add(text_hash)
        return False
    
    def is_near_duplicate(self, text: str, chunk_id: str) -> bool:
        """Check if text is a near-duplicate using MinHash LSH"""
        if not self.lsh or not MinHash:
            return False
        
        minhash = self.create_minhash(text)
        if not minhash:
            return False
        
        # Query LSH for similar documents
        result = self.lsh.query(minhash)
        
        if result:
            # Found similar documents
            return True
        
        # Add to LSH
        self.lsh.insert(chunk_id, minhash)
        return False
    
    def count_words(self, text: str) -> int:
        """Count words in text"""
        return len(text.split())
    
    def count_unique_words(self, text: str) -> int:
        """Count unique words in text"""
        words = text.lower().split()
        return len(set(words))
    
    def calculate_repetition_ratio(self, text: str) -> float:
        """Calculate ratio of repeated n-grams"""
        words = text.lower().split()
        if len(words) < 10:
            return 0.0
        
        # Check for repeated 3-grams
        trigrams = [tuple(words[i:i+3]) for i in range(len(words)-2)]
        if not trigrams:
            return 0.0
        
        unique_trigrams = len(set(trigrams))
        total_trigrams = len(trigrams)
        
        return 1.0 - (unique_trigrams / total_trigrams)
    
    def calculate_alpha_ratio(self, text: str) -> float:
        """Calculate ratio of alphabetic characters"""
        if not text:
            return 0.0
        
        alpha_chars = sum(c.isalpha() or c.isspace() for c in text)
        return alpha_chars / len(text)
    
    def calculate_readability(self, text: str) -> float:
        """Calculate readability score using Flesch Reading Ease"""
        if not textstat:
            return 50.0  # Default neutral score
        
        try:
            return textstat.flesch_reading_ease(text)
        except Exception:
            return 50.0
    
    def assess_quality(self, text: str) -> Dict:
        """Assess quality of text chunk"""
        word_count = self.count_words(text)
        
        # Check word count range
        if word_count < self.quality_config['min_words']:
            return {
                'passed': False,
                'reason': f'Too few words: {word_count} < {self.quality_config["min_words"]}'
            }
        
        if word_count > self.quality_config['max_words']:
            return {
                'passed': False,
                'reason': f'Too many words: {word_count} > {self.quality_config["max_words"]}'
            }
        
        # Check unique words ratio
        unique_words = self.count_unique_words(text)
        unique_ratio = unique_words / word_count if word_count > 0 else 0
        
        if unique_ratio < self.quality_config['min_unique_words_ratio']:
            return {
                'passed': False,
                'reason': f'Low vocabulary diversity: {unique_ratio:.2f} < {self.quality_config["min_unique_words_ratio"]}'
            }
        
        # Check repetition ratio
        repetition_ratio = self.calculate_repetition_ratio(text)
        if repetition_ratio > self.quality_config['max_repetition_ratio']:
            return {
                'passed': False,
                'reason': f'High repetition: {repetition_ratio:.2f} > {self.quality_config["max_repetition_ratio"]}'
            }
        
        # Check alpha ratio
        alpha_ratio = self.calculate_alpha_ratio(text)
        if alpha_ratio < self.quality_config['min_alpha_ratio']:
            return {
                'passed': False,
                'reason': f'Too many non-alphabetic characters: {alpha_ratio:.2f} < {self.quality_config["min_alpha_ratio"]}'
            }
        
        # Calculate readability if enabled
        readability_score = None
        if self.quality_config['calculate_readability']:
            readability_score = self.calculate_readability(text)
            if readability_score < self.quality_config['min_readability_score']:
                return {
                    'passed': False,
                    'reason': f'Low readability: {readability_score:.2f} < {self.quality_config["min_readability_score"]}',
                    'readability_score': readability_score
                }
        
        return {
            'passed': True,
            'word_count': word_count,
            'unique_words': unique_words,
            'unique_ratio': unique_ratio,
            'repetition_ratio': repetition_ratio,
            'alpha_ratio': alpha_ratio,
            'readability_score': readability_score
        }
    
    def process_chunk(self, chunk_file: Path) -> Dict:
        """Process a single chunk file"""
        # Read chunk
        try:
            with open(chunk_file, 'r', encoding='utf-8') as f:
                chunk = json.load(f)
        except Exception as e:
            self.logger.error(f"Error reading chunk {chunk_file}: {e}")
            return {'keep': False, 'reason': 'Read error'}
        
        text = chunk.get('text', '')
        chunk_id = chunk.get('id', str(chunk_file))
        
        # Check minimum length
        if len(text) < self.dedup_config['min_text_length']:
            return {
                'keep': False,
                'reason': 'Text too short'
            }
        
        # Exact deduplication
        if self.dedup_config['exact_dedup']:
            if self.is_exact_duplicate(text):
                return {
                    'keep': False,
                    'reason': 'Exact duplicate'
                }
        
        # Near-duplicate detection
        if self.dedup_config['near_dedup']:
            if self.is_near_duplicate(text, chunk_id):
                return {
                    'keep': False,
                    'reason': 'Near-duplicate'
                }
        
        # Quality assessment
        quality_result = self.assess_quality(text)
        
        if not quality_result['passed']:
            return {
                'keep': False,
                'reason': quality_result['reason']
            }
        
        # Update chunk with quality metrics
        chunk['metadata']['quality_metrics'] = {
            k: v for k, v in quality_result.items() 
            if k != 'passed' and v is not None
        }
        chunk['metadata']['filtering_timestamp'] = datetime.now().isoformat()
        chunk['metadata']['passed_filtering'] = True
        
        return {
            'keep': True,
            'chunk': chunk
        }
    
    def filter_all(self):
        """Filter all chunks"""
        # Find all chunk files
        chunk_files = list(self.input_path.rglob('*.json'))
        
        if not chunk_files:
            self.logger.warning("No chunk files found")
            return
        
        self.logger.info(f"Found {len(chunk_files)} chunks to filter")
        
        kept_count = 0
        removed_count = 0
        removal_reasons = defaultdict(int)
        
        for chunk_file in tqdm(chunk_files, desc="Filtering chunks"):
            result = self.process_chunk(chunk_file)
            
            if result['keep']:
                # Update chunk file with quality metrics
                with open(chunk_file, 'w', encoding='utf-8') as f:
                    json.dump(result['chunk'], f, indent=2, ensure_ascii=False)
                kept_count += 1
            else:
                # Remove chunk file
                chunk_file.unlink()
                removed_count += 1
                removal_reasons[result['reason']] += 1
        
        self.logger.info(f"Filtering complete: kept {kept_count}, removed {removed_count}")
        self.logger.info("Removal reasons:")
        for reason, count in removal_reasons.items():
            self.logger.info(f"  {reason}: {count}")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Deduplicate and filter chunks by quality")
    parser.add_argument(
        '--config',
        default='config/pipeline_config.yaml',
        help='Path to configuration file'
    )
    
    args = parser.parse_args()
    
    dedup_filter = DedupFilter(args.config)
    dedup_filter.filter_all()


if __name__ == '__main__':
    main()

