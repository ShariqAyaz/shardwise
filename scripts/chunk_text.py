"""
Text Chunking Module
Splits cleaned text into manageable chunks with metadata
"""

import os
import json
import re
import uuid
import logging
from pathlib import Path
from typing import Dict, List
from datetime import datetime

import yaml
from tqdm import tqdm


class TextChunker:
    """Handles text chunking with various strategies"""
    
    def __init__(self, config_path: str = "config/pipeline_config.yaml"):
        """Initialise the text chunker with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.input_path = Path(self.config['paths']['intermediate']) / 'cleaned'
        self.output_path = Path(self.config['paths']['intermediate']) / 'chunks'
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        self.chunking_config = self.config['chunking']
        
        # Set up logging
        logging.basicConfig(
            level=self.config['logging']['level'],
            format=self.config['logging']['format']
        )
        self.logger = logging.getLogger(__name__)
    
    def split_into_sentences(self, text: str) -> List[str]:
        """Split text into sentences"""
        # Simple sentence splitter - can be enhanced with NLTK/spaCy
        sentence_endings = r'[.!?]+[\s\n]+'
        sentences = re.split(sentence_endings, text)
        return [s.strip() for s in sentences if s.strip()]
    
    def split_into_paragraphs(self, text: str) -> List[str]:
        """Split text into paragraphs"""
        paragraphs = text.split('\n\n')
        return [p.strip() for p in paragraphs if p.strip()]
    
    def count_words(self, text: str) -> int:
        """Count words in text"""
        return len(text.split())
    
    def chunk_by_sentence(self, text: str, min_size: int, max_size: int, overlap: int) -> List[str]:
        """Chunk text by sentences with size constraints"""
        sentences = self.split_into_sentences(text)
        
        chunks = []
        current_chunk = []
        current_word_count = 0
        
        for sentence in sentences:
            sentence_word_count = self.count_words(sentence)
            
            # If adding this sentence exceeds max_size and we have content, save chunk
            if current_word_count + sentence_word_count > max_size and current_chunk:
                chunks.append(' '.join(current_chunk))
                
                # Create overlap by keeping last few sentences
                if overlap > 0:
                    overlap_words = 0
                    overlap_sentences = []
                    for s in reversed(current_chunk):
                        s_words = self.count_words(s)
                        if overlap_words + s_words <= overlap:
                            overlap_sentences.insert(0, s)
                            overlap_words += s_words
                        else:
                            break
                    current_chunk = overlap_sentences
                    current_word_count = overlap_words
                else:
                    current_chunk = []
                    current_word_count = 0
            
            current_chunk.append(sentence)
            current_word_count += sentence_word_count
        
        # Add remaining chunk if it meets minimum size
        if current_chunk and current_word_count >= min_size:
            chunks.append(' '.join(current_chunk))
        
        return chunks
    
    def chunk_by_paragraph(self, text: str, min_size: int, max_size: int, overlap: int) -> List[str]:
        """Chunk text by paragraphs with size constraints"""
        paragraphs = self.split_into_paragraphs(text)
        
        chunks = []
        current_chunk = []
        current_word_count = 0
        
        for paragraph in paragraphs:
            paragraph_word_count = self.count_words(paragraph)
            
            # If paragraph alone exceeds max_size, split it by sentences
            if paragraph_word_count > max_size:
                # Save current chunk first
                if current_chunk:
                    chunks.append('\n\n'.join(current_chunk))
                    current_chunk = []
                    current_word_count = 0
                
                # Split large paragraph by sentences
                sentence_chunks = self.chunk_by_sentence(paragraph, min_size, max_size, overlap)
                chunks.extend(sentence_chunks)
                continue
            
            # If adding this paragraph exceeds max_size, save current chunk
            if current_word_count + paragraph_word_count > max_size and current_chunk:
                chunks.append('\n\n'.join(current_chunk))
                
                # Create overlap
                if overlap > 0:
                    overlap_words = 0
                    overlap_paras = []
                    for p in reversed(current_chunk):
                        p_words = self.count_words(p)
                        if overlap_words + p_words <= overlap:
                            overlap_paras.insert(0, p)
                            overlap_words += p_words
                        else:
                            break
                    current_chunk = overlap_paras
                    current_word_count = overlap_words
                else:
                    current_chunk = []
                    current_word_count = 0
            
            current_chunk.append(paragraph)
            current_word_count += paragraph_word_count
        
        # Add remaining chunk
        if current_chunk and current_word_count >= min_size:
            chunks.append('\n\n'.join(current_chunk))
        
        return chunks
    
    def chunk_by_fixed_size(self, text: str, max_size: int, overlap: int) -> List[str]:
        """Chunk text by fixed word count"""
        words = text.split()
        chunks = []
        
        i = 0
        while i < len(words):
            end = min(i + max_size, len(words))
            chunk_words = words[i:end]
            chunks.append(' '.join(chunk_words))
            i = end - overlap if overlap > 0 else end
        
        return chunks
    
    def create_chunks(self, text: str) -> List[str]:
        """Create chunks based on configuration"""
        method = self.chunking_config['method']
        min_size = self.chunking_config['min_chunk_size']
        max_size = self.chunking_config['max_chunk_size']
        overlap = self.chunking_config['overlap']
        
        if method == 'sentence':
            return self.chunk_by_sentence(text, min_size, max_size, overlap)
        elif method == 'paragraph':
            return self.chunk_by_paragraph(text, min_size, max_size, overlap)
        elif method == 'fixed':
            return self.chunk_by_fixed_size(text, max_size, overlap)
        else:
            self.logger.warning(f"Unknown chunking method: {method}, using sentence method")
            return self.chunk_by_sentence(text, min_size, max_size, overlap)
    
    def process_file(self, text_file: Path, metadata_file: Path) -> List[Dict]:
        """Process a single cleaned text file into chunks"""
        # Read text
        try:
            with open(text_file, 'r', encoding='utf-8') as f:
                text = f.read()
        except Exception as e:
            self.logger.error(f"Error reading {text_file}: {e}")
            return []
        
        # Read metadata
        try:
            with open(metadata_file, 'r', encoding='utf-8') as f:
                base_metadata = json.load(f)
        except Exception as e:
            self.logger.warning(f"Error reading metadata {metadata_file}: {e}")
            base_metadata = {}
        
        # Create chunks
        chunk_texts = self.create_chunks(text)
        
        # Create chunk objects with metadata
        chunks = []
        for idx, chunk_text in enumerate(chunk_texts):
            chunk_id = str(uuid.uuid4())
            
            chunk_metadata = {
                'id': chunk_id,
                'source_file': base_metadata.get('source_path', str(text_file)),
                'filename': base_metadata.get('filename', text_file.name),
                'file_type': base_metadata.get('file_type', 'unknown'),
                'language': base_metadata.get('language', 'unknown'),
                'chunk_index': idx,
                'total_chunks': len(chunk_texts),
                'chunking_method': self.chunking_config['method'],
                'chunking_timestamp': datetime.now().isoformat(),
                'word_count': self.count_words(chunk_text),
                'char_count': len(chunk_text),
            }
            
            chunks.append({
                'id': chunk_id,
                'text': chunk_text,
                'metadata': chunk_metadata
            })
        
        return chunks
    
    def save_chunks(self, chunks: List[Dict], source_file: Path):
        """Save chunks to individual JSON files"""
        if not chunks:
            return
        
        # Create output directory based on source file
        relative_path = source_file.relative_to(self.input_path)
        output_dir = self.output_path / relative_path.parent / relative_path.stem
        output_dir.mkdir(parents=True, exist_ok=True)
        
        for chunk in chunks:
            chunk_file = output_dir / f"{chunk['id']}.json"
            with open(chunk_file, 'w', encoding='utf-8') as f:
                json.dump(chunk, f, indent=2, ensure_ascii=False)
    
    def chunk_all(self):
        """Process all cleaned text files into chunks"""
        # Find all text files
        text_files = list(self.input_path.rglob('*.txt'))
        
        if not text_files:
            self.logger.warning("No cleaned text files found")
            return
        
        self.logger.info(f"Found {len(text_files)} files to chunk")
        
        total_chunks = 0
        for text_file in tqdm(text_files, desc="Chunking text"):
            metadata_file = text_file.with_suffix('.json')
            
            if not metadata_file.exists():
                self.logger.warning(f"Metadata file not found for {text_file}")
                continue
            
            chunks = self.process_file(text_file, metadata_file)
            self.save_chunks(chunks, text_file)
            
            total_chunks += len(chunks)
        
        self.logger.info(f"Chunking complete: {total_chunks} total chunks created from {len(text_files)} files")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Chunk cleaned text into manageable pieces")
    parser.add_argument(
        '--config',
        default='config/pipeline_config.yaml',
        help='Path to configuration file'
    )
    
    args = parser.parse_args()
    
    chunker = TextChunker(args.config)
    chunker.chunk_all()


if __name__ == '__main__':
    main()

