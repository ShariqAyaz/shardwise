"""
Annotation Export Module
Exports chunks to JSONL format for annotation, organised by niche
"""

import os
import json
import logging
from pathlib import Path
from typing import List, Dict, Set
from datetime import datetime
from collections import defaultdict

import yaml
from tqdm import tqdm


class AnnotationExporter:
    """Exports chunks to annotation-ready JSONL format"""
    
    def __init__(self, config_path: str = "config/pipeline_config.yaml"):
        """Initialise the annotation exporter with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.input_path = Path(self.config['paths']['intermediate']) / 'chunks'
        self.output_path = Path(self.config['paths']['dataset']) / 'annotation_ready'
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        self.annotation_config = self.config['annotation']
        
        # Set up logging
        logging.basicConfig(
            level=self.config['logging']['level'],
            format=self.config['logging']['format']
        )
        self.logger = logging.getLogger(__name__)
    
    def load_chunk(self, chunk_file: Path) -> Dict:
        """Load a single chunk file"""
        try:
            with open(chunk_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading chunk {chunk_file}: {e}")
            return None
    
    def categorise_chunk(self, chunk: Dict) -> str:
        """Categorise chunk by niche based on keywords"""
        text = chunk.get('text', '').lower()
        
        if not self.annotation_config.get('auto_categorise', True):
            return 'general'
        
        niches = self.annotation_config.get('niches', [])
        
        # Count keyword matches for each niche
        niche_scores = defaultdict(int)
        
        for niche in niches:
            niche_name = niche.get('name', 'general')
            keywords = niche.get('keywords', [])
            
            if not keywords:
                continue
            
            for keyword in keywords:
                if keyword.lower() in text:
                    niche_scores[niche_name] += 1
        
        # Return niche with highest score, or 'general' if no matches
        if niche_scores:
            return max(niche_scores.items(), key=lambda x: x[1])[0]
        
        return 'general'
    
    def chunk_to_annotation_format(self, chunk: Dict, niche: str) -> Dict:
        """Convert chunk to annotation format"""
        metadata = chunk.get('metadata', {})
        
        # Create annotation-ready record
        record = {
            'id': chunk.get('id'),
            'text': chunk.get('text'),
            'niche': niche,
            'metadata': {
                'source_file': metadata.get('source_file'),
                'filename': metadata.get('filename'),
                'language': metadata.get('language'),
                'word_count': metadata.get('word_count'),
                'export_timestamp': datetime.now().isoformat()
            }
        }
        
        # Include quality metrics if available
        if 'quality_metrics' in metadata:
            record['metadata']['quality_metrics'] = metadata['quality_metrics']
        
        return record
    
    def export_to_jsonl(self, chunks_by_niche: Dict[str, List[Dict]]):
        """Export chunks to JSONL files organised by niche"""
        for niche, chunks in chunks_by_niche.items():
            if not chunks:
                continue
            
            output_file = self.output_path / f"{niche}.jsonl"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                for chunk in chunks:
                    json_line = json.dumps(chunk, ensure_ascii=False)
                    f.write(json_line + '\n')
            
            self.logger.info(f"Exported {len(chunks)} chunks to {output_file}")
    
    def export_all(self):
        """Export all chunks to annotation format"""
        # Find all chunk files
        chunk_files = list(self.input_path.rglob('*.json'))
        
        if not chunk_files:
            self.logger.warning("No chunk files found")
            return
        
        self.logger.info(f"Processing {len(chunk_files)} chunks for export")
        
        # Organise chunks by niche
        chunks_by_niche = defaultdict(list)
        
        for chunk_file in tqdm(chunk_files, desc="Categorising chunks"):
            chunk = self.load_chunk(chunk_file)
            
            if not chunk:
                continue
            
            # Categorise chunk
            niche = self.categorise_chunk(chunk)
            
            # Convert to annotation format
            annotation_record = self.chunk_to_annotation_format(chunk, niche)
            
            chunks_by_niche[niche].append(annotation_record)
        
        # Export to JSONL files
        self.logger.info(f"Exporting chunks to {len(chunks_by_niche)} niche categories")
        self.export_to_jsonl(chunks_by_niche)
        
        # Save export metadata
        metadata = {
            'export_timestamp': datetime.now().isoformat(),
            'total_chunks': sum(len(chunks) for chunks in chunks_by_niche.values()),
            'niches': {
                niche: len(chunks) 
                for niche, chunks in chunks_by_niche.items()
            }
        }
        
        metadata_path = self.output_path / 'export_metadata.json'
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
        
        self.logger.info(f"Export complete. Metadata saved to {metadata_path}")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Export chunks to annotation-ready JSONL")
    parser.add_argument(
        '--config',
        default='config/pipeline_config.yaml',
        help='Path to configuration file'
    )
    
    args = parser.parse_args()
    
    exporter = AnnotationExporter(args.config)
    exporter.export_all()


if __name__ == '__main__':
    main()

