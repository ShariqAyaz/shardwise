"""
Text Cleaning and Normalisation Module
Cleans and normalises extracted text
"""

import os
import json
import re
import logging
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime

import yaml
from tqdm import tqdm

try:
    import ftfy
except ImportError:
    ftfy = None

try:
    from langdetect import detect, LangDetectException
except ImportError:
    detect = None
    LangDetectException = Exception


class TextCleaner:
    """Handles text cleaning and normalisation"""
    
    def __init__(self, config_path: str = "config/pipeline_config.yaml"):
        """Initialise the text cleaner with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.input_path = Path(self.config['paths']['intermediate']) / 'extracted'
        self.output_path = Path(self.config['paths']['intermediate']) / 'cleaned'
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        self.cleaning_config = self.config['cleaning']
        
        # Set up logging
        logging.basicConfig(
            level=self.config['logging']['level'],
            format=self.config['logging']['format']
        )
        self.logger = logging.getLogger(__name__)
    
    def fix_encoding(self, text: str) -> str:
        """Fix text encoding issues"""
        if ftfy:
            return ftfy.fix_text(text)
        return text
    
    def remove_urls(self, text: str) -> str:
        """Remove URLs from text"""
        url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        return re.sub(url_pattern, '', text)
    
    def remove_emails(self, text: str) -> str:
        """Remove email addresses from text"""
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        return re.sub(email_pattern, '', text)
    
    def remove_phone_numbers(self, text: str) -> str:
        """Remove phone numbers from text"""
        phone_patterns = [
            r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',
            r'\b\(\d{3}\)\s*\d{3}[-.]?\d{4}\b',
            r'\b\+\d{1,3}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}\b'
        ]
        for pattern in phone_patterns:
            text = re.sub(pattern, '', text)
        return text
    
    def normalise_whitespace(self, text: str) -> str:
        """Normalise whitespace in text"""
        # Replace multiple spaces with single space
        text = re.sub(r' +', ' ', text)
        # Replace multiple newlines with double newline
        text = re.sub(r'\n{3,}', '\n\n', text)
        # Remove trailing/leading whitespace from lines
        lines = [line.strip() for line in text.split('\n')]
        return '\n'.join(lines)
    
    def normalise_quotes(self, text: str) -> str:
        """Normalise quote characters"""
        # Normalise various quote types
        text = re.sub(r'["""]', '"', text)
        text = re.sub(r"[''']", "'", text)
        return text
    
    def remove_short_lines(self, text: str, min_length: int) -> str:
        """Remove lines shorter than minimum length"""
        lines = text.split('\n')
        filtered_lines = [
            line for line in lines 
            if len(line.strip()) >= min_length or line.strip() == ''
        ]
        return '\n'.join(filtered_lines)
    
    def detect_language(self, text: str) -> Optional[str]:
        """Detect the language of the text"""
        if not detect:
            return 'unknown'
        
        try:
            # Use first 1000 characters for detection
            sample = text[:1000]
            return detect(sample)
        except (LangDetectException, Exception) as e:
            self.logger.debug(f"Language detection failed: {e}")
            return 'unknown'
    
    def clean_text(self, text: str) -> Dict:
        """Apply all cleaning operations to text"""
        if not text or len(text.strip()) == 0:
            return {
                'text': None,
                'language': None,
                'success': False,
                'reason': 'Empty text'
            }
        
        original_length = len(text)
        
        # Fix encoding
        if self.cleaning_config['fix_encoding']:
            text = self.fix_encoding(text)
        
        # Remove unwanted patterns
        if self.cleaning_config['remove_urls']:
            text = self.remove_urls(text)
        
        if self.cleaning_config['remove_emails']:
            text = self.remove_emails(text)
        
        if self.cleaning_config['remove_phone_numbers']:
            text = self.remove_phone_numbers(text)
        
        # Normalise
        if self.cleaning_config['normalise_quotes']:
            text = self.normalise_quotes(text)
        
        if self.cleaning_config['normalise_whitespace']:
            text = self.normalise_whitespace(text)
        
        # Remove short lines
        if self.cleaning_config['min_line_length'] > 0:
            text = self.remove_short_lines(text, self.cleaning_config['min_line_length'])
        
        # Detect language
        language = self.detect_language(text)
        
        # Language filtering
        target_languages = self.cleaning_config['languages']
        if target_languages and language not in target_languages and language != 'unknown':
            return {
                'text': None,
                'language': language,
                'success': False,
                'reason': f'Language {language} not in target languages {target_languages}'
            }
        
        # Final validation
        cleaned_length = len(text)
        if cleaned_length < 50:  # Minimum text length
            return {
                'text': None,
                'language': language,
                'success': False,
                'reason': 'Text too short after cleaning'
            }
        
        return {
            'text': text,
            'language': language,
            'success': True,
            'original_length': original_length,
            'cleaned_length': cleaned_length,
            'reduction_ratio': (original_length - cleaned_length) / original_length if original_length > 0 else 0
        }
    
    def process_file(self, text_file: Path, metadata_file: Path) -> Dict:
        """Process a single extracted text file"""
        # Read text
        try:
            with open(text_file, 'r', encoding='utf-8') as f:
                text = f.read()
        except Exception as e:
            self.logger.error(f"Error reading {text_file}: {e}")
            return {'success': False}
        
        # Read metadata
        try:
            with open(metadata_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
        except Exception as e:
            self.logger.warning(f"Error reading metadata {metadata_file}: {e}")
            metadata = {}
        
        # Clean text
        result = self.clean_text(text)
        
        # Update metadata
        metadata.update({
            'cleaning_timestamp': datetime.now().isoformat(),
            'language': result.get('language'),
            'cleaning_success': result['success'],
        })
        
        if result['success']:
            metadata.update({
                'original_length': result['original_length'],
                'cleaned_length': result['cleaned_length'],
                'reduction_ratio': result['reduction_ratio']
            })
        else:
            metadata['cleaning_failure_reason'] = result.get('reason', 'Unknown')
        
        return {
            'text': result.get('text'),
            'metadata': metadata,
            'success': result['success']
        }
    
    def save_cleaned(self, result: Dict, output_file: Path):
        """Save cleaned text and metadata"""
        if result['success'] and result['text']:
            # Save text
            text_file = output_file.with_suffix('.txt')
            text_file.parent.mkdir(parents=True, exist_ok=True)
            with open(text_file, 'w', encoding='utf-8') as f:
                f.write(result['text'])
            
            # Save metadata
            metadata_file = output_file.with_suffix('.json')
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(result['metadata'], f, indent=2)
            
            self.logger.debug(f"Saved cleaned text to {text_file}")
    
    def clean_all(self):
        """Clean all extracted text files"""
        # Find all text files
        text_files = list(self.input_path.rglob('*.txt'))
        
        if not text_files:
            self.logger.warning("No extracted text files found")
            return
        
        self.logger.info(f"Found {len(text_files)} files to clean")
        
        success_count = 0
        for text_file in tqdm(text_files, desc="Cleaning text"):
            metadata_file = text_file.with_suffix('.json')
            
            if not metadata_file.exists():
                self.logger.warning(f"Metadata file not found for {text_file}")
                continue
            
            result = self.process_file(text_file, metadata_file)
            
            # Create output path maintaining directory structure
            relative_path = text_file.relative_to(self.input_path)
            output_file = self.output_path / relative_path.with_suffix('')
            
            self.save_cleaned(result, output_file)
            
            if result['success']:
                success_count += 1
        
        self.logger.info(f"Cleaning complete: {success_count}/{len(text_files)} files successful")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Clean and normalise extracted text")
    parser.add_argument(
        '--config',
        default='config/pipeline_config.yaml',
        help='Path to configuration file'
    )
    
    args = parser.parse_args()
    
    cleaner = TextCleaner(args.config)
    cleaner.clean_all()


if __name__ == '__main__':
    main()

