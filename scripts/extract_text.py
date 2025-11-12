"""
Text Extraction Module
Extracts text from PDFs, DOCX, HTML, and plain text files
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import traceback

# PDF extraction
try:
    import pypdf
except ImportError:
    pypdf = None

try:
    from pdfminer.high_level import extract_text as pdfminer_extract
except ImportError:
    pdfminer_extract = None

# DOCX extraction
try:
    import docx
except ImportError:
    docx = None

# HTML extraction
try:
    import trafilatura
except ImportError:
    trafilatura = None

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

import yaml
from tqdm import tqdm


class TextExtractor:
    """Handles text extraction from various file formats"""
    
    def __init__(self, config_path: str = "config/pipeline_config.yaml"):
        """Initialise the text extractor with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.raw_data_path = Path(self.config['paths']['raw_data'])
        self.output_path = Path(self.config['paths']['intermediate']) / 'extracted'
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        # Set up logging
        logging.basicConfig(
            level=self.config['logging']['level'],
            format=self.config['logging']['format']
        )
        self.logger = logging.getLogger(__name__)
    
    def extract_pdf(self, file_path: Path) -> Optional[str]:
        """Extract text from PDF files"""
        method = self.config['extraction']['pdf']['method']
        
        try:
            if method == 'pypdf' and pypdf:
                with open(file_path, 'rb') as f:
                    reader = pypdf.PdfReader(f)
                    text = []
                    for page in reader.pages:
                        text.append(page.extract_text())
                    return '\n\n'.join(text)
            
            elif method == 'pdfminer' and pdfminer_extract:
                return pdfminer_extract(str(file_path))
            
            else:
                self.logger.warning(f"PDF extraction method '{method}' not available")
                return None
                
        except Exception as e:
            self.logger.error(f"Error extracting PDF {file_path}: {e}")
            return None
    
    def extract_docx(self, file_path: Path) -> Optional[str]:
        """Extract text from DOCX files"""
        if not docx:
            self.logger.warning("python-docx not installed")
            return None
        
        try:
            doc = docx.Document(file_path)
            paragraphs = [para.text for para in doc.paragraphs]
            return '\n\n'.join(paragraphs)
        except Exception as e:
            self.logger.error(f"Error extracting DOCX {file_path}: {e}")
            return None
    
    def extract_html(self, file_path: Path) -> Optional[str]:
        """Extract text from HTML files"""
        method = self.config['extraction']['html']['method']
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            if method == 'trafilatura' and trafilatura:
                text = trafilatura.extract(content)
                return text if text else None
            
            elif method == 'beautifulsoup' and BeautifulSoup:
                soup = BeautifulSoup(content, 'lxml')
                # Remove script and style elements
                for script in soup(['script', 'style']):
                    script.decompose()
                text = soup.get_text()
                # Clean up whitespace
                lines = (line.strip() for line in text.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                text = '\n'.join(chunk for chunk in chunks if chunk)
                return text
            
            else:
                self.logger.warning(f"HTML extraction method '{method}' not available")
                return None
                
        except Exception as e:
            self.logger.error(f"Error extracting HTML {file_path}: {e}")
            return None
    
    def extract_text_file(self, file_path: Path) -> Optional[str]:
        """Extract text from plain text files"""
        encoding = self.config['extraction']['encoding']
        
        try:
            with open(file_path, 'r', encoding=encoding, errors='ignore') as f:
                return f.read()
        except Exception as e:
            self.logger.error(f"Error reading text file {file_path}: {e}")
            return None
    
    def extract_file(self, file_path: Path) -> Dict:
        """Extract text from a single file and return metadata"""
        suffix = file_path.suffix.lower()
        
        # Map file extensions to extraction methods
        if suffix == '.pdf':
            text = self.extract_pdf(file_path)
            file_type = 'pdf'
        elif suffix in ['.docx', '.doc']:
            text = self.extract_docx(file_path)
            file_type = 'docx'
        elif suffix in ['.html', '.htm']:
            text = self.extract_html(file_path)
            file_type = 'html'
        elif suffix in ['.txt', '.text']:
            text = self.extract_text_file(file_path)
            file_type = 'text'
        else:
            self.logger.warning(f"Unsupported file type: {suffix}")
            text = None
            file_type = 'unknown'
        
        # Create metadata
        metadata = {
            'source_path': str(file_path),
            'filename': file_path.name,
            'file_type': file_type,
            'extraction_timestamp': datetime.now().isoformat(),
            'success': text is not None,
            'text_length': len(text) if text else 0,
        }
        
        return {
            'text': text,
            'metadata': metadata
        }
    
    def save_extracted(self, result: Dict, output_file: Path):
        """Save extracted text and metadata"""
        if result['text']:
            # Save text
            text_file = output_file.with_suffix('.txt')
            with open(text_file, 'w', encoding='utf-8') as f:
                f.write(result['text'])
            
            # Save metadata
            metadata_file = output_file.with_suffix('.json')
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(result['metadata'], f, indent=2)
            
            self.logger.info(f"Saved extracted text to {text_file}")
    
    def extract_all(self):
        """Extract text from all files in raw_data directory"""
        file_types = ['pdf', 'html', 'text']
        all_files = []
        
        # Collect all files
        for file_type in file_types:
            type_path = self.raw_data_path / file_type
            if type_path.exists():
                if file_type == 'pdf':
                    pattern = ['*.pdf']
                elif file_type == 'html':
                    pattern = ['*.html', '*.htm']
                elif file_type == 'text':
                    pattern = ['*.txt', '*.text', '*.docx', '*.doc']
                else:
                    pattern = ['*']
                
                for pat in pattern:
                    all_files.extend(type_path.glob(pat))
        
        if not all_files:
            self.logger.warning("No files found in raw_data directory")
            return
        
        self.logger.info(f"Found {len(all_files)} files to process")
        
        # Process all files
        success_count = 0
        for file_path in tqdm(all_files, desc="Extracting text"):
            result = self.extract_file(file_path)
            
            # Create output filename
            relative_path = file_path.relative_to(self.raw_data_path)
            output_file = self.output_path / relative_path.with_suffix('')
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            self.save_extracted(result, output_file)
            
            if result['metadata']['success']:
                success_count += 1
        
        self.logger.info(f"Extraction complete: {success_count}/{len(all_files)} files successful")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract text from various file formats")
    parser.add_argument(
        '--config',
        default='config/pipeline_config.yaml',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--file',
        help='Extract a single file instead of batch processing'
    )
    
    args = parser.parse_args()
    
    extractor = TextExtractor(args.config)
    
    if args.file:
        file_path = Path(args.file)
        result = extractor.extract_file(file_path)
        output_file = extractor.output_path / file_path.stem
        extractor.save_extracted(result, output_file)
    else:
        extractor.extract_all()


if __name__ == '__main__':
    main()

