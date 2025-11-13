"""
Apply Content Filtering Retroactively
Processes existing chunks and applies pattern-based content filtering
"""

import json
import logging
from pathlib import Path
from typing import Dict
from tqdm import tqdm
import yaml

try:
    from scripts.content_guardrails import ContentGuardrail
except ImportError:
    from content_guardrails import ContentGuardrail


def apply_guardrails_to_chunks(
    chunks_dir: str = "intermediate/chunks",
    config_path: str = "config/pipeline_config.yaml",
    dry_run: bool = False
):
    """Apply content filtering to existing chunks"""
    
    # Set up logging
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    logging.basicConfig(
        level=config['logging']['level'],
        format=config['logging']['format']
    )
    logger = logging.getLogger(__name__)
    
    chunks_path = Path(chunks_dir)
    
    if not chunks_path.exists():
        logger.error(f"Chunks directory not found: {chunks_dir}")
        return
    
    # Find all chunk files
    chunk_files = list(chunks_path.rglob('*.json'))
    
    if not chunk_files:
        logger.warning(f"No chunk files found in {chunks_dir}")
        return
    
    logger.info(f"Found {len(chunk_files)} chunk files to process")
    
    # Initialise guardrail
    guardrail = ContentGuardrail(config_path)
    
    # Statistics
    stats = {
        'total': 0,
        'detected': 0,
        'modified': 0,
        'rejected': 0,
        'unchanged': 0,
        'errors': 0,
        'total_chars_removed': 0
    }
    
    # Process each chunk
    for chunk_file in tqdm(chunk_files, desc="Processing chunks"):
        stats['total'] += 1
        
        try:
            # Load chunk
            with open(chunk_file, 'r', encoding='utf-8') as f:
                chunk = json.load(f)
            
            text = chunk.get('text', '')
            metadata = chunk.get('metadata', {})
            
            if not text:
                logger.warning(f"Empty text in {chunk_file}")
                continue
            
            original_length = len(text)
            
            # Apply guardrails
            result = guardrail.scan_and_guard(text, stage='retroactive')
            
            if result['detected']:
                stats['detected'] += 1
                
                action = result['action']
                
                if action == 'removed':
                    new_text = result['text']
                    
                    # Check if text was actually modified
                    if new_text != text and new_text:
                        stats['modified'] += 1
                        chars_removed = original_length - len(new_text)
                        stats['total_chars_removed'] += chars_removed
                        
                        # Update chunk
                        chunk['text'] = new_text
                        metadata['filter_applied'] = True
                        metadata['filter_action'] = action
                        metadata['patterns_detected'] = True
                        metadata['filter_confidence'] = result.get('detection', {}).get('confidence', 0.0)
                        metadata['filter_chars_removed'] = chars_removed
                        metadata['word_count'] = len(new_text.split())
                        metadata['char_count'] = len(new_text)
                        chunk['metadata'] = metadata
                        
                        # Save updated chunk (if not dry run)
                        if not dry_run:
                            with open(chunk_file, 'w', encoding='utf-8') as f:
                                json.dump(chunk, f, indent=2, ensure_ascii=False)
                        
                        logger.debug(f"Modified {chunk_file.name}: removed {chars_removed} chars")
                    
                    elif not new_text:
                        # Text completely removed
                        stats['rejected'] += 1
                        logger.warning(f"Chunk {chunk_file.name} rejected (text removed entirely)")
                        
                        # Delete chunk file (if not dry run)
                        if not dry_run:
                            chunk_file.unlink()
                    
                    else:
                        # Detected but no changes made
                        stats['unchanged'] += 1
                
                elif action == 'rejected':
                    stats['rejected'] += 1
                    logger.info(f"Chunk {chunk_file.name} rejected by content filter")
                    
                    # Delete chunk file (if not dry run)
                    if not dry_run:
                        chunk_file.unlink()
            
            else:
                stats['unchanged'] += 1
        
        except Exception as e:
            stats['errors'] += 1
            logger.error(f"Error processing {chunk_file}: {e}")
    
    # Print summary
    logger.info(f"\n{'='*80}")
    logger.info("Content Filtering Summary")
    logger.info(f"{'='*80}")
    logger.info(f"Total chunks processed: {stats['total']}")
    logger.info(f"Pattern matches detected: {stats['detected']}")
    logger.info(f"Chunks modified: {stats['modified']}")
    logger.info(f"Chunks rejected/deleted: {stats['rejected']}")
    logger.info(f"Chunks unchanged: {stats['unchanged']}")
    logger.info(f"Errors: {stats['errors']}")
    logger.info(f"Total characters removed: {stats['total_chars_removed']:,}")
    
    if stats['modified'] > 0:
        avg_removed = stats['total_chars_removed'] / stats['modified']
        logger.info(f"Average characters removed per modified chunk: {avg_removed:.1f}")
    
    if dry_run:
        logger.info("\nDRY RUN MODE - No files were modified")
    
    logger.info(f"{'='*80}\n")
    
    return stats


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Apply content filtering to existing chunks retroactively"
    )
    parser.add_argument(
        '--chunks-dir',
        default='intermediate/chunks',
        help='Path to chunks directory'
    )
    parser.add_argument(
        '--config',
        default='config/pipeline_config.yaml',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run without modifying files'
    )
    
    args = parser.parse_args()
    
    apply_guardrails_to_chunks(
        chunks_dir=args.chunks_dir,
        config_path=args.config,
        dry_run=args.dry_run
    )

