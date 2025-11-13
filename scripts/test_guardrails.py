"""
Test Content Filtering
Tests the pattern-based content filtering system on chunks
"""

import json
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.content_guardrails import ContentGuardrail


def test_chunk_file(chunk_file: str, config_path: str = "config/pipeline_config.yaml"):
    """Test content filtering on a specific chunk file"""
    print(f"\n{'='*80}")
    print(f"Testing Content Filtering on: {chunk_file}")
    print(f"{'='*80}\n")
    
    # Load chunk
    with open(chunk_file, 'r', encoding='utf-8') as f:
        chunk = json.load(f)
    
    text = chunk.get('text', '')
    metadata = chunk.get('metadata', {})
    
    print(f"Original text length: {len(text)} chars")
    print(f"Original word count: {metadata.get('word_count', 'N/A')}")
    print(f"\nFirst 200 chars: {text[:200]}...")
    print(f"\nLast 500 chars: ...{text[-500:]}")
    
    # Test detection
    guardrail = ContentGuardrail(config_path)
    
    print(f"\n{'-'*80}")
    print("Detection Phase")
    print(f"{'-'*80}\n")
    
    detection = guardrail.detect_protected_content(text)
    
    print(f"Pattern matches detected: {detection['detected']}")
    print(f"Confidence: {detection['confidence']:.2f}")
    print(f"Number of matches: {len(detection['matches'])}")
    
    if detection['detected']:
        print(f"\nMatches found:")
        for i, match in enumerate(detection['matches'][:10], 1):  # Show first 10
            print(f"  {i}. Pattern: {match['pattern']}")
            print(f"     Text: {match['text'][:80]}")
            print(f"     Position: {match['start']}-{match['end']}")
            print()
    
    # Find sections
    print(f"\n{'-'*80}")
    print("Section Identification")
    print(f"{'-'*80}\n")
    
    sections = guardrail.find_protected_sections(text, context_chars=500)
    
    print(f"Matching sections found: {len(sections)}")
    for i, (start, end) in enumerate(sections, 1):
        print(f"  Section {i}: chars {start}-{end} (length: {end-start})")
        print(f"    Preview: {text[start:min(start+100, end)]}...")
        print()
    
    # Test removal
    print(f"\n{'-'*80}")
    print("Removal Phase")
    print(f"{'-'*80}\n")
    
    removal_result = guardrail.remove_protected_content(text)
    
    print(f"Removed: {removal_result['removed']}")
    print(f"Original length: {removal_result['original_length']} chars")
    print(f"Final length: {removal_result['final_length']} chars")
    print(f"Sections removed: {removal_result['sections_removed']}")
    print(f"Characters removed: {removal_result['removed_chars']}")
    
    if removal_result['removed']:
        cleaned_text = removal_result['text']
        print(f"\nCleaned text preview (last 500 chars):")
        print(f"...{cleaned_text[-500:]}")
    
    # Test full scan and guard
    print(f"\n{'-'*80}")
    print("Full Filter Processing")
    print(f"{'-'*80}\n")
    
    result = guardrail.scan_and_guard(text, stage='test')
    
    print(f"Action taken: {result['action']}")
    print(f"Stage: {result['stage']}")
    print(f"Detected: {result['detected']}")
    
    if result.get('removal'):
        removal = result['removal']
        print(f"\nRemoval summary:")
        print(f"  - Sections removed: {removal['sections_removed']}")
        print(f"  - Characters removed: {removal['removed_chars']}")
        print(f"  - Reduction: {(removal['removed_chars']/removal['original_length']*100):.1f}%")
    
    print(f"\n{'='*80}")
    print("Test Complete")
    print(f"{'='*80}\n")
    
    return result


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Test content guardrails")
    parser.add_argument(
        'chunk_file',
        help='Path to chunk file to test'
    )
    parser.add_argument(
        '--config',
        default='config/pipeline_config.yaml',
        help='Path to configuration file'
    )
    
    args = parser.parse_args()
    
    test_chunk_file(args.chunk_file, args.config)

