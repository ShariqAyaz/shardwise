"""
Content Filtering Module
Pattern-based text redaction for data cleaning and processing
Use cases: PII removal, format cleanup, metadata filtering, boilerplate removal
Users are responsible for legal compliance with their use of this tool
"""

import re
import logging
from typing import Dict, List, Tuple, Optional
from pathlib import Path
import yaml


class ContentGuardrail:
    """Pattern-based content filtering and redaction"""
    
    def __init__(self, config_path: str = "config/pipeline_config.yaml"):
        """Initialise content filter with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Try new config key first, fall back to old for compatibility
        self.guardrail_config = self.config.get('content_filter', self.config.get('guardrails', {}))
        self.enabled = self.guardrail_config.get('enabled', False)
        
        # Set up logging
        logging.basicConfig(
            level=self.config['logging']['level'],
            format=self.config['logging']['format']
        )
        self.logger = logging.getLogger(__name__)
        
        # Compile detection patterns
        self._compile_patterns()
    
    def _compile_patterns(self):
        """Compile regex patterns for detection"""
        patterns_config = self.guardrail_config.get('patterns', {})
        
        # Initialise pattern list
        self.all_patterns = []
        
        # Custom patterns from config
        custom_patterns = patterns_config.get('custom_patterns', [])
        for pattern in custom_patterns:
            try:
                self.all_patterns.append(re.compile(pattern, re.MULTILINE | re.IGNORECASE))
            except re.error as e:
                self.logger.error(f"Invalid custom pattern '{pattern}': {e}")
    
    def detect_protected_content(self, text: str) -> Dict:
        """Detect pattern matches in text"""
        if not self.enabled or not text:
            return {
                'detected': False,
                'matches': [],
                'confidence': 0.0
            }
        
        matches = []
        positions = []
        
        # Check each pattern
        for pattern in self.all_patterns:
            for match in pattern.finditer(text):
                matches.append({
                    'pattern': pattern.pattern[:50],
                    'text': match.group(0)[:100],
                    'start': match.start(),
                    'end': match.end()
                })
                positions.append((match.start(), match.end()))
        
        # Calculate confidence based on number of matches
        confidence = min(len(matches) / 3.0, 1.0)
        
        return {
            'detected': len(matches) > 0,
            'matches': matches,
            'positions': positions,
            'confidence': confidence
        }
    
    def find_protected_sections(self, text: str, context_chars: int = 500) -> List[Tuple[int, int]]:
        """Find sections of text containing pattern matches with context"""
        if not self.enabled or not text:
            return []
        
        detection = self.detect_protected_content(text)
        if not detection['detected']:
            return []
        
        # Merge overlapping or nearby sections
        positions = sorted(detection['positions'])
        sections = []
        
        for start, end in positions:
            # Expand to include context
            section_start = max(0, start - context_chars)
            section_end = min(len(text), end + context_chars)
            
            # Try to expand to sentence/paragraph boundaries
            section_start = self._find_boundary_before(text, section_start)
            section_end = self._find_boundary_after(text, section_end)
            
            # Merge with previous section if overlapping
            if sections and section_start <= sections[-1][1]:
                sections[-1] = (sections[-1][0], max(sections[-1][1], section_end))
            else:
                sections.append((section_start, section_end))
        
        return sections
    
    def _find_boundary_before(self, text: str, pos: int) -> int:
        """Find paragraph/sentence boundary before position"""
        # Look for paragraph break
        para_break = text.rfind('\n\n', max(0, pos - 200), pos)
        if para_break != -1:
            return para_break + 2
        
        # Look for sentence break
        sent_break = max(
            text.rfind('. ', max(0, pos - 100), pos),
            text.rfind('.\n', max(0, pos - 100), pos)
        )
        if sent_break != -1:
            return sent_break + 2
        
        return max(0, pos)
    
    def _find_boundary_after(self, text: str, pos: int) -> int:
        """Find paragraph/sentence boundary after position"""
        # Look for paragraph break
        para_break = text.find('\n\n', pos, min(len(text), pos + 200))
        if para_break != -1:
            return para_break
        
        # Look for sentence break
        sent_break_space = text.find('. ', pos, min(len(text), pos + 100))
        sent_break_newline = text.find('.\n', pos, min(len(text), pos + 100))
        
        if sent_break_space != -1 and sent_break_newline != -1:
            sent_break = min(sent_break_space, sent_break_newline)
        elif sent_break_space != -1:
            sent_break = sent_break_space
        elif sent_break_newline != -1:
            sent_break = sent_break_newline
        else:
            sent_break = -1
        
        if sent_break != -1:
            return sent_break + 1
        
        return min(len(text), pos)
    
    def remove_protected_content(self, text: str) -> Dict:
        """Remove matched sections from text"""
        if not self.enabled or not text:
            return {
                'text': text,
                'removed': False,
                'original_length': len(text) if text else 0,
                'final_length': len(text) if text else 0,
                'sections_removed': 0
            }
        
        original_length = len(text)
        sections = self.find_protected_sections(
            text,
            context_chars=self.guardrail_config.get('context_chars', 500)
        )
        
        if not sections:
            return {
                'text': text,
                'removed': False,
                'original_length': original_length,
                'final_length': len(text),
                'sections_removed': 0
            }
        
        # Remove sections from end to start to preserve positions
        cleaned_text = text
        for start, end in reversed(sections):
            cleaned_text = cleaned_text[:start] + cleaned_text[end:]
        
        # Clean up multiple newlines
        cleaned_text = re.sub(r'\n{3,}', '\n\n', cleaned_text)
        cleaned_text = cleaned_text.strip()
        
        return {
            'text': cleaned_text,
            'removed': True,
            'original_length': original_length,
            'final_length': len(cleaned_text),
            'sections_removed': len(sections),
            'removed_chars': original_length - len(cleaned_text)
        }
    
    def scan_and_guard(self, text: str, stage: str = 'unknown') -> Dict:
        """Scan text and apply filtering based on config"""
        if not self.enabled:
            return {
                'text': text,
                'action': 'none',
                'stage': stage,
                'detected': False
            }
        
        # Detect pattern matches
        detection = self.detect_protected_content(text)
        
        action = self.guardrail_config.get('action', 'flag')
        
        if not detection['detected']:
            return {
                'text': text,
                'action': 'none',
                'stage': stage,
                'detected': False
            }
        
        # Log detection
        if self.guardrail_config.get('log_detections', True):
            self.logger.info(
                f"Pattern matches detected at {stage} stage: "
                f"{len(detection['matches'])} matches, "
                f"confidence: {detection['confidence']:.2f}"
            )
        
        # Apply action
        if action == 'remove':
            result = self.remove_protected_content(text)
            self.logger.info(
                f"Removed {result['sections_removed']} sections "
                f"({result['removed_chars']} chars) at {stage} stage"
            )
            return {
                'text': result['text'],
                'action': 'removed',
                'stage': stage,
                'detected': True,
                'detection': detection,
                'removal': result
            }
        
        elif action == 'flag':
            return {
                'text': text,
                'action': 'flagged',
                'stage': stage,
                'detected': True,
                'detection': detection,
                'should_review': True
            }
        
        elif action == 'reject':
            return {
                'text': None,
                'action': 'rejected',
                'stage': stage,
                'detected': True,
                'detection': detection,
                'rejected': True
            }
        
        return {
            'text': text,
            'action': 'none',
            'stage': stage,
            'detected': True,
            'detection': detection
        }


def apply_guardrails(text: str, stage: str = 'unknown', 
                    config_path: str = "config/pipeline_config.yaml") -> Dict:
    """Convenience function to apply content filtering"""
    guardrail = ContentGuardrail(config_path)
    return guardrail.scan_and_guard(text, stage)

