# Content Filtering System

## Purpose

The content filtering system provides pattern-based text redaction for data cleaning and processing. Use cases include:

- **Privacy Protection**: Removing personally identifiable information (PII)
- **Format Standardisation**: Cleaning format-specific artefacts and metadata
- **Data Quality**: Removing boilerplate, headers, footers
- **Compliance**: Redacting sensitive information as required by policy

## Legal Responsibility

**IMPORTANT**: You are responsible for:
- Ensuring your use complies with all applicable laws and regulations
- Respecting intellectual property rights and licences
- Understanding the legal status of your source materials
- Proper attribution and compliance with content licences

This tool provides technical capability for pattern matching and text filtering. It does not provide legal advice or permission to use copyrighted materials.

## Configuration

All filtering is controlled through `config/pipeline_config.yaml`.

### Basic Setup

```yaml
content_filter:
  enabled: true              # Enable the filter
  action: "flag"             # Start with flag to review
  
  patterns:
    custom_patterns:
      - "your_pattern_here"  # Add your patterns
  
  stages:
    cleaning: true           # Enable at specific stages
```

### Actions

- **flag**: Mark matching content for review (no changes)
- **remove**: Redact matching sections (use with caution)
- **reject**: Discard entire text if patterns match

### Pattern Configuration

Define regex patterns to match content you want to filter:

```yaml
patterns:
  custom_patterns:
    # Define your own patterns
    - "\\b\\d{3}-\\d{2}-\\d{4}\\b"           # Example: SSN format
    - "\\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}\\b"  # Example: Email addresses
    - "(?i)page\\s+\\d+"                     # Example: Page numbers
    - "(?i)draft|confidential"               # Example: Status markers
```

### Pipeline Stages

Control where filtering applies:

```yaml
stages:
  extraction: true   # During text extraction
  cleaning: true     # During cleaning
  chunking: true     # During chunking
  filtering: true    # During quality filtering
```

## Usage

### Test on Single File

```bash
python scripts/test_guardrails.py "path/to/chunk.json"
```

Shows what would be detected and removed.

### Process Existing Data

```bash
# Preview changes (safe)
python scripts/apply_guardrails_retroactive.py --dry-run

# Apply changes
python scripts/apply_guardrails_retroactive.py
```

### Pipeline Integration

When enabled, filtering applies automatically during pipeline execution:

```bash
python workflows/main_pipeline.py
```

## Use Case Examples

### 1. Privacy Protection (PII Removal)

```yaml
content_filter:
  enabled: true
  action: "remove"
  patterns:
    custom_patterns:
      - "\\b\\d{3}-\\d{2}-\\d{4}\\b"                    # SSN format
      - "\\b\\d{3}-\\d{3}-\\d{4}\\b"                    # Phone format
      - "[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}"        # Email addresses
```

### 2. Document Formatting Cleanup

```yaml
content_filter:
  enabled: true
  action: "remove"
  patterns:
    custom_patterns:
      - "(?i)page\\s+\\d+"              # Page numbering
      - "(?i)chapter\\s+\\d+"           # Chapter markers
      - "\\[\\d+\\]"                    # Reference numbers
      - "(?i)fig(?:ure)?\\s+\\d+"      # Figure references
```

### 3. Status Markers and Tags

```yaml
content_filter:
  enabled: true
  action: "remove"
  patterns:
    custom_patterns:
      - "(?i)\\[draft\\]"
      - "(?i)\\[internal\\]"
      - "(?i)status:\\s*\\w+"
      - "(?i)version:\\s*[\\d\\.]+"
```

### 4. Review Mode (Flag Without Removing)

```yaml
content_filter:
  enabled: true
  action: "flag"
  patterns:
    custom_patterns:
      - "pattern_to_review"
```

## Best Practises

### 1. Start with Flag Mode

```yaml
action: "flag"
```

Review detections before enabling removal.

### 2. Test on Samples

Always test patterns on sample data first:

```bash
python scripts/test_guardrails.py "sample/file.json"
```

### 3. Use Dry Run

```bash
python scripts/apply_guardrails_retroactive.py --dry-run
```

Preview changes before applying.

### 4. Document Your Patterns

Comment your patterns to explain their purpose:

```yaml
custom_patterns:
  - "pattern_here"  # Removes X because Y
```

### 5. Review Logs

Check logs to ensure legitimate content isn't affected:

```yaml
log_detections: true
log_removals: true
```

## Technical Details

### Detection

Uses compiled regex patterns to scan text.

### Section Identification

Finds matches and expands to natural boundaries (sentences/paragraphs).

### Removal

Removes identified sections whilst preserving text structure.

### Metadata

Records filtering actions in metadata:

```json
{
  "guardrail_applied": true,
  "guardrail_action": "removed",
  "guardrail_confidence": 0.75
}
```

## API

```python
from scripts.content_guardrails import ContentGuardrail

filter = ContentGuardrail("config/pipeline_config.yaml")

# Detect patterns
detection = filter.detect_protected_content(text)

# Remove matches
result = filter.remove_protected_content(text)

# Full processing
result = filter.scan_and_guard(text, stage='custom')
```

## Troubleshooting

**Issue**: Legitimate content being filtered  
**Solution**: Review and refine your patterns

**Issue**: Patterns not matching  
**Solution**: Test patterns with regex tester, check escaping

**Issue**: Too much context removed  
**Solution**: Reduce `context_chars` value

## Files

- `scripts/content_guardrails.py` - Filtering engine
- `scripts/test_guardrails.py` - Testing tool
- `scripts/apply_guardrails_retroactive.py` - Batch processing
- `config/pipeline_config.yaml` - Configuration

## Summary

The content filtering system provides flexible, pattern-based text redaction for data processing. Configure patterns appropriate for your use case and legal requirements.

**Remember**: This tool provides technical capabilities. Legal compliance is your responsibility.

