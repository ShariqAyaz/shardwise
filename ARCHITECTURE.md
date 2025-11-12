# ShardWise Architecture

This document describes the technical architecture of the ShardWise pipeline.

## System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         ShardWise Pipeline                          │
│                                                                     │
│    Raw Data → Extract → Clean → Chunk → Dedup → Shard → Annotate    │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Flow

```
┌──────────────┐
│   Raw Data   │  PDF, DOCX, HTML, TXT
└──────┬───────┘
       │
       ▼
┌──────────────────────┐
│  Text Extraction     │  pypdf, python-docx, trafilatura
│  extract_text.py     │  → intermediate/extracted/
└──────┬───────────────┘
       │
       ▼
┌──────────────────────┐
│  Text Cleaning       │  ftfy, langdetect, regex
│  clean_text.py       │  → intermediate/cleaned/
└──────┬───────────────┘
       │
       ▼
┌──────────────────────┐
│  Text Chunking       │  Sentence/paragraph boundaries
│  chunk_text.py       │  → intermediate/chunks/
└──────┬───────────────┘
       │
       ▼
┌──────────────────────┐
│  Deduplication       │  MinHash LSH, quality metrics
│  dedup_filter.py     │  → intermediate/chunks/ (filtered)
└──────┬───────────────┘
       │
       ├────────────────┐
       │                │
       ▼                ▼
┌──────────────┐  ┌─────────────────┐
│  Sharding    │  │  Annotation     │
│  Parquet     │  │  Export JSONL   │
│  *.parquet   │  │  *.jsonl        │
└──────────────┘  └────────┬────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │  Label Studio   │
                  │  Annotation UI  │
                  └────────┬────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │  SFT Dataset    │
                  │  instruction +  │
                  │  response pairs │
                  └─────────────────┘
```

## Module Architecture

### Core Pipeline Modules

```
scripts/
├── extract_text.py      → TextExtractor class
├── clean_text.py        → TextCleaner class
├── chunk_text.py        → TextChunker class
├── dedup_filter.py      → DedupFilter class
├── create_shards.py     → ShardCreator class
├── export_annotation.py → AnnotationExporter class
└── labelstudio_setup.py → LabelStudioManager class
```

### Workflow Orchestration

```
workflows/
├── main_pipeline.py     → Prefect flow for preprocessing
└── annotation_sync.py   → Prefect flows for Label Studio
```

## Class Hierarchy

### TextExtractor
```
TextExtractor
├── extract_pdf()          # PDF → text
├── extract_docx()         # DOCX → text
├── extract_html()         # HTML → text
├── extract_text_file()    # TXT → text
└── extract_all()          # Batch processing
```

### TextCleaner
```
TextCleaner
├── fix_encoding()         # Fix UTF-8 issues
├── remove_urls()          # Strip URLs
├── normalise_whitespace() # Clean spacing
├── detect_language()      # Language detection
└── clean_all()            # Batch processing
```

### TextChunker
```
TextChunker
├── split_into_sentences()   # Sentence tokenisation
├── chunk_by_sentence()      # Sentence-based chunks
├── chunk_by_paragraph()     # Paragraph-based chunks
└── create_chunks()          # Configurable chunking
```

### DedupFilter
```
DedupFilter
├── compute_hash()           # SHA256 hashing
├── create_minhash()         # MinHash signature
├── is_exact_duplicate()     # Exact dedup
├── is_near_duplicate()      # Fuzzy dedup (LSH)
├── assess_quality()         # Quality metrics
└── filter_all()             # Batch filtering
```

### ShardCreator
```
ShardCreator
├── load_all_chunks()        # Load chunks
├── chunks_to_dataframe()    # Convert to DataFrame
├── save_shard()             # Save Parquet shard
└── create_shards()          # Create all shards
```

### AnnotationExporter
```
AnnotationExporter
├── categorise_chunk()              # Niche classification
├── chunk_to_annotation_format()    # Format conversion
└── export_to_jsonl()               # JSONL export
```

### LabelStudioManager
```
LabelStudioManager
├── create_project()         # Project setup
├── import_tasks()           # Import JSONL
├── export_annotations()     # Export completed
└── get_project_stats()      # Statistics
```

## Workflow Architecture

### Main Preprocessing Pipeline

```python
@flow(name="data-preprocessing-pipeline")
def main_pipeline():
    extraction_result = extract_text_task()
    cleaning_result = clean_text_task(extraction_result)
    chunking_result = chunk_text_task(cleaning_result)
    filtering_result = deduplicate_filter_task(chunking_result)
    
    # Parallel execution
    sharding_result = create_shards_task(filtering_result)
    annotation_result = export_annotation_task(filtering_result)
    
    return results
```

### Annotation Workflow

```python
@flow(name="labelstudio-import-flow")
def import_to_labelstudio_flow():
    project_result = setup_labelstudio_project_task()
    import_result = import_tasks_task(project_result)
    stats_result = get_stats_task(project_result)
    return results

@flow(name="labelstudio-export-flow")
def export_from_labelstudio_flow():
    export_result = export_annotations_task()
    validation_result = validate_annotations_task(export_result)
    organisation_result = organise_by_niche_task(export_result)
    return results
```

## Data Schema

### Chunk Schema (JSON)
```json
{
  "id": "uuid4-string",
  "text": "chunk text content",
  "metadata": {
    "source_file": "/path/to/original",
    "filename": "document.pdf",
    "file_type": "pdf",
    "language": "en",
    "chunk_index": 0,
    "total_chunks": 10,
    "word_count": 500,
    "char_count": 3000,
    "quality_metrics": {
      "unique_ratio": 0.75,
      "repetition_ratio": 0.1,
      "alpha_ratio": 0.95,
      "readability_score": 60.0
    }
  }
}
```

### Parquet Schema
```
id: string
text: string
source_file: string
filename: string
file_type: string
language: string
chunk_index: int64
word_count: int64
char_count: int64
unique_words: int64
unique_ratio: float64
repetition_ratio: float64
alpha_ratio: float64
readability_score: float64
```

### Annotation Schema (JSONL)
```json
{
  "id": "uuid4-string",
  "instruction": "Explain the concept...",
  "input": "optional context",
  "response": "The concept is...",
  "niche": "technology",
  "quality_rating": 4,
  "original_text": "...",
  "annotation_timestamp": "2025-11-12T..."
}
```

## Configuration Schema

### Pipeline Configuration (YAML)
```yaml
paths:
  raw_data: "raw_data"
  intermediate: "intermediate"
  dataset: "dataset"

extraction:
  pdf: {method: "pypdf"}
  html: {method: "trafilatura"}

cleaning:
  languages: ["en"]
  min_line_length: 10

chunking:
  method: "sentence"
  min_chunk_size: 500
  max_chunk_size: 2000
  overlap: 100

deduplication:
  exact_dedup: true
  near_dedup: true
  minhash: {num_perm: 128, threshold: 0.8}

quality:
  min_words: 20
  max_words: 10000

sharding:
  format: "parquet"
  max_shard_size_mb: 500
  compression: "snappy"

annotation:
  niches: [...]
  auto_categorise: true

labelstudio:
  url: "http://localhost:8080"
  sampling_rate: 1.0

prefect:
  max_retries: 3
  parallel_workers: 4
```

## Technology Stack

### Core Libraries

**Text Processing:**
- pypdf, pdfminer.six - PDF extraction
- python-docx - DOCX extraction
- trafilatura, beautifulsoup4 - HTML extraction
- ftfy - Encoding fixes
- langdetect - Language detection

**Data Processing:**
- pandas - Data manipulation
- pyarrow - Parquet I/O
- datasketch - MinHash deduplication

**Workflow:**
- prefect - Orchestration
- label-studio-sdk - Annotation

**Utilities:**
- pyyaml - Configuration
- tqdm - Progress bars
- textstat - Readability metrics

### Infrastructure

**Containerisation:**
- Docker - Label Studio deployment
- Docker Compose - Multi-container orchestration

**Storage:**
- Local filesystem - Development
- Parquet format - Production data

## Quality Metrics

### Text Quality Scoring

```python
quality_score = weighted_average(
    word_count_score,        # 20% - appropriate length
    unique_ratio_score,      # 30% - vocabulary diversity
    repetition_ratio_score,  # 20% - low repetition
    alpha_ratio_score,       # 15% - mostly text
    readability_score        # 15% - readability
)
```

### Deduplication Strategy

1. **Exact Deduplication:** SHA256 hash comparison (O(1))
2. **Near Deduplication:** MinHash LSH with Jaccard similarity
   - num_perm = 128 (number of hash functions)
   - threshold = 0.8 (80% similarity)
   - Time complexity: O(n) average case

## Performance Considerations

### Memory Management
- Streaming processing where possible
- Batch size limits for large datasets
- Chunk-based processing

### Parallelisation
- File-level parallelisation in extraction
- Configurable worker count
- Task-level retries

### Storage Optimisation
- Parquet columnar format
- Snappy compression (default)
- Shard size limits (500MB default)

## Error Handling

### Retry Strategy
- 3 retries with exponential backoff
- 60-second retry delay
- Skip failed files, continue processing

### Logging
- Hierarchical logging (DEBUG, INFO, WARNING, ERROR)
- File and console outputs
- Per-stage statistics

## Security Considerations

- API keys via environment variables
- .gitignore for sensitive data
- No data committed to repository
- Docker isolation for Label Studio

## Scalability

### Current Capacity
- Single machine processing
- 10K+ documents
- GBs of text data

### Scale-Out Options
- Distributed Prefect execution
- Cloud storage (S3, GCS)
- Kubernetes deployment for Label Studio
- Spark for massive datasets

## Extensibility

### Adding New File Formats
1. Add extraction method in `extract_text.py`
2. Register file extension mapping
3. Update configuration schema

### Custom Quality Metrics
1. Add metric function in `dedup_filter.py`
2. Update quality assessment logic
3. Add to configuration

### New Niches
1. Update `annotation.niches` in config
2. Add keyword mappings
3. Regenerate annotation exports

## Monitoring & Observability

### Pipeline Metrics
- Files processed per stage
- Success/failure rates
- Processing durations
- Quality score distributions

### Annotation Metrics
- Tasks imported
- Completion rate
- Annotation quality ratings
- Inter-annotator agreement (manual)

---

For implementation details, see source code documentation in individual modules.

