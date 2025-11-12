# ShardWise

> A comprehensive data preprocessing and annotation pipeline for LLM training

ShardWise is a production-ready pipeline that transforms raw, messy data into clean, annotated datasets ready for supervised fine-tuning (SFT) of large language models.

## 🎯 Features

- **Multi-format Ingestion**: Extract text from PDFs, DOCX, HTML, and plain text files
- **Intelligent Cleaning**: Language detection, encoding fixes, and quality filtering
- **Semantic Chunking**: Split text whilst preserving sentence boundaries
- **Deduplication**: Exact and near-duplicate detection using MinHash LSH
- **Quality Filtering**: Automated quality assessment with configurable metrics
- **Efficient Storage**: Parquet sharding with compression
- **Niche Categorisation**: Automatic categorisation by domain (finance, health, tech, etc.)
- **Label Studio Integration**: Seamless annotation workflow
- **Workflow Orchestration**: Prefect-powered automation with retry logic
- **Traceable Metadata**: Complete audit trail from raw data to final annotations

## 📁 Project Structure

```
shardwise/
├── raw_data/              # Raw input data (gitignored)
│   ├── pdf/
│   ├── html/
│   └── text/
├── intermediate/          # Processing intermediates (gitignored)
│   ├── extracted/
│   ├── cleaned/
│   └── chunks/
├── dataset/               # Final outputs (gitignored)
│   ├── shards/           # Parquet shards
│   ├── annotation_ready/ # JSONL for annotation
│   └── annotated/        # Completed annotations
├── scripts/               # Core pipeline modules
│   ├── extract_text.py
│   ├── clean_text.py
│   ├── chunk_text.py
│   ├── dedup_filter.py
│   ├── create_shards.py
│   ├── export_annotation.py
│   └── labelstudio_setup.py
├── workflows/             # Prefect workflow orchestration
│   ├── main_pipeline.py
│   └── annotation_sync.py
├── config/                # Configuration files
│   ├── pipeline_config.yaml
│   └── labelstudio_config.xml
├── docker-compose.yml     # Label Studio setup
└── requirements.txt       # Python dependencies
```

## 🚀 Quick Start

### 1. Installation

```bash
# Clone the repository
cd shardwise

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Optional: Download NLTK data for better sentence splitting
python -c "import nltk; nltk.download('punkt')"
```

### 2. Configuration

Edit `config/pipeline_config.yaml` to customise:
- Chunk sizes and overlap
- Quality thresholds
- Language filters
- Niche categories
- Label Studio connection

### 3. Prepare Your Data

Place your raw data in the appropriate directories:

```bash
# PDFs
cp your_pdfs/*.pdf raw_data/pdf/

# HTML files
cp your_html/*.html raw_data/html/

# Text/DOCX files
cp your_documents/*.{txt,docx} raw_data/text/
```

### 4. Run the Pipeline

#### Option A: Run Complete Pipeline

```bash
# Execute the full preprocessing pipeline
python workflows/main_pipeline.py --config config/pipeline_config.yaml
```

#### Option B: Run Individual Stages

```bash
# Extract text
python scripts/extract_text.py --config config/pipeline_config.yaml

# Clean text
python scripts/clean_text.py --config config/pipeline_config.yaml

# Chunk text
python scripts/chunk_text.py --config config/pipeline_config.yaml

# Deduplicate and filter
python scripts/dedup_filter.py --config config/pipeline_config.yaml

# Create shards
python scripts/create_shards.py --config config/pipeline_config.yaml

# Export for annotation
python scripts/export_annotation.py --config config/pipeline_config.yaml
```

## 📊 Pipeline Stages

### Stage 1: Text Extraction

Extracts text from various file formats whilst preserving metadata.

**Supported formats:**
- PDF (pypdf, pdfminer.six)
- DOCX (python-docx)
- HTML (trafilatura, BeautifulSoup)
- Plain text

**Output:** `intermediate/extracted/`

### Stage 2: Text Cleaning

Cleans and normalises extracted text.

**Operations:**
- Fix encoding issues
- Remove URLs, emails, phone numbers
- Normalise whitespace and quotes
- Language detection and filtering
- Remove short/noisy lines

**Output:** `intermediate/cleaned/`

### Stage 3: Text Chunking

Splits text into manageable chunks with configurable size and overlap.

**Features:**
- Sentence-based chunking (default)
- Paragraph-based chunking
- Fixed-size chunking
- Configurable overlap for context preservation
- UUID generation for each chunk
- Metadata preservation

**Output:** `intermediate/chunks/`

### Stage 4: Deduplication & Filtering

Removes duplicate and low-quality chunks.

**Deduplication:**
- Exact deduplication (SHA256 hashing)
- Near-duplicate detection (MinHash LSH)

**Quality Metrics:**
- Word count range
- Vocabulary diversity
- Repetition ratio
- Alphabetic character ratio
- Readability score (Flesch Reading Ease)

**Output:** Filtered chunks in `intermediate/chunks/`

### Stage 5: Shard Creation

Creates compressed Parquet shards for efficient storage and access.

**Features:**
- Configurable shard sizes
- Multiple compression options (snappy, gzip, brotli)
- Columnar storage format
- Schema preservation
- Metadata tracking

**Output:** `dataset/shards/part-*.parquet`

### Stage 6: Annotation Export

Exports chunks to JSONL format, organised by niche.

**Features:**
- Automatic niche categorisation by keywords
- JSONL format for easy import
- Per-niche files for targeted annotation
- Export metadata

**Output:** `dataset/annotation_ready/*.jsonl`

## 🏷️ Label Studio Integration

### Setup Label Studio

```bash
# Start Label Studio with Docker
docker-compose up -d

# Access Label Studio at http://localhost:8080
# Create an account and get your API key
```

### Configure API Access

```bash
# Set API key
export LABELSTUDIO_API_KEY="your-api-key-here"

# Or add to config/pipeline_config.yaml
```

### Import Tasks for Annotation

```bash
# Create project and import tasks
python scripts/labelstudio_setup.py --config config/pipeline_config.yaml import

# Or use Prefect workflow
python workflows/annotation_sync.py import --project "ShardWise Annotation"
```

### Export Completed Annotations

```bash
# Export annotations
python scripts/labelstudio_setup.py --config config/pipeline_config.yaml export \
  --project-id 1 \
  --output dataset/annotated/annotations.jsonl

# Or use Prefect workflow
python workflows/annotation_sync.py export --project-id 1
```

### Annotation Output Format

Exported annotations are in SFT-ready format:

```json
{
  "id": "uuid",
  "instruction": "Explain the concept of...",
  "input": "optional context",
  "response": "The concept is...",
  "niche": "technology",
  "quality_rating": 4,
  "original_text": "...",
  "annotation_timestamp": "2025-11-12T..."
}
```

## 🔄 Workflow Orchestration

ShardWise uses Prefect for workflow orchestration with automatic retries, logging, and progress tracking.

### Run Complete Pipeline with Prefect

```bash
# Main preprocessing pipeline
python workflows/main_pipeline.py

# Annotation import workflow
python workflows/annotation_sync.py import

# Annotation export workflow
python workflows/annotation_sync.py export --project-id 1

# Complete annotation pipeline
python workflows/annotation_sync.py full --project "ShardWise"
```

### Prefect UI (Optional)

```bash
# Start Prefect server
prefect server start

# Access UI at http://localhost:4200
```

## ⚙️ Configuration

Key configuration options in `config/pipeline_config.yaml`:

### Chunking

```yaml
chunking:
  method: "sentence"  # sentence, paragraph, fixed
  min_chunk_size: 500  # words
  max_chunk_size: 2000  # words
  overlap: 100  # words
```

### Quality Filtering

```yaml
quality:
  min_words: 20
  max_words: 10000
  min_unique_words_ratio: 0.3
  max_repetition_ratio: 0.5
  min_alpha_ratio: 0.7
  calculate_readability: true
```

### Deduplication

```yaml
deduplication:
  exact_dedup: true
  near_dedup: true
  minhash:
    num_perm: 128
    threshold: 0.8
```

### Niche Categorisation

```yaml
annotation:
  niches:
    - name: "finance"
      keywords: ["financial", "investment", "trading"]
    - name: "health"
      keywords: ["medical", "health", "disease"]
    - name: "technology"
      keywords: ["software", "programming", "AI"]
```

## 📈 Monitoring & Logs

Pipeline execution logs are written to:
- Console output (INFO level)
- `logs/pipeline.log` (configurable)

Each stage reports:
- Files processed
- Success/failure counts
- Processing statistics
- Error details

## 🔧 Advanced Usage

### Custom Extraction for Single File

```bash
python scripts/extract_text.py --file path/to/document.pdf
```

### Quality Metrics Analysis

After filtering, examine `dataset/shards/shards_metadata.json` for:
- Total chunks processed
- Removal statistics
- Quality score distributions

### Sampling for Annotation

Adjust sampling rate in `config/pipeline_config.yaml`:

```yaml
labelstudio:
  sampling_rate: 0.1  # Annotate 10% of data
```

### Parallel Processing

Configure parallel workers in `config/pipeline_config.yaml`:

```yaml
prefect:
  parallel_workers: 4
```

## 🐛 Troubleshooting

### Label Studio Connection Failed

```bash
# Check if Label Studio is running
docker-compose ps

# Restart Label Studio
docker-compose restart

# Check logs
docker-compose logs labelstudio
```

### Memory Issues with Large Files

Adjust chunking parameters to process smaller pieces:

```yaml
chunking:
  max_chunk_size: 1000  # Reduce from 2000
```

### Missing Dependencies

```bash
# Install all dependencies
pip install -r requirements.txt

# For PDF processing issues, try pdfminer.six instead
# Update config/pipeline_config.yaml:
extraction:
  pdf:
    method: "pdfminer"
```

## 📚 Dependencies

Core libraries:
- **Text Extraction**: pypdf, python-docx, trafilatura, beautifulsoup4
- **Text Processing**: ftfy, langdetect, clean-text, textstat
- **Deduplication**: datasketch
- **Storage**: pyarrow, pandas
- **Orchestration**: prefect
- **Annotation**: label-studio-sdk

See `requirements.txt` for complete list.

## 🎯 Output Formats

### Parquet Shards

Columnar format with schema:
- `id`: Chunk UUID
- `text`: Chunk text
- `source_file`: Original file path
- `language`: Detected language
- `word_count`, `char_count`: Size metrics
- `quality_metrics`: Quality scores

### Annotation JSONL

One record per line:
```json
{"id": "...", "text": "...", "niche": "...", "metadata": {...}}
```

### Annotated JSONL

SFT-ready format:
```json
{"instruction": "...", "input": "...", "response": "...", "niche": "..."}
```

## 🤝 Contributing

Contributions are welcome! This pipeline is designed to be extensible.

### Adding New File Formats

Add extraction logic to `scripts/extract_text.py`:

```python
def extract_custom_format(self, file_path: Path) -> Optional[str]:
    # Your extraction logic
    return text
```

### Adding Custom Quality Metrics

Add to `scripts/dedup_filter.py`:

```python
def calculate_custom_metric(self, text: str) -> float:
    # Your metric calculation
    return score
```

## 📝 Licence

MIT

## 🙏 Acknowledgements

Built with:
- [Prefect](https://www.prefect.io/) for workflow orchestration
- [Label Studio](https://labelstud.io/) for annotation
- [Apache Arrow](https://arrow.apache.org/) for efficient data storage

---

**ShardWise** - Transform messy data into training-ready datasets for LLMs.

