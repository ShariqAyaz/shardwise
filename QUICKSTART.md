# ShardWise Quick Start Guide

Get up and running with ShardWise in 5 minutes.

## Prerequisites

- Python 3.8+
- Docker and Docker Compose (for Label Studio)
- 2GB+ free disk space

## Step 1: Environment Setup

```bash
# Navigate to project directory
cd shardwise

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Step 2: Add Your Data

```bash
# Copy your files to appropriate directories
cp /path/to/your/pdfs/*.pdf raw_data/pdf/
cp /path/to/your/html/*.html raw_data/html/
cp /path/to/your/text/*.txt raw_data/text/
```

## Step 3: Configure (Optional)

Edit `config/pipeline_config.yaml` if you need to customise:
- Language filters (default: English only)
- Chunk sizes (default: 500-2000 words)
- Quality thresholds
- Niche categories

## Step 4: Run the Pipeline

```bash
# Option A: Using Python directly
python workflows/main_pipeline.py

# Option B: Using Make
make pipeline
```

The pipeline will:
1. Extract text from all files in `raw_data/`
2. Clean and normalise the text
3. Chunk into manageable pieces
4. Remove duplicates and low-quality chunks
5. Create Parquet shards in `dataset/shards/`
6. Export JSONL files in `dataset/annotation_ready/`

## Step 5: Annotation (Optional)

### Start Label Studio

```bash
# Start Label Studio
docker-compose up -d

# Access at http://localhost:8080
# Create account and copy your API key
```

### Configure API Key

```bash
# Set environment variable
export LABELSTUDIO_API_KEY="your-key-here"

# Or create .env file
cp .env.example .env
# Edit .env and add your key
```

### Import Tasks

```bash
# Import all JSONL files to Label Studio
python workflows/annotation_sync.py import

# Or use Make
make import-ls
```

### Annotate

1. Go to http://localhost:8080
2. Open your project
3. Start annotating tasks
4. For each chunk, provide:
   - Instruction (what task does this represent?)
   - Response (the expected answer/output)
   - Niche category
   - Quality rating

### Export Annotations

```bash
# Export completed annotations
python workflows/annotation_sync.py export --project-id 1

# Or use Make (interactive)
make export-ls
```

Annotations will be saved in `dataset/annotated/` in SFT-ready format.

## Output Files

After running the pipeline, you'll have:

### Parquet Shards (for pre-training)
```
dataset/shards/
├── part-0000.parquet
├── part-0001.parquet
└── shards_metadata.json
```

### Annotation-Ready JSONL (for annotation)
```
dataset/annotation_ready/
├── general.jsonl
├── finance.jsonl
├── health.jsonl
├── technology.jsonl
└── science.jsonl
```

### Annotated Data (after Label Studio export)
```
dataset/annotated/
├── annotations.jsonl
└── by_niche/
    ├── sft_finance.jsonl
    ├── sft_health.jsonl
    └── sft_technology.jsonl
```

## Common Issues

### "No files found in raw_data directory"
- Make sure you've placed files in the correct subdirectories
- Check that files have the correct extensions (.pdf, .html, .txt, .docx)

### "Label Studio connection failed"
- Ensure Docker is running: `docker ps`
- Check Label Studio is accessible: `curl http://localhost:8080`
- Verify API key is set: `echo $LABELSTUDIO_API_KEY`

### "Memory error during processing"
- Reduce chunk size in config: `max_chunk_size: 1000`
- Process files in smaller batches
- Increase available RAM

### "Import failed: module not found"
- Ensure virtual environment is activated
- Reinstall dependencies: `pip install -r requirements.txt`

## Next Steps

1. **Customise Categories**: Edit niche definitions in `config/pipeline_config.yaml`
2. **Adjust Quality**: Tune quality thresholds based on your data
3. **Scale Up**: Add more data files and re-run the pipeline
4. **Export for Training**: Use Parquet shards or annotated JSONL for model training

## Getting Help

- Read the full [README.md](README.md) for detailed documentation
- Check pipeline logs in `logs/pipeline.log`
- Review configuration in `config/pipeline_config.yaml`

## Example Workflow

```bash
# Complete workflow from scratch
make setup              # Set up environment
cp ~/data/*.pdf raw_data/pdf/
make pipeline           # Run preprocessing
make docker-up          # Start Label Studio
make import-ls          # Import to Label Studio
# ... do annotations in Label Studio UI ...
make export-ls          # Export annotations
```

That's it! You now have a complete data preprocessing and annotation pipeline running.

