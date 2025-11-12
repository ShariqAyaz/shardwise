"""
Main Pipeline Workflow
Orchestrates the complete data preprocessing pipeline using Prefect
"""

import logging
from pathlib import Path
from typing import Dict, Optional

from prefect import flow, task
import yaml

# Import pipeline scripts
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.extract_text import TextExtractor
from scripts.clean_text import TextCleaner
from scripts.chunk_text import TextChunker
from scripts.dedup_filter import DedupFilter
from scripts.create_shards import ShardCreator
from scripts.export_annotation import AnnotationExporter


@task(
    name="extract_text",
    description="Extract text from raw data files",
    retries=3,
    retry_delay_seconds=60
)
def extract_text_task(config_path: str) -> Dict:
    """Task to extract text from raw files"""
    logger = logging.getLogger(__name__)
    logger.info("Starting text extraction")
    
    try:
        extractor = TextExtractor(config_path)
        extractor.extract_all()
        
        # Count extracted files
        output_path = Path(extractor.output_path)
        extracted_files = list(output_path.rglob('*.txt'))
        
        return {
            'status': 'success',
            'extracted_files': len(extracted_files)
        }
    except Exception as e:
        logger.error(f"Text extraction failed: {e}")
        return {
            'status': 'failed',
            'error': str(e)
        }


@task(
    name="clean_text",
    description="Clean and normalise extracted text",
    retries=3,
    retry_delay_seconds=60
)
def clean_text_task(config_path: str, extraction_result: Dict) -> Dict:
    """Task to clean and normalise text"""
    logger = logging.getLogger(__name__)
    logger.info("Starting text cleaning")
    
    if extraction_result['status'] != 'success':
        logger.warning("Skipping cleaning due to extraction failure")
        return {'status': 'skipped'}
    
    try:
        cleaner = TextCleaner(config_path)
        cleaner.clean_all()
        
        # Count cleaned files
        output_path = Path(cleaner.output_path)
        cleaned_files = list(output_path.rglob('*.txt'))
        
        return {
            'status': 'success',
            'cleaned_files': len(cleaned_files)
        }
    except Exception as e:
        logger.error(f"Text cleaning failed: {e}")
        return {
            'status': 'failed',
            'error': str(e)
        }


@task(
    name="chunk_text",
    description="Chunk cleaned text into manageable pieces",
    retries=3,
    retry_delay_seconds=60
)
def chunk_text_task(config_path: str, cleaning_result: Dict) -> Dict:
    """Task to chunk cleaned text"""
    logger = logging.getLogger(__name__)
    logger.info("Starting text chunking")
    
    if cleaning_result['status'] != 'success':
        logger.warning("Skipping chunking due to cleaning failure")
        return {'status': 'skipped'}
    
    try:
        chunker = TextChunker(config_path)
        chunker.chunk_all()
        
        # Count chunks
        output_path = Path(chunker.output_path)
        chunk_files = list(output_path.rglob('*.json'))
        
        return {
            'status': 'success',
            'total_chunks': len(chunk_files)
        }
    except Exception as e:
        logger.error(f"Text chunking failed: {e}")
        return {
            'status': 'failed',
            'error': str(e)
        }


@task(
    name="deduplicate_filter",
    description="Deduplicate and filter chunks by quality",
    retries=2,
    retry_delay_seconds=60
)
def deduplicate_filter_task(config_path: str, chunking_result: Dict) -> Dict:
    """Task to deduplicate and filter chunks"""
    logger = logging.getLogger(__name__)
    logger.info("Starting deduplication and filtering")
    
    if chunking_result['status'] != 'success':
        logger.warning("Skipping deduplication due to chunking failure")
        return {'status': 'skipped'}
    
    try:
        dedup_filter = DedupFilter(config_path)
        dedup_filter.filter_all()
        
        # Count remaining chunks
        output_path = Path(dedup_filter.input_path)
        remaining_chunks = list(output_path.rglob('*.json'))
        
        return {
            'status': 'success',
            'filtered_chunks': len(remaining_chunks),
            'removed_chunks': chunking_result['total_chunks'] - len(remaining_chunks)
        }
    except Exception as e:
        logger.error(f"Deduplication and filtering failed: {e}")
        return {
            'status': 'failed',
            'error': str(e)
        }


@task(
    name="create_shards",
    description="Create Parquet shards from filtered chunks",
    retries=2,
    retry_delay_seconds=60
)
def create_shards_task(config_path: str, filtering_result: Dict) -> Dict:
    """Task to create Parquet shards"""
    logger = logging.getLogger(__name__)
    logger.info("Starting shard creation")
    
    if filtering_result['status'] != 'success':
        logger.warning("Skipping shard creation due to filtering failure")
        return {'status': 'skipped'}
    
    try:
        shard_creator = ShardCreator(config_path)
        shard_creator.create_shards()
        
        # Count shards
        output_path = Path(shard_creator.output_path)
        shard_files = list(output_path.glob('*.parquet'))
        
        return {
            'status': 'success',
            'total_shards': len(shard_files)
        }
    except Exception as e:
        logger.error(f"Shard creation failed: {e}")
        return {
            'status': 'failed',
            'error': str(e)
        }


@task(
    name="export_annotation",
    description="Export chunks for annotation",
    retries=2,
    retry_delay_seconds=60
)
def export_annotation_task(config_path: str, filtering_result: Dict) -> Dict:
    """Task to export chunks for annotation"""
    logger = logging.getLogger(__name__)
    logger.info("Starting annotation export")
    
    if filtering_result['status'] != 'success':
        logger.warning("Skipping annotation export due to filtering failure")
        return {'status': 'skipped'}
    
    try:
        exporter = AnnotationExporter(config_path)
        exporter.export_all()
        
        # Count exported files
        output_path = Path(exporter.output_path)
        jsonl_files = list(output_path.glob('*.jsonl'))
        
        return {
            'status': 'success',
            'exported_niches': len(jsonl_files)
        }
    except Exception as e:
        logger.error(f"Annotation export failed: {e}")
        return {
            'status': 'failed',
            'error': str(e)
        }


@flow(
    name="data-preprocessing-pipeline",
    description="Complete data preprocessing pipeline for LLM training",
    log_prints=True
)
def main_pipeline(config_path: str = "config/pipeline_config.yaml") -> Dict:
    """
    Main pipeline flow orchestrating all preprocessing steps
    
    Args:
        config_path: Path to pipeline configuration file
    
    Returns:
        Dictionary with pipeline execution results
    """
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("Starting ShardWise Data Preprocessing Pipeline")
    logger.info("=" * 80)
    
    # Load configuration
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Execute pipeline stages sequentially
    logger.info("\n[1/6] Text Extraction")
    extraction_result = extract_text_task(config_path)
    logger.info(f"Extraction result: {extraction_result}")
    
    logger.info("\n[2/6] Text Cleaning")
    cleaning_result = clean_text_task(config_path, extraction_result)
    logger.info(f"Cleaning result: {cleaning_result}")
    
    logger.info("\n[3/6] Text Chunking")
    chunking_result = chunk_text_task(config_path, cleaning_result)
    logger.info(f"Chunking result: {chunking_result}")
    
    logger.info("\n[4/6] Deduplication & Filtering")
    filtering_result = deduplicate_filter_task(config_path, chunking_result)
    logger.info(f"Filtering result: {filtering_result}")
    
    # Run sharding and annotation export in parallel
    logger.info("\n[5/6] Creating Shards")
    sharding_result = create_shards_task(config_path, filtering_result)
    logger.info(f"Sharding result: {sharding_result}")
    
    logger.info("\n[6/6] Exporting for Annotation")
    annotation_result = export_annotation_task(config_path, filtering_result)
    logger.info(f"Annotation export result: {annotation_result}")
    
    # Compile final results
    final_result = {
        'extraction': extraction_result,
        'cleaning': cleaning_result,
        'chunking': chunking_result,
        'filtering': filtering_result,
        'sharding': sharding_result,
        'annotation_export': annotation_result,
        'pipeline_status': 'success' if all(
            r['status'] in ['success', 'skipped'] 
            for r in [extraction_result, cleaning_result, chunking_result, 
                     filtering_result, sharding_result, annotation_result]
        ) else 'failed'
    }
    
    logger.info("\n" + "=" * 80)
    logger.info("Pipeline Execution Complete")
    logger.info(f"Overall Status: {final_result['pipeline_status']}")
    logger.info("=" * 80)
    
    return final_result


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Run the complete data preprocessing pipeline")
    parser.add_argument(
        '--config',
        default='config/pipeline_config.yaml',
        help='Path to configuration file'
    )
    
    args = parser.parse_args()
    
    # Run the pipeline
    result = main_pipeline(args.config)
    
    # Print summary
    print("\n" + "=" * 80)
    print("PIPELINE SUMMARY")
    print("=" * 80)
    for stage, stage_result in result.items():
        if stage != 'pipeline_status':
            print(f"{stage:20s}: {stage_result}")
    print("=" * 80)
    print(f"Overall Status: {result['pipeline_status']}")
    print("=" * 80)

