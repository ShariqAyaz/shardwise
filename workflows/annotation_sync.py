"""
Annotation Synchronisation Workflow
Manages Label Studio annotation import/export and post-processing
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional
from datetime import timedelta
import json

from prefect import flow, task
from prefect.tasks import task_input_hash
import yaml

# Import Label Studio manager
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.labelstudio_setup import LabelStudioManager


@task(
    name="setup_labelstudio_project",
    description="Create or get Label Studio project",
    retries=3,
    retry_delay_seconds=30
)
def setup_labelstudio_project_task(config_path: str, project_name: Optional[str] = None) -> Dict:
    """Task to set up Label Studio project"""
    logger = logging.getLogger(__name__)
    logger.info("Setting up Label Studio project")
    
    try:
        manager = LabelStudioManager(config_path)
        project = manager.create_project(project_name)
        
        if not project:
            return {
                'status': 'failed',
                'error': 'Failed to create or get project'
            }
        
        return {
            'status': 'success',
            'project_id': project.get('id'),
            'project_name': project.get('title')
        }
    except Exception as e:
        logger.error(f"Project setup failed: {e}")
        return {
            'status': 'failed',
            'error': str(e)
        }


@task(
    name="import_tasks_to_labelstudio",
    description="Import annotation tasks to Label Studio",
    retries=3,
    retry_delay_seconds=30
)
def import_tasks_task(config_path: str, project_result: Dict) -> Dict:
    """Task to import tasks to Label Studio"""
    logger = logging.getLogger(__name__)
    logger.info("Importing tasks to Label Studio")
    
    if project_result['status'] != 'success':
        logger.warning("Skipping import due to project setup failure")
        return {'status': 'skipped'}
    
    try:
        manager = LabelStudioManager(config_path)
        project_name = project_result.get('project_name')
        
        import_stats = manager.import_all_niches(project_name)
        
        total_imported = sum(import_stats.values())
        
        return {
            'status': 'success',
            'total_imported': total_imported,
            'niche_stats': import_stats
        }
    except Exception as e:
        logger.error(f"Task import failed: {e}")
        return {
            'status': 'failed',
            'error': str(e)
        }


@task(
    name="export_annotations",
    description="Export completed annotations from Label Studio",
    retries=2,
    retry_delay_seconds=30
)
def export_annotations_task(config_path: str, project_id: int, output_path: str) -> Dict:
    """Task to export annotations from Label Studio"""
    logger = logging.getLogger(__name__)
    logger.info(f"Exporting annotations from project {project_id}")
    
    try:
        manager = LabelStudioManager(config_path)
        output_file = Path(output_path)
        
        count = manager.export_annotations(project_id, output_file)
        
        return {
            'status': 'success',
            'exported_count': count,
            'output_file': str(output_file)
        }
    except Exception as e:
        logger.error(f"Annotation export failed: {e}")
        return {
            'status': 'failed',
            'error': str(e)
        }


@task(
    name="validate_annotations",
    description="Validate exported annotations",
    retries=1
)
def validate_annotations_task(export_result: Dict) -> Dict:
    """Task to validate exported annotations"""
    logger = logging.getLogger(__name__)
    logger.info("Validating exported annotations")
    
    if export_result['status'] != 'success':
        logger.warning("Skipping validation due to export failure")
        return {'status': 'skipped'}
    
    try:
        output_file = Path(export_result['output_file'])
        
        if not output_file.exists():
            return {
                'status': 'failed',
                'error': 'Export file not found'
            }
        
        # Read and validate annotations
        valid_count = 0
        invalid_count = 0
        
        with open(output_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    record = json.loads(line)
                    
                    # Check required fields
                    if not record.get('instruction') or not record.get('response'):
                        invalid_count += 1
                        logger.warning(f"Line {line_num}: Missing instruction or response")
                    else:
                        valid_count += 1
                        
                except json.JSONDecodeError:
                    invalid_count += 1
                    logger.warning(f"Line {line_num}: Invalid JSON")
        
        return {
            'status': 'success',
            'valid_annotations': valid_count,
            'invalid_annotations': invalid_count,
            'validation_rate': valid_count / (valid_count + invalid_count) if (valid_count + invalid_count) > 0 else 0
        }
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        return {
            'status': 'failed',
            'error': str(e)
        }


@task(
    name="organise_by_niche",
    description="Organise exported annotations by niche",
    retries=1
)
def organise_by_niche_task(export_result: Dict, output_dir: str) -> Dict:
    """Task to organise annotations by niche"""
    logger = logging.getLogger(__name__)
    logger.info("Organising annotations by niche")
    
    if export_result['status'] != 'success':
        logger.warning("Skipping organisation due to export failure")
        return {'status': 'skipped'}
    
    try:
        output_file = Path(export_result['output_file'])
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Group by niche
        niche_records = {}
        
        with open(output_file, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                
                record = json.loads(line)
                niche = record.get('niche', 'general')
                
                if niche not in niche_records:
                    niche_records[niche] = []
                
                niche_records[niche].append(record)
        
        # Save separate files per niche
        niche_stats = {}
        for niche, records in niche_records.items():
            niche_file = output_path / f"sft_{niche}.jsonl"
            
            with open(niche_file, 'w', encoding='utf-8') as f:
                for record in records:
                    json_line = json.dumps(record, ensure_ascii=False)
                    f.write(json_line + '\n')
            
            niche_stats[niche] = len(records)
            logger.info(f"Saved {len(records)} annotations to {niche_file}")
        
        return {
            'status': 'success',
            'niches': list(niche_records.keys()),
            'niche_stats': niche_stats
        }
    except Exception as e:
        logger.error(f"Organisation failed: {e}")
        return {
            'status': 'failed',
            'error': str(e)
        }


@task(
    name="get_annotation_stats",
    description="Get annotation project statistics",
    retries=2
)
def get_stats_task(config_path: str, project_id: int) -> Dict:
    """Task to get annotation statistics"""
    logger = logging.getLogger(__name__)
    
    try:
        manager = LabelStudioManager(config_path)
        stats = manager.get_project_stats(project_id)
        
        return {
            'status': 'success',
            'stats': stats
        }
    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        return {
            'status': 'failed',
            'error': str(e)
        }


@flow(
    name="labelstudio-import-flow",
    description="Import tasks to Label Studio for annotation",
    log_prints=True
)
def import_to_labelstudio_flow(
    config_path: str = "config/pipeline_config.yaml",
    project_name: Optional[str] = None
) -> Dict:
    """
    Flow to import annotation tasks to Label Studio
    
    Args:
        config_path: Path to configuration file
        project_name: Optional project name override
    
    Returns:
        Dictionary with import results
    """
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("Label Studio Import Flow")
    logger.info("=" * 80)
    
    # Set up project
    project_result = setup_labelstudio_project_task(config_path, project_name)
    logger.info(f"Project setup: {project_result}")
    
    # Import tasks
    import_result = import_tasks_task(config_path, project_result)
    logger.info(f"Import result: {import_result}")
    
    # Get stats
    if project_result['status'] == 'success':
        stats_result = get_stats_task(config_path, project_result['project_id'])
        logger.info(f"Project stats: {stats_result}")
    else:
        stats_result = {'status': 'skipped'}
    
    return {
        'project': project_result,
        'import': import_result,
        'stats': stats_result
    }


@flow(
    name="labelstudio-export-flow",
    description="Export and process completed annotations from Label Studio",
    log_prints=True
)
def export_from_labelstudio_flow(
    config_path: str = "config/pipeline_config.yaml",
    project_id: Optional[int] = None,
    output_path: Optional[str] = None
) -> Dict:
    """
    Flow to export and process annotations from Label Studio
    
    Args:
        config_path: Path to configuration file
        project_id: Label Studio project ID
        output_path: Output path for exported annotations
    
    Returns:
        Dictionary with export and processing results
    """
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("Label Studio Export Flow")
    logger.info("=" * 80)
    
    # Load config to get defaults
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    if not output_path:
        output_path = str(Path(config['paths']['dataset']) / 'annotated' / 'annotations.jsonl')
    
    # Export annotations
    export_result = export_annotations_task(config_path, project_id, output_path)
    logger.info(f"Export result: {export_result}")
    
    # Validate annotations
    validation_result = validate_annotations_task(export_result)
    logger.info(f"Validation result: {validation_result}")
    
    # Organise by niche
    output_dir = str(Path(config['paths']['dataset']) / 'annotated' / 'by_niche')
    organisation_result = organise_by_niche_task(export_result, output_dir)
    logger.info(f"Organisation result: {organisation_result}")
    
    logger.info("=" * 80)
    logger.info("Export Flow Complete")
    logger.info("=" * 80)
    
    return {
        'export': export_result,
        'validation': validation_result,
        'organisation': organisation_result
    }


@flow(
    name="annotation-pipeline",
    description="Complete annotation workflow: import, monitor, export",
    log_prints=True
)
def full_annotation_pipeline(
    config_path: str = "config/pipeline_config.yaml",
    project_name: Optional[str] = None,
    project_id: Optional[int] = None,
    skip_import: bool = False,
    skip_export: bool = False
) -> Dict:
    """
    Complete annotation pipeline
    
    Args:
        config_path: Path to configuration file
        project_name: Optional project name
        project_id: Optional project ID for export
        skip_import: Skip import phase
        skip_export: Skip export phase
    
    Returns:
        Dictionary with complete pipeline results
    """
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("Full Annotation Pipeline")
    logger.info("=" * 80)
    
    results = {}
    
    # Import phase
    if not skip_import:
        logger.info("\n[Phase 1] Importing to Label Studio")
        import_result = import_to_labelstudio_flow(config_path, project_name)
        results['import'] = import_result
        
        # Get project ID for export
        if not project_id and import_result['project']['status'] == 'success':
            project_id = import_result['project']['project_id']
    else:
        logger.info("\n[Phase 1] Import skipped")
        results['import'] = {'status': 'skipped'}
    
    # Export phase
    if not skip_export and project_id:
        logger.info("\n[Phase 2] Exporting from Label Studio")
        export_result = export_from_labelstudio_flow(config_path, project_id)
        results['export'] = export_result
    else:
        logger.info("\n[Phase 2] Export skipped")
        results['export'] = {'status': 'skipped'}
    
    logger.info("\n" + "=" * 80)
    logger.info("Annotation Pipeline Complete")
    logger.info("=" * 80)
    
    return results


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Manage Label Studio annotation workflows")
    parser.add_argument(
        '--config',
        default='config/pipeline_config.yaml',
        help='Path to configuration file'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Import command
    import_parser = subparsers.add_parser('import', help='Import tasks to Label Studio')
    import_parser.add_argument('--project', help='Project name')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export annotations from Label Studio')
    export_parser.add_argument('--project-id', type=int, required=True, help='Project ID')
    export_parser.add_argument('--output', help='Output file path')
    
    # Full pipeline command
    full_parser = subparsers.add_parser('full', help='Run complete annotation pipeline')
    full_parser.add_argument('--project', help='Project name')
    full_parser.add_argument('--project-id', type=int, help='Project ID')
    full_parser.add_argument('--skip-import', action='store_true', help='Skip import phase')
    full_parser.add_argument('--skip-export', action='store_true', help='Skip export phase')
    
    args = parser.parse_args()
    
    if args.command == 'import':
        result = import_to_labelstudio_flow(args.config, args.project)
        print(f"\nImport result: {result}")
    
    elif args.command == 'export':
        result = export_from_labelstudio_flow(args.config, args.project_id, args.output)
        print(f"\nExport result: {result}")
    
    elif args.command == 'full':
        result = full_annotation_pipeline(
            args.config,
            args.project,
            args.project_id,
            args.skip_import,
            args.skip_export
        )
        print(f"\nPipeline result: {result}")
    
    else:
        parser.print_help()

