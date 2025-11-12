"""
Label Studio Integration Module
Manages Label Studio projects, imports, and exports
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

import yaml

try:
    from label_studio_sdk import Client
except ImportError:
    Client = None


class LabelStudioManager:
    """Manages Label Studio integration"""
    
    def __init__(self, config_path: str = "config/pipeline_config.yaml"):
        """Initialise Label Studio manager"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.ls_config = self.config['labelstudio']
        self.annotation_path = Path(self.config['paths']['dataset']) / 'annotation_ready'
        
        # Set up logging
        logging.basicConfig(
            level=self.config['logging']['level'],
            format=self.config['logging']['format']
        )
        self.logger = logging.getLogger(__name__)
        
        if not Client:
            raise ImportError("label-studio-sdk is required. Install with: pip install label-studio-sdk")
        
        # Get API key from environment or config
        api_key = os.getenv('LABELSTUDIO_API_KEY', self.ls_config.get('api_key', ''))
        
        if not api_key:
            self.logger.warning("No Label Studio API key found. Some operations may fail.")
        
        # Initialise client
        try:
            self.client = Client(
                url=self.ls_config['url'],
                api_key=api_key
            )
            self.logger.info(f"Connected to Label Studio at {self.ls_config['url']}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Label Studio: {e}")
            self.client = None
    
    def load_labeling_config(self) -> str:
        """Load Label Studio labeling configuration"""
        config_path = Path('config/labelstudio_config.xml')
        
        if not config_path.exists():
            self.logger.warning(f"Label Studio config not found at {config_path}")
            return self.get_default_config()
        
        with open(config_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    def get_default_config(self) -> str:
        """Get default Label Studio configuration"""
        return """
<View>
  <Header value="LLM Instruction Fine-tuning Annotation"/>
  <Text name="text" value="$text"/>
  <View>
    <Header value="Create Instruction-Response Pair"/>
    <TextArea name="instruction" toName="text" placeholder="Instruction..." rows="3"/>
    <TextArea name="response" toName="text" placeholder="Response..." rows="5"/>
  </View>
  <Choices name="niche" toName="text" choice="single">
    <Choice value="general"/>
    <Choice value="finance"/>
    <Choice value="health"/>
    <Choice value="technology"/>
    <Choice value="science"/>
  </Choices>
  <Rating name="quality" toName="text" maxRating="5"/>
</View>
"""
    
    def create_project(self, project_name: Optional[str] = None) -> Optional[Dict]:
        """Create a new Label Studio project"""
        if not self.client:
            self.logger.error("Label Studio client not initialised")
            return None
        
        project_name = project_name or self.ls_config['project_name']
        
        try:
            # Check if project already exists
            projects = self.client.get_projects()
            for project in projects:
                if project.get_params()['title'] == project_name:
                    self.logger.info(f"Project '{project_name}' already exists (ID: {project.id})")
                    return project.get_params()
            
            # Create new project
            label_config = self.load_labeling_config()
            
            project = self.client.start_project(
                title=project_name,
                description=self.ls_config.get('project_description', ''),
                label_config=label_config
            )
            
            self.logger.info(f"Created project '{project_name}' (ID: {project.id})")
            return project.get_params()
            
        except Exception as e:
            self.logger.error(f"Error creating project: {e}")
            return None
    
    def get_project_by_name(self, project_name: str) -> Optional[int]:
        """Get project ID by name"""
        if not self.client:
            return None
        
        try:
            projects = self.client.get_projects()
            for project in projects:
                if project.get_params()['title'] == project_name:
                    return project.id
            return None
        except Exception as e:
            self.logger.error(f"Error getting project: {e}")
            return None
    
    def import_tasks(self, project_id: int, jsonl_file: Path) -> int:
        """Import tasks from JSONL file to Label Studio project"""
        if not self.client:
            self.logger.error("Label Studio client not initialised")
            return 0
        
        try:
            # Read JSONL file
            tasks = []
            with open(jsonl_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        data = json.loads(line)
                        # Convert to Label Studio task format
                        task = {
                            'data': {
                                'text': data.get('text', ''),
                                'id': data.get('id', ''),
                                'niche': data.get('niche', 'general')
                            }
                        }
                        tasks.append(task)
            
            if not tasks:
                self.logger.warning(f"No tasks found in {jsonl_file}")
                return 0
            
            # Apply sampling if configured
            sampling_rate = self.ls_config.get('sampling_rate', 1.0)
            if sampling_rate < 1.0:
                import random
                sample_size = int(len(tasks) * sampling_rate)
                tasks = random.sample(tasks, sample_size)
                self.logger.info(f"Sampled {len(tasks)} tasks (rate: {sampling_rate})")
            
            # Import tasks
            project = self.client.get_project(project_id)
            project.import_tasks(tasks)
            
            self.logger.info(f"Imported {len(tasks)} tasks to project {project_id}")
            return len(tasks)
            
        except Exception as e:
            self.logger.error(f"Error importing tasks from {jsonl_file}: {e}")
            return 0
    
    def import_all_niches(self, project_name: Optional[str] = None) -> Dict[str, int]:
        """Import all niche JSONL files to Label Studio"""
        project_name = project_name or self.ls_config['project_name']
        
        # Get or create project
        project_id = self.get_project_by_name(project_name)
        if not project_id:
            project = self.create_project(project_name)
            if not project:
                self.logger.error("Failed to create project")
                return {}
            project_id = project['id']
        
        # Find all JSONL files
        jsonl_files = list(self.annotation_path.glob('*.jsonl'))
        
        if not jsonl_files:
            self.logger.warning(f"No JSONL files found in {self.annotation_path}")
            return {}
        
        # Import each file
        import_stats = {}
        for jsonl_file in jsonl_files:
            niche = jsonl_file.stem
            count = self.import_tasks(project_id, jsonl_file)
            import_stats[niche] = count
        
        self.logger.info(f"Import complete: {sum(import_stats.values())} total tasks")
        return import_stats
    
    def export_annotations(self, project_id: int, output_path: Path) -> int:
        """Export completed annotations from Label Studio"""
        if not self.client:
            self.logger.error("Label Studio client not initialised")
            return 0
        
        try:
            project = self.client.get_project(project_id)
            tasks = project.get_labeled_tasks()
            
            if not tasks:
                self.logger.warning(f"No completed annotations found in project {project_id}")
                return 0
            
            # Convert to SFT format
            sft_data = []
            for task in tasks:
                annotations = task.get('annotations', [])
                if not annotations:
                    continue
                
                # Get the latest annotation
                annotation = annotations[-1]
                result = annotation.get('result', [])
                
                # Extract fields
                instruction = None
                input_context = None
                response = None
                niche = None
                quality = None
                
                for item in result:
                    if item.get('from_name') == 'instruction':
                        instruction = item.get('value', {}).get('text', [''])[0]
                    elif item.get('from_name') == 'input_context':
                        input_context = item.get('value', {}).get('text', [''])[0]
                    elif item.get('from_name') == 'response':
                        response = item.get('value', {}).get('text', [''])[0]
                    elif item.get('from_name') == 'niche':
                        niche = item.get('value', {}).get('choices', ['general'])[0]
                    elif item.get('from_name') == 'quality':
                        quality = item.get('value', {}).get('rating')
                
                # Create SFT record
                sft_record = {
                    'id': task['data'].get('id'),
                    'instruction': instruction or '',
                    'input': input_context or '',
                    'response': response or '',
                    'niche': niche or 'general',
                    'quality_rating': quality,
                    'original_text': task['data'].get('text', ''),
                    'annotation_timestamp': annotation.get('created_at')
                }
                
                sft_data.append(sft_record)
            
            # Save to JSONL
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                for record in sft_data:
                    json_line = json.dumps(record, ensure_ascii=False)
                    f.write(json_line + '\n')
            
            self.logger.info(f"Exported {len(sft_data)} annotations to {output_path}")
            return len(sft_data)
            
        except Exception as e:
            self.logger.error(f"Error exporting annotations: {e}")
            return 0
    
    def get_project_stats(self, project_id: int) -> Dict:
        """Get annotation statistics for a project"""
        if not self.client:
            return {}
        
        try:
            project = self.client.get_project(project_id)
            params = project.get_params()
            
            total_tasks = params.get('task_number', 0)
            total_annotations = params.get('num_tasks_with_annotations', 0)
            
            return {
                'project_id': project_id,
                'project_name': params.get('title'),
                'total_tasks': total_tasks,
                'annotated_tasks': total_annotations,
                'completion_rate': total_annotations / total_tasks if total_tasks > 0 else 0
            }
        except Exception as e:
            self.logger.error(f"Error getting project stats: {e}")
            return {}


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Manage Label Studio projects and annotations")
    parser.add_argument(
        '--config',
        default='config/pipeline_config.yaml',
        help='Path to configuration file'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Create project command
    create_parser = subparsers.add_parser('create', help='Create Label Studio project')
    create_parser.add_argument('--name', help='Project name')
    
    # Import command
    import_parser = subparsers.add_parser('import', help='Import tasks to Label Studio')
    import_parser.add_argument('--project', help='Project name')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export annotations from Label Studio')
    export_parser.add_argument('--project-id', type=int, required=True, help='Project ID')
    export_parser.add_argument('--output', required=True, help='Output file path')
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Get project statistics')
    stats_parser.add_argument('--project-id', type=int, required=True, help='Project ID')
    
    args = parser.parse_args()
    
    manager = LabelStudioManager(args.config)
    
    if args.command == 'create':
        project = manager.create_project(args.name)
        if project:
            print(f"Project created: {project}")
    
    elif args.command == 'import':
        stats = manager.import_all_niches(args.project)
        print(f"Import statistics: {stats}")
    
    elif args.command == 'export':
        count = manager.export_annotations(args.project_id, Path(args.output))
        print(f"Exported {count} annotations")
    
    elif args.command == 'stats':
        stats = manager.get_project_stats(args.project_id)
        print(f"Project statistics: {stats}")
    
    else:
        parser.print_help()


if __name__ == '__main__':
    main()

