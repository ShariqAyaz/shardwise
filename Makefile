.PHONY: help install setup clean docker-up docker-down pipeline test

help:
	@echo "ShardWise Pipeline Commands"
	@echo "============================"
	@echo "make install          - Install all dependencies"
	@echo "make setup            - Set up the environment and create directories"
	@echo "make docker-up        - Start Label Studio with Docker"
	@echo "make docker-down      - Stop Label Studio"
	@echo "make pipeline         - Run the complete preprocessing pipeline"
	@echo "make import-ls        - Import tasks to Label Studio"
	@echo "make export-ls        - Export annotations from Label Studio"
	@echo "make clean            - Clean intermediate files"
	@echo "make clean-all        - Clean all generated files including outputs"

install:
	pip install -r requirements.txt
	@echo "Dependencies installed successfully"

setup: install
	@echo "Creating directory structure..."
	mkdir -p raw_data/pdf raw_data/html raw_data/text
	mkdir -p intermediate/extracted intermediate/cleaned intermediate/chunks
	mkdir -p dataset/shards dataset/annotation_ready dataset/annotated
	mkdir -p logs
	@echo "Setup complete! Place your data in raw_data/ directories"

docker-up:
	docker-compose up -d
	@echo "Label Studio started at http://localhost:8080"

docker-down:
	docker-compose down

pipeline:
	python workflows/main_pipeline.py --config config/pipeline_config.yaml

import-ls:
	python workflows/annotation_sync.py import --config config/pipeline_config.yaml

export-ls:
	@read -p "Enter Project ID: " pid; \
	python workflows/annotation_sync.py export --project-id $$pid --config config/pipeline_config.yaml

clean:
	rm -rf intermediate/
	@echo "Cleaned intermediate files"

clean-all: clean
	rm -rf dataset/
	@echo "Cleaned all generated files"

test:
	@echo "Running pipeline validation..."
	python -c "from scripts import extract_text, clean_text, chunk_text, dedup_filter, create_shards; print('All imports successful')"

