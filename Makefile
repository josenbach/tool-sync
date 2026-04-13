.PHONY: help install setup run run-local daily-sync daily-sync-prod subset subset-analysis docker-build docker-build-legacy docker-run docker-up docker-down docker-clean clean lint format format-check lint-fix

# Default target
.DEFAULT_GOAL := help

# Python interpreter
PYTHON := python3

# Daily sync script
DAILY_SYNC := daily_tool_sync.py

# Docker image name
DOCKER_IMAGE := ion-tool-daily-sync

help:
	@echo "Available targets:"
	@echo ""
	@echo "Docker execution (default):"
	@echo "  make run              - Run daily sync: use Docker if running, else run locally (V2 Production)"
	@echo "  make docker-up        - Run using docker-compose"
	@echo "  make docker-down      - Stop docker-compose containers"
	@echo ""
	@echo "Direct execution (for testing):"
	@echo "  make run-local        - Run daily sync directly on host (V2 Production)"
	@echo "  make daily-sync       - Run daily sync in V1 Sandbox (for testing)"
	@echo "  make daily-sync-prod  - Run daily sync in V2 Production (explicit)"
	@echo "  make subset           - Run subset test (edit tests/subset_tools.csv first)"
	@echo "  make subset-analysis  - Run subset test in analysis-only mode (no changes)"
	@echo "  make si-conflicts     - Generate service interval conflicts CSV (active tools only)"
	@echo "  make si-conflicts-all - Generate SI conflicts (include lost/offsite, exclude only I-status)"
	@echo ""
	@echo "Code quality:"
	@echo "  make lint             - Run Ruff linter to check code"
	@echo "  make format           - Format code with Ruff"
	@echo "  make format-check     - Check if code is formatted correctly"
	@echo "  make lint-fix         - Run Ruff linter and auto-fix issues"
	@echo ""
	@echo "Setup and maintenance:"
	@echo "  make docker-build     - Build Docker image only"
	@echo "  make docker-build-legacy - Build without BuildKit (if docker-build fails with lease error)"
	@echo "  make install          - Install Python dependencies"
	@echo "  make setup            - Set up environment file from template"
	@echo "  make clean            - Clean up temporary files"
	@echo "  make docker-clean     - Clean up Docker images and containers"

# Default run target - use Docker if available, otherwise run locally (V2 Production)
run:
	@if docker info >/dev/null 2>&1; then \
		$(MAKE) docker-run; \
	else \
		echo "Docker is not running. Running daily sync locally (V2 Production)..."; \
		$(MAKE) run-local; \
	fi

# Run directly on host (for testing)
run-local:
	@echo "Running daily sync directly on host (V2 Production)..."
	ENVIRONMENT=v2_production $(PYTHON) $(DAILY_SYNC)

# Daily sync in V1 Sandbox (for testing)
daily-sync:
	@echo "Running daily sync in V1 Sandbox..."
	ENVIRONMENT=v1_sandbox $(PYTHON) $(DAILY_SYNC)

# Daily sync in V2 Production (explicit)
daily-sync-prod:
	@echo "Running daily sync in V2 Production..."
	ENVIRONMENT=v2_production $(PYTHON) $(DAILY_SYNC)

# Subset test targets
CSV ?= tests/subset_tools.csv
ENV ?= v2_production

subset:
	@echo "Running subset test (LIVE) with $(CSV)..."
	$(PYTHON) tests/subset_test.py --environment $(ENV) --csv-file $(CSV)

subset-analysis:
	@echo "Running subset test (analysis only) with $(CSV)..."
	$(PYTHON) tests/subset_test.py --environment $(ENV) --csv-file $(CSV) --analysis-only

si-conflicts:
	@echo "Generating service interval conflicts (excluding inactive tools)..."
	$(PYTHON) tests/generate_service_interval_conflicts.py

si-conflicts-all:
	@echo "Generating service interval conflicts (excluding only I-status, including lost/offsite)..."
	$(PYTHON) tests/generate_service_interval_conflicts.py --include-lost

# Install dependencies
install:
	@echo "Installing Python dependencies..."
	$(PYTHON) -m pip install -r requirements.txt

# Set up environment file
setup:
	@if [ ! -f .env ]; then \
		cp env_template.txt .env; \
		echo "Created .env file from template. Please edit it with your credentials."; \
	else \
		echo ".env file already exists. Skipping setup."; \
	fi

# Docker targets
docker-build:
	@echo "Building Docker image..."
	PATH="/usr/local/bin:/Applications/Docker.app/Contents/Resources/bin:$$PATH" docker build -t $(DOCKER_IMAGE) .

# Fallback if docker-build fails with "lease does not exist" (BuildKit/metadata glitch)
docker-build-legacy:
	@echo "Building Docker image (legacy builder, no BuildKit)..."
	DOCKER_BUILDKIT=0 docker build -t $(DOCKER_IMAGE) .

docker-run: docker-build
	@echo "Running daily sync in Docker container (V2 Production)..."
	docker run --rm --env-file .env -e ENVIRONMENT=v2_production -v $(PWD)/tests:/app/tests $(DOCKER_IMAGE)

docker-up:
	@echo "Running daily sync using docker-compose..."
	docker-compose up --build

docker-down:
	@echo "Stopping docker-compose containers..."
	docker-compose down

docker-clean:
	@echo "Cleaning up Docker images and containers..."
	docker rmi $(DOCKER_IMAGE) 2>/dev/null || true
	docker-compose down --rmi all 2>/dev/null || true
	@echo "Docker cleanup complete."

# Code quality targets
lint:
	@echo "Running Ruff linter..."
	ruff check .

format:
	@echo "Formatting code with Ruff..."
	ruff format .

format-check:
	@echo "Checking code formatting..."
	ruff format --check .

lint-fix:
	@echo "Running Ruff linter with auto-fix..."
	ruff check --fix .

# Clean up temporary files
clean:
	@echo "Cleaning up temporary files..."
	find . -type d -name __pycache__ -exec rm -r {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.log" -delete 2>/dev/null || true
	@echo "Cleanup complete."

