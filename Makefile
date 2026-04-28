.PHONY: help install install-dev clean test test-coverage lint format type-check security-check build docker-build run-example all ci

.DEFAULT_GOAL := help

PYTHON := python3
PIP := $(PYTHON) -m pip

SRC_DIR := kafka_analyzer
TEST_DIR := tests
SCRIPTS_DIR := scripts

BLUE := \033[0;34m
GREEN := \033[0;32m
RED := \033[0;31m
YELLOW := \033[0;33m
NC := \033[0m

help: ## Show this help message
	@echo "$(BLUE)Kafka Analyzer - Development Commands$(NC)"
	@echo ""
	@echo "$(GREEN)Available targets:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-20s$(NC) %s\n", $$1, $$2}'

install: ## Install production dependencies
	$(PIP) install --upgrade pip setuptools wheel
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

install-dev: ## Install development dependencies
	$(PIP) install --upgrade pip setuptools wheel
	$(PIP) install -r requirements.txt
	$(PIP) install -r requirements-dev.txt
	$(PIP) install -e .
	pre-commit install

clean: ## Clean build artifacts and cache files
	find . -type f -name '*.pyc' -delete
	find . -type d -name '__pycache__' -delete
	find . -type d -name '*.egg-info' -exec rm -rf {} + 2>/dev/null || true
	rm -rf build/ dist/ .coverage htmlcov/ .pytest_cache/ .mypy_cache/ .tox/
	rm -f coverage.xml

test: ## Run unit tests
	pytest $(TEST_DIR) -v

test-coverage: ## Run tests with coverage report
	pytest $(TEST_DIR) -v --cov=$(SRC_DIR) --cov-report=html --cov-report=term --cov-report=xml

lint: ## Run linters
	flake8 --config=.flake8 $(SRC_DIR) $(TEST_DIR)

format: ## Format code with black and isort
	black $(SRC_DIR) $(TEST_DIR)
	isort $(SRC_DIR) $(TEST_DIR)

type-check: ## Run type checking with mypy
	mypy $(SRC_DIR) --ignore-missing-imports

security-check: ## Run security checks with bandit
	bandit -r $(SRC_DIR) -ll

build: clean ## Build distribution packages
	$(PYTHON) -m build

build-exe: clean ## Build standalone executable using PyInstaller
	pyinstaller kafka-analyzer.spec --clean

build-exe-onedir: clean ## Build executable in one-folder mode (for debugging)
	pyinstaller kafka-analyzer.spec --onedir --clean

docker-build: ## Build Docker image
	docker build -t kafka-analyzer:latest .

docker-run: ## Run Docker container with example config
	docker run -v $(PWD)/example_config.yaml:/config.yaml:ro \
		-v $(PWD)/reports:/home/analyzer/reports \
		kafka-analyzer:latest --config /config.yaml

run-example: ## Run analyzer with example configuration
	$(PYTHON) -m kafka_analyzer --config example_config.yaml --verbose

license-headers: ## Add Apache license headers to Python files
	$(PYTHON) $(SCRIPTS_DIR)/add_license_headers.py

pre-commit: ## Run pre-commit hooks on all files
	pre-commit run --all-files

ci: lint type-check security-check test-coverage ## Run all CI checks locally

all: clean install-dev ci build ## Run full build pipeline

tox: ## Run tests on multiple Python versions using tox
	tox

tox-recreate: ## Recreate tox environments
	tox -r
