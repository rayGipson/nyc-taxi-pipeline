.PHONY: help setup up down logs clean test lint run extract validate transform load pipeline

# Default target
.DEFAULT_GOAL := help

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
RED := \033[0;31m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)NYC Taxi Pipeline - Available Commands$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-15s$(NC) %s\n", $$1, $$2}'

setup: ## Initial setup (copy .env.example to .env)
	@echo "$(BLUE)Setting up environment...$(NC)"
	@if [ ! -f .env ]; then cp .env.example .env; echo "$(GREEN)Created .env file$(NC)"; else echo "$(GREEN).env already exists$(NC)"; fi
	@mkdir -p data/raw data/staging data/rejected
	@touch data/raw/.gitkeep data/staging/.gitkeep data/rejected/.gitkeep
	@echo "$(GREEN)Setup complete!$(NC)"

up: ## Start all services
	@echo "$(BLUE)Starting services...$(NC)"
	docker compose up -d
	@echo "$(GREEN)Services started. Waiting for postgres...$(NC)"
	@sleep 5
	@echo "$(GREEN)Ready!$(NC)"

down: ## Stop all services
	@echo "$(BLUE)Stopping services...$(NC)"
	docker compose down
	@echo "$(GREEN)Services stopped$(NC)"

down-v: ## Stop services and remove volumes (DESTRUCTIVE)
	@echo "$(RED)Stopping services and removing volumes...$(NC)"
	docker compose down -v
	@echo "$(GREEN)Services and volumes removed$(NC)"

logs: ## Tail logs from all services
	docker compose logs -f

logs-postgres: ## Tail postgres logs
	docker compose logs -f postgres

logs-pipeline: ## Tail pipeline logs
	docker compose logs -f pipeline

clean: ## Clean generated files and data
	@echo "$(BLUE)Cleaning...$(NC)"
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf data/raw/* data/staging/* data/rejected/*
	@echo "$(GREEN)Cleaned!$(NC)"

test: ## Run all tests
	@echo "$(BLUE)Running tests...$(NC)"
	docker compose run --rm pipeline pytest tests/ -v --cov=src --cov-report=term-missing

test-unit: ## Run unit tests only
	@echo "$(BLUE)Running unit tests...$(NC)"
	docker compose run --rm pipeline pytest tests/unit/ -v

test-integration: ## Run integration tests only
	@echo "$(BLUE)Running integration tests...$(NC)"
	docker compose run --rm pipeline pytest tests/integration/ -v

lint: ## Run linting (future: add ruff/black)
	@echo "$(BLUE)Linting...$(NC)"
	@echo "$(GREEN)Linting not configured yet$(NC)"

extract: ## Download raw taxi data
	@echo "$(BLUE)Extracting data...$(NC)"
	docker compose run --rm pipeline python -m src.extract.downloader

validate: ## Validate raw data
	@echo "$(BLUE)Validating data...$(NC)"
	docker compose run --rm pipeline python -m src.validate.validator

transform: ## Transform validated data
	@echo "$(BLUE)Transforming data...$(NC)"
	docker compose run --rm pipeline python -m src.transform.cleaner

load: ## Load data to Postgres
	@echo "$(BLUE)Loading data...$(NC)"
	docker compose run --rm pipeline python -m src.load.loader

pipeline: ## Run full pipeline (extract -> validate -> transform -> load)
	@echo "$(BLUE)Running full pipeline...$(NC)"
	docker compose run --rm pipeline python -m src.main

db-shell: ## Open psql shell
	docker compose exec postgres psql -U dataeng -d taxi_analytics

db-reset: down-v up ## Reset database (DESTRUCTIVE)
	@echo "$(GREEN)Database reset complete$(NC)"
