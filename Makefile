.PHONY: help bootstrap format test lint etl migrate qa clean

help: ## Show this help message
	@echo "Cricket Database System"
	@echo "======================"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

bootstrap: ## Bootstrap the project (install dependencies and setup)
	pip install -e .
	pip install -e ".[dev]"
	playwright install
	@echo "✅ Project bootstrapped successfully!"

format: ## Format code with black and isort
	black src/ tests/
	isort src/ tests/
	@echo "✅ Code formatted successfully!"

test: ## Run tests with pytest
	pytest tests/ -v --cov=src --cov-report=term-missing
	@echo "✅ Tests completed!"

lint: ## Run linting with flake8 and mypy
	flake8 src/ tests/
	mypy src/
	@echo "✅ Linting completed!"

etl: ## Run ETL pipeline (scrape and process data)
	python -m src.cli scrape
	python -m src.cli update --incremental
	@echo "✅ ETL pipeline completed!"

migrate: ## Run database migrations (SQL files) and setup
	python -m src.cli migrate-sql
	@echo "✅ SQL migrations applied!"

qa: ## Run data quality checks and validation
	python -m src.cricket_database.qa.runner
	python -m src.cli quality-check
	python -m src.cli validate-sources
	@echo "✅ Quality assurance completed!"

clean: ## Clean up temporary files
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf build/ dist/ .coverage htmlcov/ .pytest_cache/
	@echo "✅ Cleanup completed!"

# Additional useful targets
install: ## Install production dependencies
	pip install -e .

dev-install: ## Install development dependencies
	pip install -e ".[dev]"

setup-db: ## Initialize MySQL database and schema
	python -m src.cli setup-db

run-scraper: ## Run the cricket data scraper
	python -m src.cli scrape

dry-run: ## Run scraper in dry-run mode
	python -m src.cli scrape --dry-run

update-incremental: ## Run incremental data update
	python -m src.cli update --incremental

update-full: ## Run full data refresh
	python -m src.cli update --full

install-playwright: ## Install Playwright browsers
	playwright install

# Local development setup (no Docker)
setup-mysql: ## Setup local MySQL database
	@echo "Setting up local MySQL database..."
	@echo "Please ensure MySQL 8.0+ is installed and running"
	@echo "Run the setup script:"
	@echo "  Windows: scripts\\setup_mysql.bat"
	@echo "  Linux/macOS: ./scripts/setup_mysql.sh"
	@echo "  Or manually: mysql -u root -p < scripts/setup_mysql.sql"

start-mysql: ## Start local MySQL service (Windows)
	net start mysql80

stop-mysql: ## Stop local MySQL service (Windows)
	net stop mysql80

# Development shortcuts
dev: bootstrap setup-mysql ## Full development setup
ci: lint test ## Run CI pipeline locally

# Nightly chained job (ETL then refresh summaries). Usage: make nightly SEASON_ID=123
nightly: ## Run nightly ETL then refresh season summaries (requires SEASON_ID)
	@if [ -z "$(SEASON_ID)" ]; then echo "SEASON_ID is required (e.g., make nightly SEASON_ID=123)"; exit 1; fi
	python -m src.cli scrape
	python -m src.cli update --incremental
	python -m src.cli refresh-season-all $(SEASON_ID)
	@echo "✅ Nightly ETL + refresh completed for season $(SEASON_ID)!"

test-integration: ## Run integration tests (requires TEST_DB_DSN env var)
	@if [ -z "$(TEST_DB_DSN)" ]; then echo "Set TEST_DB_DSN to run integration tests"; exit 1; fi
	pytest -m integration tests/integration -v
