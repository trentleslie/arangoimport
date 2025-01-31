.PHONY: all clean lint format type-check test coverage install dev-install

PYTHON = python3
PACKAGE = arangoimport
SRC_DIR = src/$(PACKAGE)
TEST_DIR = tests

all: lint type-check test

clean:
	rm -rf .pytest_cache .coverage .mypy_cache .ruff_cache htmlcov
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

install:
	$(PYTHON) -m pip install .

dev-install:
	$(PYTHON) -m pip install -e ".[dev]"

lint:
	poetry run ruff check $(SRC_DIR) $(TEST_DIR)

format:
	poetry run ruff format $(SRC_DIR) $(TEST_DIR)
	poetry run ruff check --fix $(SRC_DIR) $(TEST_DIR)

type-check:
	poetry run mypy $(SRC_DIR)

test:
	poetry run pytest $(TEST_DIR)

coverage:
	poetry run pytest --cov=$(PACKAGE) --cov-report=html $(TEST_DIR)
	@echo "Open htmlcov/index.html in your browser to view the coverage report"

# Development workflow targets
check-all: format lint type-check test

watch-test:
	pytest-watch -- $(TEST_DIR)

# Help target
help:
	@echo "Available targets:"
	@echo "  all          : Run lint, type-check, and test"
	@echo "  clean        : Remove all build and test artifacts"
	@echo "  install      : Install the package"
	@echo "  dev-install  : Install the package in development mode with dev dependencies"
	@echo "  lint         : Run ruff linter"
	@echo "  format       : Format code with ruff"
	@echo "  type-check   : Run mypy type checker"
	@echo "  test         : Run pytest"
	@echo "  coverage     : Generate test coverage report"
	@echo "  check-all    : Run format, lint, type-check, and test"
	@echo "  watch-test   : Run tests continuously on file changes"
