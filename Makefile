VENV_DIR ?= .venv
VENV_RUN = . $(VENV_DIR)/bin/activate
VENV_ACTIVATE = $(VENV_DIR)/bin/activate
TEST_PATH ?= .
TEST_EXEC ?= python -m
PYTEST_LOGLEVEL ?= warning

install:
	uv venv --clear
	uv sync --no-dev

install-dev:
	uv venv --clear
	uv sync --group testing --group linting

clean:
	rm -rf $(VENV_DIR)

clean-dist:
	rm -rf dist/

build:
	uv build

format:
	($(VENV_RUN); python -m ruff format; python -m ruff check --output-format=full --fix .)

lint:
	($(VENV_RUN); python -m ruff check --output-format=full . && python -m ruff format --check .)

test:
	($(VENV_RUN); $(TEST_EXEC) pytest --durations=10 --log-cli-level=$(PYTEST_LOGLEVEL) $(PYTEST_ARGS) $(TEST_PATH))

.PHONY: clean install install-dev clean clean-dist build publish format lint test
