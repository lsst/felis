.PHONY: help build docs check test numpydoc mypy all

MAKEFLAGS += --no-print-directory

help:
	@echo "Available targets for Felis:"
	@echo "  build    - Build the package"
	@echo "  docs     - Generate the documentation"
	@echo "  check    - Run pre-commit checks"
	@echo "  test     - Run tests"
	@echo "  numpydoc - Check numpydoc style"
	@echo "  mypy     - Run mypy static type checker"
	@echo "  all      - Run all tasks"

build docs check test numpydoc mypy: print_target

print_target:
	@echo "Executing $(MAKECMDGOALS)..."

build:
	@uv pip install --force-reinstall --no-deps -e .

docs:
	@rm -rf docs/dev/internals docs/_build
	@tox -e docs

check:
	@pre-commit run --all-files

test:
	@pytest -s --log-level DEBUG

numpydoc:
	@python -m numpydoc.hooks.validate_docstrings $(shell find python -name "*.py" ! -name "cli.py")

mypy:
	@mypy python/

all:
	@$(MAKE) build
	@$(MAKE) docs
	@$(MAKE) check
	@$(MAKE) test
	@$(MAKE) numpydoc
	@$(MAKE) mypy
	@echo "All tasks completed."
