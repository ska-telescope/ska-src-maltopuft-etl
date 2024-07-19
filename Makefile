# Use base Makefiles from ska-cicd-makefiles repository
include .make/base.mk
include .make/docs.mk
include .make/python.mk

# Define variables
PYTHON_LINT_TARGET := ./src
PYTHON_SWITCHES_FOR_PYLINT = --disable=R0903,W1203

# Note:
# PYTHON_VARS_AFTER_PYTEST are defined in pyproject.toml
# [tool.pytest.ini_options], addopts

# Run pre-commit checks
pre-commit:
	@black -l 79 {docs,src,tests}
	@ruff check . --fix
	@python -m mypy {src,tests}
	make python-format
	make python-lint
	make python-test
	make docs-build html
