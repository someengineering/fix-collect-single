.PHONY: clean clean-test clean-pyc clean-build clean-env docs setup test test-all
.SILENT: clean clean-build clean-pyc clean-test setup


clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr out/
	rm -fr gen/
	rm -fr dist/
	rm -fr .eggs/
	rm -fr .hypothesis/
	rm -fr .mypy_cache/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -fr {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

clean-env: ## remove environment
	rm -fr venv

venv:
	python3 -m venv venv --prompt "collect_single"
	. ./venv/bin/activate && python3 -m pip install --upgrade pip
	. ./venv/bin/activate && pip install -r ../fixinventory/requirements.txt
	. ./venv/bin/activate && pip install ../fixinventory/fixlib ../fixinventory/fixcore ../fixinventory/fixworker ../fixinventory/plugins/k8s ../fixinventory/plugins/aws
	. ./venv/bin/activate && pip install -e ".[test]"

test:
	pytest

lint: ## static code analysis
	black --line-length 120 --check collect_single tests
	flake8 collect_single
	mypy --python-version 3.9 --strict collect_single tests

setup: clean clean-env venv
