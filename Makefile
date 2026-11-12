PYTHON ?= python3
PY_MM := $(shell $(PYTHON) -c 'import sys; print("%d.%d" % sys.version_info[:2])')

.PHONY: install lint test benchmark

install:
	$(PYTHON) -m pip install "apache-airflow==2.10.5" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-$(PY_MM).txt"
	$(PYTHON) -m pip install -e ".[dev]"

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

test:
	$(PYTHON) -m pytest

benchmark:
	$(PYTHON) scripts/benchmark.py
