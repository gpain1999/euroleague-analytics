.PHONY: help install install-node install-python dev build lint test test-python \
        demo-db pipeline-run clean clean-all

SHELL := /bin/bash

# Variables
VENV_DIR := pipeline/.venv
VENV_BIN := $(VENV_DIR)/bin
ifeq ($(OS),Windows_NT)
	VENV_BIN := $(VENV_DIR)/Scripts
endif
PYTHON := $(VENV_BIN)/python
PIP := $(VENV_BIN)/pip

help: ## Affiche cette aide
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: install-node install-python ## Installe toutes les dépendances

install-node: ## Installe les dépendances npm
	npm install

install-python: $(VENV_DIR) ## Installe les dépendances Python dans un venv
	$(PIP) install --upgrade pip
	$(PIP) install -r pipeline/requirements.txt -r pipeline/requirements-dev.txt

$(VENV_DIR):
	python -m venv $(VENV_DIR)

dev: ## Lance Next.js en mode dev
	npm run dev

build: ## Build Next.js pour production
	npm run build

lint: ## Lint TypeScript + ESLint
	npm run lint

test: lint test-python ## Tous les tests

test-python: ## Tests Python
	$(PYTHON) -m pytest pipeline/tests -v

demo-db: $(VENV_DIR) ## Génère la base DuckDB de démonstration
	$(PYTHON) pipeline/scripts/bootstrap_demo_db.py

pipeline-run: $(VENV_DIR) ## Exécute le pipeline complet (Phase 1+)
	$(PYTHON) -m pipeline.run

clean: ## Nettoie les artefacts de build
	rm -rf .next out dist build

clean-all: clean ## Nettoie tout, y compris node_modules et venv
	rm -rf node_modules $(VENV_DIR)
