"""Chemins des fichiers curated."""

from __future__ import annotations

from pathlib import Path

from pipeline import config


def curated_path(table_name: str) -> Path:
    """Chemin d'une table curated : curated/<table_name>.parquet"""
    return config.CURATED_DIR / f"{table_name}.parquet"