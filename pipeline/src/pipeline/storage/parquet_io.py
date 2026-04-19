"""Lecture et écriture de fichiers parquet avec garanties d'intégrité."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipeline.logging import get_logger

log = get_logger(__name__)


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Écrit un DataFrame en parquet, crée les répertoires au besoin.

    L'écriture se fait via un fichier temporaire pour éviter les fichiers
    corrompus en cas d'interruption.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, engine="pyarrow", compression="snappy", index=False)
    tmp.replace(path)
    log.debug("parquet written", path=str(path), rows=len(df))


def read_parquet(path: Path) -> pd.DataFrame:
    """Lit un DataFrame depuis un parquet."""
    df = pd.read_parquet(path, engine="pyarrow")
    log.debug("parquet read", path=str(path), rows=len(df))
    return df


def parquet_exists(path: Path) -> bool:
    """Vérifie qu'un fichier parquet existe et n'est pas vide."""
    return path.exists() and path.stat().st_size > 0