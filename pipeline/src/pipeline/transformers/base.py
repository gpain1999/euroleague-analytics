"""Helpers communs aux transformers."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.logging import get_logger
from pipeline.storage.parquet_io import read_parquet, write_parquet

log = get_logger(__name__)


def safe_read(path: Path) -> pd.DataFrame | None:
    """Lit un parquet s'il existe, retourne None sinon.

    Utile pour les transformations qui agrègent plusieurs fichiers : un
    fichier manquant (rare) ne doit pas faire planter tout le processus.
    """
    if not path.exists():
        log.warning("missing file", path=str(path))
        return None
    try:
        return read_parquet(path)
    except Exception as e:  # noqa: BLE001
        log.warning("read failed", path=str(path), error=str(e))
        return None


def write_curated(df: pd.DataFrame, target: Path, label: str) -> dict[str, Any]:
    """Écrit une table curated avec log structuré."""
    t0 = time.time()
    write_parquet(df, target)
    elapsed = time.time() - t0
    log.info(
        "curated written",
        table=label,
        rows=len(df),
        cols=len(df.columns),
        path=str(target),
        elapsed_s=round(elapsed, 2),
    )
    return {"table": label, "rows": len(df), "cols": len(df.columns)}


def strip_object_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Strip les espaces des colonnes string.

    L'API EuroLeague a tendance à renvoyer des strings paddées à droite
    (notamment Player_ID = "P006436   "). On nettoie systématiquement.
    """
    out = df.copy()
    for col in out.select_dtypes(include=["object"]).columns:
        # Seulement les colonnes qui contiennent des strings (pas des dicts ou listes)
        sample = out[col].dropna().head(1)
        if len(sample) == 0:
            continue
        if isinstance(sample.iloc[0], str):
            out[col] = out[col].str.strip()
    return out