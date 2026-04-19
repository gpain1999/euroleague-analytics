"""Classe de base pour tous les fetchers."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd

from pipeline.logging import get_logger
from pipeline.storage.parquet_io import parquet_exists, read_parquet, write_parquet

log = get_logger(__name__)


class BaseFetcher(ABC):
    """Interface commune aux fetchers.

    Chaque sous-classe implémente :
      - _fetch_from_api() : appel à euroleague_api
      - target_path() : chemin parquet où stocker le résultat

    Le flow standard est :
      - fetcher.run() vérifie le cache → si absent/force, appelle l'API et écrit
      - fetcher.read() relit depuis le cache (implicitement run() d'abord)
    """

    # Nom court utilisé dans les logs
    name: str = "base"

    @abstractmethod
    def target_path(self) -> Path:
        """Chemin parquet de destination."""

    @abstractmethod
    def _fetch_from_api(self) -> pd.DataFrame:
        """Appel réel à l'API EuroLeague."""

    def run(self, force: bool = False) -> pd.DataFrame:
        """Fetch avec cache. Retourne toujours le DataFrame."""
        path = self.target_path()

        if not force and parquet_exists(path):
            log.info(f"{self.name} cache hit", path=str(path))
            return read_parquet(path)

        log.info(f"{self.name} fetching from API", path=str(path))
        t0 = time.time()
        df = self._fetch_from_api()
        elapsed = time.time() - t0

        write_parquet(df, path)
        log.info(
            f"{self.name} fetched",
            rows=len(df),
            cols=len(df.columns),
            elapsed_s=round(elapsed, 2),
        )
        return df