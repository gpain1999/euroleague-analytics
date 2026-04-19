"""Test smoke : valide que le package s'importe et que la config est cohérente."""

from __future__ import annotations

import pipeline
from pipeline import config


def test_version() -> None:
    assert pipeline.__version__
    assert isinstance(pipeline.__version__, str)


def test_seasons_ordered() -> None:
    """Les saisons configurées doivent être des entiers croissants."""
    assert config.SEASONS == sorted(config.SEASONS)
    assert all(isinstance(s, int) for s in config.SEASONS)
    assert config.CURRENT_SEASON == max(config.SEASONS)


def test_min_possessions() -> None:
    """Chaque taille de lineup (2-5) doit avoir un seuil défini."""
    for size in (2, 3, 4, 5):
        assert size in config.MIN_POSSESSIONS_LINEUP
        assert config.MIN_POSSESSIONS_LINEUP[size] > 0


def test_paths_are_pathlike() -> None:
    """Les chemins critiques doivent exposer l'interface Path."""
    for path_attr in ("STORAGE_DIR", "RAW_DIR", "CURATED_DIR", "DUCKDB_FILE"):
        path = getattr(config, path_attr)
        assert hasattr(path, "exists")
        assert hasattr(path, "mkdir")


def test_clutch_definition() -> None:
    """Fenêtre clutch standard : 5 minutes, écart ≤ 5."""
    assert config.CLUTCH_LAST_SECONDS == 300
    assert config.CLUTCH_MAX_SCORE_DIFF == 5
