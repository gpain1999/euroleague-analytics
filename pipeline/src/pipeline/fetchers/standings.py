"""Fetcher pour les classements.

On snapshotte les standings à chaque round disponible. Cinq endpoints :
basicstandings, calendarstandings, streaks, aheadbehind, margins.

Pour la Phase 1, on se contente de snapshotter au "dernier round disponible"
de chaque saison. En Phase 6 on pourra élargir à tous les rounds pour
alimenter des graphiques d'évolution.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from euroleague_api.standings import Standings

from pipeline import config
from pipeline.fetchers.base import BaseFetcher
from pipeline.logging import get_logger
from pipeline.storage.paths import season_dir

log = get_logger(__name__)


STANDINGS_ENDPOINTS = [
    "basicstandings",
    "calendarstandings",
    "streaks",
    "aheadbehind",
    "margins",
]


def standings_path(season: int, round_number: int, endpoint: str) -> Path:
    """Chemin : raw/season=N/standings_round=R_endpoint=X.parquet"""
    return season_dir(season) / f"standings_round={round_number}_endpoint={endpoint}.parquet"


class StandingsFetcher(BaseFetcher):
    """Classement d'une saison à un round donné, pour un endpoint donné."""

    name = "standings"

    def __init__(self, season: int, round_number: int, endpoint: str):
        assert endpoint in STANDINGS_ENDPOINTS, f"endpoint {endpoint} invalide"
        self.season = season
        self.round_number = round_number
        self.endpoint = endpoint

    def target_path(self) -> Path:
        return standings_path(self.season, self.round_number, self.endpoint)

    def _fetch_from_api(self) -> pd.DataFrame:
        client = Standings(competition=config.COMPETITION_CODE)
        return client.get_standings(
            season=self.season,
            round_number=self.round_number,
            endpoint=self.endpoint,
        )


def _detect_latest_played_round(season: int) -> int:
    """Lit le schedule et retourne le plus grand round avec au moins un match joué.

    Pour la saison courante, ça donne "la photo à date". Pour les saisons
    passées, ça donne la dernière journée de Regular Season (car les phases
    suivantes ont des numéros de round différents qu'on ne veut pas ici).
    """
    from pipeline.fetchers.schedule import ScheduleFetcher
    df = ScheduleFetcher(season=season).run()

    # On ne garde que la Regular Season pour détecter le round courant,
    # car après RS les numéros de round changent de logique.
    rs = df[df["round"] == "RS"].copy()
    played = rs[rs["played"].astype(str).str.lower() == "true"]
    if len(played) == 0:
        return 1
    return int(played["gameday"].max())


def fetch_season_standings(
    season: int,
    round_number: int | None = None,
    force: bool = False,
) -> dict[str, dict[str, Any]]:
    """Télécharge les 5 endpoints standings d'une saison à un round donné.

    Si round_number est None, détecte automatiquement la dernière journée
    de Regular Season jouée.
    """
    if round_number is None:
        round_number = _detect_latest_played_round(season)
        log.info("auto-detected round", season=season, round_number=round_number)

    result: dict[str, dict[str, Any]] = {}
    for endpoint in STANDINGS_ENDPOINTS:
        fetcher = StandingsFetcher(
            season=season, round_number=round_number, endpoint=endpoint
        )
        try:
            df = fetcher.run(force=force)
            result[endpoint] = {"ok": True, "rows": len(df), "round": round_number}
        except Exception as e:  # noqa: BLE001
            log.warning(
                "standings failed",
                season=season,
                round_number=round_number,
                endpoint=endpoint,
                error=str(e),
            )
            result[endpoint] = {"ok": False, "error": str(e), "round": round_number}
    return result