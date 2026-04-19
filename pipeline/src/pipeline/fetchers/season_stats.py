"""Fetchers pour les stats agrégées à la saison.

Deux familles × quatre endpoints = 8 fichiers par saison :
  - player_stats : traditional, advanced, misc, scoring
  - team_stats   : traditional, advanced, opponentsTraditional, opponentsAdvanced

Ces stats sont déjà calculées côté API EuroLeague. Elles nous servent de
source de vérité (croisement avec nos propres calculs) et de raccourci pour
certaines pages (leaderboards simples).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from euroleague_api.player_stats import PlayerStats
from euroleague_api.team_stats import TeamStats

from pipeline import config
from pipeline.fetchers.base import BaseFetcher
from pipeline.logging import get_logger
from pipeline.storage.paths import season_stats_path

log = get_logger(__name__)


PLAYER_ENDPOINTS = ["traditional", "advanced", "misc", "scoring"]
TEAM_ENDPOINTS = [
    "traditional",
    "advanced",
    "opponentsTraditional",
    "opponentsAdvanced",
]


class PlayerStatsFetcher(BaseFetcher):
    """Stats agrégées joueurs pour une saison, un endpoint donné."""

    name = "player_stats"

    def __init__(self, season: int, endpoint: str):
        assert endpoint in PLAYER_ENDPOINTS, f"endpoint {endpoint} invalide"
        self.season = season
        self.endpoint = endpoint

    def target_path(self) -> Path:
        return season_stats_path(self.season, "player", self.endpoint)

    def _fetch_from_api(self) -> pd.DataFrame:
        client = PlayerStats(competition=config.COMPETITION_CODE)
        return client.get_player_stats_single_season(
            endpoint=self.endpoint,
            season=self.season,
            phase_type_code=None,          # toutes phases confondues
            statistic_mode="PerGame",
        )


class TeamStatsFetcher(BaseFetcher):
    """Stats agrégées équipes pour une saison, un endpoint donné."""

    name = "team_stats"

    def __init__(self, season: int, endpoint: str):
        assert endpoint in TEAM_ENDPOINTS, f"endpoint {endpoint} invalide"
        self.season = season
        self.endpoint = endpoint

    def target_path(self) -> Path:
        return season_stats_path(self.season, "team", self.endpoint)

    def _fetch_from_api(self) -> pd.DataFrame:
        client = TeamStats(competition=config.COMPETITION_CODE)
        return client.get_team_stats_single_season(
            endpoint=self.endpoint,
            season=self.season,
            phase_type_code=None,
            statistic_mode="PerGame",
        )


def fetch_season_stats(
    season: int,
    force: bool = False,
) -> dict[str, dict[str, Any]]:
    """Télécharge les 8 fichiers de stats agrégées d'une saison."""
    result: dict[str, dict[str, Any]] = {}

    for endpoint in PLAYER_ENDPOINTS:
        fetcher = PlayerStatsFetcher(season=season, endpoint=endpoint)
        try:
            df = fetcher.run(force=force)
            result[f"player.{endpoint}"] = {"ok": True, "rows": len(df)}
        except Exception as e:  # noqa: BLE001
            log.warning(
                "player_stats failed",
                season=season,
                endpoint=endpoint,
                error=str(e),
            )
            result[f"player.{endpoint}"] = {"ok": False, "error": str(e)}

    for endpoint in TEAM_ENDPOINTS:
        fetcher = TeamStatsFetcher(season=season, endpoint=endpoint)
        try:
            df = fetcher.run(force=force)
            result[f"team.{endpoint}"] = {"ok": True, "rows": len(df)}
        except Exception as e:  # noqa: BLE001
            log.warning(
                "team_stats failed",
                season=season,
                endpoint=endpoint,
                error=str(e),
            )
            result[f"team.{endpoint}"] = {"ok": False, "error": str(e)}

    return result