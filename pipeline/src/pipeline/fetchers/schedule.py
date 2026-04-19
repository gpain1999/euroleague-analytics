"""Fetcher : calendrier d'une saison."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from euroleague_api.schedule import Schedule

from pipeline import config
from pipeline.fetchers.base import BaseFetcher
from pipeline.storage.paths import schedule_path


class ScheduleFetcher(BaseFetcher):
    """Récupère le calendrier complet d'une saison.

    Le schedule contient tous les matchs (RS + PI + PO + FF) avec dates,
    équipes, arenas. C'est le point de départ de tout le pipeline : il
    nous dit quels gamecodes existent pour chaque saison.
    """

    name = "schedule"

    def __init__(self, season: int):
        self.season = season

    def target_path(self) -> Path:
        return schedule_path(self.season)

    def _fetch_from_api(self) -> pd.DataFrame:
        client = Schedule(competition=config.COMPETITION_CODE)
        df = client.get_schedule(season=self.season)
        # Ajoute la saison en colonne pour faciliter l'union multi-saisons
        df = df.copy()
        df["season"] = self.season
        return df