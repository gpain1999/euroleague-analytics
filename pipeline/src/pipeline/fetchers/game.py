"""Fetchers pour un match : metadata, boxscore, PBP, shots, quarter scores.

Chaque match produit 5 fichiers parquet dans storage/raw/season=N/games/gamecode=K/.
Le fetcher de haut niveau `fetch_game()` orchestre les 5 appels et gère les
erreurs individuellement : un artefact qui échoue n'empêche pas les autres.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from euroleague_api.boxscore_data import BoxScoreData
from euroleague_api.game_metadata import GameMetadata
from euroleague_api.play_by_play_data import PlayByPlay
from euroleague_api.shot_data import ShotData

from pipeline import config
from pipeline.fetchers.base import BaseFetcher
from pipeline.logging import get_logger
from pipeline.storage.paths import game_file_path

log = get_logger(__name__)


class GameMetadataFetcher(BaseFetcher):
    """Metadata d'un match : équipes, score final, arbitres, scores par quart cumulés."""

    name = "metadata"

    def __init__(self, season: int, gamecode: int):
        self.season = season
        self.gamecode = gamecode

    def target_path(self) -> Path:
        return game_file_path(self.season, self.gamecode, "metadata")

    def _fetch_from_api(self) -> pd.DataFrame:
        client = GameMetadata(competition=config.COMPETITION_CODE)
        df = client.get_game_metadata(season=self.season, gamecode=self.gamecode)
        if isinstance(df, pd.Series):
            df = df.to_frame().T
        return df


class BoxscorePlayersFetcher(BaseFetcher):
    """Boxscore joueurs : 1 ligne par joueur + 2 lignes de totaux par équipe."""

    name = "boxscore"

    def __init__(self, season: int, gamecode: int):
        self.season = season
        self.gamecode = gamecode

    def target_path(self) -> Path:
        return game_file_path(self.season, self.gamecode, "boxscore")

    def _fetch_from_api(self) -> pd.DataFrame:
        client = BoxScoreData(competition=config.COMPETITION_CODE)
        return client.get_players_boxscore_stats(
            season=self.season, gamecode=self.gamecode
        )


class QuarterScoresFetcher(BaseFetcher):
    """Points marqués par quart (ByQuarter, non cumulé).

    Le cumul (EndOfQuarter) est dérivable par cumsum, inutile de le stocker.
    """

    name = "quarter_scores"

    def __init__(self, season: int, gamecode: int):
        self.season = season
        self.gamecode = gamecode

    def target_path(self) -> Path:
        return game_file_path(self.season, self.gamecode, "quarter_scores")

    def _fetch_from_api(self) -> pd.DataFrame:
        client = BoxScoreData(competition=config.COMPETITION_CODE)
        return client.get_teams_boxscore_quarter_scores(
            season=self.season,
            gamecode=self.gamecode,
            boxscore_type="ByQuarter",
        )


class PlayByPlayFetcher(BaseFetcher):
    """PBP brut, sans reconstruction de lineups (faite en couche curated)."""

    name = "pbp"

    def __init__(self, season: int, gamecode: int):
        self.season = season
        self.gamecode = gamecode

    def target_path(self) -> Path:
        return game_file_path(self.season, self.gamecode, "pbp")

    def _fetch_from_api(self) -> pd.DataFrame:
        client = PlayByPlay(competition=config.COMPETITION_CODE)
        return client.get_game_play_by_play_data(
            season=self.season, gamecode=self.gamecode
        )


class ShotsFetcher(BaseFetcher):
    """Tous les tirs du match avec coordonnées, zone, résultat."""

    name = "shots"

    def __init__(self, season: int, gamecode: int):
        self.season = season
        self.gamecode = gamecode

    def target_path(self) -> Path:
        return game_file_path(self.season, self.gamecode, "shots")

    def _fetch_from_api(self) -> pd.DataFrame:
        client = ShotData(competition=config.COMPETITION_CODE)
        return client.get_game_shot_data(
            season=self.season, gamecode=self.gamecode
        )


# =========================================================================
# Orchestrator
# =========================================================================

GAME_ARTIFACT_CLASSES = [
    GameMetadataFetcher,
    BoxscorePlayersFetcher,
    QuarterScoresFetcher,
    PlayByPlayFetcher,
    ShotsFetcher,
]


def fetch_game(
    season: int,
    gamecode: int,
    force: bool = False,
) -> dict[str, Any]:
    """Télécharge les 5 artefacts d'un match.

    Retourne un dict par artefact :
      - {"ok": True, "rows": N} si succès
      - {"ok": False, "error": str} si échec

    Un échec sur un artefact n'arrête pas les autres.
    """
    result: dict[str, Any] = {}
    for FetcherCls in GAME_ARTIFACT_CLASSES:
        fetcher = FetcherCls(season=season, gamecode=gamecode)
        try:
            df = fetcher.run(force=force)
            result[fetcher.name] = {"ok": True, "rows": len(df)}
        except Exception as e:  # noqa: BLE001
            log.warning(
                "artifact fetch failed",
                season=season,
                gamecode=gamecode,
                artifact=fetcher.name,
                error=str(e),
            )
            result[fetcher.name] = {"ok": False, "error": str(e)}
    return result


def extract_played_gamecodes(schedule_df: pd.DataFrame) -> list[int]:
    """Extrait la liste des gamecodes joués depuis un DataFrame schedule.

    Le gamecode dans le schedule est au format "E2023_7" (string) ; les autres
    endpoints attendent juste 7 (int). On parse donc.
    """
    played_mask = schedule_df["played"].astype(str).str.lower() == "true"
    gc_str = schedule_df.loc[played_mask, "gamecode"].astype(str)
    return gc_str.str.split("_").str[-1].astype(int).tolist()


def fetch_season_games(
    season: int,
    gamecodes: list[int] | None = None,
    force: bool = False,
    limit: int | None = None,
) -> dict[int, dict[str, Any]]:
    """Télécharge les artefacts des matchs d'une saison.

    Si `gamecodes` est None, lit le schedule et fetch tous les matchs joués.
    `limit` permet de restreindre (utile pour tests).
    """
    from tqdm import tqdm  # import local

    if gamecodes is None:
        from pipeline.fetchers.schedule import ScheduleFetcher
        schedule_df = ScheduleFetcher(season=season).run()
        gamecodes = extract_played_gamecodes(schedule_df)

    if limit is not None:
        gamecodes = gamecodes[:limit]

    log.info(
        "fetching season games",
        season=season,
        games=len(gamecodes),
        force=force,
    )

    results: dict[int, dict[str, Any]] = {}
    for gc in tqdm(gamecodes, desc=f"season {season}", unit="game"):
        results[gc] = fetch_game(season=season, gamecode=gc, force=force)

    # Résumé
    ok_games = sum(
        1 for r in results.values()
        if all(v.get("ok") for v in r.values())
    )
    partial = sum(
        1 for r in results.values()
        if any(v.get("ok") for v in r.values()) and not all(v.get("ok") for v in r.values())
    )
    failed = len(results) - ok_games - partial

    log.info(
        "season games done",
        season=season,
        total=len(results),
        all_ok=ok_games,
        partial_ok=partial,
        all_failed=failed,
    )
    return results