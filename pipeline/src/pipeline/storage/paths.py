"""Helpers de chemins pour l'organisation des parquets.

Convention :
    raw/season=2024/schedule.parquet
    raw/season=2024/games/gamecode=1/boxscore.parquet
    raw/season=2024/games/gamecode=1/pbp.parquet
    ...
"""

from __future__ import annotations

from pathlib import Path

from pipeline import config


def season_dir(season: int) -> Path:
    """Répertoire raw d'une saison."""
    return config.RAW_DIR / f"season={season}"


def schedule_path(season: int) -> Path:
    """Chemin du fichier schedule pour une saison."""
    return season_dir(season) / "schedule.parquet"


def standings_path(season: int, round_number: int) -> Path:
    """Chemin des standings à un round donné."""
    return season_dir(season) / f"standings_round={round_number}.parquet"


def season_stats_path(season: int, kind: str, endpoint: str) -> Path:
    """Chemin des stats saisonnières (kind=player|team, endpoint=traditional|advanced|...)."""
    return season_dir(season) / f"{kind}_stats_{endpoint}.parquet"


def game_dir(season: int, gamecode: int) -> Path:
    """Répertoire d'un match spécifique."""
    return season_dir(season) / "games" / f"gamecode={gamecode}"


def game_file_path(season: int, gamecode: int, kind: str) -> Path:
    """Chemin d'un fichier match : kind=metadata|boxscore|pbp|shots|quarter_scores."""
    return game_dir(season, gamecode) / f"{kind}.parquet"