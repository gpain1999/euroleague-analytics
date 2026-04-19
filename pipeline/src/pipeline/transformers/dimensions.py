"""Transformations : dimensions (seasons, teams, games, players)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from pipeline import config
from pipeline.logging import get_logger
from pipeline.storage.paths import game_file_path, schedule_path
from pipeline.transformers.base import safe_read, strip_object_cols, write_curated
from pipeline.transformers.paths import curated_path

log = get_logger(__name__)


# =========================================================================
# dim_seasons
# =========================================================================
def build_dim_seasons(seasons: list[int]) -> pd.DataFrame:
    """Construit la table des saisons.

    Convention : season=N désigne la saison N N+1 (ex: 2023 = 2023-24).
    """
    rows = []
    for s in sorted(seasons):
        rows.append({
            "season_id": s,
            "name": f"EuroLeague {s}-{(s + 1) % 100:02d}",
            "start_year": s,
            "end_year": s + 1,
        })
    return pd.DataFrame(rows)


# =========================================================================
# dim_teams
# =========================================================================
def build_dim_teams(seasons: list[int]) -> pd.DataFrame:
    """Construit la table des équipes en consolidant sur plusieurs saisons.

    Source : les schedules (colonnes hometeam/homecode + awayteam/awaycode).
    Les métadonnées supplémentaires (tv_code, couleurs, etc) viendront plus
    tard du game_report ou seront renseignées manuellement.

    Une équipe qui apparaît sur plusieurs saisons est comptée une seule fois.
    Le nom retenu est celui de la saison la plus récente où elle apparaît
    (les clubs peuvent changer de sponsor/nom entre saisons).
    """
    frames = []
    for season in seasons:
        df = safe_read(schedule_path(season))
        if df is None:
            continue
        # Home side
        home = df[["homecode", "hometeam", "hometv"]].copy()
        home.columns = ["team_code", "name", "tv_code"]
        home["season"] = season
        # Away side
        away = df[["awaycode", "awayteam", "awaytv"]].copy()
        away.columns = ["team_code", "name", "tv_code"]
        away["season"] = season
        frames.append(pd.concat([home, away], ignore_index=True))

    all_appearances = pd.concat(frames, ignore_index=True)
    all_appearances = strip_object_cols(all_appearances)

    # Normalise le nom en Title Case plutôt qu'en CAPITAL LOCK
    all_appearances["name"] = all_appearances["name"].str.title()

    # Pour chaque team_code, prendre le nom de la saison la plus récente
    latest_idx = all_appearances.groupby("team_code")["season"].idxmax()
    latest_name = all_appearances.loc[latest_idx, ["team_code", "name", "tv_code"]]

    # Liste des saisons où chaque équipe apparaît
    seasons_by_team = (
        all_appearances.groupby("team_code")["season"]
        .apply(lambda s: sorted(s.unique().tolist()))
        .reset_index()
        .rename(columns={"season": "seasons"})
    )

    result = latest_name.merge(seasons_by_team, on="team_code")
    result["first_season"] = result["seasons"].apply(min)
    result["last_season"] = result["seasons"].apply(max)
    result = result[["team_code", "name", "tv_code", "first_season",
                      "last_season", "seasons"]]
    return result.sort_values("team_code").reset_index(drop=True)


# =========================================================================
# dim_games
# =========================================================================
def build_dim_games(seasons: list[int]) -> pd.DataFrame:
    """Construit la table des matchs consolidée.

    Source : les schedules (pour tous les matchs, joués ou non) + metadata
    de chaque match joué pour enrichir avec scores finaux et info arena.
    """
    frames = []
    for season in seasons:
        sched = safe_read(schedule_path(season))
        if sched is None:
            continue

        # Extraire le gamecode numérique depuis "E2023_7"
        sched = sched.copy()
        sched["gamecode"] = sched["gamecode"].astype(str).str.split("_").str[-1].astype(int)
        sched["played"] = sched["played"].astype(str).str.lower() == "true"
        sched["season"] = season

        # Normalisation date : "Oct 05, 2023" → pd.Timestamp
        sched["date"] = pd.to_datetime(sched["date"], format="%b %d, %Y", errors="coerce")

        # Phase : 'RS', 'PI', 'PO', 'FF' est déjà dans la colonne 'round'
        sched = sched.rename(columns={"round": "phase"})
        sched["round"] = sched["gameday"]

        keep = ["season", "gamecode", "date", "phase", "round",
                "group", "homecode", "awaycode",
                "hometeam", "awayteam",
                "arenacode", "arenaname", "arenacapacity",
                "startime", "endtime", "played"]
        keep = [c for c in keep if c in sched.columns]
        frames.append(sched[keep])

    games = pd.concat(frames, ignore_index=True)
    games = strip_object_cols(games)

    # Noms d'équipes : les schedules sont en CAPS, on harmonise avec dim_teams
    games["hometeam"] = games["hometeam"].str.title()
    games["awayteam"] = games["awayteam"].str.title()

    # Enrichir avec les scores depuis la metadata (matchs joués uniquement)
    games["home_score"] = pd.NA
    games["away_score"] = pd.NA

    for (season, gamecode), _ in games[games["played"]].groupby(["season", "gamecode"]):
        meta = safe_read(game_file_path(season, gamecode, "metadata"))
        if meta is None or len(meta) == 0:
            continue
        try:
            hs = int(meta["ScoreA"].iloc[0])
            as_ = int(meta["ScoreB"].iloc[0])
        except (ValueError, KeyError):
            continue
        mask = (games["season"] == season) & (games["gamecode"] == gamecode)
        games.loc[mask, "home_score"] = hs
        games.loc[mask, "away_score"] = as_

    # Casting propre
    games["arenacapacity"] = pd.to_numeric(games["arenacapacity"], errors="coerce").astype("Int64")
    games["home_score"] = games["home_score"].astype("Int64")
    games["away_score"] = games["away_score"].astype("Int64")

    # Tri cohérent
    return games.sort_values(["season", "gamecode"]).reset_index(drop=True)


# =========================================================================
# dim_players
# =========================================================================
def build_dim_players(seasons: list[int]) -> pd.DataFrame:
    """Construit la table des joueurs consolidée.

    Source : les boxscores de tous les matchs. On déduplique sur Player_ID
    (après strip des espaces). On enrichit avec les stats agrégées saison
    (player_stats_traditional) pour récupérer position, images et équipe.
    """
    boxscore_frames = []
    for season in seasons:
        # Liste des gamecodes dispos
        season_game_dir = config.RAW_DIR / f"season={season}" / "games"
        if not season_game_dir.exists():
            continue
        for game_dir in sorted(season_game_dir.iterdir()):
            if not game_dir.is_dir():
                continue
            box = safe_read(game_dir / "boxscore.parquet")
            if box is None:
                continue
            # Filtre les lignes Team/Total qui ne sont pas des joueurs
            real = box[~box["Player"].isin(["Team", "Total"])].copy()
            real["season"] = season
            boxscore_frames.append(real[["season", "Player_ID", "Player", "Team", "Dorsal"]])

    if not boxscore_frames:
        log.warning("no boxscore found, returning empty dim_players")
        return pd.DataFrame(columns=["player_code", "full_name", "last_team_code",
                                      "last_jersey", "first_season", "last_season",
                                      "seasons"])

    all_appearances = pd.concat(boxscore_frames, ignore_index=True)
    all_appearances = strip_object_cols(all_appearances)
    all_appearances = all_appearances.rename(columns={
        "Player_ID": "player_code",
        "Player": "full_name",
        "Team": "team_code",
        "Dorsal": "jersey",
    })

    # Nom : prendre la version la plus récente (au cas où un joueur change
    # d'orthographe dans les données entre saisons)
    latest_idx = all_appearances.groupby("player_code")["season"].idxmax()
    latest = all_appearances.loc[latest_idx, ["player_code", "full_name",
                                               "team_code", "jersey"]]
    latest.columns = ["player_code", "full_name", "last_team_code", "last_jersey"]

    # Saisons de présence
    seasons_by_player = (
        all_appearances.groupby("player_code")["season"]
        .apply(lambda s: sorted(s.unique().tolist()))
        .reset_index()
        .rename(columns={"season": "seasons"})
    )

    result = latest.merge(seasons_by_player, on="player_code")
    result["first_season"] = result["seasons"].apply(min)
    result["last_season"] = result["seasons"].apply(max)
    result = result[["player_code", "full_name", "last_team_code",
                      "last_jersey", "first_season", "last_season", "seasons"]]

    # Format du nom : "LASTNAME, FIRSTNAME" → on garde tel quel, c'est le
    # format standard EuroLeague
    return result.sort_values("player_code").reset_index(drop=True)


# =========================================================================
# Orchestrator
# =========================================================================
def build_all_dimensions(seasons: list[int]) -> list[dict[str, Any]]:
    """Construit les 4 dimensions et écrit les parquet curated."""
    results = []

    log.info("building dim_seasons")
    df = build_dim_seasons(seasons)
    results.append(write_curated(df, curated_path("dim_seasons"), "dim_seasons"))

    log.info("building dim_teams")
    df = build_dim_teams(seasons)
    results.append(write_curated(df, curated_path("dim_teams"), "dim_teams"))

    log.info("building dim_games")
    df = build_dim_games(seasons)
    results.append(write_curated(df, curated_path("dim_games"), "dim_games"))

    log.info("building dim_players")
    df = build_dim_players(seasons)
    results.append(write_curated(df, curated_path("dim_players"), "dim_players"))

    return results