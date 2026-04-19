"""Transformations : facts simples (boxscore players, boxscore teams, shots).

Ces tables n'exigent pas le PBP. Elles sont construites par agrégation
directe depuis les artefacts raw de chaque match.
"""

from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from pipeline import config
from pipeline.logging import get_logger
from pipeline.storage.paths import game_file_path
from pipeline.transformers.base import safe_read, strip_object_cols, write_curated
from pipeline.transformers.paths import curated_path

log = get_logger(__name__)


# =========================================================================
# Helpers
# =========================================================================
_MM_SS_RE = re.compile(r"^(\d+):(\d{2})$")


def parse_minutes(value: Any) -> float:
    """Convertit une valeur Minutes en float.

    Formats rencontrés :
      - "14:37" → 14.616...
      - "DNP"   → 0.0
      - ""      → 0.0
      - NaN     → 0.0
    """
    if pd.isna(value):
        return 0.0
    s = str(value).strip()
    if s == "" or s.upper() == "DNP":
        return 0.0
    m = _MM_SS_RE.match(s)
    if not m:
        return 0.0
    mm, ss = int(m.group(1)), int(m.group(2))
    return mm + ss / 60.0


def _list_game_dirs(season: int) -> list[Path]:
    """Liste tous les game_dir d'une saison."""
    season_games = config.RAW_DIR / f"season={season}" / "games"
    if not season_games.exists():
        return []
    return sorted([d for d in season_games.iterdir() if d.is_dir()])


def _gamecode_from_dir(game_dir: Path) -> int:
    """Extrait le gamecode depuis le nom du dossier 'gamecode=7' → 7."""
    return int(game_dir.name.split("=")[-1])


# =========================================================================
# fact_boxscore_players
# =========================================================================
def build_fact_boxscore_players(seasons: list[int]) -> pd.DataFrame:
    """Consolide les boxscore joueurs de tous les matchs."""
    frames = []
    total_games = 0
    for season in seasons:
        for game_dir in _list_game_dirs(season):
            gc = _gamecode_from_dir(game_dir)
            box = safe_read(game_dir / "boxscore.parquet")
            if box is None:
                continue
            # Enlève les lignes Team / Total
            players = box[~box["Player"].isin(["Team", "Total"])].copy()
            if len(players) == 0:
                continue

            players["season"] = season
            players["gamecode"] = gc
            frames.append(players)
            total_games += 1

    log.info("scanned games", total_games=total_games)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df = strip_object_cols(df)

    # Renommage propre : snake_case et nom des colonnes cohérent
    rename_map = {
        "Player_ID": "player_code",
        "Player": "player_name",
        "Team": "team_code",
        "Dorsal": "jersey",
        "Home": "is_home",
        "IsStarter": "is_starter",
        "IsPlaying": "is_playing",
        "Minutes": "minutes_str",
        "Points": "points",
        "FieldGoalsMade2": "fg2m",
        "FieldGoalsAttempted2": "fg2a",
        "FieldGoalsMade3": "fg3m",
        "FieldGoalsAttempted3": "fg3a",
        "FreeThrowsMade": "ftm",
        "FreeThrowsAttempted": "fta",
        "OffensiveRebounds": "oreb",
        "DefensiveRebounds": "dreb",
        "TotalRebounds": "treb",
        "Assistances": "ast",
        "Steals": "stl",
        "Turnovers": "tov",
        "BlocksFavour": "blk",
        "BlocksAgainst": "blk_against",
        "FoulsCommited": "pf",
        "FoulsReceived": "pf_drawn",
        "Valuation": "pir",
        "Plusminus": "plus_minus",
    }
    df = df.rename(columns=rename_map)

    # Conversion Minutes en float
    df["minutes"] = df["minutes_str"].apply(parse_minutes)

    # Normalisation booléens
    df["is_home"] = df["is_home"].astype("Int64").astype("boolean")

    df["is_starter"] = df["is_starter"].fillna(0).astype(int).astype(bool)
    # L'API renvoie IsPlaying=0 pour beaucoup de joueurs qui ont pourtant des
    # minutes > 0 (bug connu côté source). On conserve la valeur brute pour
    # traçabilité (is_playing_api) et on dérive la version fiable depuis les
    # minutes, qu'on utilise partout ailleurs (is_playing).
    df["is_playing_api"] = df["is_playing"].fillna(0).astype(int).astype(bool)
    df["is_playing"] = df["minutes"] > 0
    # Colonnes finales dans un ordre stable
    cols = [
        "season", "gamecode", "player_code", "player_name",
        "team_code", "jersey", "is_home", "is_starter", "is_playing","is_playing_api",
        "minutes", "minutes_str",
        "points",
        "fg2m", "fg2a", "fg3m", "fg3a", "ftm", "fta",
        "oreb", "dreb", "treb",
        "ast", "stl", "tov", "blk", "blk_against",
        "pf", "pf_drawn",
        "pir", "plus_minus",
    ]
    cols = [c for c in cols if c in df.columns]
    return df[cols].sort_values(["season", "gamecode", "team_code"]).reset_index(drop=True)


# =========================================================================
# fact_boxscore_teams
# =========================================================================
def build_fact_boxscore_teams(
    seasons: list[int],
    players_df: pd.DataFrame,
) -> pd.DataFrame:
    """Totaux équipe par match + scores par quart.

    Produit 2 lignes par match : 1 pour l'équipe locale, 1 pour la visiteuse.
    """
    # 1) Agrégation depuis fact_boxscore_players
    agg_cols = {
        "points": "sum",
        "fg2m": "sum", "fg2a": "sum",
        "fg3m": "sum", "fg3a": "sum",
        "ftm": "sum", "fta": "sum",
        "oreb": "sum", "dreb": "sum", "treb": "sum",
        "ast": "sum", "stl": "sum", "tov": "sum",
        "blk": "sum", "blk_against": "sum",
        "pf": "sum", "pf_drawn": "sum",
        "pir": "sum",
        "minutes": "sum",
    }
    team_agg = (
        players_df
        .groupby(["season", "gamecode", "team_code", "is_home"], as_index=False)
        .agg(agg_cols)
    )

    # 2) Enrichissement avec les quarter scores (ByQuarter)
    q_frames = []
    for season in seasons:
        for game_dir in _list_game_dirs(season):
            gc = _gamecode_from_dir(game_dir)
            q = safe_read(game_dir / "quarter_scores.parquet")
            if q is None or len(q) == 0:
                continue
            # Le quarter_scores a les colonnes: Team (nom complet), Quarter1..4
            # Il faut matcher par nom d'équipe, ce qui est fragile.
            # On préfère matcher via is_home et la metadata.
            meta = safe_read(game_dir / "metadata.parquet")
            if meta is None or len(meta) == 0:
                continue
            code_home = str(meta["CodeTeamA"].iloc[0]).strip()
            code_away = str(meta["CodeTeamB"].iloc[0]).strip()
            name_home = str(meta["TeamA"].iloc[0]).strip()
            name_away = str(meta["TeamB"].iloc[0]).strip()

            q = q.copy()
            q["Team"] = q["Team"].astype(str).str.strip()
            q["season"] = season
            q["gamecode"] = gc
            # Résolution team_code via le nom
            q["team_code"] = np.where(
                q["Team"].str.upper() == name_home.upper(), code_home,
                np.where(q["Team"].str.upper() == name_away.upper(), code_away, None)
            )
            q_frames.append(q[["season", "gamecode", "team_code",
                                "Quarter1", "Quarter2", "Quarter3", "Quarter4"]])

    if q_frames:
        quarters = pd.concat(q_frames, ignore_index=True)
        quarters = quarters.rename(columns={
            "Quarter1": "q1_points",
            "Quarter2": "q2_points",
            "Quarter3": "q3_points",
            "Quarter4": "q4_points",
        })
        team_agg = team_agg.merge(
            quarters,
            on=["season", "gamecode", "team_code"],
            how="left",
        )

    # 3) Colonnes finales
    cols = [
        "season", "gamecode", "team_code", "is_home",
        "points",
        "q1_points", "q2_points", "q3_points", "q4_points",
        "fg2m", "fg2a", "fg3m", "fg3a", "ftm", "fta",
        "oreb", "dreb", "treb",
        "ast", "stl", "tov", "blk", "blk_against",
        "pf", "pf_drawn",
        "pir", "minutes",
    ]
    cols = [c for c in cols if c in team_agg.columns]
    return team_agg[cols].sort_values(["season", "gamecode", "is_home"],
                                       ascending=[True, True, False]).reset_index(drop=True)


# =========================================================================
# fact_shots
# =========================================================================
# Coordonnées EuroLeague (observées) :
#   COORD_X in [-750, 750] environ (largeur terrain)
#   COORD_Y in [-50, 1050] environ (longueur terrain)
#   Origine : milieu du terrain
#   L'équipe qui tire change de côté en 2e mi-temps (observé)

# Cercle situé à (0, ~160) en pixels euroleague ? À vérifier sur données réelles.
# Pour l'instant on garde les coords brutes + ajoute zone officielle
# et distance approximative.

# Dimensions réelles d'un terrain FIBA : 28 m × 15 m.
# Coordonnées EuroLeague : ~1500 unités × ~1000 unités sur la largeur.
# Ratio approximatif : 1 unité ≈ 0.01 m mais à calibrer.
# → on conserve les coords brutes et on calcule une distance relative
#   à partir de l'hypothèse d'un cercle à Y=0 ou Y=~160.

# Convention adoptée : on renvoie la distance brute au centre (0,0) ; le
# vrai calibrage se fera en analyse / visu quand on verra les données.

def _compute_shot_zone(coord_x: float, coord_y: float) -> str:
    """Zone logique simplifiée basée sur les coordonnées.

    Zones utilisées (convention custom, à affiner en Phase 2) :
      - RIM    : très proche du cercle
      - PAINT  : dans la raquette
      - MID    : mi-distance
      - C3_L   : corner 3 gauche
      - C3_R   : corner 3 droit
      - ABOVE3 : 3pts au-dessus du break
      - DEEP3  : 3pts très loin (au-delà de la ligne)
    """
    if pd.isna(coord_x) or pd.isna(coord_y):
        return "UNK"

    x = abs(coord_x)
    y = coord_y

    # Distance au cercle (estimation ; calibrée à 0,0 pour l'instant)
    dist = math.hypot(coord_x, coord_y)

    # Seuils approximatifs en unités EuroLeague (à calibrer plus tard)
    # Observés : COORD_X in [-727, 702], COORD_Y in [-12, 897]
    if dist < 150:
        return "RIM"
    if dist < 400 and abs(y) < 450:
        return "PAINT"
    # 3pts
    if dist > 675:
        if y < 150 and x > 600:
            return "C3_L" if coord_x < 0 else "C3_R"
        if dist > 900:
            return "DEEP3"
        return "ABOVE3"
    return "MID"


def build_fact_shots(seasons: list[int]) -> pd.DataFrame:
    """Consolide tous les tirs avec enrichissements coordonnées."""
    frames = []
    for season in seasons:
        for game_dir in _list_game_dirs(season):
            gc = _gamecode_from_dir(game_dir)
            sh = safe_read(game_dir / "shots.parquet")
            if sh is None or len(sh) == 0:
                continue
            sh = sh.copy()
            sh["season"] = season
            sh["gamecode"] = gc
            frames.append(sh)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df = strip_object_cols(df)

    rename_map = {
        "NUM_ANOT": "play_num",
        "TEAM": "team_code",
        "ID_PLAYER": "player_code",
        "PLAYER": "player_name",
        "ID_ACTION": "action_type",
        "ACTION": "action_label",
        "POINTS": "points",
        "COORD_X": "coord_x",
        "COORD_Y": "coord_y",
        "ZONE": "zone_official",
        "FASTBREAK": "is_fastbreak",
        "SECOND_CHANCE": "is_second_chance",
        "POINTS_OFF_TURNOVER": "is_poto",
        "MINUTE": "minute_game",
        "CONSOLE": "marker_time",
        "POINTS_A": "score_home",
        "POINTS_B": "score_away",
        "UTC": "utc_timestamp",
    }
    df = df.rename(columns=rename_map)

    # Booléens 0/1 en bool
    for col in ["is_fastbreak", "is_second_chance", "is_poto"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip() == "1"

    # Score made vs missed
    df["is_made"] = df["points"] > 0

    # Distance (brute, calibrage en Phase 2)
    df["shot_distance"] = np.sqrt(df["coord_x"]**2 + df["coord_y"]**2)

    # Zone custom
    df["zone_custom"] = df.apply(
        lambda r: _compute_shot_zone(r["coord_x"], r["coord_y"]), axis=1
    )

    cols = [
        "season", "gamecode", "play_num",
        "team_code", "player_code", "player_name",
        "action_type", "action_label",
        "points", "is_made",
        "coord_x", "coord_y", "shot_distance",
        "zone_official", "zone_custom",
        "is_fastbreak", "is_second_chance", "is_poto",
        "minute_game", "marker_time",
        "score_home", "score_away",
        "utc_timestamp",
    ]
    cols = [c for c in cols if c in df.columns]
    return df[cols].sort_values(["season", "gamecode", "play_num"]).reset_index(drop=True)


# =========================================================================
# Orchestrator
# =========================================================================
def build_all_facts_simple(seasons: list[int]) -> list[dict[str, Any]]:
    """Construit les 3 facts simples."""
    results = []

    log.info("building fact_boxscore_players")
    players = build_fact_boxscore_players(seasons)
    results.append(write_curated(
        players, curated_path("fact_boxscore_players"), "fact_boxscore_players"
    ))

    log.info("building fact_boxscore_teams")
    teams = build_fact_boxscore_teams(seasons, players)
    results.append(write_curated(
        teams, curated_path("fact_boxscore_teams"), "fact_boxscore_teams"
    ))

    log.info("building fact_shots")
    shots = build_fact_shots(seasons)
    results.append(write_curated(
        shots, curated_path("fact_shots"), "fact_shots"
    ))

    return results