"""Validation : on croise nos facts simples avec les sources de vérité.

Tests :
  1. fact_boxscore_teams.points matche les scores de dim_games
  2. fact_boxscore_players minutes totalisent ~200 par équipe par match
  3. fact_shots.is_made est cohérent avec POINTS_A/B finaux du dim_games
"""

from __future__ import annotations

import sys
from pathlib import Path

_PIPELINE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_PIPELINE_ROOT / "src"))

import pandas as pd  # noqa: E402

from pipeline.logging import get_logger  # noqa: E402
from pipeline.transformers.paths import curated_path  # noqa: E402

log = get_logger(__name__)


def test_team_scores_match_games() -> None:
    """fact_boxscore_teams.points doit égaler dim_games.home_score / away_score."""
    games = pd.read_parquet(curated_path("dim_games"))
    teams = pd.read_parquet(curated_path("fact_boxscore_teams"))

    played = games[games["played"]].copy()

    # Home scores
    home_teams = teams[teams["is_home"]].set_index(["season", "gamecode"])["points"]
    away_teams = teams[~teams["is_home"]].set_index(["season", "gamecode"])["points"]

    merged = played.set_index(["season", "gamecode"]).join(
        home_teams.rename("bs_home_points")
    ).join(
        away_teams.rename("bs_away_points")
    )

    mismatches_home = merged[merged["home_score"].astype("Int64") != merged["bs_home_points"].astype("Int64")]
    mismatches_away = merged[merged["away_score"].astype("Int64") != merged["bs_away_points"].astype("Int64")]

    log.info(
        "team scores check",
        total_played=len(played),
        mismatches_home=len(mismatches_home),
        mismatches_away=len(mismatches_away),
    )
    if len(mismatches_home) > 0:
        log.warning("home mismatches sample", sample=mismatches_home.head(3).reset_index().to_dict("records"))
    if len(mismatches_away) > 0:
        log.warning("away mismatches sample", sample=mismatches_away.head(3).reset_index().to_dict("records"))


def test_player_minutes_sum_to_200() -> None:
    """Les minutes totales d'une équipe doivent faire ~200 par match régulier.

    Prolongations : 5 min × 5 joueurs = 25 min supplémentaires par équipe.
    Donc on s'attend à 200, 225, 250, etc. Tolérance ±1 minute pour les arrondis.
    """
    players = pd.read_parquet(curated_path("fact_boxscore_players"))

    team_minutes = (
        players[players["is_playing"]]
        .groupby(["season", "gamecode", "team_code"], as_index=False)["minutes"]
        .sum()
    )

    # On s'attend à 200 + 25*N où N = nb de prolos
    def classify(m: float) -> str:
        for expected in (200, 225, 250, 275, 300):
            if abs(m - expected) <= 1.0:
                return f"ok_{expected}"
        return f"weird_{round(m, 1)}"

    team_minutes["status"] = team_minutes["minutes"].apply(classify)
    status_counts = team_minutes["status"].value_counts()

    log.info("player minutes sum check", **status_counts.to_dict())

    weird = team_minutes[team_minutes["status"].str.startswith("weird")]
    if len(weird) > 0:
        log.warning("weird minute totals sample", sample=weird.head(5).to_dict("records"))


def test_shot_counts_vs_players() -> None:
    """Les FGA/FGM agrégés depuis fact_shots doivent matcher ceux de fact_boxscore_teams."""
    shots = pd.read_parquet(curated_path("fact_shots"))
    teams = pd.read_parquet(curated_path("fact_boxscore_teams"))

    # FGA depuis shots : tirs non-FT (action 2FG*, 3FG*)
    fg_shots = shots[shots["action_type"].isin(["2FGM", "2FGA", "3FGM", "3FGA"])]
    fga_from_shots = (
        fg_shots.groupby(["season", "gamecode", "team_code"], as_index=False)
        .size()
        .rename(columns={"size": "fga_shots"})
    )

    # FGA depuis boxscore
    teams_fga = teams.copy()
    teams_fga["fga_box"] = teams_fga["fg2a"] + teams_fga["fg3a"]

    merged = teams_fga.merge(
        fga_from_shots, on=["season", "gamecode", "team_code"], how="left"
    )
    merged["fga_shots"] = merged["fga_shots"].fillna(0).astype(int)
    merged["diff"] = merged["fga_box"] - merged["fga_shots"]

    non_zero = merged[merged["diff"] != 0]
    log.info(
        "FGA consistency",
        total=len(merged),
        matching=(merged["diff"] == 0).sum(),
        mismatches=len(non_zero),
    )
    if len(non_zero) > 0:
        log.warning("FGA mismatch sample", sample=non_zero.head(5)[
            ["season", "gamecode", "team_code", "fga_box", "fga_shots", "diff"]
        ].to_dict("records"))


def main() -> int:
    log.info("=" * 60)
    log.info("validating facts_simple")
    log.info("=" * 60)

    test_team_scores_match_games()
    test_player_minutes_sum_to_200()
    test_shot_counts_vs_players()

    log.info("=" * 60)
    log.info("validation done")
    return 0


if __name__ == "__main__":
    sys.exit(main())