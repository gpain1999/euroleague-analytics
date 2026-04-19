"""Génère une base DuckDB de démonstration.

Ce script produit un fichier public/data/euroleague.duckdb rempli avec des
données fictives mais cohérentes (équipes réelles, matchs plausibles, joueurs
fictifs) permettant au frontend de tourner immédiatement après un clone.

Il n'interroge PAS l'API EuroLeague : il crée des données synthétiques.
La base réelle sera produite par le pipeline en Phase 1+.

Usage :
    python pipeline/scripts/bootstrap_demo_db.py
"""

from __future__ import annotations

import random
import sys
from datetime import date, timedelta
from pathlib import Path

# Permet d'importer pipeline.config même si lancé directement
_PIPELINE_ROOT = Path(__file__).resolve().parents[1]  # pipeline/
sys.path.insert(0, str(_PIPELINE_ROOT / "src"))

import duckdb  # noqa: E402

from pipeline import config  # noqa: E402

random.seed(42)  # reproductibilité


# =========================================================================
# Données de base : les 18 équipes EuroLeague 2023-24
# =========================================================================
TEAMS: list[dict] = [
    {"code": "MAD", "name": "Real Madrid", "tv_code": "RMB", "city": "Madrid", "country": "ESP", "primary_color": "#FFFFFF"},
    {"code": "BAR", "name": "FC Barcelona", "tv_code": "BAR", "city": "Barcelona", "country": "ESP", "primary_color": "#004D98"},
    {"code": "MIL", "name": "EA7 Emporio Armani Milan", "tv_code": "EA7", "city": "Milan", "country": "ITA", "primary_color": "#CC0000"},
    {"code": "PAN", "name": "Panathinaikos AKTOR Athens", "tv_code": "PAO", "city": "Athens", "country": "GRE", "primary_color": "#007937"},
    {"code": "OLY", "name": "Olympiacos Piraeus", "tv_code": "OLY", "city": "Piraeus", "country": "GRE", "primary_color": "#CC0000"},
    {"code": "ULK", "name": "Fenerbahce Beko Istanbul", "tv_code": "FBB", "city": "Istanbul", "country": "TUR", "primary_color": "#003366"},
    {"code": "IST", "name": "Anadolu Efes Istanbul", "tv_code": "EFS", "city": "Istanbul", "country": "TUR", "primary_color": "#1C306B"},
    {"code": "BAS", "name": "Baskonia Vitoria-Gasteiz", "tv_code": "BKN", "city": "Vitoria", "country": "ESP", "primary_color": "#005CA9"},
    {"code": "MUN", "name": "FC Bayern Munich", "tv_code": "BAY", "city": "Munich", "country": "GER", "primary_color": "#DC052D"},
    {"code": "TEL", "name": "Maccabi Playtika Tel Aviv", "tv_code": "MTA", "city": "Tel Aviv", "country": "ISR", "primary_color": "#FFD700"},
    {"code": "RED", "name": "Crvena Zvezda Meridianbet Belgrade", "tv_code": "CZV", "city": "Belgrade", "country": "SRB", "primary_color": "#C8102E"},
    {"code": "PAR", "name": "Partizan Mozzart Bet Belgrade", "tv_code": "PAR", "city": "Belgrade", "country": "SRB", "primary_color": "#000000"},
    {"code": "ZAL", "name": "Zalgiris Kaunas", "tv_code": "ZAL", "city": "Kaunas", "country": "LTU", "primary_color": "#006633"},
    {"code": "ASV", "name": "LDLC ASVEL Villeurbanne", "tv_code": "ASV", "city": "Villeurbanne", "country": "FRA", "primary_color": "#005BAC"},
    {"code": "VIR", "name": "Virtus Segafredo Bologna", "tv_code": "VIR", "city": "Bologna", "country": "ITA", "primary_color": "#000000"},
    {"code": "MCO", "name": "AS Monaco", "tv_code": "ASM", "city": "Monaco", "country": "MCO", "primary_color": "#E2001A"},
    {"code": "BER", "name": "ALBA Berlin", "tv_code": "BER", "city": "Berlin", "country": "GER", "primary_color": "#FFFF00"},
    {"code": "PAM", "name": "Valencia Basket", "tv_code": "VBC", "city": "Valencia", "country": "ESP", "primary_color": "#F39C12"},
]

POSITIONS = ["G", "G", "F", "F", "C"]
FIRST_NAMES = ["MARCUS", "JORDAN", "ALEX", "NICOLAS", "TYLER", "WADE", "LUCA", "MATEO", "ADRIEN", "ROKAS"]
LAST_NAMES = ["HOWARD", "JAMES", "MIROTIC", "SHENGELIA", "CAMPAZZO", "NUNN", "LARKIN", "SLOUKAS", "TAVARES", "HEZONJA"]


# =========================================================================
# Connexion DuckDB
# =========================================================================
def create_db(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    print(f"[demo-db] création : {db_path}")
    return duckdb.connect(str(db_path))


# =========================================================================
# Création des tables (schéma simplifié — sera étoffé en Phase 1)
# =========================================================================
def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE dim_seasons (
            season_id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            start_date DATE
        );
    """)

    con.execute("""
        CREATE TABLE dim_teams (
            team_code VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL,
            tv_code VARCHAR,
            city VARCHAR,
            country VARCHAR,
            primary_color VARCHAR,
            logo_path VARCHAR
        );
    """)

    con.execute("""
        CREATE TABLE dim_players (
            player_code VARCHAR PRIMARY KEY,
            full_name VARCHAR NOT NULL,
            position VARCHAR,
            team_code VARCHAR,
            jersey VARCHAR,
            image_path VARCHAR
        );
    """)

    con.execute("""
        CREATE TABLE dim_games (
            season INTEGER NOT NULL,
            gamecode INTEGER NOT NULL,
            date DATE NOT NULL,
            phase VARCHAR NOT NULL,
            round INTEGER NOT NULL,
            home_code VARCHAR NOT NULL,
            away_code VARCHAR NOT NULL,
            home_score INTEGER,
            away_score INTEGER,
            played BOOLEAN NOT NULL DEFAULT FALSE,
            PRIMARY KEY (season, gamecode)
        );
    """)

    # Table minimale pour valider la lecture côté frontend
    con.execute("""
        CREATE TABLE fact_boxscore_players (
            season INTEGER,
            gamecode INTEGER,
            player_code VARCHAR,
            team_code VARCHAR,
            is_home BOOLEAN,
            minutes FLOAT,
            points INTEGER,
            rebounds INTEGER,
            assists INTEGER,
            valuation INTEGER
        );
    """)


# =========================================================================
# Seed données
# =========================================================================
def seed_seasons(con: duckdb.DuckDBPyConnection) -> None:
    rows = []
    for s in config.SEASONS:
        rows.append((s, f"EuroLeague {s}-{(s + 1) % 100:02d}", date(s, 10, 1)))
    con.executemany(
        "INSERT INTO dim_seasons (season_id, name, start_date) VALUES (?, ?, ?)",
        rows,
    )
    print(f"[demo-db] dim_seasons : {len(rows)} lignes")


def seed_teams(con: duckdb.DuckDBPyConnection) -> None:
    rows = [
        (t["code"], t["name"], t["tv_code"], t["city"], t["country"],
         t["primary_color"], None)
        for t in TEAMS
    ]
    con.executemany(
        """INSERT INTO dim_teams (team_code, name, tv_code, city, country,
                                   primary_color, logo_path)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    print(f"[demo-db] dim_teams : {len(rows)} lignes")


def seed_players(con: duckdb.DuckDBPyConnection) -> None:
    rows = []
    for team in TEAMS:
        for i in range(12):
            code = f"DEMO_{team['code']}_{i:02d}"
            full_name = f"{random.choice(LAST_NAMES)}, {random.choice(FIRST_NAMES)}"
            position = POSITIONS[i % 5]
            jersey = str(random.randint(1, 99))
            rows.append((code, full_name, position, team["code"], jersey, None))
    con.executemany(
        """INSERT INTO dim_players (player_code, full_name, position, team_code,
                                     jersey, image_path)
           VALUES (?, ?, ?, ?, ?, ?)""",
        rows,
    )
    print(f"[demo-db] dim_players : {len(rows)} lignes")


def seed_games(con: duckdb.DuckDBPyConnection) -> None:
    """Génère un mini-calendrier : 3 rounds de Regular Season par saison,
    soit 9 matchs x 3 saisons = 27 matchs. Suffisant pour tester le rendu."""
    rows = []
    for season in config.SEASONS:
        start = date(season, 10, 5)
        gamecode = 1
        for round_n in range(1, 4):
            round_date = start + timedelta(days=7 * (round_n - 1))
            # 9 matchs par round (18 équipes / 2), shuffle pour variété
            teams_shuffled = [t["code"] for t in TEAMS]
            random.shuffle(teams_shuffled)
            for i in range(0, 18, 2):
                home, away = teams_shuffled[i], teams_shuffled[i + 1]
                # Scores plausibles
                home_score = random.randint(70, 100)
                away_score = random.randint(70, 100)
                rows.append((
                    season, gamecode, round_date, "RS", round_n,
                    home, away, home_score, away_score, True
                ))
                gamecode += 1
    con.executemany(
        """INSERT INTO dim_games
           (season, gamecode, date, phase, round, home_code, away_code,
            home_score, away_score, played)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    print(f"[demo-db] dim_games : {len(rows)} lignes")
    return rows


def seed_boxscore(con: duckdb.DuckDBPyConnection, games: list) -> None:
    """Génère un boxscore minimal pour chaque match."""
    rows = []
    for game in games:
        season, gamecode, _, _, _, home, away, home_score, away_score, _ = game
        for team_code, is_home, total_score in [
            (home, True, home_score),
            (away, False, away_score),
        ]:
            # Récupère 8 joueurs random de l'équipe
            players = con.execute(
                "SELECT player_code FROM dim_players WHERE team_code = ? LIMIT 8",
                [team_code],
            ).fetchall()
            # Distribue le score entre les joueurs
            remaining = total_score
            for i, (pcode,) in enumerate(players):
                pts = random.randint(0, min(remaining, 25)) if i < 7 else remaining
                remaining = max(0, remaining - pts)
                rows.append((
                    season, gamecode, pcode, team_code, is_home,
                    round(random.uniform(5, 35), 1),  # minutes
                    pts,
                    random.randint(0, 10),   # rebounds
                    random.randint(0, 8),    # assists
                    random.randint(-5, 30),  # valuation
                ))
    con.executemany(
        """INSERT INTO fact_boxscore_players
           (season, gamecode, player_code, team_code, is_home,
            minutes, points, rebounds, assists, valuation)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    print(f"[demo-db] fact_boxscore_players : {len(rows)} lignes")


# =========================================================================
# Main
# =========================================================================
def main() -> int:
    print("=" * 60)
    print("  EuroLeague Analytics — bootstrap de la base de démo")
    print("=" * 60)

    config.ensure_dirs()
    con = create_db(config.DUCKDB_FILE)

    try:
        create_schema(con)
        seed_seasons(con)
        seed_teams(con)
        seed_players(con)
        games = seed_games(con)
        seed_boxscore(con, games)

        # Récap
        print()
        print("  Récapitulatif")
        print("  " + "-" * 40)
        for table in ["dim_seasons", "dim_teams", "dim_players", "dim_games",
                      "fact_boxscore_players"]:
            n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"    {table:30s} {n:>6} lignes")

        print()
        print(f"  Base générée : {config.DUCKDB_FILE}")
        print(f"  Taille : {config.DUCKDB_FILE.stat().st_size / 1024:.1f} Ko")
        print()
        print("  Prochaine étape : npm run dev  → http://localhost:3000")

    finally:
        con.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
