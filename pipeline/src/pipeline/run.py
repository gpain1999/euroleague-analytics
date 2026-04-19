"""Entrypoint du pipeline.

Phase 0 : ce fichier est un squelette. L'implémentation réelle arrive
en Phase 1 (voir docs/ARCHITECTURE.md).

Usage :
    python -m pipeline.run
    python -m pipeline.run --season 2024
    python -m pipeline.run --season 2024 --only games,standings
"""

from __future__ import annotations

import argparse
import sys

from pipeline import config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline d'ingestion EuroLeague Analytics"
    )
    parser.add_argument(
        "--season",
        type=int,
        action="append",
        help="Saison(s) à traiter. Répétable. Défaut : toutes les saisons configurées.",
    )
    parser.add_argument(
        "--only",
        type=str,
        help="Étapes à exécuter, séparées par des virgules. Ex : fetch,transform,aggregate",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simule sans écrire sur disque.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config.ensure_dirs()

    seasons = args.season or config.SEASONS
    print(f"[pipeline] saisons : {seasons}")
    print(f"[pipeline] storage : {config.STORAGE_DIR}")
    print(f"[pipeline] duckdb  : {config.DUCKDB_FILE}")
    print(f"[pipeline] dry-run : {args.dry_run}")

    # TODO Phase 1 :
    #   1. fetch_schedule(seasons)
    #   2. fetch_games(seasons)
    #   3. fetch_standings(seasons)
    #   4. fetch_player_team_stats(seasons)
    #   5. download_missing_images()
    #   6. transform_raw_to_curated()
    #   7. derive_possessions()
    #   8. build_aggregates()
    #   9. validate()
    #  10. export_to_duckdb()
    print("\n[pipeline] Squelette Phase 0 : aucune étape implémentée.")
    print("[pipeline] L'implémentation réelle arrive en Phase 1.")
    print("[pipeline] Pour obtenir une base de démo : python pipeline/scripts/bootstrap_demo_db.py")

    return 0


if __name__ == "__main__":
    sys.exit(main())
