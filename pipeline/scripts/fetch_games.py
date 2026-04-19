"""Script : fetch les artefacts (metadata, boxscore, PBP, shots, quarter scores)
des matchs joués sur une ou plusieurs saisons.

Usage :
    python pipeline/scripts/fetch_games.py                     # toutes saisons
    python pipeline/scripts/fetch_games.py --season 2023       # une saison
    python pipeline/scripts/fetch_games.py --limit 3           # 3 matchs (test)
    python pipeline/scripts/fetch_games.py --force             # ignore cache
    python pipeline/scripts/fetch_games.py --season 2023 --limit 1 --force
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_PIPELINE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_PIPELINE_ROOT / "src"))

from pipeline import config  # noqa: E402
from pipeline.fetchers.game import fetch_season_games  # noqa: E402
from pipeline.logging import get_logger  # noqa: E402

log = get_logger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, action="append",
                        help="Saison(s). Répétable. Défaut : toutes.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Nombre max de matchs par saison (pour tests).")
    parser.add_argument("--force", action="store_true",
                        help="Ignore le cache, re-télécharge tout.")
    args = parser.parse_args()

    config.ensure_dirs()
    seasons = args.season or config.SEASONS

    grand_totals = {"all_ok": 0, "partial": 0, "failed": 0}

    for season in seasons:
        log.info("=" * 60)
        log.info("processing season", season=season)
        results = fetch_season_games(
            season=season,
            force=args.force,
            limit=args.limit,
        )
        for r in results.values():
            ok_parts = sum(1 for v in r.values() if v.get("ok"))
            if ok_parts == len(r):
                grand_totals["all_ok"] += 1
            elif ok_parts == 0:
                grand_totals["failed"] += 1
            else:
                grand_totals["partial"] += 1

    log.info("=" * 60)
    log.info("GRAND TOTAL", **grand_totals)
    return 0


if __name__ == "__main__":
    sys.exit(main())