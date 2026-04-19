"""Script : fetch les stats agrégées saison (player/team) et les standings.

Usage :
    python pipeline/scripts/fetch_season_aggregates.py
    python pipeline/scripts/fetch_season_aggregates.py --season 2025
    python pipeline/scripts/fetch_season_aggregates.py --force
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_PIPELINE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_PIPELINE_ROOT / "src"))

from pipeline import config  # noqa: E402
from pipeline.fetchers.season_stats import fetch_season_stats  # noqa: E402
from pipeline.fetchers.standings import fetch_season_standings  # noqa: E402
from pipeline.logging import get_logger  # noqa: E402

log = get_logger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, action="append",
                        help="Saison(s). Répétable. Défaut : toutes.")
    parser.add_argument("--force", action="store_true",
                        help="Ignore le cache, re-télécharge tout.")
    args = parser.parse_args()

    config.ensure_dirs()
    seasons = args.season or config.SEASONS

    for season in seasons:
        log.info("=" * 60)
        log.info("season stats", season=season)
        stats_res = fetch_season_stats(season=season, force=args.force)
        ok = sum(1 for r in stats_res.values() if r.get("ok"))
        log.info("season stats done", season=season, ok=ok, total=len(stats_res))

        log.info("standings", season=season)
        st_res = fetch_season_standings(season=season, force=args.force)
        ok_s = sum(1 for r in st_res.values() if r.get("ok"))
        log.info("standings done", season=season, ok=ok_s, total=len(st_res))

    return 0


if __name__ == "__main__":
    sys.exit(main())