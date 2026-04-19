"""Script ad-hoc : fetch le schedule des 3 saisons.

Usage :
    python pipeline/scripts/fetch_schedule.py
    python pipeline/scripts/fetch_schedule.py --force
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_PIPELINE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_PIPELINE_ROOT / "src"))

from pipeline import config  # noqa: E402
from pipeline.fetchers.schedule import ScheduleFetcher  # noqa: E402
from pipeline.logging import get_logger  # noqa: E402

log = get_logger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Ignore le cache.")
    parser.add_argument("--season", type=int, action="append",
                        help="Saison(s) à traiter. Défaut : toutes.")
    args = parser.parse_args()

    config.ensure_dirs()
    seasons = args.season or config.SEASONS

    for season in seasons:
        log.info("=" * 60)
        log.info("fetching schedule", season=season)
        fetcher = ScheduleFetcher(season=season)
        df = fetcher.run(force=args.force)
        log.info(
            "schedule ready",
            season=season,
            rows=len(df),
            rounds=df["round"].nunique() if "round" in df.columns else "?",
            teams=df["homecode"].nunique() if "homecode" in df.columns else "?",
        )
        # Aperçu
        if len(df) > 0:
            cols = ["gameday", "round", "date", "hometeam", "homecode",
                    "awayteam", "awaycode", "played"]
            available = [c for c in cols if c in df.columns]
            print(df[available].head(5).to_string(index=False))
            print(f"... ({len(df) - 5} autres lignes)" if len(df) > 5 else "")

    return 0


if __name__ == "__main__":
    sys.exit(main())