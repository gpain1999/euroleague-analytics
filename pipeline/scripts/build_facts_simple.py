"""Construit fact_boxscore_players, fact_boxscore_teams, fact_shots.

Usage :
    python pipeline/scripts/build_facts_simple.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_PIPELINE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_PIPELINE_ROOT / "src"))

from pipeline import config  # noqa: E402
from pipeline.logging import get_logger  # noqa: E402
from pipeline.transformers.facts_simple import build_all_facts_simple  # noqa: E402

log = get_logger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, action="append",
                        help="Saisons à inclure. Défaut : toutes.")
    args = parser.parse_args()

    config.ensure_dirs()
    seasons = args.season or config.SEASONS

    log.info("=" * 60)
    log.info("building facts_simple", seasons=seasons)
    log.info("=" * 60)

    results = build_all_facts_simple(seasons)

    log.info("=" * 60)
    for r in results:
        log.info("fact ready", **r)
    return 0


if __name__ == "__main__":
    sys.exit(main())