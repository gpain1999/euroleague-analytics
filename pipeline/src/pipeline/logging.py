"""Setup du logging structuré pour tout le pipeline.

Usage :
    from pipeline.logging import get_logger
    log = get_logger(__name__)
    log.info("fetching schedule", season=2024)
"""

from __future__ import annotations

import logging
import sys

import structlog

from pipeline import config


def setup_logging() -> None:
    """Initialise structlog avec un rendu console lisible en dev."""
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=config.LOG_LEVEL,
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="%H:%M:%S"),
            structlog.dev.ConsoleRenderer(colors=True),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, config.LOG_LEVEL, logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Retourne un logger prêt à l'emploi."""
    return structlog.get_logger(name)


# Setup au premier import
setup_logging()