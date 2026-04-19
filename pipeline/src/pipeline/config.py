"""Configuration centralisée du pipeline.

Toutes les constantes et chemins transitent par ce module pour éviter la
dispersion. Les valeurs peuvent être surchargées via variables d'environnement.
"""

from __future__ import annotations

import os
from pathlib import Path

# =========================================================================
# Racine du projet (remonte depuis src/pipeline/config.py)
# =========================================================================
PROJECT_ROOT: Path = Path(__file__).resolve().parents[3]

# =========================================================================
# Compétition et saisons
# =========================================================================
# Code compétition EuroLeague dans l'API : "E"
COMPETITION_CODE: str = "E"

# Saisons gérées. Convention euroleague_api : 2023 = saison 2023-24.
SEASONS: list[int] = [2023, 2024, 2025]
CURRENT_SEASON: int = max(SEASONS)

# =========================================================================
# Chemins de stockage
# =========================================================================
# Répertoire de travail local, non versionné (sauf le .gitkeep)
STORAGE_DIR: Path = PROJECT_ROOT / "storage"

RAW_DIR: Path = STORAGE_DIR / "raw"
CURATED_DIR: Path = STORAGE_DIR / "curated"
AGGREGATED_DIR: Path = STORAGE_DIR / "aggregated"
IMAGES_DIR: Path = STORAGE_DIR / "images"

# Cible finale servie au frontend (commitée sur la branche data en prod)
PUBLIC_DATA_DIR: Path = PROJECT_ROOT / "public" / "data"
DUCKDB_FILE: Path = PUBLIC_DATA_DIR / "euroleague.duckdb"

# =========================================================================
# Paramètres métier (peuvent évoluer)
# =========================================================================
# Possessions minimum pour considérer un lineup/duo/trio comme statistiquement
# significatif dans les pages de classement.
MIN_POSSESSIONS_LINEUP: dict[int, int] = {
    2: 50,   # duos
    3: 30,   # trios
    4: 20,   # quartets
    5: 20,   # quintets
}

# Fenêtre clutch : dernières N secondes du 4e quart ou prolongation,
# avec un écart de score maximum.
CLUTCH_LAST_SECONDS: int = 300  # 5 minutes
CLUTCH_MAX_SCORE_DIFF: int = 5

# =========================================================================
# Logging
# =========================================================================
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()


def ensure_dirs() -> None:
    """Crée tous les répertoires de stockage s'ils n'existent pas."""
    for directory in (
        STORAGE_DIR,
        RAW_DIR,
        CURATED_DIR,
        AGGREGATED_DIR,
        IMAGES_DIR,
        PUBLIC_DATA_DIR,
    ):
        directory.mkdir(parents=True, exist_ok=True)
