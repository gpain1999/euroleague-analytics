# Architecture

Synthèse technique du projet. Version condensée du dossier de cadrage,
destinée à servir de référence quotidienne pendant le développement.

> Pour la version complète voir le dossier de présentation
> (`dossier_euroleague.docx` à la racine du repo).

## Principe directeur : tout est un split

Toute question analytique (stats d'un joueur sur la saison, ORtg d'un duo
en Q4 à domicile, etc.) est une instance du même problème : appliquer des
filtres sur une base event-based et calculer des métriques sur l'échantillon.

Conséquence : un seul endpoint central (`POST /api/splits`) concentre la
logique métier. Toutes les pages en sont des instanciations visuelles.

## Couches de données

### 1. Raw — dumps API immuables

Fichiers Parquet partitionnés par saison et par match. Jamais modifiés
après écriture. Permettent de retracer la source d'une anomalie et de
rejouer les transformations sans re-télécharger.

Localisation en prod : **branche `raw` du repo** (ignorée par Vercel).

### 2. Curated — données nettoyées et enrichies

Tables pivots utilisées par tout le reste :

| Table                      | Grain                               |
| -------------------------- | ----------------------------------- |
| `dim_seasons`              | Saison                              |
| `dim_teams`                | Équipe                              |
| `dim_players`              | Joueur                              |
| `dim_games`                | Match                               |
| `fact_boxscore_players`    | Joueur × match                      |
| `fact_boxscore_teams`      | Équipe × match (incl. par quart)    |
| `fact_shots`               | Tir                                 |
| `fact_pbp_enriched`        | Action PBP + lineups + contexte     |
| `fact_possessions`         | Possession                          |

**`fact_pbp_enriched` est la table centrale** : c'est elle qui permet les
splits dynamiques. Elle contient pour chaque action :

- Lineup offensif et défensif (listes de 5 codes joueurs)
- Score home/away à cet instant, score differential
- Possession_id
- Seconds elapsed game
- is_home_team calculé (l'API le renvoie toujours null)

### 3. Aggregated — pré-calculs pour lectures rapides

Tables matérialisées pour servir les pages "normales" instantanément sans
recalcul sur plusieurs millions de lignes PBP.

| Table                 | Clé                                                 |
| --------------------- | --------------------------------------------------- |
| `player_season_splits`| player × season × phase × home_away                 |
| `team_season_splits`  | team × season × phase × home_away × side            |
| `lineup_splits`       | team × season × lineup_hash × size (2-5)            |
| `player_onoff`        | player × season                                     |
| `player_duo_splits`   | team × season × (player_a, player_b) × mode         |
| `shot_zones_agg`      | subject × season × zone                             |
| `standings_snapshots` | season × round                                      |

## Pipeline (Phase 1+)

Étapes séquentielles, chacune idempotente :

1. **Fetch schedule** — nouveaux matchs à ingérer
2. **Fetch games** — metadata, boxscore, PBP, shots par match manquant
3. **Fetch standings + stats saison**
4. **Download missing images**
5. **Transform raw → curated** (le plus complexe : lineups, coordonnées, Player_ID)
6. **Derive possessions** — parse PBP, segmente par outcome
7. **Build aggregates** — tous les pré-calculs
8. **Validate** — sanity checks (score PBP = final, minutes = 200, etc.)
9. **Export to DuckDB** — écriture du fichier servi au frontend
10. **Commit to data branch** — déclenche Vercel redeploy

Entrypoint : `python -m pipeline.run` (voir `--help`).

## Moteur de splits

### Contrat unifié

```ts
POST /api/splits
{
  "subject": "player" | "team" | "lineup" | "duo" | "trio" | "quartet" | "quintet",
  "subject_ids": string[],
  "filters": {
    "seasons"?: number[],
    "phases"?: ("RS" | "PO" | "FF")[],
    "home_away"?: "home" | "away" | "both",
    "opponent_codes"?: string[],
    "periods"?: number[],
    "time_remaining_max_sec"?: number,
    "score_diff_range"?: [number, number],
    "shot_zones"?: string[],
    "game_codes"?: number[],
    "min_possessions"?: number,
    "date_range"?: [string, string]
  },
  "metrics": string[]  // voir catalogue
}
```

### Stratégie de routage

- **Cas simple (saison complète, filtres = agrégats pré-calculés)** → lecture directe
  des tables aggregated. Latence < 100 ms.
- **Cas complexe (filtres croisés dynamiques)** → requête SQL DuckDB à la volée sur
  `fact_pbp_enriched` + `fact_possessions`. Latence cible < 2 s.

### Principe d'implémentation

Toutes les métriques sont des **fonctions pures** SQL ou TS, isolées dans
`lib/metrics/` (frontend) et `pipeline/src/pipeline/metrics/` (Python pour
les agrégats). **Testées unitairement à 90% de couverture minimum.**

## Métriques disponibles

Voir section 6 du dossier de cadrage pour le détail complet (50+ métriques).
Principales familles :

- Base (PTS, REB, AST, STL, BLK, TOV, MIN, +/-, PIR)
- Shooting (FG%, 2P%, 3P%, FT%, eFG%, TS%, PPS)
- Volume (3PA rate, 2PA rate, FT rate, rim rate, corner 3 rate, %pts from 2/3/FT, Usage)
- Ball Care (formule C retenue)
- Rebond (OREB% et DREB% séparés)
- Playmaking (AST%, Assist Ratio)
- Ratings (ORtg, DRtg, Net, Pace)
- Défense (Opp TS%, Opp eFG%, STL rate, BLK rate, forced TOV, fouls drawn rate)
- Clutch (dernières 5 min Q4/OT, diff ≤ 5)
- On/Off (on-court net, off-court net, swing)
- Shot Quality (distribution, accuracy par zone, eFG% par zone, heatmap)

## Déploiement

```
 ┌─────────────┐          ┌──────────────┐          ┌─────────┐
 │ GitHub      │ cron 00h │ Pipeline     │ parquets │ Branch  │
 │ Actions     │ ────────▶│ Python       │ ────────▶│ `data`  │
 │             │          │ (euroleague_api)        │         │
 └─────────────┘          └──────────────┘          └────┬────┘
        ▲                                                │
        │ POST /admin/refresh                            │ push
        │                                                ▼
 ┌──────┴──────┐          ┌──────────────┐         ┌──────────┐
 │ Page Admin  │          │ Vercel       │◀────────│ Vercel   │
 │ (Next.js)   │          │ rebuild      │ webhook │ GitHub   │
 └─────────────┘          └──────────────┘         └──────────┘
                                  │
                                  ▼
                          ┌──────────────┐
                          │ Site en prod │
                          │ (Vercel)     │
                          └──────────────┘
```

Auth admin : mot de passe unique hashé bcrypt, cookie HttpOnly signé,
expiration 24h.

## Pièges techniques à retenir

Ces pièges sont issus de l'exploration préalable du package `euroleague_api`.
Ils doivent être traités dans les transformers de la Phase 1.

| Piège                               | Solution                                                       |
| ----------------------------------- | -------------------------------------------------------------- |
| PBP n'a pas de colonne `POINTS`     | Utiliser `POINTS_A` et `POINTS_B` (scores cumulés home/away)   |
| `IsHomeTeam` toujours null          | Recalculer via `CODETEAM == CodeTeamA` (GameMetadata)          |
| `Player_ID` avec espaces à droite   | `str.strip()` systématique avant jointure                      |
| `Minutes` contient "DNP" et "MM:SS"| Fonction `parse_minutes` qui gère les trois cas                |
| `localLast5Form` = liste Python     | Accéder comme liste, pas comme string                          |
| `ScoreQuarter[n]` est cumulatif     | Utiliser `ByQuarter` pour les points par quart, `EndOfQuarter` pour le cumul |
| `teamsComparison` 404 au round 1    | try/except, non bloquant                                       |
| `gamecode` schedule = "E2023_7"     | Split sur `_`, prendre la partie numérique                     |
| Lineups ~6% d'incohérences          | Flag `validate_on_court_player` pour filtrer/signaler          |
| Jointure sur nom d'équipe           | **Jamais** — utiliser le code 3 lettres                        |

## Phases de développement

| Phase | Nom              | Durée      | Livrable                                      |
| ----- | ---------------- | ---------- | --------------------------------------------- |
| 0     | Setup            | 1-2 jours  | Repo, stack, premier site qui tourne          |
| 1     | Pipeline         | 5-7 jours  | Ingestion + curated + agrégats sur 3 saisons  |
| 2     | Moteur splits    | 3-5 jours  | `/api/splits` complet avec tests 90%          |
| 3     | Pages base       | 5-7 jours  | Dashboard, équipe, joueur, match              |
| 4     | Pages avancées   | 4-5 jours  | Comparateur, Lineup Lab, Leaderboards, Shots  |
| 5     | Polish           | 2-3 jours  | Admin, mobile, dark theme, info-bulles        |
| 6     | Déploiement      | 1-2 jours  | Prod Vercel, cron actif, README, portfolio    |
