# EuroLeague Analytics

Plateforme web d'analyse statistique avancée dédiée à l'EuroLeague de basket-ball.
Exploitation en profondeur des données play-by-play et boxscore via le package
open-source [`euroleague_api`](https://pypi.org/project/euroleague-api/), avec un
moteur de splits dynamiques pour filtrer et croiser les statistiques sur tous les
angles : joueur, équipe, duo, trio, quintet, adversaire, quart-temps, zone de tir,
clutch, etc.

> **Statut** : Phase 0 (Setup). Les fonctionnalités décrites dans le [dossier de
> cadrage](docs/ARCHITECTURE.md) ne sont pas encore implémentées.

## Stack

| Couche            | Technologie                                          |
| ----------------- | ---------------------------------------------------- |
| Frontend          | Next.js 14 (App Router) + TypeScript + Tailwind      |
| API               | Next.js Route Handlers (serverless Vercel)          |
| Moteur analytique | DuckDB (lu depuis Node.js)                          |
| Pipeline          | Python 3.11 + `euroleague_api` + pandas + pyarrow   |
| Orchestration     | GitHub Actions (cron quotidien + `workflow_dispatch`) |
| Hébergement       | Vercel (Hobby, gratuit)                             |

## Démarrage rapide

Pré-requis : Node.js ≥ 20, Python ≥ 3.11, Git.

```bash
# 1. Cloner
git clone https://github.com/gpain1999/euroleague-analytics.git
cd euroleague-analytics

# 2. Installer les dépendances frontend
npm install

# 3. Installer les dépendances pipeline (venv Python)
cd pipeline
python -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -r requirements.txt -r requirements-dev.txt
cd ..

# 4. Générer une base DuckDB de démonstration
python pipeline/scripts/bootstrap_demo_db.py

# 5. Lancer le site en local
npm run dev
# → http://localhost:3000
```

Si tout est bien configuré, la page d'accueil affiche un badge "DuckDB OK" et
affiche un tableau de démonstration alimenté par la base `public/data/euroleague.duckdb`.

## Commandes utiles

Un `Makefile` regroupe les commandes courantes (voir `make help`).

```bash
make install        # Installe tout (npm + pip)
make dev            # Lance Next.js en dev
make demo-db        # Régénère la base DuckDB de démo
make test           # Tests Python + Next.js lint
make pipeline-run   # (plus tard) Exécute le pipeline réel
```

## Structure du repo

```
euroleague-analytics/
├── app/                # Next.js App Router (pages + API routes)
├── components/         # Composants React réutilisables
├── lib/                # Helpers frontend (dont accès DuckDB)
├── public/
│   └── data/           # .duckdb et parquets (alimentés par la branche data)
├── pipeline/           # Pipeline Python d'ingestion
│   ├── src/pipeline/   # Code Python (wrappers, transformers, aggregators)
│   ├── scripts/        # Scripts outillage (bootstrap demo, ad-hoc)
│   └── tests/          # pytest
├── docs/
│   ├── ARCHITECTURE.md # Synthèse technique (référence de dev)
│   └── DECISIONS.md    # Journal des choix techniques
├── storage/            # Working directory local (ignoré par git)
├── .github/workflows/  # CI + refresh quotidien
├── Makefile
├── next.config.mjs
├── package.json
├── tailwind.config.ts
├── tsconfig.json
└── README.md
```

## Déploiement

Le frontend est déployé sur Vercel (offre Hobby gratuite). Le pipeline Python
tourne sur GitHub Actions (cron quotidien + déclenchement manuel depuis la page
admin). Les données traitées sont versionnées sur la branche `data` du repo ;
chaque mise à jour de cette branche déclenche un redéploiement Vercel automatique.

Voir [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) pour le détail.

## Sources

- Données : [`euroleague_api`](https://pypi.org/project/euroleague-api/) (API officielle EuroLeague)
- Code : licence MIT — voir [`LICENSE`](LICENSE)

## Auteur

Projet personnel open-source. Contributions et retours bienvenus via issues.
