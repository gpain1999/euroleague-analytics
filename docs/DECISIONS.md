# Journal des décisions

Format ADR (Architecture Decision Records) léger. Chaque décision structurante
est tracée ici avec son contexte, les alternatives considérées et la raison du
choix. Utile pour comprendre pourquoi tel choix a été fait 6 mois plus tard, ou
pour faire évoluer le projet proprement.

---

## ADR-001 — Stack frontend : Next.js 14 App Router

**Date** : Phase 0
**Statut** : Accepté

### Contexte
Besoin d'un frontend moderne, déployable gratuitement, qui permette aussi de
servir les API routes (backend embarqué).

### Alternatives considérées
- **Streamlit** : ultra-rapide à dev, 100% Python, rejeté pour manque de
  souplesse design et apparence "data app"
- **Remix, SvelteKit** : excellents, mais moindre familiarité et écosystème
  Tailwind/shadcn moins mûr
- **Astro + Vercel Functions Python** : intéressant mais complique la mono-repo

### Décision
Next.js 14 App Router en TypeScript.

### Conséquences
- Apprentissage Next.js requis (user a peu d'expérience)
- Écosystème shadcn/ui, Tailwind, Vercel très intégré
- Même codebase pour pages et API routes

---

## ADR-002 — Backend data : DuckDB sur Parquet

**Date** : Phase 0
**Statut** : Accepté

### Contexte
Besoin d'un moteur SQL performant sur des volumes moyens (millions de lignes
PBP), lisible en serverless, gratuit.

### Alternatives considérées
- **Postgres** (Neon, Supabase) : classique, mais overkill pour read-only
  analytics et introduit une latence réseau
- **SQLite** : simple, mais moins performant en analytique (colonnaire vs row)
- **JSON statiques** : très rapide mais perd la flexibilité du moteur de splits

### Décision
DuckDB en mode embedded, fichier unique commité sur la branche `data`.

### Conséquences
- Zéro serveur à gérer
- Performance colonnaire excellente
- Limite écriture concurrente (non-bloquant ici : pipeline = unique writer)
- Taille du fichier à surveiller (limite repo Git)

---

## ADR-003 — Déploiement : Vercel + GitHub Actions

**Date** : Phase 0
**Statut** : Accepté

### Contexte
Contrainte budgétaire : hébergement gratuit. Besoin de refresh quotidien
automatique + déclenchement manuel depuis la page admin.

### Alternatives considérées
- **Fly.io free + FastAPI** : permet un vrai backend Python, mais 256Mo RAM
  tendu, moins de zero-config
- **GitHub Pages + JSON statiques** : 100% gratuit mais perd la flexibilité
- **Self-hosted VPS** : non gratuit

### Décision
Vercel Hobby pour le frontend et les API routes. GitHub Actions pour le
pipeline Python (cron quotidien + workflow_dispatch pour manuel).

### Conséquences
- Pas de backend Python en prod (FastAPI = outil dev/pipeline uniquement)
- Refresh manuel = appel API GitHub depuis la page admin via token
- Latence acceptable (Vercel sans cold start)

---

## ADR-004 — Python packaging : pip + venv

**Date** : Phase 0
**Statut** : Accepté

### Contexte
User préfère la simplicité.

### Alternatives considérées
- **uv** : plus rapide, plus moderne
- **poetry** : gestion de dépendances plus sophistiquée

### Décision
`pip` + `venv` + `requirements.txt` / `requirements-dev.txt`.

### Conséquences
- Setup immédiatement compris par tout dev Python
- Moins de fonctionnalités (lock file, dépendances optionnelles)
- Possibilité de migrer plus tard sans douleur

---

## ADR-005 — Ball Care : formule C (ratio pur)

**Date** : Phase 0
**Statut** : Accepté

### Contexte
Trois formules possibles pour "ball care" :
- A : `(FGA + 0,44×FTA) / Possessions` — brut
- B : `1 - TOV / Possessions` — complément du TOV rate
- C : `(FGA + 0,44×FTA) / (FGA + 0,44×FTA + TOV)` — ratio pur

### Décision
Formule C.

### Justification
Ignore les rebonds offensifs qui gonflent artificiellement le décompte
de possessions. Lecture directe : "sur l'ensemble de mes fins de possession,
combien finissent par une opportunité de marquer ?".

---

## ADR-006 — Rebond : OREB% et DREB% séparés

**Date** : Phase 0
**Statut** : Accepté

### Décision
Utiliser les formules :
- OREB% = OREB / (OREB + Opp DREB)
- DREB% = DREB / (DREB + Opp OREB)

### Justification
Mesure la capacité effective à capter les rebonds disponibles, plutôt qu'une
moyenne par match dépendante du rythme du match. Standard NBA analytics.
Permet d'identifier une équipe excellente défensivement au rebond mais
médiocre offensivement (ou inversement).

---

## ADR-007 — Périmètre temporel initial : 3 saisons

**Date** : Phase 0
**Statut** : Accepté

### Décision
Saisons 2023-24, 2024-25, 2025-26 (convention API : année = année de départ
de la saison).

### Justification
- 3 saisons = volume suffisant pour des moyennes crédibles et la détection
  d'évolutions
- L'architecture supporte l'ajout de saisons sans refonte
- Évite les complications de rétrocompatibilité avec d'anciens formats API

---

## ADR-008 — Navigation : liens cliquables partout

**Date** : Phase 0
**Statut** : Accepté

### Décision
Tous les noms de joueurs et d'équipes affichés dans l'application sont des
liens cliquables vers leur fiche respective. Règle absolue, sans exception.

### Conséquences
- Composants `<LinkPlayer />` et `<LinkTeam />` à utiliser systématiquement
- Cohérence visuelle et navigationnelle garantie
- Permet une exploration en chaîne type Wikipedia / Basketball Reference

---

## Template pour les ADR suivantes

```
## ADR-NNN — Titre court

**Date** : PhaseX
**Statut** : Accepté | Superseded par ADR-MMM | Rejeté

### Contexte
...

### Alternatives considérées
...

### Décision
...

### Conséquences
...
```
