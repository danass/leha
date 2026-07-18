# leha — synchro RNCP (France Compétences)

Synchronise le **Répertoire National des Certifications Professionnelles (RNCP)**
et le **Répertoire Spécifique (RS)** depuis l'open data de France Compétences vers
une base PostgreSQL.
Alimente le moteur de recherche de [diplome.app/rncp](https://www.diplome.app/rncp).

## Ce que fait le script

`sync_lowercase.py` :

1. télécharge les derniers exports **XML RNCP et RS (v4-1)** publiés sur data.gouv.fr ;
2. les parse et remplit quatre tables PostgreSQL (le RS n'a ni niveau européen ni blocs de compétences) :
   - `fiches` — les titres (intitulé, niveau, dates, statut actif/échu…)
   - `certificateurs` — les organismes certificateurs
   - `partenaires` — les organismes habilités à préparer/délivrer
   - `bloc_competences` — les blocs de compétences
3. reconstruit ces tables puis **bascule dessus de façon atomique** (aucune
   coupure de service pendant la synchro) ;
4. enregistre un statut horodaté dans `rncp_sync_status` (date + volumes), utilisé
   pour afficher la fraîcheur des données sur diplome.app.

## Installation

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

Créez un fichier `.env` avec la connexion PostgreSQL :

```
DATABASE_URL_FRANCECOMPETENCES=postgres://user:password@host:port/base
```

## Lancement

```bash
python3 sync_lowercase.py              # télécharge le dernier export et synchronise
python3 sync_lowercase.py export.xml   # utilise un export XML local
```

### En cron (2×/jour)

`sync-cron.sh` lance la synchro et journalise l'horodatage + le code de sortie
dans `sync-cron.log` :

```cron
0 6,18 * * * /chemin/vers/leha/sync-cron.sh
```

## Source & licence

Données : [Répertoire national des certifications professionnelles](https://www.data.gouv.fr/fr/datasets/5eebbc067a14b6fecc9c9976/),
publié par France Compétences sur data.gouv.fr, sous
[Licence Ouverte / Etalab 2.0](https://www.etalab.gouv.fr/licence-ouverte-open-licence/).

## Prérequis

- Python 3.9+
- PostgreSQL

---

> `main.py` (version historique : import CSV vers des tables PascalCase) est
> conservé pour référence mais n'est plus utilisé — `sync_lowercase.py` le remplace.
