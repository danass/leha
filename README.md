# Projet Leha 📊

## Description

Le script `main.py` est conçu pour automatiser le processus de téléchargement, de traitement et de synchronisation des données provenant de l'API de France Compétences. Voici un aperçu de ce que fait chaque partie du script :

### Fonctionnalités principales

1. **Récupération des liens de téléchargement** 🌐
   - La fonction `fetch_and_process_links` interroge l'API de France Compétences pour obtenir les liens de téléchargement des fichiers CSV les plus récents et les traite.


2. **Création des tables dans la base de données** 🛠️
   - La fonction `create_tables` crée les tables nécessaires dans la base de données PostgreSQL si elles n'existent pas déjà.

3. **Téléchargement et extraction des fichiers CSV** 📥
   - La fonction `download_and_unzip` télécharge les fichiers ZIP depuis une URL donnée, les extrait et traite les fichiers CSV pertinents.

4. **Traitement des fichiers CSV** 📄
   - La fonction `process_csv` lit les fichiers CSV et appelle les fonctions de synchronisation appropriées (`sync_fiches`, `sync_certificateurs`, `sync_partenaires`, `sync_bloc_competences`) pour mettre à jour les tables de la base de données.

5. **Synchronisation des données** 🔄
   - Chaque fonction de synchronisation (`sync_fiches`, `sync_certificateurs`, `sync_partenaires`, `sync_bloc_competences`) compare les données des fichiers CSV avec celles de la base de données et effectue les insertions, mises à jour et suppressions nécessaires pour maintenir la base de données à jour.


### Comment utiliser le script

1. Assurez-vous d'avoir une base de données PostgreSQL configurée et accessible.
2. Créez un fichier `.env` dans le même répertoire que `main.py` avec les variables suivantes :
   ```
   DB_USER=your_db_user
   DB_PASSWORD=your_db_password
   ```
3. Exécutez le script `main.py` :
   ```bash
   python main.py
   ```

Le script téléchargera les données les plus récentes, les traitera et mettra à jour votre base de données automatiquement. 🚀

### Prérequis

- Python 3.x
- PostgreSQL
- Bibliothèques Python : `pandas`, `psycopg2`, `dotenv`, `requests`

### Installation des dépendances

```bash
pip install pandas psycopg2-binary python-dotenv requests
```

### Auteur

Ce script a été développé par Daniel Assayag pour automatiser la gestion des données de France Compétences. 📈