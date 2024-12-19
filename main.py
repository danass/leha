import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from datetime import datetime
import requests
from io import BytesIO
import zipfile
import time

# Load environment variables from .env file
load_dotenv()

def create_tables():
    conn = psycopg2.connect(
        dbname="francecompetences",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("HOST"),
        port="5432"
    )
    cur = conn.cursor()

    # Create Fiches table if it does not exist
    print("Creating Fiches table...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Fiches (
            Id_Fiche TEXT PRIMARY KEY,
            Numero_Fiche TEXT UNIQUE,
            Intitule TEXT,
            Abrege_Libelle TEXT,
            Abrege_Intitule TEXT,
            Nomenclature_Europe_Niveau TEXT,
            Nomenclature_Europe_Intitule TEXT,
            Accessible_Nouvelle_Caledonie TEXT,
            Accessible_Polynesie_Francaise TEXT,
            Date_dernier_jo TEXT,
            Date_Decision TEXT,
            Date_Fin_Enregistrement TEXT,
            Date_Effet TEXT,
            Type_Enregistrement TEXT,
            Validation_Partielle TEXT,
            Actif TEXT
        )
    """)
    print("Fiches table created.")

    # Ensure index on Id_Fiche
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_fiches_id_fiche ON Fiches(Id_Fiche)
    """)
    print("Index on Id_Fiche created.")

    # Create Certificateurs table if it does not exist
    print("Creating Certificateurs table...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Certificateurs (
            Numero_Fiche TEXT,
            Siret_Certificateur TEXT,
            Nom_Certificateur TEXT,
            PRIMARY KEY (Numero_Fiche, Siret_Certificateur),
            FOREIGN KEY (Numero_Fiche) REFERENCES Fiches(Numero_Fiche) ON DELETE CASCADE
        )
    """)
    print("Certificateurs table created.")

    # Ensure index on Numero_Fiche
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_certificateurs_numero_fiche ON Certificateurs(Numero_Fiche)
    """)
    print("Index on Numero_Fiche created.")

    # Create Partenaires table if it does not exist
    print("Creating Partenaires table...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Partenaires (
            id SERIAL PRIMARY KEY,
            Numero_Fiche TEXT,
            Nom_Partenaire TEXT,
            Siret_Partenaire TEXT,
            Habilitation_Partenaire TEXT,
            FOREIGN KEY (Numero_Fiche) REFERENCES Fiches(Numero_Fiche) ON DELETE CASCADE
        )
    """)
    print("Partenaires table created.")

    # Ensure index on Numero_Fiche
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_partenaires_numero_fiche ON Partenaires(Numero_Fiche)
    """)
    print("Index on Numero_Fiche created.")

    # Create Bloc_Competences table if it does not exist
    print("Creating Blocs_de_Competences table...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Bloc_Competences (
            id SERIAL PRIMARY KEY,
            Numero_Fiche TEXT,
            Bloc_Competences_Code TEXT,
            Bloc_Competences_Libelle TEXT,
            FOREIGN KEY (Numero_Fiche) REFERENCES Fiches(Numero_Fiche) ON DELETE CASCADE
        )
    """)
    print("Bloc_Competences table created.")

    # Ensure index on Numero_Fiche
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_bloc_competences_numero_fiche ON Bloc_Competences(Numero_Fiche)
    """)
    print("Index on Numero_Fiche created.")

    conn.commit()
    cur.close()
    conn.close()

def row_size(row):
    return sum(len(str(value).encode('utf-8')) for value in row)

def sync_fiches(df):
    start_time = time.time()
    conn = psycopg2.connect(
        dbname="francecompetences",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("HOST"),
        port="5432"
    )
    cur = conn.cursor()

    # Ensure DataFrame columns match the SQL table columns
    expected_columns = [
        "Id_Fiche", "Numero_Fiche", "Intitule", "Abrege_Libelle", "Abrege_Intitule",
        "Nomenclature_Europe_Niveau", "Nomenclature_Europe_Intitule",
        "Accessible_Nouvelle_Caledonie", "Accessible_Polynesie_Francaise",
        "Date_dernier_jo", "Date_Decision", "Date_Fin_Enregistrement",
        "Date_Effet", "Type_Enregistrement", "Validation_Partielle", "Actif"
    ]
    df = df.reindex(columns=expected_columns)

    # Create a temporary table
    cur.execute("DROP TABLE IF EXISTS staging_fiches")
    cur.execute("""
        CREATE TEMP TABLE staging_fiches (
            Id_Fiche TEXT,
            Numero_Fiche TEXT,
            Intitule TEXT,
            Abrege_Libelle TEXT,
            Abrege_Intitule TEXT,
            Nomenclature_Europe_Niveau TEXT,
            Nomenclature_Europe_Intitule TEXT,
            Accessible_Nouvelle_Caledonie TEXT,
            Accessible_Polynesie_Francaise TEXT,
            Date_dernier_jo TEXT,
            Date_Decision TEXT,
            Date_Fin_Enregistrement TEXT,
            Date_Effet TEXT,
            Type_Enregistrement TEXT,
            Validation_Partielle TEXT,
            Actif TEXT
        )
    """)

    # Upload DataFrame to the temporary table
    print("Uploading data to staging_fiches table...")
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO staging_fiches (Id_Fiche, Numero_Fiche, Intitule, Abrege_Libelle, Abrege_Intitule, 
                                        Nomenclature_Europe_Niveau, Nomenclature_Europe_Intitule, 
                                        Accessible_Nouvelle_Caledonie, Accessible_Polynesie_Francaise, 
                                        Date_dernier_jo, Date_Decision, Date_Fin_Enregistrement, 
                                        Date_Effet, Type_Enregistrement, Validation_Partielle, Actif)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, tuple(row))
    print("Uploaded data to staging_fiches table.")

    # Delete rows that are not in the staging table
    print("Deleting rows from Fiches table that are not in staging_fiches...")
    cur.execute("""
        DELETE FROM Certificateurs WHERE Numero_Fiche IN (
            SELECT Numero_Fiche FROM Fiches WHERE Id_Fiche NOT IN (SELECT Id_Fiche FROM staging_fiches)
        )
    """)
    cur.execute("""
        DELETE FROM Partenaires WHERE Numero_Fiche IN (
            SELECT Numero_Fiche FROM Fiches WHERE Id_Fiche NOT IN (SELECT Id_Fiche FROM staging_fiches)
        )
    """)
    cur.execute("""
        DELETE FROM Fiches WHERE Id_Fiche NOT IN (SELECT Id_Fiche FROM staging_fiches)
    """)
    print("Deleted rows from Fiches table that are not in staging_fiches.")

    # Insert new rows
    print("Inserting new rows into Fiches table...")
    cur.execute("""
        INSERT INTO Fiches (Id_Fiche, Numero_Fiche, Intitule, Abrege_Libelle, Abrege_Intitule, 
                            Nomenclature_Europe_Niveau, Nomenclature_Europe_Intitule, 
                            Accessible_Nouvelle_Caledonie, Accessible_Polynesie_Francaise, 
                            Date_dernier_jo, Date_Decision, Date_Fin_Enregistrement, 
                            Date_Effet, Type_Enregistrement, Validation_Partielle, Actif)
        SELECT s.Id_Fiche, s.Numero_Fiche, s.Intitule, s.Abrege_Libelle, s.Abrege_Intitule, 
               s.Nomenclature_Europe_Niveau, s.Nomenclature_Europe_Intitule, 
               s.Accessible_Nouvelle_Caledonie, s.Accessible_Polynesie_Francaise, 
               s.Date_dernier_jo, s.Date_Decision, s.Date_Fin_Enregistrement, 
               s.Date_Effet, s.Type_Enregistrement, s.Validation_Partielle, s.Actif
        FROM staging_fiches s
        LEFT JOIN Fiches f ON s.Id_Fiche = f.Id_Fiche
        WHERE f.Id_Fiche IS NULL
    """)
    print("Inserted new rows into Fiches table.")

    # Update existing rows
    print("Updating existing rows in Fiches table...")
    cur.execute("""
        UPDATE Fiches SET
            Numero_Fiche = s.Numero_Fiche,
            Intitule = s.Intitule,
            Abrege_Libelle = s.Abrege_Libelle,
            Abrege_Intitule = s.Abrege_Intitule,
            Nomenclature_Europe_Niveau = s.Nomenclature_Europe_Niveau,
            Nomenclature_Europe_Intitule = s.Nomenclature_Europe_Intitule,
            Accessible_Nouvelle_Caledonie = s.Accessible_Nouvelle_Caledonie,
            Accessible_Polynesie_Francaise = s.Accessible_Polynesie_Francaise,
            Date_dernier_jo = s.Date_dernier_jo,
            Date_Decision = s.Date_Decision,
            Date_Fin_Enregistrement = s.Date_Fin_Enregistrement,
            Date_Effet = s.Date_Effet,
            Type_Enregistrement = s.Type_Enregistrement,
            Validation_Partielle = s.Validation_Partielle,
            Actif = s.Actif
        FROM staging_fiches s
        WHERE Fiches.Id_Fiche = s.Id_Fiche
    """)
    print("Updated existing rows in Fiches table.")

    conn.commit()
    cur.close()
    conn.close()
    print(f"sync_fiches took {time.time() - start_time} seconds")

def sync_certificateurs(df):
    start_time = time.time()
    conn = psycopg2.connect(
        dbname="francecompetences",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("HOST"),
        port="5432"
    )
    cur = conn.cursor()

    # Ensure DataFrame columns match the SQL table columns
    expected_columns = ["Numero_Fiche", "Siret_Certificateur", "Nom_Certificateur"]
    df = df.reindex(columns=expected_columns)

    # Create a temporary table
    cur.execute("DROP TABLE IF EXISTS staging_certificateurs")
    cur.execute("""
        CREATE TEMP TABLE staging_certificateurs (
            Numero_Fiche TEXT,
            Siret_Certificateur TEXT,
            Nom_Certificateur TEXT
        )
    """)

    # Upload DataFrame to the temporary table
    print("Uploading data to staging_certificateurs table...")
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO staging_certificateurs (Numero_Fiche, Siret_Certificateur, Nom_Certificateur)
            VALUES (%s, %s, %s)
        """, tuple(row))
    print("Uploaded data to staging_certificateurs table.")

    # Delete rows that are not in the staging table
    print("Deleting rows from Certificateurs table that are not in staging_certificateurs...")
    cur.execute("""
        DELETE FROM Certificateurs WHERE (Numero_Fiche, Siret_Certificateur) NOT IN (
            SELECT Numero_Fiche, Siret_Certificateur FROM staging_certificateurs
        )
    """)
    print("Deleted rows from Certificateurs table that are not in staging_certificateurs.")

    # Insert new rows
    print("Inserting new rows into Certificateurs table...")
    cur.execute("""
        INSERT INTO Certificateurs (Numero_Fiche, Siret_Certificateur, Nom_Certificateur)
        SELECT s.Numero_Fiche, s.Siret_Certificateur, s.Nom_Certificateur
        FROM staging_certificateurs s
        LEFT JOIN Certificateurs c ON s.Numero_Fiche = c.Numero_Fiche AND s.Siret_Certificateur = c.Siret_Certificateur
        WHERE c.Numero_Fiche IS NULL AND c.Siret_Certificateur IS NULL
    """)
    print("Inserted new rows into Certificateurs table.")

    # Update existing rows
    print("Updating existing rows in Certificateurs table...")
    cur.execute("""
        UPDATE Certificateurs SET
            Nom_Certificateur = s.Nom_Certificateur
        FROM staging_certificateurs s
        WHERE Certificateurs.Numero_Fiche = s.Numero_Fiche AND Certificateurs.Siret_Certificateur = s.Siret_Certificateur
    """)
    print("Updated existing rows in Certificateurs table.")

    conn.commit()
    cur.close()
    conn.close()
    print(f"sync_certificateurs took {time.time() - start_time} seconds")

def sync_partenaires(df):
    start_time = time.time()
    conn = psycopg2.connect(
        dbname="francecompetences",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("HOST"),
        port="5432"
    )
    cur = conn.cursor()

    # Ensure DataFrame columns match the SQL table columns
    expected_columns = ["Numero_Fiche", "Nom_Partenaire", "Siret_Partenaire", "Habilitation_Partenaire"]
    df = df.reindex(columns=expected_columns)

    # Handle empty Siret_Partenaire values and strip whitespace
    df["Siret_Partenaire"] = df["Siret_Partenaire"].fillna("UNKNOWN").str.strip()

    # Create a temporary table
    cur.execute("DROP TABLE IF EXISTS staging_partenaires")
    cur.execute("""
        CREATE TEMP TABLE staging_partenaires (
            Numero_Fiche TEXT,
            Nom_Partenaire TEXT,
            Siret_Partenaire TEXT,
            Habilitation_Partenaire TEXT
        )
    """)

    # Upload DataFrame to the temporary table
    print("Uploading data to staging_partenaires table...")
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO staging_partenaires (Numero_Fiche, Nom_Partenaire, Siret_Partenaire, Habilitation_Partenaire)
            VALUES (%s, %s, %s, %s)
        """, tuple(row))
    print("Uploaded data to staging_partenaires table.")

    # Delete rows that are not in the staging table
    print("Deleting rows from Partenaires table that are not in staging_partenaires...")
    cur.execute("""
        DELETE FROM Partenaires WHERE id NOT IN (
            SELECT id FROM staging_partenaires
        )
    """)
    print("Deleted rows from Partenaires table that are not in staging_partenaires.")

    # Insert new rows
    print("Inserting new rows into Partenaires table...")
    cur.execute("""
        INSERT INTO Partenaires (Numero_Fiche, Nom_Partenaire, Siret_Partenaire, Habilitation_Partenaire)
        SELECT s.Numero_Fiche, s.Nom_Partenaire, s.Siret_Partenaire, s.Habilitation_Partenaire
        FROM staging_partenaires s
        LEFT JOIN Partenaires p ON s.Numero_Fiche = p.Numero_Fiche AND s.Siret_Partenaire = p.Siret_Partenaire AND s.Nom_Partenaire = p.Nom_Partenaire
        WHERE p.Numero_Fiche IS NULL AND p.Siret_Partenaire IS NULL AND p.Nom_Partenaire IS NULL
    """)
    print("Inserted new rows into Partenaires table.")

    # Update existing rows
    print("Updating existing rows in Partenaires table...")
    cur.execute("""
        UPDATE Partenaires SET
            Nom_Partenaire = s.Nom_Partenaire,
            Habilitation_Partenaire = s.Habilitation_Partenaire
        FROM staging_partenaires s
        WHERE Partenaires.Numero_Fiche = s.Numero_Fiche AND Partenaires.Siret_Partenaire = s.Siret_Partenaire AND Partenaires.Nom_Partenaire = s.Nom_Partenaire
    """)
    print("Updated existing rows in Partenaires table.")

    conn.commit()
    cur.close()
    conn.close()
    print(f"sync_partenaires took {time.time() - start_time} seconds")

def sync_bloc_competences(df):
    start_time = time.time()
    conn = psycopg2.connect(
        dbname="francecompetences",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("HOST"),
        port="5432"
    )
    cur = conn.cursor()

    # Ensure DataFrame columns match the SQL table columns
    expected_columns = ["Numero_Fiche", "Bloc_Competences_Code", "Bloc_Competences_Libelle"]
    df = df.reindex(columns=expected_columns)

    # Create a temporary table
    cur.execute("DROP TABLE IF EXISTS staging_bloc_competences")
    cur.execute("""
        CREATE TEMP TABLE staging_bloc_competences (
            Numero_Fiche TEXT,
            Bloc_Competences_Code TEXT,
            Bloc_Competences_Libelle TEXT
        )
    """)

    # Upload DataFrame to the temporary table
    print("Uploading data to staging_bloc_competences table...")
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO staging_bloc_competences (Numero_Fiche, Bloc_Competences_Code, Bloc_Competences_Libelle)
            VALUES (%s, %s, %s)
        """, tuple(row))
    print("Uploaded data to staging_bloc_competences table.")

    # Delete rows that are not in the staging table
    print("Deleting rows from Bloc_Competences table that are not in staging_bloc_competences...")
    cur.execute("""
        DELETE FROM Bloc_Competences WHERE id NOT IN (
            SELECT id FROM staging_bloc_competences
        )
    """)
    print("Deleted rows from Bloc_Competences table that are not in staging_bloc_competences.")

    # Insert new rows
    print("Inserting new rows into Bloc_Competences table...")
    cur.execute("""
        INSERT INTO Bloc_Competences (Numero_Fiche, Bloc_Competences_Code, Bloc_Competences_Libelle)
        SELECT s.Numero_Fiche, s.Bloc_Competences_Code, s.Bloc_Competences_Libelle
        FROM staging_bloc_competences s
        LEFT JOIN Bloc_Competences b ON s.Numero_Fiche = b.Numero_Fiche AND s.Bloc_Competences_Code = b.Bloc_Competences_Code
        WHERE b.Numero_Fiche IS NULL AND b.Bloc_Competences_Code IS NULL
    """)
    print("Inserted new rows into Bloc_Competences table.")

    # Update existing rows
    print("Updating existing rows in Bloc_Competences table...")
    cur.execute("""
        UPDATE Bloc_Competences SET
            Bloc_Competences_Libelle = s.Bloc_Competences_Libelle
        FROM staging_bloc_competences s
        WHERE Bloc_Competences.Numero_Fiche = s.Numero_Fiche AND Bloc_Competences.Bloc_Competences_Code = s.Bloc_Competences_Code
    """)
    print("Updated existing rows in Bloc_Competences table.")

    conn.commit()
    cur.close()
    conn.close()
    print(f"sync_bloc_competences took {time.time() - start_time} seconds")

def process_csv(file_path):
    start_time = time.time()
    df = pd.read_csv(file_path, delimiter=";", dtype=str)
    if "Certificateurs" in file_path:
        sync_certificateurs(df)
    elif "Standard" in file_path:
        sync_fiches(df)
    elif "Partenaires" in file_path:
        sync_partenaires(df)
    elif "Blocs" in file_path:
        sync_bloc_competences(df)
    print(f"process_csv for {file_path} took {time.time() - start_time} seconds")

def download_and_unzip(url, title):
    start_time = time.time()
    response = requests.get(url)
    if response.status_code == 200:
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            for file_info in z.infolist():
                if "Standard" in file_info.filename or "Certificateurs" in file_info.filename or "Partenaires" in file_info.filename or "Blocs" in file_info.filename:
                    file_info.filename = f"{os.path.splitext(title)[0]}_{file_info.filename}"
                    z.extract(file_info, "downloads")
                    print(f"Extracted: {file_info.filename}")
                    process_csv(os.path.join("downloads", file_info.filename))
        print(f"Downloaded and unzipped: {title}")
    else:
        print(f"Failed to download {title}: {response.status_code}")
    print(f"download_and_unzip took {time.time() - start_time} seconds")

def fetch_and_process_links():
    start_time = time.time()
    API_URL = "https://www.data.gouv.fr/api/2/datasets/5eebbc067a14b6fecc9c9976/resources/?page=1"
    response = requests.get(API_URL)
    if response.status_code != 200:
        print("Failed to fetch data from API:", response.status_code)
        return
    
    data = response.json().get("data", [])
    if not data:
        print("No data found in the API response.")
        return
    
    today_date = datetime.now().strftime("%Y-%m-%d")
    target_title = "export-fiches-csv"
    
    for item in data:
        title = item.get("title", "")
        url = item.get("url", "")
        
        if today_date in title and target_title in title:
            print(f"Title: {title}")
            print(f"Link: {url}\n")
            download_and_unzip(url, title)
            break
    print(f"fetch_and_process_links took {time.time() - start_time} seconds")

if __name__ == "__main__":
    create_tables()
    fetch_and_process_links()
