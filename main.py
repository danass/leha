import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from datetime import datetime
import requests
from io import BytesIO
import zipfile
import time
from urllib.parse import urlparse

# Load environment variables from .env file
load_dotenv()

def get_db_connection():
    """Get database connection using DATABASE_URL_FRANCECOMPETENCES or fallback to DB_USER/DB_PASSWORD/HOST"""
    db_url = os.getenv("DATABASE_URL_FRANCECOMPETENCES")
    
    if db_url:
        # Parse DATABASE_URL (format: postgresql://user:password@host:port/database)
        parsed = urlparse(db_url)
        return psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            user=parsed.username,
            password=parsed.password,
            database=parsed.path.lstrip('/').split('?')[0]  # Remove query params
        )
    
    # Fallback to old method
    return psycopg2.connect(
        dbname="francecompetences",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("HOST"),
        port="5432"
    )

def create_tables():
    conn = get_db_connection()
    cur = conn.cursor()

    # Create Fiches table if it does not exist
    print("Creating Fiches table...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS "Fiches" (
            "Id_Fiche" TEXT PRIMARY KEY,
            "Numero_Fiche" TEXT UNIQUE,
            "Intitule" TEXT,
            "Abrege_Libelle" TEXT,
            "Abrege_Intitule" TEXT,
            "Nomenclature_Europe_Niveau" TEXT,
            "Nomenclature_Europe_Intitule" TEXT,
            "Accessible_Nouvelle_Caledonie" TEXT,
            "Accessible_Polynesie_Francaise" TEXT,
            "Date_dernier_jo" TEXT,
            "Date_Decision" TEXT,
            "Date_Fin_Enregistrement" TEXT,
            "Date_Effet" TEXT,
            "Type_Enregistrement" TEXT,
            "Validation_Partielle" TEXT,
            "Actif" TEXT,
            -- New detailed fields from XML
            activites_visees TEXT,
            capacites_attestees TEXT,
            secteurs_activite TEXT,
            type_emploi_accessibles TEXT,
            reglementations_activites TEXT,
            objectifs_contexte TEXT,
            prerequis_entree_formation TEXT
        )
    """)
    conn.commit()  # Commit table creation
    print("Fiches table created.")

    # Add new detailed columns if they don't exist
    print("Adding detailed columns to Fiches table...")
    detailed_columns = [
        "activites_visees TEXT",
        "capacites_attestees TEXT", 
        "secteurs_activite TEXT",
        "type_emploi_accessibles TEXT",
        "reglementations_activites TEXT",
        "objectifs_contexte TEXT",
        "prerequis_entree_formation TEXT"
    ]
    
    for column in detailed_columns:
        try:
            # Use quoted table name to preserve case
            cur.execute(f'ALTER TABLE "Fiches" ADD COLUMN {column}')
            conn.commit()  # Commit after each successful column addition
            print(f"Added column: {column}")
        except psycopg2.ProgrammingError as e:
            # Column already exists, skip
            conn.rollback()  # Rollback on error
            if 'already exists' in str(e) or 'duplicate' in str(e).lower():
                print(f"Column already exists: {column.split()[0]}")
            else:
                raise
        except Exception as e:
            # Other errors - rollback and log but continue
            conn.rollback()
            if 'does not exist' in str(e) or 'relation' in str(e).lower():
                print(f"Table or column issue: {e}")
                break  # Stop trying if table doesn't exist
            print(f"Warning adding column {column.split()[0]}: {e}")

    # Ensure index on Id_Fiche
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_fiches_id_fiche ON "Fiches"("Id_Fiche")
    """)
    conn.commit()
    print("Index on Id_Fiche created.")

    # Create Certificateurs table if it does not exist
    print("Creating Certificateurs table...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS "Certificateurs" (
            "Numero_Fiche" TEXT,
            "Siret_Certificateur" TEXT,
            "Nom_Certificateur" TEXT,
            PRIMARY KEY ("Numero_Fiche", "Siret_Certificateur"),
            FOREIGN KEY ("Numero_Fiche") REFERENCES "Fiches"("Numero_Fiche") ON DELETE CASCADE
        )
    """)
    conn.commit()
    print("Certificateurs table created.")

    # Ensure index on Numero_Fiche
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_certificateurs_numero_fiche ON "Certificateurs"("Numero_Fiche")
    """)
    conn.commit()
    print("Index on Numero_Fiche created.")

    # Create Partenaires table if it does not exist
    print("Creating Partenaires table...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS "Partenaires" (
            id SERIAL PRIMARY KEY,
            "Numero_Fiche" TEXT,
            "Nom_Partenaire" TEXT,
            "Siret_Partenaire" TEXT,
            "Habilitation_Partenaire" TEXT,
            FOREIGN KEY ("Numero_Fiche") REFERENCES "Fiches"("Numero_Fiche") ON DELETE CASCADE
        )
    """)
    conn.commit()
    print("Partenaires table created.")

    # Ensure index on Numero_Fiche
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_partenaires_numero_fiche ON "Partenaires"("Numero_Fiche")
    """)
    conn.commit()
    print("Index on Numero_Fiche created.")

    # Create Bloc_Competences table if it does not exist
    print("Creating Blocs_de_Competences table...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS "Bloc_Competences" (
            id SERIAL PRIMARY KEY,
            "Numero_Fiche" TEXT,
            "Bloc_Competences_Code" TEXT,
            "Bloc_Competences_Libelle" TEXT,
            FOREIGN KEY ("Numero_Fiche") REFERENCES "Fiches"("Numero_Fiche") ON DELETE CASCADE
        )
    """)
    conn.commit()
    print("Bloc_Competences table created.")

    # Ensure index on Numero_Fiche
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_bloc_competences_numero_fiche ON "Bloc_Competences"("Numero_Fiche")
    """)
    conn.commit()
    print("Index on Numero_Fiche created.")

    conn.commit()
    cur.close()
    conn.close()

def row_size(row):
    return sum(len(str(value).encode('utf-8')) for value in row)

def sync_fiches(df):
    start_time = time.time()
    conn = get_db_connection()
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
    conn = get_db_connection()
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
    conn = get_db_connection()
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
    conn = get_db_connection()
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

def process_xml(file_path):
    start_time = time.time()
    import xml.etree.ElementTree as ET
    
    print(f"  📖 Parsing XML file...")
    tree = ET.parse(file_path)
    root = tree.getroot()
    
    # Process each FICHE in the XML
    fiches = root.findall('FICHE')
    total_fiches = len(fiches)
    print(f"  📋 Found {total_fiches} fiches to process")
    
    processed = 0
    conn = None
    consecutive_errors = 0
    max_consecutive_errors = 5
    
    for idx, fiche in enumerate(fiches, 1):
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                # Reuse connection, reconnect if needed
                if conn is None or conn.closed:
                    conn = get_db_connection()
                    consecutive_errors = 0
                
                process_fiche_xml(fiche, conn)
                processed += 1
                consecutive_errors = 0
                break  # Success, exit retry loop
                
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                retry_count += 1
                consecutive_errors += 1
                print(f"  ⚠️  Connection error (attempt {retry_count}/{max_retries}): {e}")
                
                # Close broken connection
                if conn and not conn.closed:
                    try:
                        conn.close()
                    except:
                        pass
                conn = None
                
                if retry_count < max_retries:
                    time.sleep(2 ** retry_count)  # Exponential backoff
                else:
                    print(f"  ❌ Failed to process fiche after {max_retries} attempts, skipping...")
                    break
                    
            except Exception as e:
                print(f"  ❌ Unexpected error: {e}")
                break  # Don't retry on other errors
        
        if consecutive_errors >= max_consecutive_errors:
            print(f"  ❌ Too many consecutive errors ({consecutive_errors}), stopping...")
            break
            
        if idx % 100 == 0 or idx == total_fiches:
            print(f"  ⏳ Progress: {processed}/{total_fiches} fiches processed ({processed*100/total_fiches:.1f}%)")
    
    # Close connection if still open
    if conn and not conn.closed:
        try:
            conn.close()
        except:
            pass
    
    print(f"  ✅ Processed {processed}/{total_fiches} fiches in {time.time() - start_time:.2f} seconds")

def process_fiche_xml(fiche, conn=None):
    """Process a single FICHE element from XML and store detailed data"""
    # Reuse connection if provided, otherwise create new one
    should_close = False
    if conn is None:
        conn = get_db_connection()
        should_close = True
    
    cur = conn.cursor()
    
    try:
        # Extract basic information
        numero_fiche = fiche.find('NUMERO_FICHE').text if fiche.find('NUMERO_FICHE') is not None else None
        intitule = fiche.find('INTITULE').text if fiche.find('INTITULE') is not None else None
        
        if not numero_fiche:
            return
        
        # Extract detailed information
        activites_visees = fiche.find('ACTIVITES_VISEES').text if fiche.find('ACTIVITES_VISEES') is not None else None
        capacites_attestees = fiche.find('CAPACITES_ATTESTEES').text if fiche.find('CAPACITES_ATTESTEES') is not None else None
        secteurs_activite = fiche.find('SECTEURS_ACTIVITE').text if fiche.find('SECTEURS_ACTIVITE') is not None else None
        type_emploi_accessibles = fiche.find('TYPE_EMPLOI_ACCESSIBLES').text if fiche.find('TYPE_EMPLOI_ACCESSIBLES') is not None else None
        reglementations_activites = fiche.find('REGLEMENTATIONS_ACTIVITES').text if fiche.find('REGLEMENTATIONS_ACTIVITES') is not None else None
        objectifs_contexte = fiche.find('OBJECTIFS_CONTEXTE').text if fiche.find('OBJECTIFS_CONTEXTE') is not None else None
        prerequis_entree_formation = fiche.find('PREREQUIS_ENTREE_FORMATION').text if fiche.find('PREREQUIS_ENTREE_FORMATION') is not None else None
        
        # Update the existing fiche with detailed information
        cur.execute("""
            UPDATE "Fiches" SET
                activites_visees = %s,
                capacites_attestees = %s,
                secteurs_activite = %s,
                type_emploi_accessibles = %s,
                reglementations_activites = %s,
                objectifs_contexte = %s,
                prerequis_entree_formation = %s
            WHERE "Numero_Fiche" = %s
        """, (activites_visees, capacites_attestees, secteurs_activite, type_emploi_accessibles, 
              reglementations_activites, objectifs_contexte, prerequis_entree_formation, numero_fiche))
        
        conn.commit()
        print(f"Updated detailed info for {numero_fiche}")
        
    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
        print(f"Error processing fiche {numero_fiche}: {e}")
        if should_close and conn:
            try:
                conn.rollback()
            except:
                pass
            try:
                conn.close()
            except:
                pass
        # Re-raise to let caller handle reconnection
        raise
    except Exception as e:
        print(f"Error processing fiche {numero_fiche}: {e}")
        try:
            conn.rollback()
        except:
            pass
    finally:
        cur.close()
        if should_close and conn:
            try:
                conn.close()
            except:
                pass

def am(url, title):
    start_time = time.time()
    print(f"📥 Downloading {title}...")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        # Download to memory
        content = BytesIO()
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                content.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    percent = (downloaded / total_size) * 100
                    print(f"\r📥 Downloaded: {percent:.1f}% ({downloaded}/{total_size} bytes)", end='', flush=True)
        print()  # New line after progress
        
        content.seek(0)
        print(f"📦 Extracting files...")
        with zipfile.ZipFile(content) as z:
            # Extract all files first
            files_to_process = []
            file_list = z.infolist()
            total_files = len([f for f in file_list if f.filename.endswith('.xml')])
            processed_files = 0
            
            for file_info in file_list:
                if file_info.filename.endswith('.xml'):
                    file_info.filename = f"{os.path.splitext(title)[0]}_{file_info.filename}"
                    z.extract(file_info, "downloads")
                    processed_files += 1
                    print(f"📄 Extracted {processed_files}/{total_files}: {os.path.basename(file_info.filename)}")
                    files_to_process.append(file_info.filename)
            
            # Process XML files with progress
            print(f"\n🔄 Processing {len(files_to_process)} XML files...")
            for idx, filename in enumerate(files_to_process, 1):
                print(f"\n[{idx}/{len(files_to_process)}] Processing: {os.path.basename(filename)}")
                process_xml(os.path.join("downloads", filename))
                print(f"✅ Completed: {os.path.basename(filename)}")
        print(f"\n✅ Downloaded and processed: {title}")
    else:
        print(f"❌ Failed to download {title}: {response.status_code}")
    print(f"⏱️  Total time: {time.time() - start_time:.2f} seconds")

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
    target_title = "export-fiches-rncp-v4-1"  # Changed to XML files for detailed data
    
    for item in data:
        title = item.get("title", "")
        url = item.get("url", "")
        
        if today_date in title and target_title in title:
            print(f"Title: {title}")
            print(f"Link: {url}\n")
            am(url, title)
            break
    print(f"fetch_and_process_links took {time.time() - start_time} seconds")

if __name__ == "__main__":
    create_tables()
    fetch_and_process_links()
