#!/usr/bin/env python3
"""
Enrich existing database with detailed XML data
This script will:
1. Keep existing CSV data as base
2. Add detailed XML data to existing records
3. Not modify the core structure
"""

import os
import psycopg2
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def add_detailed_columns():
    """Add detailed columns to existing Fiches table"""
    conn = psycopg2.connect(
        dbname="francecompetences",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("HOST"),
        port="5432"
    )
    cur = conn.cursor()
    
    try:
        # Add detailed columns if they don't exist
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
                cur.execute(f"ALTER TABLE Fiches ADD COLUMN {column}")
                print(f"Added column: {column}")
            except psycopg2.ProgrammingError:
                # Column already exists, skip
                print(f"Column already exists: {column}")
        
        conn.commit()
        print("Database schema updated successfully")
        
    except Exception as e:
        print(f"Error updating schema: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def enrich_with_xml_data(xml_path):
    """Enrich existing records with XML data"""
    conn = psycopg2.connect(
        dbname="francecompetences",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("HOST"),
        port="5432"
    )
    cur = conn.cursor()
    
    try:
        print(f"Parsing XML file: {xml_path}")
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        fiches = root.findall('FICHE')
        print(f"Total fiches in XML: {len(fiches)}")
        
        updated_count = 0
        not_found_count = 0
        
        for i, fiche in enumerate(fiches):
            if i % 1000 == 0:
                print(f"Processed {i}/{len(fiches)} fiches...")
            
            # Get basic info
            numero_fiche = fiche.find('NUMERO_FICHE').text if fiche.find('NUMERO_FICHE') is not None else None
            
            if not numero_fiche:
                continue
            
            # Check if this fiche exists in our database
            cur.execute("SELECT COUNT(*) FROM Fiches WHERE Numero_Fiche = %s", (numero_fiche,))
            exists = cur.fetchone()[0] > 0
            
            if not exists:
                not_found_count += 1
                continue
            
            # Extract detailed information
            activites_visees = fiche.find('ACTIVITES_VISEES').text if fiche.find('ACTIVITES_VISEES') is not None else None
            capacites_attestees = fiche.find('CAPACITES_ATTESTEES').text if fiche.find('CAPACITES_ATTESTEES') is not None else None
            secteurs_activite = fiche.find('SECTEURS_ACTIVITE').text if fiche.find('SECTEURS_ACTIVITE') is not None else None
            type_emploi_accessibles = fiche.find('TYPE_EMPLOI_ACCESSIBLES').text if fiche.find('TYPE_EMPLOI_ACCESSIBLES') is not None else None
            reglementations_activites = fiche.find('REGLEMENTATIONS_ACTIVITES').text if fiche.find('REGLEMENTATIONS_ACTIVITES') is not None else None
            objectifs_contexte = fiche.find('OBJECTIFS_CONTEXTE').text if fiche.find('OBJECTIFS_CONTEXTE') is not None else None
            prerequis_entree_formation = fiche.find('PREREQUIS_ENTREE_FORMATION').text if fiche.find('PREREQUIS_ENTREE_FORMATION') is not None else None
            
            # Update the record
            cur.execute("""
                UPDATE Fiches SET
                    activites_visees = %s,
                    capacites_attestees = %s,
                    secteurs_activite = %s,
                    type_emploi_accessibles = %s,
                    reglementations_activites = %s,
                    objectifs_contexte = %s,
                    prerequis_entree_formation = %s
                WHERE Numero_Fiche = %s
            """, (activites_visees, capacites_attestees, secteurs_activite, type_emploi_accessibles, 
                  reglementations_activites, objectifs_contexte, prerequis_entree_formation, numero_fiche))
            
            updated_count += 1
        
        conn.commit()
        print(f"\nEnrichment completed:")
        print(f"  - Updated records: {updated_count}")
        print(f"  - Records not found in DB: {not_found_count}")
        
    except Exception as e:
        print(f"Error enriching data: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def main():
    print("=== Enriching Existing Data with XML Details ===")
    
    # Step 1: Add detailed columns to database
    print("\n1. Adding detailed columns to database...")
    add_detailed_columns()
    
    # Step 2: Check if XML file exists
    xml_path = "downloads/export_fiches_RNCP_V4_1_2025-09-25.xml"
    if not os.path.exists(xml_path):
        print(f"\nXML file not found: {xml_path}")
        print("Please run test_xml_parsing.py first to download the XML file")
        return
    
    # Step 3: Enrich existing data
    print(f"\n2. Enriching existing data with XML details...")
    enrich_with_xml_data(xml_path)
    
    print("\n=== Enrichment completed ===")

if __name__ == "__main__":
    main()







