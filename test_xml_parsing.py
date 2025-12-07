#!/usr/bin/env python3
"""
Test script to download and parse XML data from France Compétences
without modifying the database
"""

import os
import requests
import zipfile
import xml.etree.ElementTree as ET
from io import BytesIO
from datetime import datetime

def download_latest_xml():
    """Download the latest XML export from France Compétences"""
    API_URL = "https://www.data.gouv.fr/api/2/datasets/5eebbc067a14b6fecc9c9976/resources/?page=1"
    response = requests.get(API_URL)
    
    if response.status_code != 200:
        print("Failed to fetch data from API:", response.status_code)
        return None
    
    data = response.json().get("data", [])
    if not data:
        print("No data found in the API response.")
        return None
    
    # Look for the latest XML export
    target_title = "export-fiches-rncp-v4-1"
    
    # Find all XML exports and get the most recent one
    xml_exports = []
    for item in data:
        title = item.get("title", "")
        url = item.get("url", "")
        created_at = item.get("created_at", "")
        
        if target_title in title:
            xml_exports.append({
                'title': title,
                'url': url,
                'created_at': created_at
            })
    
    if not xml_exports:
        print("No XML export found")
        return None
    
    # Sort by creation date and get the most recent
    xml_exports.sort(key=lambda x: x['created_at'], reverse=True)
    latest = xml_exports[0]
    
    print(f"Found XML export: {latest['title']}")
    print(f"URL: {latest['url']}")
    print(f"Created: {latest['created_at']}")
    return latest['url']

def download_and_extract_xml(url):
    """Download and extract XML file"""
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to download: {response.status_code}")
        return None
    
    with zipfile.ZipFile(BytesIO(response.content)) as z:
        # Find XML file
        xml_files = [f for f in z.infolist() if f.filename.endswith('.xml')]
        if not xml_files:
            print("No XML file found in zip")
            return None
        
        xml_file = xml_files[0]
        print(f"Extracting: {xml_file.filename}")
        z.extract(xml_file, "downloads")
        return f"downloads/{xml_file.filename}"

def parse_xml_sample(xml_path, sample_size=5):
    """Parse XML and show sample data"""
    print(f"\nParsing XML file: {xml_path}")
    
    tree = ET.parse(xml_path)
    root = tree.getroot()
    
    print(f"Root tag: {root.tag}")
    print(f"Version: {root.find('VERSION_FLUX').text if root.find('VERSION_FLUX') is not None else 'Unknown'}")
    
    fiches = root.findall('FICHE')
    print(f"Total fiches found: {len(fiches)}")
    
    # Process first few fiches as sample
    for i, fiche in enumerate(fiches[:sample_size]):
        print(f"\n--- FICHE {i+1} ---")
        
        # Basic info
        numero_fiche = fiche.find('NUMERO_FICHE').text if fiche.find('NUMERO_FICHE') is not None else None
        intitule = fiche.find('INTITULE').text if fiche.find('INTITULE') is not None else None
        
        print(f"Numéro: {numero_fiche}")
        print(f"Intitulé: {intitule}")
        
        # Detailed info
        activites_visees = fiche.find('ACTIVITES_VISEES').text if fiche.find('ACTIVITES_VISEES') is not None else None
        capacites_attestees = fiche.find('CAPACITES_ATTESTEES').text if fiche.find('CAPACITES_ATTESTEES') is not None else None
        secteurs_activite = fiche.find('SECTEURS_ACTIVITE').text if fiche.find('SECTEURS_ACTIVITE') is not None else None
        type_emploi_accessibles = fiche.find('TYPE_EMPLOI_ACCESSIBLES').text if fiche.find('TYPE_EMPLOI_ACCESSIBLES') is not None else None
        
        print(f"Activités visées: {activites_visees[:100] if activites_visees else 'None'}...")
        print(f"Capacités attestées: {capacites_attestees[:100] if capacites_attestees else 'None'}...")
        print(f"Secteurs d'activité: {secteurs_activite}")
        print(f"Types d'emploi accessibles: {type_emploi_accessibles[:100] if type_emploi_accessibles else 'None'}...")
        
        # Check for ROME codes
        codes_rome = fiche.findall('CODES_ROME/ROME')
        if codes_rome:
            print("Codes ROME:")
            for rome in codes_rome:
                code = rome.find('CODE').text if rome.find('CODE') is not None else None
                libelle = rome.find('LIBELLE').text if rome.find('LIBELLE') is not None else None
                print(f"  - {code}: {libelle}")

def main():
    print("=== France Compétences XML Parser Test ===")
    
    # Create downloads directory if it doesn't exist
    os.makedirs("downloads", exist_ok=True)
    
    # Download latest XML
    print("\n1. Downloading latest XML export...")
    xml_url = download_latest_xml()
    if not xml_url:
        return
    
    # Extract XML file
    print("\n2. Extracting XML file...")
    xml_path = download_and_extract_xml(xml_url)
    if not xml_path:
        return
    
    # Parse and show sample data
    print("\n3. Parsing XML and showing sample data...")
    parse_xml_sample(xml_path, sample_size=3)
    
    print("\n=== Test completed ===")

if __name__ == "__main__":
    main()
