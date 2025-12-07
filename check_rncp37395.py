#!/usr/bin/env python3
"""
Check specific RNCP37395 data from the XML file
"""

import xml.etree.ElementTree as ET
import os

def find_rncp37395():
    """Find RNCP37395 in the XML file"""
    xml_path = "downloads/export_fiches_RNCP_V4_1_2025-09-25.xml"
    
    if not os.path.exists(xml_path):
        print(f"XML file not found: {xml_path}")
        return
    
    print("Parsing XML file...")
    tree = ET.parse(xml_path)
    root = tree.getroot()
    
    fiches = root.findall('FICHE')
    print(f"Total fiches: {len(fiches)}")
    
    # Look for RNCP37395
    for fiche in fiches:
        numero_fiche = fiche.find('NUMERO_FICHE').text if fiche.find('NUMERO_FICHE') is not None else None
        
        if numero_fiche == "RNCP37395":
            print(f"\n=== FOUND RNCP37395 ===")
            
            # Basic info
            intitule = fiche.find('INTITULE').text if fiche.find('INTITULE') is not None else None
            print(f"Intitulé: {intitule}")
            
            # Detailed info
            activites_visees = fiche.find('ACTIVITES_VISEES').text if fiche.find('ACTIVITES_VISEES') is not None else None
            capacites_attestees = fiche.find('CAPACITES_ATTESTEES').text if fiche.find('CAPACITES_ATTESTEES') is not None else None
            secteurs_activite = fiche.find('SECTEURS_ACTIVITE').text if fiche.find('SECTEURS_ACTIVITE') is not None else None
            type_emploi_accessibles = fiche.find('TYPE_EMPLOI_ACCESSIBLES').text if fiche.find('TYPE_EMPLOI_ACCESSIBLES') is not None else None
            reglementations_activites = fiche.find('REGLEMENTATIONS_ACTIVITES').text if fiche.find('REGLEMENTATIONS_ACTIVITES') is not None else None
            objectifs_contexte = fiche.find('OBJECTIFS_CONTEXTE').text if fiche.find('OBJECTIFS_CONTEXTE') is not None else None
            prerequis_entree_formation = fiche.find('PREREQUIS_ENTREE_FORMATION').text if fiche.find('PREREQUIS_ENTREE_FORMATION') is not None else None
            
            print(f"\n--- ACTIVITÉS VISÉES ---")
            print(activites_visees)
            
            print(f"\n--- CAPACITÉS ATTESTÉES ---")
            print(capacites_attestees)
            
            print(f"\n--- SECTEURS D'ACTIVITÉ ---")
            print(secteurs_activite)
            
            print(f"\n--- TYPES D'EMPLOI ACCESSIBLES ---")
            print(type_emploi_accessibles)
            
            print(f"\n--- RÉGLEMENTATIONS D'ACTIVITÉS ---")
            print(reglementations_activites)
            
            print(f"\n--- OBJECTIFS ET CONTEXTE ---")
            print(objectifs_contexte)
            
            print(f"\n--- PRÉREQUIS D'ENTRÉE EN FORMATION ---")
            print(prerequis_entree_formation)
            
            # Check for ROME codes
            codes_rome = fiche.findall('CODES_ROME/ROME')
            if codes_rome:
                print(f"\n--- CODES ROME ---")
                for rome in codes_rome:
                    code = rome.find('CODE').text if rome.find('CODE') is not None else None
                    libelle = rome.find('LIBELLE').text if rome.find('LIBELLE') is not None else None
                    print(f"  - {code}: {libelle}")
            
            # Check for NSF codes
            codes_nsf = fiche.findall('CODES_NSF/NSF')
            if codes_nsf:
                print(f"\n--- CODES NSF ---")
                for nsf in codes_nsf:
                    code = nsf.find('CODE').text if nsf.find('CODE') is not None else None
                    libelle = nsf.find('INTITULE').text if nsf.find('INTITULE') is not None else None
                    print(f"  - {code}: {libelle}")
            
            return
    
    print("RNCP37395 not found in XML file")

if __name__ == "__main__":
    find_rncp37395()







