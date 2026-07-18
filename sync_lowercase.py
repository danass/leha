#!/usr/bin/env python3
"""
Synchronise l'export XML RNCP v4-1 (France Compétences / data.gouv) vers les
tables minuscules lues par les APIs diplome : fiches, certificateurs,
partenaires, bloc_competences.

Remplace l'usage de main.py (qui écrit dans des tables PascalCase vides).

- fiches : upsert (ON CONFLICT numero_fiche) + colonnes détaillées ajoutées si absentes
- certificateurs / partenaires / bloc_competences : reconstruits dans des
  tables *_new puis bascule atomique (DROP + RENAME dans une transaction)

Usage : python3 sync_lowercase.py [chemin.xml]
Sans argument, télécharge le dernier export disponible sur data.gouv.
"""
import glob
import io
import os
import re
import sys
import time
import zipfile
import xml.etree.ElementTree as ET

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATASET_API = "https://www.data.gouv.fr/api/2/datasets/5eebbc067a14b6fecc9c9976/resources/?page=1&page_size=50"
BATCH = 500

DB_URL = os.getenv("DATABASE_URL_FRANCECOMPETENCES") or os.getenv("DATABASE_URL")
if not DB_URL:
    sys.exit("DATABASE_URL_FRANCECOMPETENCES ou DATABASE_URL requis")


def connect():
    return psycopg2.connect(DB_URL)


def txt(el, tag):
    c = el.find(tag)
    return c.text.strip() if c is not None and c.text and c.text.strip() else None


DATE_ISO = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")

# L'app attend la convention historique ACTIVE/INACTIVE (l'export XML donne Oui/Non)
ACTIF_MAP = {"Oui": "ACTIVE", "Non": "INACTIVE"}


def fr_date(v):
    """Normalise en JJ/MM/AAAA (format des données existantes)."""
    if not v:
        return None
    m = DATE_ISO.match(v)
    if m:
        return f"{m.group(3)}/{m.group(2)}/{m.group(1)}"
    return v


# Les deux répertoires publiés par France Compétences dans le même jeu de données :
#  - RNCP : titres à finalité professionnelle (avec niveau européen, blocs de compétences)
#  - RS   : Répertoire Spécifique (habilitations, certifications type TOSA…), sans niveau
EXPORTS = ("export-fiches-rncp-v4-1", "export-fiches-rs-v4-1")


def download_latest_export(prefix="export-fiches-rncp-v4-1"):
    print(f"🔎 Recherche du dernier export « {prefix} » sur data.gouv...")
    data = requests.get(DATASET_API, timeout=60).json().get("data", [])
    candidates = [r for r in data if prefix in (r.get("title") or "")]
    if not candidates:
        sys.exit(f"Aucun export {prefix} trouvé sur data.gouv")
    latest = sorted(candidates, key=lambda r: r["title"])[-1]
    print(f"📥 Téléchargement : {latest['title']}")
    resp = requests.get(latest["url"], timeout=600)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        xml_names = [n for n in z.namelist() if n.endswith(".xml")]
        os.makedirs("downloads", exist_ok=True)
        path = z.extract(xml_names[0], "downloads")
    print(f"📄 Extrait : {path}")
    return path


def ensure_detail_columns(conn):
    cols = [
        "activites_visees", "capacites_attestees", "secteurs_activite",
        "type_emploi_accessibles", "reglementations_activites",
        "objectifs_contexte", "prerequis_entree_formation",
    ]
    cur = conn.cursor()
    for c in cols:
        cur.execute(f"ALTER TABLE fiches ADD COLUMN IF NOT EXISTS {c} TEXT")
    conn.commit()
    cur.close()


def create_new_tables(conn):
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS certificateurs_new, partenaires_new, bloc_competences_new")
    cur.execute("""
        CREATE TABLE certificateurs_new (
            numero_fiche TEXT, siret_certificateur TEXT, nom_certificateur TEXT,
            site_internet TEXT,
            PRIMARY KEY (numero_fiche, siret_certificateur))
    """)
    cur.execute("""
        CREATE TABLE partenaires_new (
            id SERIAL PRIMARY KEY, numero_fiche TEXT, nom_partenaire TEXT,
            siret_partenaire TEXT, habilitation_partenaire TEXT)
    """)
    cur.execute("""
        CREATE TABLE bloc_competences_new (
            id SERIAL PRIMARY KEY, numero_fiche TEXT,
            bloc_competences_code TEXT, bloc_competences_libelle TEXT)
    """)
    conn.commit()
    cur.close()


FICHE_COLS = (
    "id_fiche, numero_fiche, intitule, abrege_libelle, abrege_intitule, "
    "nomenclature_europe_niveau, nomenclature_europe_intitule, "
    "accessible_nouvelle_caledonie, accessible_polynesie_francaise, "
    "date_dernier_jo, date_decision, date_fin_enregistrement, date_effet, "
    "type_enregistrement, validation_partielle, actif, "
    "activites_visees, capacites_attestees, secteurs_activite, "
    "type_emploi_accessibles, reglementations_activites, objectifs_contexte, "
    "prerequis_entree_formation"
)
FICHE_UPDATE = ", ".join(
    f"{c} = EXCLUDED.{c}" for c in
    [c.strip() for c in FICHE_COLS.split(",")] if c.strip() != "numero_fiche"
)


def parse_fiche(el):
    numero = txt(el, "NUMERO_FICHE")
    if not numero:
        return None
    abrege = el.find("ABREGE")
    nom_eu = el.find("NOMENCLATURE_EUROPE")
    fiche = (
        txt(el, "ID_FICHE"), numero, txt(el, "INTITULE"),
        txt(abrege, "CODE") if abrege is not None else None,
        txt(abrege, "LIBELLE") if abrege is not None else None,
        txt(nom_eu, "NIVEAU") if nom_eu is not None else None,
        txt(nom_eu, "LIBELLE") if nom_eu is not None else None,
        txt(el, "ACCESSIBLE_NOUVELLE_CALEDONIE"), txt(el, "ACCESSIBLE_POLYNESIE_FRANCAISE"),
        fr_date(txt(el, "DATE_DERNIER_JO")), fr_date(txt(el, "DATE_DECISION")),
        fr_date(txt(el, "DATE_FIN_ENREGISTREMENT")), fr_date(txt(el, "DATE_EFFET")),
        txt(el, "TYPE_ENREGISTREMENT"), txt(el, "VALIDATION_PARTIELLE"),
        ACTIF_MAP.get(txt(el, "ACTIF"), txt(el, "ACTIF")),
        txt(el, "ACTIVITES_VISEES"), txt(el, "CAPACITES_ATTESTEES"),
        txt(el, "SECTEURS_ACTIVITE"), txt(el, "TYPE_EMPLOI_ACCESSIBLES"),
        txt(el, "REGLEMENTATIONS_ACTIVITES"), txt(el, "OBJECTIFS_CONTEXTE"),
        txt(el, "PREREQUIS_ENTREE_FORMATION"),
    )
    certifs_by_key = {}
    certs_el = el.find("CERTIFICATEURS")
    if certs_el is not None:
        for c in certs_el.findall("CERTIFICATEUR"):
            key = (numero, txt(c, "SIRET_CERTIFICATEUR") or "")
            # Un même (fiche, siret) peut apparaître plusieurs fois dans la source ;
            # on garde la première valeur de site_internet non vide.
            if key not in certifs_by_key:
                certifs_by_key[key] = (numero, key[1], txt(c, "NOM_CERTIFICATEUR"), txt(c, "SITE_INTERNET"))
    certifs = list(certifs_by_key.values())
    parts = []
    parts_el = el.find("PARTENAIRES")
    if parts_el is not None:
        for p in parts_el.findall("PARTENAIRE"):
            parts.append((numero, txt(p, "NOM_PARTENAIRE"), txt(p, "SIRET_PARTENAIRE"),
                          txt(p, "HABILITATION_PARTENAIRE")))
    blocs = []
    blocs_el = el.find("BLOCS_COMPETENCES")
    if blocs_el is not None:
        for b in blocs_el.findall("BLOC_COMPETENCES"):
            blocs.append((numero, txt(b, "CODE"), txt(b, "LIBELLE")))
    return fiche, certifs, parts, blocs


def flush(conn, fiches, certifs, parts, blocs, retries=5):
    for attempt in range(1, retries + 1):
        try:
            cur = conn.cursor()
            if fiches:
                execute_values(cur, f"""
                    INSERT INTO fiches ({FICHE_COLS}) VALUES %s
                    ON CONFLICT (numero_fiche) DO UPDATE SET {FICHE_UPDATE}
                """, fiches, page_size=200)
            if certifs:
                execute_values(cur,
                    "INSERT INTO certificateurs_new VALUES %s ON CONFLICT (numero_fiche, siret_certificateur) DO UPDATE SET site_internet = COALESCE(EXCLUDED.site_internet, certificateurs_new.site_internet)",
                    certifs, page_size=500)
            if parts:
                execute_values(cur,
                    "INSERT INTO partenaires_new (numero_fiche, nom_partenaire, siret_partenaire, habilitation_partenaire) VALUES %s",
                    parts, page_size=500)
            if blocs:
                execute_values(cur,
                    "INSERT INTO bloc_competences_new (numero_fiche, bloc_competences_code, bloc_competences_libelle) VALUES %s",
                    blocs, page_size=500)
            conn.commit()
            cur.close()
            return conn
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            if attempt == retries:
                raise
            print(f"⚠️  Connexion perdue ({e}), reconnexion {attempt}/{retries}...")
            time.sleep(2 * attempt)
            try:
                conn.close()
            except Exception:
                pass
            conn = connect()
    return conn


def swap_tables(conn):
    cur = conn.cursor()
    cur.execute("BEGIN")
    cur.execute("DROP TABLE certificateurs")
    cur.execute("ALTER TABLE certificateurs_new RENAME TO certificateurs")
    cur.execute("DROP TABLE partenaires")
    cur.execute("ALTER TABLE partenaires_new RENAME TO partenaires")
    cur.execute("DROP TABLE bloc_competences")
    cur.execute("ALTER TABLE bloc_competences_new RENAME TO bloc_competences")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_certificateurs_nf ON certificateurs(numero_fiche)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_partenaires_nf ON partenaires(numero_fiche)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_partenaires_siret_digits ON partenaires ((regexp_replace(siret_partenaire, '[^0-9]', '', 'g')) text_pattern_ops)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bloc_competences_nf ON bloc_competences(numero_fiche)")
    conn.commit()
    cur.close()


def write_sync_status(conn, totals, duration_seconds):
    """Enregistre l'horodatage et les volumes de la synchro, pour affichage public
    sur la page /rncp de diplome.app (transparence sur la fraîcheur des données
    France Compétences). Table séparée, non touchée par la bascule atomique."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rncp_sync_status (
            id serial PRIMARY KEY,
            synced_at timestamptz NOT NULL DEFAULT now(),
            fiches integer,
            certificateurs integer,
            partenaires integer,
            blocs integer,
            duration_seconds integer,
            source text DEFAULT 'france-competences-datagouv'
        )
    """)
    cur.execute(
        "INSERT INTO rncp_sync_status "
        "(fiches, certificateurs, partenaires, blocs, duration_seconds) "
        "VALUES (%s, %s, %s, %s, %s)",
        (totals["fiches"], totals["certificateurs"], totals["partenaires"],
         totals["blocs"], duration_seconds),
    )
    conn.commit()
    cur.close()


def main():
    start = time.time()
    # Un argument explicite = un ou plusieurs XML locaux ; sinon on télécharge
    # les derniers exports RNCP puis RS. Les deux alimentent les mêmes tables
    # (tables *_new remplies avant UNE seule bascule atomique en fin de run).
    xml_paths = sys.argv[1:] if len(sys.argv) > 1 else [download_latest_export(p) for p in EXPORTS]

    conn = connect()
    ensure_detail_columns(conn)
    create_new_tables(conn)

    fiches, certifs, parts, blocs = [], [], [], []
    totals = {"fiches": 0, "certificateurs": 0, "partenaires": 0, "blocs": 0}

    print("🔄 Parsing + insertion...")
    for xml_path in xml_paths:
        for _, el in ET.iterparse(xml_path, events=("end",)):
            if el.tag != "FICHE":
                continue
            parsed = parse_fiche(el)
            el.clear()
            if not parsed:
                continue
            f, c, p, b = parsed
            fiches.append(f); certifs.extend(c); parts.extend(p); blocs.extend(b)
            totals["fiches"] += 1
            totals["certificateurs"] += len(c)
            totals["partenaires"] += len(p)
            totals["blocs"] += len(b)
            if len(fiches) >= BATCH:
                conn = flush(conn, fiches, certifs, parts, blocs)
                fiches, certifs, parts, blocs = [], [], [], []
                print(f"\r⬆️  {totals['fiches']} fiches | {totals['partenaires']} partenaires", end="", flush=True)
    conn = flush(conn, fiches, certifs, parts, blocs)

    print("\n🔁 Bascule atomique des tables...")
    swap_tables(conn)
    write_sync_status(conn, totals, int(time.time() - start))
    conn.close()

    print(f"✅ Synchro terminée en {time.time() - start:.0f}s : "
          f"{totals['fiches']} fiches, {totals['certificateurs']} certificateurs, "
          f"{totals['partenaires']} partenaires, {totals['blocs']} blocs")


if __name__ == "__main__":
    main()
