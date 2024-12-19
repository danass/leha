import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

def get_table_counts():
    conn = psycopg2.connect(
        dbname="francecompetences",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    
    tables = ["rs", "rncp", "fiches", "certificateurs", "partenaires", "bloc_competences"]
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"Table {table} has {count} rows.")
    
    cur.close()
    conn.close()

def empty_tables():
    conn = psycopg2.connect(
        dbname="francecompetences",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    
    tables = ["rs", "rncp", "fiches", "certificateurs", "partenaires"]
    for table in tables:
        cur.execute(f"TRUNCATE TABLE {table}")
        print(f"Table {table} has been emptied.")
    
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    get_table_counts()
    # empty_tables()