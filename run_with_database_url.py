#!/usr/bin/env python3
"""
Script to run leha/main.py using DATABASE_URL_FRANCECOMPETENCES instead of DB_USER/DB_PASSWORD/HOST
This allows leha to populate the Prisma Postgres database used by Vercel
"""

import os
import sys
import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def parse_database_url(url):
    """Parse a PostgreSQL connection URL into connection parameters"""
    if not url:
        return None
    
    parsed = urlparse(url)
    
    return {
        'host': parsed.hostname,
        'port': parsed.port or 5432,
        'user': parsed.username,
        'password': parsed.password,
        'database': parsed.path.lstrip('/').split('?')[0]  # Remove query params
    }

def get_db_connection():
    """Get database connection using DATABASE_URL_FRANCECOMPETENCES or fallback to DB_USER/DB_PASSWORD/HOST"""
    db_url = os.getenv("DATABASE_URL_FRANCECOMPETENCES")
    
    if db_url:
        print(f"Using DATABASE_URL_FRANCECOMPETENCES")
        params = parse_database_url(db_url)
        if params:
            return psycopg2.connect(**params)
    
    # Fallback to old method
    print("Using DB_USER/DB_PASSWORD/HOST")
    return psycopg2.connect(
        dbname="francecompetences",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("HOST"),
        port="5432"
    )

def check_database_status():
    """Check if database has data"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Check if Fiches table exists and has data
        cur.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('Fiches', 'fiches')
        """)
        
        table_exists = cur.fetchone()[0] > 0
        
        if not table_exists:
            print("❌ Tables don't exist yet")
            return False
        
        # Try both PascalCase and lowercase
        try:
            cur.execute('SELECT COUNT(*) FROM "Fiches"')
            count = cur.fetchone()[0]
            print(f"✅ Found {count} rows in Fiches table")
        except:
            try:
                cur.execute('SELECT COUNT(*) FROM "fiches"')
                count = cur.fetchone()[0]
                print(f"✅ Found {count} rows in fiches table")
            except Exception as e:
                print(f"❌ Error checking table: {e}")
                return False
        
        cur.close()
        conn.close()
        
        return count > 0
        
    except Exception as e:
        print(f"❌ Error checking database: {e}")
        return False

def main():
    print("🔍 Checking database status...")
    has_data = check_database_status()
    
    if has_data:
        print("✅ Database already has data!")
        response = input("Do you want to re-populate it? (y/N): ")
        if response.lower() != 'y':
            print("Skipping population.")
            return
    
    print("\n🚀 Running leha to populate database...")
    print("=" * 50)
    
    # Import and run leha's main functions
    sys.path.insert(0, os.path.dirname(__file__))
    
    # Monkey patch the connection function in main.py
    import main as leha_main
    
    # Replace all psycopg2.connect calls with our version
    original_connect = psycopg2.connect
    
    def patched_connect(*args, **kwargs):
        # If called with dbname="francecompetences", use our connection
        if kwargs.get('dbname') == 'francecompetences' or (args and len(args) > 0):
            return get_db_connection()
        return original_connect(*args, **kwargs)
    
    psycopg2.connect = patched_connect
    
    # Run leha
    try:
        leha_main.create_tables()
        leha_main.fetch_and_process_links()
        print("\n✅ Database population complete!")
    except Exception as e:
        print(f"\n❌ Error running leha: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

