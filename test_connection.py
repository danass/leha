import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def test_connection():
    try:
        conn = psycopg2.connect(
            dbname="francecompetences",
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("HOST"),
            port="5432"
        )
        print("✅ Database connection successful!")
        
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print(f"PostgreSQL version: {version[0]}")
        
        # Check if tables exist
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = cur.fetchall()
        print(f"Tables in database: {[table[0] for table in tables]}")
        
        cur.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

if __name__ == "__main__":
    test_connection()

