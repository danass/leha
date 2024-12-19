import requests
from datetime import datetime
import zipfile
import os
from io import BytesIO
import psycopg2
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# API endpoint
API_URL = "https://www.data.gouv.fr/api/2/datasets/5eebbc067a14b6fecc9c9976/resources/?page=1"

def fetch_and_print_links():
    # Fetch data from the API
    response = requests.get(API_URL)
    if response.status_code != 200:
        print("Failed to fetch data from API:", response.status_code)
        return
    
    data = response.json().get("data", [])
    if not data:
        print("No data found in the API response.")
        return
    
    # Get today's date in the required format
    today_date = datetime.now().strftime("%Y-%m-%d")
    
    # Keywords to match
    target_titles = ["export-fiches-rs-v4", "export-fiches-rncp-v4"]
    
    # Filter and print links
    for item in data:
        title = item.get("title", "")
        url = item.get("url", "")
        
        # Check if today's date and target titles are in the title
        if today_date in title:
            for target in target_titles:
                if target in title:
                    print(f"Title: {title}")
                    print(f"Link: {url}\n")
                    download_and_unzip(url, title)
                    store_in_db(title)
                    break

def download_and_unzip(url, title):
    response = requests.get(url)
    if response.status_code == 200:
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            for file_info in z.infolist():
                file_info.filename = f"{os.path.splitext(title)[0]}.xml"
                z.extract(file_info, "downloads")
        print(f"Downloaded and unzipped: {title}")
    else:
        print(f"Failed to download {title}: {response.status_code}")


def store_in_db(title):
    conn = psycopg2.connect(
        dbname="francecompetences",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    if "rs" in title:
        table_name = "rs"
    elif "rncp" in title:
        table_name = "rncp"
    else:
        print("Title does not match RS or RNCP format.")
        return

    file_path = os.path.join("downloads", f"{os.path.splitext(title)[0]}.xml")
    print(f"Processing XML file: {file_path}")

    try:
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return

        # Parse XML file and collect all unique column names
        tree = ET.parse(file_path)
        root = tree.getroot()

        if len(root) == 0:
            print("No records found in XML file.")
            return

        all_columns = set()
        for record in root:
            for elem in record:
                all_columns.add(elem.tag)  # Add unique column names

        all_columns = sorted(all_columns)  # Sort for consistency
        print("Extracted Columns:", all_columns)

        # Drop table if it exists and recreate it
        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")

        # Create table dynamically
        columns_str = ", ".join([f'"{col}" TEXT' for col in all_columns])
        cur.execute(f"""
            CREATE TABLE {table_name} (
                id SERIAL PRIMARY KEY,
                {columns_str}
            )
        """)

        # Ensure index on id
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{table_name}_id ON {table_name}(id)
        """)
        print(f"Index on id created for table {table_name}.")

        # Insert data with dynamic column handling
        for record in root:
            record_data = {col: "" for col in all_columns}  # Initialize all columns with empty strings
            for elem in record:
                record_data[elem.tag] = elem.text.strip() if elem.text else ""  # Populate available data

            columns_placeholder = ", ".join([f'"{col}"' for col in all_columns])
            values_placeholder = ", ".join(["%s"] * len(all_columns))
            values = [record_data[col] for col in all_columns]

            cur.execute(f"""
                INSERT INTO {table_name} ({columns_placeholder})
                VALUES ({values_placeholder})
            """, values)

        conn.commit()
        print(f"Data stored successfully in table: {table_name}")

    except ET.ParseError as e:
        print("Error parsing XML:", e)
    except Exception as e:
        print("Error storing data:", e)
    finally:
        cur.close()
        conn.close()
        

if __name__ == "__main__":
    fetch_and_print_links()
