import requests
from datetime import datetime
import zipfile
import os
from io import BytesIO
import pandas as pd
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
    
    # Keyword to match
    target_title = "export-fiches-csv"
    
    # Filter and print links
    for item in data:
        title = item.get("title", "")
        url = item.get("url", "")
        
        # Check if today's date and target title are in the title
        if today_date in title and target_title in title:
            print(f"Title: {title}")
            print(f"Link: {url}\n")
            download_and_unzip(url, title)
            break

def download_and_unzip(url, title):
    response = requests.get(url)
    if response.status_code == 200:
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            for file_info in z.infolist():
                file_info.filename = f"{os.path.splitext(title)[0]}_{file_info.filename}"
                z.extract(file_info, "downloads")
                print(f"Extracted: {file_info.filename}")
                list_csv_structure(os.path.join("downloads", file_info.filename))
        print(f"Downloaded and unzipped: {title}")
    else:
        print(f"Failed to download {title}: {response.status_code}")

def list_csv_structure(file_path):
    if file_path.endswith(".csv"):
        delimiters = [";"]
        for delimiter in delimiters:
            try:
                df = pd.read_csv(file_path, delimiter=delimiter, dtype={'Id_Fiche': str})
                print(f"Structure of {file_path}:")
                print(df.head())
                print("\n")
                combine_data(df)
                break
            except pd.errors.ParserError as e:
                print(f"Error parsing CSV file {file_path} with delimiter '{delimiter}': {e}")

combined_data = pd.DataFrame()

def combine_data(df):
    global combined_data
    if 'Numero_Fiche' in df.columns:
        df = df.rename(columns={'Numero_Fiche': 'RNCP_ID'})
    df = df.reset_index(drop=True)  # Ensure the index is reset
    combined_data = pd.concat([combined_data, df], ignore_index=True)

def save_combined_data():
    combined_data.insert(0, 'id', range(1, len(combined_data) + 1))  # Add unique ID column
    combined_data.to_csv("combined_data.csv", index=False)
    print("Combined data saved to combined_data.csv")

if __name__ == "__main__":
    fetch_and_print_links()
    save_combined_data()
