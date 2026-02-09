import requests
import json
import os

# 1. Setup Paths
# We use 'os.path.expanduser' to work on both Linux and Windows reliably
BASE_DIR = os.path.expanduser("~/retail_pipeline")
RAW_DIR = os.path.join(BASE_DIR, "data/raw")

# Make sure the folder exists, or the code will crash
os.makedirs(RAW_DIR, exist_ok=True)

def extract_data(endpoint):
    print(f"--- Extracting {endpoint} ---")
    
    # 2. Call the API
    # limit=0 gets ALL data (not just the first 30)
    url = f"https://dummyjson.com/{endpoint}?limit=0"
    response = requests.get(url)
    
    # 3. Check for Errors
    if response.status_code == 200:
        data = response.json()
        
        # The API wraps data in a key (e.g., {'products': [...]})
        # We need to grab just the list inside
        key = endpoint 
        records = data[key]
        
        # 4. Save to File
        file_path = os.path.join(RAW_DIR, f"{endpoint}.json")
        with open(file_path, "w") as f:
            json.dump(records, f)
            
        print(f"✅ Success! Saved {len(records)} records to {file_path}")
    else:
        print(f"❌ Failed to get data: {response.status_code}")

if __name__ == "__main__":
    # We grab 3 different datasets to build our Star Schema
    extract_data("products")
    extract_data("users")
    extract_data("carts")