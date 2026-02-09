from google.cloud import bigquery
import os
import glob

# Setup Paths
BASE_DIR = os.path.expanduser("~/retail_pipeline")
PROC_DIR = os.path.join(BASE_DIR, "data/processed")
KEY_PATH = os.path.join(BASE_DIR, "key.json") # <--- This file must exist!

# Authenticate with Google
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH
client = bigquery.Client()

# ⚠️ CHANGE THIS TO YOUR PROJECT ID IF NEEDED ⚠️
# If you leave it as just "retail_dw", it assumes the dataset is in your default project.
DATASET_ID = "retail_dw" 

def load_to_bq(table_name):
    print(f"--- Loading {table_name} ---")
    
    # Define Target: project.dataset.table
    table_id = f"{client.project}.{DATASET_ID}.{table_name}"
    
    # Configure the Job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_TRUNCATE", # Overwrite table every time
        autodetect=True                     # Let BigQuery figure out schema
    )
    
    # Find the Parquet file (Spark saves it as a folder with parts inside)
    files = glob.glob(os.path.join(PROC_DIR, table_name, "*.parquet"))
    
    if not files:
        print(f"❌ No files found for {table_name}")
        return
        
    # Upload the first part
    with open(files[0], "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        
    # Wait for completion
    job.result()
    print(f"✅ Loaded {table_name} into BigQuery")

if __name__ == "__main__":
    load_to_bq("dim_product")
    load_to_bq("dim_customer")
    load_to_bq("fact_sales")