# End-to-End Retail ELT Pipeline

## Project Overview
An automated data pipeline that extracts retail transaction data, transforms it into a Star Schema using **Apache Spark**, and loads it into **Google BigQuery** for analytics. The entire workflow is orchestrated by **Apache Airflow**.

## Architecture
**Extract** ➔ **Transform** ➔ **Load**
- **Source:** Simulated Retail API (JSON data)
- **Processing:** Apache Spark (PySpark) for data cleaning and schema modeling.
- **Orchestration:** Apache Airflow (DAGs scheduled daily).
- **Data Warehouse:** Google BigQuery.

## Tech Stack
- **Language:** Python 3.12
- **ETL Engine:** Apache Spark
- **Orchestration:** Apache Airflow
- **Cloud Storage:** Google BigQuery
- **Environment:** WSL2 (Ubuntu Linux)

## Data Model (Star Schema)
The pipeline transforms raw nested JSON into optimized tables:
1.  **Fact Table:** `fact_sales` (Transactions, revenue, quantity)
2.  **Dimension:** `dim_product` (Product details, categories)
3.  **Dimension:** `dim_customer` (User demographics, emails)

## How to Run
1.  **Setup Environment:**
    ```bash
    source venv/bin/activate
    export AIRFLOW_HOME=~/retail_pipeline/airflow
    ```
2.  **Trigger Airflow:**
    ```bash
    airflow standalone
    ```
3.  **Access UI:**

    Open `localhost:8080` and trigger `retail_pipeline_dag`.
