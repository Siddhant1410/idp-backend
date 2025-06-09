from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.mysql.hooks.mysql import MySqlHook

import json
import requests
import os

# === CONFIG ===
LOCAL_DOWNLOAD_DIR = "/opt/airflow/downloaded_docs"

def fetch_blueprint_and_download_docs(**context):
    process_id = context["dag_run"].conf.get("process_id")
    if not process_id:
        raise ValueError("Missing process_id in dag_run.conf")

    # Connect to MySQL
    hook = MySqlHook(mysql_conn_id="idp_mysql")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Step 1: Get bluePrintId from Processes
    cursor.execute("SELECT bluePrintId FROM Processes WHERE id = %s", (process_id,))
    row = cursor.fetchone()
    if not row or not row[0]:
        raise ValueError(f"No bluePrintId found for process ID {process_id}")
    blueprint_id = row[0]

    # Step 2: Get blueprint JSON from BluePrint table
    cursor.execute("SELECT bluePrint FROM BluePrint WHERE id = %s", (blueprint_id,))
    row = cursor.fetchone()
    if not row or not row[0]:
        raise ValueError(f"No blueprint found for blueprint ID {blueprint_id}")
    
    blueprint_json = json.loads(row[0])  # parse TEXT field into JSON

    # Step 3: Find ingestion node
    ingestion_node = next((node for node in blueprint_json if node.get("nodeName") == "ingestion"), None)
    if not ingestion_node:
        raise ValueError("No ingestion node found in blueprint")

    ingestion_path = ingestion_node["component"].get("path")
    if not ingestion_path:
        raise ValueError("Ingestion path missing in blueprint")

    # Step 4: Fetch document list
    print(f"Fetching document list from: {ingestion_path}")
    response = requests.get(ingestion_path, timeout=30)
    response.raise_for_status()

    documents = response.json()  # Assuming it's a list of objects with file URLs
    os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)

    for doc in documents:
        filename = doc.get("filename") or os.path.basename(doc.get("url"))
        file_url = doc.get("url")

        if not file_url:
            print("Skipping item with no URL:", doc)
            continue

        file_path = os.path.join(LOCAL_DOWNLOAD_DIR, filename)
        print(f"Downloading {file_url} → {file_path}")
        with requests.get(file_url, stream=True) as r:
            r.raise_for_status()
            with open(file_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

    print(f"✅ {len(documents)} documents downloaded to {LOCAL_DOWNLOAD_DIR}")

# === DAG Definition ===
with DAG(
    dag_id="ingest_documents_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["idp", "ingestion"],
) as dag:

    ingest_task = PythonOperator(
        task_id="fetch_and_download_documents",
        python_callable=fetch_blueprint_and_download_docs,
        provide_context=True
    )
