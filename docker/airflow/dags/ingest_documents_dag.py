from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook

import json
import requests
import os

# === CONFIG ===
LOCAL_DOWNLOAD_DIR = "/opt/airflow/downloaded_docs"
BLUEPRINT_JSON_PATH = os.path.join(LOCAL_DOWNLOAD_DIR, "blueprint.json")

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
    
    blueprint_json = json.loads(row[0])

    # Save blueprint locally for other DAGs
    os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)
    with open(BLUEPRINT_JSON_PATH, "w") as f:
        json.dump(blueprint_json, f, indent=2)
    print(f"✅ Blueprint saved to {BLUEPRINT_JSON_PATH}")

    # Step 3: Update ProcessInstances table
    cursor.execute("""
    UPDATE ProcessInstances
    SET currentStage = %s,
        isInstanceRunning = %s,
        updatedAt = NOW()
    WHERE processesId = %s
    """, ("Ingestion", 1, process_id))
    conn.commit()
    print(f"✅ ProcessInstances updated: currentStage='Ingestion', isInstanceRunning=1 for process_id = {process_id}")

    # Step 4: Find ingestion node
    ingestion_node = next((node for node in blueprint_json if node.get("nodeName", "").lower() == "ingestion"), None)
    if not ingestion_node:
        raise ValueError("No ingestion node found in blueprint")

    ingestion_url = ingestion_node["component"].get("url")
    old_ingestion_url = ingestion_url
    ingestion_url = ingestion_url + "process-instance-4"
    if not ingestion_url:
        raise ValueError("Ingestion URL is missing in blueprint")

    # Step 5: Fetch document list
    print(f"Fetching document list from: {ingestion_url}")
    response = requests.get(ingestion_url, timeout=30)
    response.raise_for_status()

    documents = response.json()
    print("Documents from ingestion path:", documents)

    # Normalize base URL
    base_url = ingestion_url.rstrip("/")


    valid_extensions = [".pdf", ".doc", ".docx", ".png", ".jpg", ".jpeg"]
    for file_name in documents:
        if not isinstance(file_name, str) or not any(file_name.lower().endswith(ext) for ext in valid_extensions):
            print(f"⚠️ Skipping invalid item (likely a folder or unsupported type): {file_name}")
            continue

        file_url = f"{old_ingestion_url}/file/process-instance-4/{file_name}"
        file_path = os.path.join(LOCAL_DOWNLOAD_DIR, file_name)

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
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    catchup=False,
    tags=["idp", "ingestion"],
) as dag:

    ingest_task = PythonOperator(
        task_id="fetch_and_download_documents",
        python_callable=fetch_blueprint_and_download_docs,
    )
