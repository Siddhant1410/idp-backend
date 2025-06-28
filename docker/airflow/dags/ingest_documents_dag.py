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
    # Get process instance ID from DAG run configuration
    process_instance_id = context["dag_run"].conf.get("id")
    if not process_instance_id:
        raise ValueError("Missing process_instance_id in dag_run.conf")
    
    # Initialize MySQL connection
    hook = MySqlHook(mysql_conn_id="idp_mysql")
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # 1. Get the process_id linked to this instance
        cursor.execute("""
            SELECT processesId 
            FROM ProcessInstances 
            WHERE id = %s
        """, (process_instance_id,))
        instance_data = cursor.fetchone()
        
        if not instance_data:
            raise ValueError(f"No process instance found with ID {process_instance_id}")
        
        process_id = instance_data[0]  # Using index instead of dictionary access

        # 2. Get bluePrintId from Processes
        cursor.execute("SELECT bluePrintId FROM Processes WHERE id = %s", (process_id,))
        blueprint_id_row = cursor.fetchone()
        if not blueprint_id_row or not blueprint_id_row[0]:
            raise ValueError(f"No bluePrintId found for process ID {process_id}")
        blueprint_id = blueprint_id_row[0]

        # 3. Get blueprint JSON from BluePrint table
        cursor.execute("SELECT bluePrint FROM BluePrint WHERE id = %s", (blueprint_id,))
        blueprint_row = cursor.fetchone()
        if not blueprint_row or not blueprint_row[0]:
            raise ValueError(f"No blueprint found for blueprint ID {blueprint_id}")
        
        blueprint_json = json.loads(blueprint_row[0])

        # Save blueprint locally for other DAGs
        os.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)
        with open(BLUEPRINT_JSON_PATH, "w") as f:
            json.dump(blueprint_json, f, indent=2)
        print(f"✅ Blueprint saved to {BLUEPRINT_JSON_PATH}")

        # 4. Update ProcessInstances table
        cursor.execute("""
        UPDATE ProcessInstances
        SET currentStage = %s,
            isInstanceRunning = %s,
            updatedAt = NOW()
        WHERE id = %s
        """, ("Ingestion", 1, process_instance_id))
        conn.commit()
        print(f"✅ Updated ProcessInstance {process_instance_id} to Ingestion stage")

        # 5. Find ingestion node configuration
        ingestion_node = next(
            (node for node in blueprint_json 
             if isinstance(node, dict) and node.get("nodeName", "").lower() == "ingestion"), 
            None
        )
        if not ingestion_node:
            raise ValueError("No ingestion node found in blueprint")

        ingestion_config = ingestion_node.get("component", {})
        ingestion_url = ingestion_config.get("url")
        old_ingestion_url = ingestion_url
        ingestion_url = ingestion_url + "process-instance-4"

        if not ingestion_url:
            raise ValueError("Ingestion URL is missing in blueprint")

        # 6. Fetch document list from ingestion source
        print(f"🔍 Fetching document list from: {ingestion_url}")
        response = requests.get(ingestion_url, timeout=30)
        response.raise_for_status()
        documents = response.json()
        print(f"📄 Found {len(documents)} documents at source")

        # 7. Download valid documents
        valid_extensions = [".pdf", ".doc", ".docx", ".png", ".jpg", ".jpeg"]
        downloaded_count = 0
        
        for file_name in documents:
            if not isinstance(file_name, str):
                continue
                
            file_name_lower = file_name.lower()
            if not any(file_name_lower.endswith(ext) for ext in valid_extensions):
                print(f"⚠️ Skipping unsupported file: {file_name}")
                continue

            file_url = f"{old_ingestion_url}/file/process-instance-4/{file_name}"
            file_path = os.path.join(LOCAL_DOWNLOAD_DIR, file_name)

            print(f"⬇️ Downloading {file_name}...")
            try:
                with requests.get(file_url, stream=True, timeout=30) as r:
                    r.raise_for_status()
                    with open(file_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                downloaded_count += 1
            except Exception as e:
                print(f"❌ Failed to download {file_name}: {str(e)}")

        print(f"✅ Successfully downloaded {downloaded_count}/{len(documents)} documents to {LOCAL_DOWNLOAD_DIR}")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error in ingestion process: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

# === DAG Definition ===
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="ingest_documents_dag",
    default_args=default_args,
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    catchup=False,
    tags=["idp", "ingestion"],
) as dag:

    ingest_task = PythonOperator(
        task_id="fetch_and_download_documents",
        python_callable=fetch_blueprint_and_download_docs,
    )