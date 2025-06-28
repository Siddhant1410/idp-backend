from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook
import json
import requests
import os

# === DAG Trigger CONFIG === #
AIRFLOW_API_URL = "http://airflow-airflow-apiserver-1:8080/api/v2"  # or localhost in local mode
AIRFLOW_USERNAME = "airflow"
AIRFLOW_PASSWORD = "airflow"
LOCAL_MODE = os.getenv("LOCAL_MODE", "false").lower() == "true"

if LOCAL_MODE:
    AIRFLOW_API_URL = "http://localhost:8080/api/v2"

# === CONFIG ===
LOCAL_DOWNLOAD_DIR = "/opt/airflow/downloaded_docs"


def get_auth_token():
    """Get JWT token from Airflow API"""
    auth_url = f"{AIRFLOW_API_URL.replace('/api/v2', '')}/auth/token"
    response = requests.post(
        auth_url,
        json={"username": AIRFLOW_USERNAME, "password": AIRFLOW_PASSWORD},
        headers={"Content-Type": "application/json"},
        timeout=10
    )
    response.raise_for_status()
    return response.json()["access_token"]


def fetch_blueprint_and_download_docs(**context):
    # Get process instance ID from DAG run configuration
    process_instance_id = context["dag_run"].conf.get("id")
    if not process_instance_id:
        raise ValueError("Missing process_instance_id in dag_run.conf")

    process_instance_dir_path = os.path.join(LOCAL_DOWNLOAD_DIR, "process-instance-" + process_instance_id)
    os.makedirs(process_instance_dir_path, exist_ok=True)
    BLUEPRINT_JSON_PATH = os.path.join(process_instance_dir_path, "blueprint.json")
    
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


        #get the process-instance-folder-name
        cursor.execute("""
            SELECT processInstanceFolderName 
            FROM ProcessInstances 
            WHERE id = %s
        """, (process_instance_id,))
        instance_data = cursor.fetchone()

        process_instance_folder = instance_data[0]

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
        os.makedirs(process_instance_dir_path, exist_ok=True)
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
        ingestion_url = ingestion_url + process_instance_folder

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

            file_url = f"{old_ingestion_url}/file/{process_instance_folder}/{file_name}"
            file_path = os.path.join(process_instance_dir_path, file_name)

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

        print(f"✅ Successfully downloaded {downloaded_count}/{len(documents)} documents to {process_instance_dir_path}")

        # 8. Trigger classify_documents_dag
        print("🚀 Triggering classify_documents_dag...")
        token = get_auth_token()
        trigger_url = f"{AIRFLOW_API_URL}/dags/classify_documents_dag/dagRuns"
        run_id = f"triggered_by_ingest_{process_instance_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        payload = {
            "dag_run_id": run_id,
            "logical_date": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            "conf": {"id": process_instance_id}
        }

        response = requests.post(trigger_url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        print(f"✅ Successfully triggered extract_documents_dag with ID {process_instance_id}")
        
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