from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import os
import json

# === CONFIG ===
LOCAL_DOWNLOAD_DIR = "/opt/airflow/downloaded_docs"
BLUEPRINT_PATH = os.path.join(LOCAL_DOWNLOAD_DIR, "blueprint.json")
RESPONSE_BODY_PATH = os.path.join(LOCAL_DOWNLOAD_DIR, "response_body.json")

def deliver_documents(**context):
    process_id = context["dag_run"].conf.get("process_id")
    if not process_id:
        raise ValueError("Missing process_id in dag_run.conf")

    # DB connection
    hook = MySqlHook(mysql_conn_id="idp_mysql")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Load blueprint locally
    if not os.path.exists(BLUEPRINT_PATH):
        raise FileNotFoundError("❌ blueprint.json not found locally")

    with open(BLUEPRINT_PATH, "r") as f:
        blueprint_json = json.load(f)

    # Find Deliver node
    deliver_node = next((n for n in blueprint_json if n["nodeName"].lower() == "deliver"), None)
    if not deliver_node:
        raise ValueError("Deliver node not found in blueprint")

    ftp_path = deliver_node["component"].get("path")
    if not ftp_path:
        raise ValueError("FTP path missing in deliver node")

    # Update current stage to 'Delivery'
    cursor.execute("""
        UPDATE ProcessInstances
        SET currentStage = %s,
            isInstanceRunning = 1,
            updatedAt = NOW()
        WHERE processesId = %s
    """, ("Delivery", process_id))
    conn.commit()

    # Connect to FTP
    ftp_hook = FTPHook(ftp_conn_id="ftp_conn")
    ftp_conn = ftp_hook.get_conn()
    ftp_conn.cwd(ftp_path)
    print(f"✅ Connected to FTP at {ftp_path}")

    # Upload all local PDFs
    uploaded_count = 0
    for filename in os.listdir(LOCAL_DOWNLOAD_DIR):
        if filename.endswith(".pdf"):
            local_file_path = os.path.join(LOCAL_DOWNLOAD_DIR, filename)
            with open(local_file_path, "rb") as f:
                ftp_conn.storbinary(f"STOR {filename}", f)
                print(f"📤 Uploaded: {filename}")
                uploaded_count += 1

    print(f"✅ Uploaded {uploaded_count} files to FTP path: {ftp_path}")

    # Read processInstanceId from response_body.json and update DB
    if not os.path.exists(RESPONSE_BODY_PATH):
        raise FileNotFoundError("❌ response_body.json not found")

    with open(RESPONSE_BODY_PATH, "r") as f:
        response_data = json.load(f)

    updated_count = 0
    for entry in response_data:
        process_instance_id = entry.get("processInstanceId")
        if process_instance_id:
            cursor.execute("""
                UPDATE ProcessInstanceDocuments
                SET isActive = 1,
                    isDeleted = 0,
                    isHumanUpdated = 1,
                    updatedAt = NOW()
                WHERE id = %s
            """, (process_instance_id,))
            updated_count += 1

    conn.commit()
    print(f"✅ Updated {updated_count} ProcessInstanceDocuments records")

    #Mark the Current Process Instance status as completed
    cursor.execute("""
        UPDATE ProcessInstances
        SET currentStage = %s,
            isInstanceRunning = 0,
            updatedAt = NOW()
        WHERE processesId = %s
    """, ("Completed", process_id))
    conn.commit()
    print(f"✅ Updated Process Instance Status as Completed.")

# === DAG Definition ===
with DAG(
    dag_id="deliver_dag",
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    catchup=False,
    tags=["idp", "deliver"]
) as dag:

    deliver_task = PythonOperator(
        task_id="deliver_documents_to_ftp",
        python_callable=deliver_documents
    )
