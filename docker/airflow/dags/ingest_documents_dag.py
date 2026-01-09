from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook
import json
import requests
import os
from ftplib import FTP
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding as sym_padding
from cryptography.hazmat.backends import default_backend
import base64
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv() 


# === Secrets === #
SECRET_KEY = os.getenv("SECRET_KEY")  # Must be exactly 32 bytes
MONGO_URI = os.getenv("MONGO_URI") 
INGESTION_URL = os.getenv("UI_PORTAL_INGESTION_URL") #Ingestion URL of UI portal

# === DAG Trigger CONFIG === #
AIRFLOW_API_URL = "http://airflow-airflow-apiserver-1:8080/api/v2"  # or localhost in local mode
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD")
LOCAL_MODE = os.getenv("LOCAL_MODE", "false").lower() == "true"

if LOCAL_MODE:
    AIRFLOW_API_URL = "http://localhost:8080/api/v2"

# === CONFIG === #
LOCAL_DOWNLOAD_DIR = "/opt/airflow/downloaded_docs"
AUTO_EXECUTE_NEXT_NODE = 0
MONGO_DB_NAME = "idp"
MONGO_COLLECTION = "LogEntry"
mongo_client = MongoClient(MONGO_URI)
mongo_collection = mongo_client[MONGO_DB_NAME][MONGO_COLLECTION]

def get_transaction_id(process_instance_id: int) -> str:
    """
    Reads transactionId from tid.json for a given process instance
    """

    tid_path = os.path.join(
        LOCAL_DOWNLOAD_DIR,
        f"process-instance-{process_instance_id}",
        "tid.json"
    )

    if not os.path.exists(tid_path):
        raise FileNotFoundError(f"tid.json not found for processInstanceId={process_instance_id}")

    with open(tid_path, "r") as f:
        data = json.load(f)

    transaction_id = data.get("transactionId")
    if not transaction_id:
        raise ValueError("transactionId missing in tid.json")

    return transaction_id

def fix_base64_padding(s: str) -> str:
    return s + '=' * (-len(s) % 4)

def decrypt_password(encrypted_base64: str, secret_key: bytes) -> str:
    try:
        encrypted_base64 = fix_base64_padding(encrypted_base64)
        raw = base64.b64decode(encrypted_base64)
        iv = raw[:16]
        ciphertext = raw[16:]

        cipher = Cipher(algorithms.AES(secret_key), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()

        unpadder = sym_padding.PKCS7(128).unpadder()
        plaintext = unpadder.update(padded_plaintext) + unpadder.finalize()

        return plaintext.decode('utf-8')
    except Exception as e:
        print("❌ Failed to decrypt FTP password:", e)
        AUTO_EXECUTE_NEXT_NODE = 0
        log_to_mongo(process_instance_id, "Failed to decrypt FTP password", node_name = "Ingestion", log_type=1)

        raise

def log_to_mongo(transaction_id, node_name, message, log_type=1, remark=""):
    try:
        log_entry = {
            "id": transaction_id,
            "nodeName": node_name,
            "logsDescription": message,
            "logType": log_type,  # 0=info, 1=error, 2=success, 3=warning
            "isDeleted": False,
            "isActive": True,
            "remark": remark,
            "createdAt": datetime.utcnow()
        }
        mongo_collection.insert_one(log_entry)
        print(f"📝 Logged to MongoDB: {message}")
    except Exception as mongo_err:
        print(f"⚠️ Failed to log to MongoDB: {mongo_err}")

def log_success(transaction_id, step, msg):
    log_to_mongo(transaction_id, step, msg, node_name = "Ingestion", log_type=2)

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
    process_instance_id = context["dag_run"].conf.get("id")
    if not process_instance_id:
        raise ValueError("Missing process_instance_id in dag_run.conf")

    global AUTO_EXECUTE_NEXT_NODE
    valid_extensions = ['.pdf']

    process_instance_dir_path = os.path.join(
        LOCAL_DOWNLOAD_DIR, f"process-instance-{process_instance_id}"
    )
    os.makedirs(process_instance_dir_path, exist_ok=True)

    BLUEPRINT_JSON_PATH = os.path.join(process_instance_dir_path, "blueprint.json")

    if not os.path.exists(BLUEPRINT_JSON_PATH):
        raise ValueError("Blueprint JSON not found. Orchestrator must generate it first.")

    # Load blueprint written by orchestrator
    with open(BLUEPRINT_JSON_PATH, "r") as f:
        blueprint_json = json.load(f)

    # ---------------- MySQL connection (unchanged) ---------------- #
    hook = MySqlHook(mysql_conn_id="idp_mysql")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # 1. Get processInstanceFolderName
        cursor.execute("""
            SELECT processInstanceFolderName
            FROM ProcessInstances
            WHERE id = %s
        """, (process_instance_id,))
        instance_data = cursor.fetchone()

        if not instance_data:
            raise ValueError(f"No process instance found with ID {process_instance_id}")

        process_instance_folder = instance_data[0]

        # 2. Update ProcessInstances stage
        cursor.execute("""
            UPDATE ProcessInstances
            SET currentStage = %s,
                isInstanceRunning = %s,
                updatedAt = NOW()
            WHERE id = %s
        """, ("Ingestion", 1, process_instance_id))
        conn.commit()

        transaction_id = get_transaction_id(process_instance_id)
        log_to_mongo(
            transaction_id,
            "Ingestion",
            "ProcessInstance stage updated to 'Ingestion'",
            log_type=2
        )

        # 3. Find ingestion node from blueprint
        ingestion_node = next(
            (node for node in blueprint_json
             if node.get("nodeName", "").lower() == "ingestion"),
            None
        )

        if not ingestion_node:
            raise ValueError("No ingestion node found in blueprint")

        ingestion_config = ingestion_node.get("component", {})
        channel_type = ingestion_config.get("channelType", "").lower()

        print(f"📥 Ingestion Channel Type: {channel_type}")

        documents = []

        # ---------------- FTP INGESTION ---------------- #
        if channel_type == "ftp":
            ftp_path = ingestion_config.get("path")
            ftp_host = ingestion_config.get("host")
            ftp_user = ingestion_config.get("userName", "anonymous")
            encrypted_ftp_pass = ingestion_config.get("password", "")

            ftp_pass = decrypt_password(encrypted_ftp_pass, SECRET_KEY)

            ftp = FTP()
            ftp.connect(ftp_host, 21)
            ftp.login(ftp_user, ftp_pass)

            ftp_pi_path = f"{ftp_path}/process-instance-{process_instance_id}"
            ftp.cwd(ftp_pi_path)

            documents = ftp.nlst()

            for file_name in documents:
                if not file_name.lower().endswith(tuple(valid_extensions)):
                    continue

                file_path = os.path.join(process_instance_dir_path, file_name)
                with open(file_path, "wb") as f:
                    ftp.retrbinary(f"RETR {file_name}", f.write)

                transaction_id = get_transaction_id(process_instance_id)
                log_to_mongo(
                    transaction_id,
                    "Ingestion",
                    f"Downloaded {file_name}",
                    log_type=2
                )

            ftp.quit()

        # ---------------- UI / API INGESTION ---------------- #
        elif channel_type in ["ui", "api"]:
            ingestion_url = INGESTION_URL + process_instance_folder
            response = requests.get(ingestion_url, timeout=30)
            response.raise_for_status()

            documents = response.json()

            for file_name in documents:
                if not file_name.lower().endswith(tuple(valid_extensions)):
                    continue

                file_url = f"{INGESTION_URL}/file/{process_instance_folder}/{file_name}"
                file_path = os.path.join(process_instance_dir_path, file_name)

                with requests.get(file_url, stream=True, timeout=30) as r:
                    r.raise_for_status()
                    with open(file_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)

                transaction_id = get_transaction_id(process_instance_id)
                log_to_mongo(
                    transaction_id,
                    "Ingestion",
                    f"Downloaded {file_name}",
                    log_type=2
                )

        else:
            raise ValueError(f"Unsupported channelType: {channel_type}")

    except Exception as e:
        conn.rollback()
        AUTO_EXECUTE_NEXT_NODE = 0

        transaction_id = get_transaction_id(process_instance_id)
        log_to_mongo(
            transaction_id,
            "Ingestion",
            str(e),
            log_type=1,
            remark="Ingestion failed"
        )
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
