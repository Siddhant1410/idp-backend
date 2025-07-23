from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
from ftplib import FTP
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding as sym_padding
import os
import json
import requests
import shutil
import base64
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv() 

AUTO_EXECUTE_NEXT_NODE = 1

# === CONFIG ===
LOCAL_DOWNLOAD_DIR = "/opt/airflow/downloaded_docs"
SECRET_KEY = os.getenv("SECRET_KEY").encode() # Must be 32 bytes
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = "idp"
MONGO_COLLECTION = "LogEntry"
mongo_client = MongoClient(MONGO_URI)
mongo_collection = mongo_client[MONGO_DB_NAME][MONGO_COLLECTION]

if len(SECRET_KEY) != 32:
    raise ValueError("SECRET_KEY must be exactly 32 bytes (after encoding)")

# === AES Decryption ===
def fix_base64_padding(s: str) -> str:
    return s + '=' * (-len(s) % 4)

def decrypt_password(encrypted_base64: str, secret_key: bytes) -> str:
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

def log_to_mongo(process_instance_id, node_name, message, log_type=1, remark=""):
    try:
        log_entry = {
            "processInstanceId": process_instance_id,
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

# === Deliver Logic ===
def deliver_documents(**context):
    process_instance_id = context["dag_run"].conf.get("id")
    if not process_instance_id:
        raise ValueError("Missing process_instance_id in dag_run.conf")
    
    process_instance_dir_path = os.path.join(LOCAL_DOWNLOAD_DIR, f"process-instance-{process_instance_id}")
    BLUEPRINT_PATH = os.path.join(process_instance_dir_path, "blueprint.json")
    RESPONSE_BODY_PATH = os.path.join(process_instance_dir_path, "cleaned_extracted_fields.json")
    print(f"Blueprint path:", BLUEPRINT_PATH)

    hook = MySqlHook(mysql_conn_id="idp_mysql")
    conn = hook.get_conn()
    cursor = conn.cursor()

    if not os.path.exists(BLUEPRINT_PATH):
        raise FileNotFoundError("❌ blueprint.json not found locally")
        log_to_mongo(process_instance_id, message = "blueprint.json not found locally", node_name = "Deliver", log_type=1)
    with open(BLUEPRINT_PATH, "r") as f:
        blueprint_json = json.load(f)

    deliver_node = next((n for n in blueprint_json if n.get("nodeName", "").lower() == "deliver"), None)
    if not deliver_node:
        raise ValueError("Deliver node not found in blueprint")
        log_to_mongo(process_instance_id, message = "Deliver node not found in blueprint", node_name = "Deliver", log_type=1)
        

    component = deliver_node.get("component", {})
    channel_type = component.get("channelType", "").lower()

    cursor.execute("""
        UPDATE ProcessInstances
        SET currentStage = %s,
            isInstanceRunning = 1,
            updatedAt = NOW()
        WHERE id = %s
    """, ("Delivery", process_instance_id))
    conn.commit()

    uploaded_count = 0

    if channel_type == "ftp":
        ftp_host = component.get("host").replace(" ", "")
        ftp_username = component.get("userName")
        ftp_encrypted_password = component.get("password")
        ftp_path = component.get("path")  # e.g. "upload/documents"
        print(f"FTP path:", ftp_path)
        print(f"FTP host:", ftp_host)

        if not all([ftp_host, ftp_username, ftp_encrypted_password, ftp_path]):
            raise ValueError("Incomplete FTP details in blueprint")
            log_to_mongo(process_instance_id, message = "Incomplete FTP details in blueprint", node_name = "Deliver", log_type=1)

        ftp_password = decrypt_password(ftp_encrypted_password, SECRET_KEY)
        remote_folder_path = f"{ftp_path}process-instance-{process_instance_id}"

        with FTP(ftp_host) as ftp:
            ftp.login(user=ftp_username, passwd=ftp_password)
            print(f"✅ Connected to FTP: {ftp_host}")
            log_to_mongo(process_instance_id, message = f"Connected to FTP: {ftp_host}", node_name = "Deliver", log_type=2)

            # Navigate and create folders
            for folder in remote_folder_path.strip("/").split("/"):
                if folder not in ftp.nlst():
                    ftp.mkd(folder)
                ftp.cwd(folder)

            for filename in os.listdir(process_instance_dir_path):
                if filename.endswith(".pdf"):
                    local_file_path = os.path.join(process_instance_dir_path, filename)
                    with open(local_file_path, "rb") as f:
                        ftp.storbinary(f"STOR {filename}", f)
                        print(f"📤 Uploaded: {filename}")
                        log_to_mongo(process_instance_id, message = f"Uploaded: {filename}", node_name = "Deliver", log_type=2)
                        uploaded_count += 1

    elif channel_type in ["http", "https"]:
        post_url = component.get("url")
        if not post_url:
            raise ValueError("HTTP/HTTPS URL missing in deliver blueprint")
            log_to_mongo(process_instance_id, message = f"HTTP/HTTPS URL missing in deliver blueprint", node_name = "Deliver", log_type=1)

        for filename in os.listdir(process_instance_dir_path):
            if filename.endswith(".pdf"):
                file_path = os.path.join(process_instance_dir_path, filename)
                with open(file_path, 'rb') as f:
                    files = {"file": (filename, f)}
                    response = requests.post(post_url, files=files, timeout=30)
                    response.raise_for_status()
                    print(f"📤 Posted: {filename}")
                    log_to_mongo(process_instance_id, message = f"Posted: {filename}", node_name = "Deliver", log_type=2)
                    uploaded_count += 1
    else:
        raise ValueError(f"Unsupported channelType in deliver node: {channel_type}")
        log_to_mongo(process_instance_id, message = f"Unsupported channelType in deliver node: {channel_type}", node_name = "Deliver", log_type=1)

    print(f"✅ Delivered {uploaded_count} documents via {channel_type.upper()}")
    log_to_mongo(process_instance_id, message = f"Delivered {uploaded_count} documents via {channel_type.upper()}", node_name = "Deliver", log_type=2)

    if not os.path.exists(RESPONSE_BODY_PATH):
        raise FileNotFoundError("❌ cleaned_extracted_fields.json not found")
        log_to_mongo(process_instance_id, message = f"cleaned_extracted_fields.json not found", node_name = "Deliver", log_type=1)
        
    with open(RESPONSE_BODY_PATH, "r") as f:
        response_data = json.load(f)

    updated_count = 0
    for entry in response_data:
        pid = entry.get("processInstanceId")
        if pid:
            cursor.execute("""
                UPDATE ProcessInstanceDocuments
                SET isActive = 1,
                    isDeleted = 0,
                    isHumanUpdated = 1,
                    updatedAt = NOW()
                WHERE id = %s
            """, (pid,))
            updated_count += 1

    conn.commit()
    print(f"✅ Updated {updated_count} ProcessInstanceDocuments records")
    log_to_mongo(process_instance_id, message = f"Updated {updated_count} ProcessInstanceDocuments records", node_name = "Deliver", log_type=2)

    cursor.execute("""
        UPDATE ProcessInstances
        SET currentStage = %s,
            isInstanceRunning = 0,
            updatedAt = NOW()
        WHERE id = %s
    """, ("Completed", process_instance_id))
    conn.commit()

    shutil.rmtree(process_instance_dir_path)
    print("✅ Cleaned up local process instance folder.")
    log_to_mongo(process_instance_id, message = f"Cleaned up local process instance folder.", node_name = "Deliver", log_type=2)

# === DAG Definition ===
with DAG(
    dag_id="deliver_dag",
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    catchup=False,
    tags=["idp", "deliver"]
) as dag:
    deliver_task = PythonOperator(
        task_id="deliver_documents",
        python_callable=deliver_documents
    )
