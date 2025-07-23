from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import os
import json
import openai
import pytesseract
from pdf2image import convert_from_path
import tempfile
import requests
from dotenv import load_dotenv
from PyPDF2 import PdfReader
from pymongo import MongoClient

load_dotenv() 

AUTO_EXECUTE_NEXT_NODE = 1
MONGO_URI = os.getenv("MONGO_URI")

# === DAG Trigger CONFIG === #
AIRFLOW_API_URL = "http://airflow-airflow-apiserver-1:8080/api/v2"  # or localhost in local mode
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD")
LOCAL_MODE = os.getenv("LOCAL_MODE", "false").lower() == "true"

if LOCAL_MODE:
    AIRFLOW_API_URL = "http://localhost:8080/api/v2"

# === CONFIG ===
LOCAL_DOWNLOAD_DIR = "/opt/airflow/downloaded_docs"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # from .env
openai.api_key = OPENAI_API_KEY
MONGO_DB_NAME = "idp"
MONGO_COLLECTION = "LogEntry"
mongo_client = MongoClient(MONGO_URI)
mongo_collection = mongo_client[MONGO_DB_NAME][MONGO_COLLECTION]


if not openai.api_key or not openai.api_key.startswith("sk-") and not openai.api_key.startswith("sk-proj-"):
    raise EnvironmentError("❌ OpenAI API key missing or invalid. Please set OPENAI_API_KEY as an environment variable.")

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

def get_auth_token():
    auth_url = f"{AIRFLOW_API_URL.replace('/api/v2', '')}/auth/token"
    response = requests.post(
        auth_url,
        json={"username": AIRFLOW_USERNAME, "password": AIRFLOW_PASSWORD},
        headers={"Content-Type": "application/json"},
        timeout=10
    )
    response.raise_for_status()
    return response.json()["access_token"]

def extract_text_per_page(pdf_path, max_pages=10):
    try:
        reader = PdfReader(pdf_path)
        total_pages = min(len(reader.pages), max_pages)
        for i in range(total_pages):
            images = convert_from_path(pdf_path, first_page=i+1, last_page=i+1)
            if not images:
                continue
            text = pytesseract.image_to_string(images[0])
            yield text
    except Exception as e:
        print(f"❌ OCR failed for {pdf_path}: {e}")
        log_to_mongo(process_instance_id, message = f"OCR failed for {pdf_path}: {e}", node_name = "Classification", log_type=1)
        return

def classify_documents(**context):
    process_instance_id = context["dag_run"].conf.get("id")
    if not process_instance_id:
        raise ValueError("Missing process_instance_id in dag_run.conf")

    process_instance_dir_path = os.path.join(LOCAL_DOWNLOAD_DIR, "process-instance-" + str(process_instance_id))
    os.makedirs(process_instance_dir_path, exist_ok=True)
    blueprint_path = os.path.join(process_instance_dir_path, "blueprint.json")

    hook = MySqlHook(mysql_conn_id="idp_mysql")
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        if not os.path.exists(blueprint_path):
            raise FileNotFoundError(f"❌ blueprint.json not found at {blueprint_path}. Please run ingestion DAG first.")
            log_to_mongo(process_instance_id, message = f" blueprint.json not found at {blueprint_path}. Please run ingestion DAG first.", node_name = "Classification", log_type=1)

        with open(blueprint_path, "r") as f:
            blueprint = json.load(f)

        cursor.execute(
            """
            UPDATE ProcessInstances
            SET currentStage = %s,
                isInstanceRunning = %s,
                updatedAt = NOW()
            WHERE id = %s
            """,
            ("Classification", 1, process_instance_id)
        )
        conn.commit()
        print(f"🟢 ProcessInstances updated → currentStage='Classification', isInstanceRunning=1 for process_instance_id={process_instance_id}")
        log_to_mongo(process_instance_id, message = f"ProcessInstances updated → currentStage='Classification', isInstanceRunning=1 for process_instance_id={process_instance_id}", node_name = "Classification", log_type=2)

        classify_node = next((n for n in blueprint if n["nodeName"].lower() == "classify"), None)
        if not classify_node:
            raise ValueError("Classification node not found in blueprint")
            log_to_mongo(process_instance_id, message = f"Classification node not found in blueprint", node_name = "Classification", log_type=1)

        categories = classify_node["component"].get("categories", [])
        if not categories:
            raise ValueError("No categories found in Classify component")
            log_to_mongo(process_instance_id, message = f"No categories found in Classify component", node_name = "Classification", log_type=1)

        target_labels = [c["documentType"] for c in categories]
        label_str = ", ".join(target_labels)
        results = {}

        for doc_type in target_labels:
            cursor.execute("SELECT id FROM DocumentType WHERE documentType = %s", (doc_type,))
            exists = cursor.fetchone()
            if exists:
                cursor.execute("""
                    UPDATE DocumentType
                    SET isActive = 1, updatedAt = NOW()
                    WHERE documentType = %s
                """, (doc_type,))
            else:
                cursor.execute("""
                    INSERT INTO DocumentType (documentType, isActive, createdAt)
                    VALUES (%s, 1, NOW())
                """, (doc_type,))
        conn.commit()
        print("✅ DocumentType table updated → isActive=1 for target document types")
        log_to_mongo(process_instance_id, message = f"No categories found in Classify component", node_name = "Classification", log_type=2)

        for file_name in os.listdir(process_instance_dir_path):
            if not file_name.endswith(".pdf"):
                continue

            file_path = os.path.join(process_instance_dir_path, file_name)
            try:
                print(f"📄 Classifying: {file_name}")
                accumulated_text = ""
                for page_number, page_text in enumerate(extract_text_per_page(file_path, max_pages=20), start=1):
                    accumulated_text += page_text + "\n"
                    prompt = f"""
                    Classify the document based on the following content into one of these categories: {label_str}.
                    Return only the label, nothing else.
                    
                    Content:
                    {accumulated_text[:2000]}
                    """

                    response = openai.ChatCompletion.create(
                        model="gpt-4o",
                        messages=[{"role": "user", "content": prompt}],
                        temperature=0.2,
                        timeout=20,
                    )

                    classification = response["choices"][0]["message"]["content"].strip()
                    print(f"🔍 Page {page_number}: classified as {classification}")
                    log_to_mongo(process_instance_id, message = f"Page {page_number}: classified as {classification}", node_name = "Classification", log_type=2)

                    if classification in target_labels:
                        results[file_name] = classification
                        print(f"✅ {file_name} → {classification} (stopped early at page {page_number})")
                        log_to_mongo(process_instance_id, message = f"{file_name} → {classification} (stopped early at page {page_number})", node_name = "Classification", log_type=2)
                        break
                else:
                    results[file_name] = "Unknown"
                    print(f"⚠️ {file_name} → Unable to classify after scanning max pages")
                    log_to_mongo(process_instance_id, message = f"{file_name} → Unable to classify after scanning max pages", node_name = "Classification", log_type=3)

            except Exception as e:
                results[file_name] = f"Error: {e}"
                print(f"❌ {file_name} → {e}")
                log_to_mongo(process_instance_id, message = f"{file_name} → {e}", node_name = "Classification", log_type=1)

        with open(os.path.join(process_instance_dir_path, "classified_documents.json"), "w") as f:
            json.dump(results, f)
            print("📝 Classification results saved to classified_documents.json")

        for doc_type in target_labels:
            cursor.execute("""
                UPDATE DocumentType
                SET isActive = 0, updatedAt = NOW()
                WHERE documentType = %s
            """, (doc_type,))
        conn.commit()
        print("🔕 DocumentType table updated → isActive=0 after classification.")

        if AUTO_EXECUTE_NEXT_NODE == 1:
            print("🚀 Triggering extract_documents_dag...")
            token = get_auth_token()
            trigger_url = f"{AIRFLOW_API_URL}/dags/extract_documents_dag/dagRuns"
            run_id = f"triggered_by_classify_{process_instance_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
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
            log_to_mongo(process_instance_id, message = f"Successfully triggered extract_documents_dag with ID {process_instance_id}", node_name = "Classification", log_type=2)

    except Exception as e:
        conn.rollback()
        error_message = f"{type(e).__name__}: {str(e)}"
        print(f"❌ Error in classification process: {error_message}")
        log_to_mongo(process_instance_id, message = f"Error in classification process: {error_message}", node_name = "Classification", log_type=1)

        log_to_mongo(
            process_instance_id=process_instance_id,
            node_name="Classification",
            message=error_message,
            log_type=1,
            remark="DAG failed at classification"
        )

        raise

with DAG(
    dag_id="classify_documents_dag",
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    catchup=False,
    tags=["idp", "classification"],
) as dag:

    classify_task = PythonOperator(
        task_id="classify_documents_task",
        python_callable=classify_documents
    )
