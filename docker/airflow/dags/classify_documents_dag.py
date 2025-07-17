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

load_dotenv() 

AUTO_EXECUTE_NEXT_NODE = 0

# === DAG Trigger CONFIG === #
AIRFLOW_API_URL = "http://airflow-airflow-apiserver-1:8080/api/v2"  # or localhost in local mode
AIRFLOW_USERNAME = "airflow"
AIRFLOW_PASSWORD = "airflow"
LOCAL_MODE = os.getenv("LOCAL_MODE", "false").lower() == "true"

if LOCAL_MODE:
    AIRFLOW_API_URL = "http://localhost:8080/api/v2"

# === CONFIG ===
LOCAL_DOWNLOAD_DIR = "/opt/airflow/downloaded_docs"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # from .env
openai.api_key = OPENAI_API_KEY

if not openai.api_key or not openai.api_key.startswith("sk-") and not openai.api_key.startswith("sk-proj-"):
    raise EnvironmentError("❌ OpenAI API key missing or invalid. Please set OPENAI_API_KEY as an environment variable.")

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

# === OCR Extraction Function === #
def extract_text_from_pdf(pdf_path):
    try:
        reader = PdfReader(pdf_path)
        total_pages = len(reader.pages)
        texts = []
        for i in range(1, total_pages + 1):
            images = convert_from_path(pdf_path, first_page=i, last_page=i)
            if images:
                text = pytesseract.image_to_string(images[0])
                texts.append(text)
        return texts
    except Exception as e:
        print(f"❌ OCR failed for {pdf_path}: {e}")
        return []

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

    # Step 2: Fetch blueprint JSON
    if not os.path.exists(blueprint_path):
        raise FileNotFoundError(f"❌ blueprint.json not found at {blueprint_path}. Please run ingestion DAG first.")

    with open(blueprint_path, "r") as f:
        blueprint = json.load(f)

    # Step 3: Update ProcessInstances table to reflect current stage
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

    # Step 4: Extract classify node
    classify_node = next((n for n in blueprint if n["nodeName"].lower() == "classify"), None)
    if not classify_node:
        raise ValueError("Classification node not found in blueprint")

    categories = classify_node["component"].get("categories", [])
    if not categories:
        raise ValueError("No categories found in Classify component")

    target_labels = [c["documentType"] for c in categories]
    label_str = ", ".join(target_labels)

    results = {}

    # Step 5: Insert or activate document types in DocumentType table
    for doc_type in target_labels:
        # Check if documentType already exists
        cursor.execute("SELECT id FROM DocumentType WHERE documentType = %s", (doc_type,))
        exists = cursor.fetchone()

        if exists:
            # Set isActive = 1 if already present
            cursor.execute("""
                UPDATE DocumentType
                SET isActive = 1, updatedAt = NOW()
                WHERE documentType = %s
            """, (doc_type,))
        else:
            # Insert new documentType
            cursor.execute("""
                INSERT INTO DocumentType (documentType, isActive, createdAt)
                VALUES (%s, 1, NOW())
            """, (doc_type,))
    conn.commit()
    print("✅ DocumentType table updated → isActive=1 for target document types")


    # Step 6: Classify each file using OCR + GenAI
    for file_name in os.listdir(process_instance_dir_path):
        if not file_name.endswith(".pdf"):
            continue

        file_path = os.path.join(process_instance_dir_path, file_name)
        try:
            print(f"📄 Classifying: {file_name}")

            page_text = extract_text_from_pdf(file_path)

            if not page_text:
                raise ValueError("Empty or unreadable text extracted from first few pages.")

            prompt = f"""
            Given this partial document content, classify it into one of these categories: {label_str}.
            Respond with only the most likely document type from the list. No explanation.

            Content:
            {page_text[:1500]}
            """

            response = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                timeout=20,
            )

            classification = response["choices"][0]["message"]["content"].strip()
            results[file_name] = classification

            if classification not in target_labels:
                print(f"⚠️ Warning: {file_name} → unexpected classification: {classification}")
            else:
                print(f"✅ {file_name} → {classification}")

        except Exception as e:
            results[file_name] = f"Error: {e}"
            print(f"❌ {file_name} → {e}")

    # Save classification results to JSON for next DAG
    with open(os.path.join(process_instance_dir_path, "classified_documents.json"), "w") as f:
        json.dump(results, f)
        print("📝 Classification results saved to classified_documents.json")


    # Step 7: Deactivate the document types (mark isActive = 0)
    for doc_type in target_labels:
        cursor.execute("""
            UPDATE DocumentType
            SET isActive = 0, updatedAt = NOW()
            WHERE documentType = %s
        """, (doc_type,))
    conn.commit()
    print("🔕 DocumentType table updated → isActive=0 after classification.")

    # 8. Trigger extract_documents_dag
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


# === DAG Definition ===
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
