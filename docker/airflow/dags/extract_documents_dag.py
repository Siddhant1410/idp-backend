from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import openai
import os
import json
import pytesseract
import requests
from pdf2image import convert_from_path
from airflow.utils.log.logging_mixin import LoggingMixin
log = LoggingMixin().log

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv() 

AUTO_EXECUTE_NEXT_NODE = 1

# === DAG Trigger CONFIG === #
AIRFLOW_API_URL = "http://airflow-airflow-apiserver-1:8080/api/v2"  # or localhost in local mode
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD")
LOCAL_MODE = os.getenv("LOCAL_MODE", "false").lower() == "true"
MAX_PAGES_TO_SCAN = 100

if LOCAL_MODE:
    AIRFLOW_API_URL = "http://localhost:8080/api/v2"

# === CONFIG ===
LOCAL_DOWNLOAD_DIR = "/opt/airflow/downloaded_docs"
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = "idp"
MONGO_COLLECTION = "LogEntry"
mongo_client = MongoClient(MONGO_URI)
mongo_collection = mongo_client[MONGO_DB_NAME][MONGO_COLLECTION]

openai.api_key = os.getenv("OPENAI_API_KEY")  # from .env
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

def extract_text_from_pdf(pdf_path):
    try:
        from PyPDF2 import PdfReader
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
        log_to_mongo(process_instance_id, message = f"OCR failed for {pdf_path}: {e}", node_name = "Extraction", log_type=1)
        return []

def correct_typos_with_genai(extracted_data):
    try:
        prompt = f"""
        You are a helpful assistant. The following JSON contains field names and their extracted values from OCR-scanned documents.
        Some values may contain spelling errors. Correct typos only in field values.
        Do not make unneccesary corrections in Name unless it is of english Origin.
        Do not change field names, structure or formatting. Return only corrected JSON.

        JSON:
        {json.dumps(extracted_data, indent=2)}
        """
        response = openai.ChatCompletion.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            timeout=60
        )
        content = response["choices"][0]["message"]["content"].strip()
        if content.startswith("```json"):
            content = content.replace("```json", "").replace("```", "").strip()
        return json.loads(content)
    except Exception as e:
        print(f"❌ GenAI typo correction failed: {e}")
        log_to_mongo(process_instance_id, message = f"GenAI typo correction failed: {e}", node_name = "Extraction", log_type=1)
        return extracted_data

def extract_fields_from_documents(**context):
    # Get process instance ID from DAG run configuration
    process_instance_id = context["dag_run"].conf.get("id")
    if not process_instance_id:
        raise ValueError("Missing process_instance_id in dag_run.conf")
    
    process_instance_dir_path = os.path.join(LOCAL_DOWNLOAD_DIR, "process-instance-" + str(process_instance_id))
    os.makedirs(process_instance_dir_path, exist_ok=True)
    EXTRACTED_FIELDS_PATH = os.path.join(process_instance_dir_path, "extracted_fields.json")
    CLEANED_FIELDS_PATH = os.path.join(process_instance_dir_path, "cleaned_extracted_fields.json")
    CLASSIFIED_JSON_PATH = os.path.join(process_instance_dir_path, "classified_documents.json")
    BLUEPRINT_PATH = os.path.join(process_instance_dir_path, "blueprint.json")

    # MySQL connection
    hook = MySqlHook(mysql_conn_id="idp_mysql")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Load blueprint
    if not os.path.exists(BLUEPRINT_PATH):
        raise FileNotFoundError(f"❌ Missing blueprint.json at {BLUEPRINT_PATH}")
        log_to_mongo(process_instance_id, message = f"Missing blueprint.json at {BLUEPRINT_PATH}", node_name = "Extraction", log_type=1)
    with open(BLUEPRINT_PATH, "r") as f:
        blueprint = json.load(f)

    # Load classification results
    if not os.path.exists(CLASSIFIED_JSON_PATH):
        raise FileNotFoundError("❌ classified_documents.json not found.")
        log_to_mongo(process_instance_id, message = f"classified_documents.json not found.", node_name = "Extraction", log_type=1)
    with open(CLASSIFIED_JSON_PATH, "r") as f:
        classified_docs = json.load(f)

    # Update current stage
    cursor.execute("""
        UPDATE ProcessInstances
        SET currentStage = %s, isInstanceRunning = %s, updatedAt = NOW()
        WHERE id = %s
    """, ("Extraction", 1, process_instance_id))
    conn.commit()


    extract_node = next((n for n in blueprint if n["nodeName"].lower() == "extract"), None)
    if not extract_node:
        raise ValueError("❌ No extract node found in blueprint.")
        log_to_mongo(process_instance_id, message = f"No extract node found in blueprint.", node_name = "Extraction", log_type=1)

    rules = extract_node["component"]
    categories = {c["documentType"].lower(): c["id"] for c in rules["categories"]}
    extractors = rules["extractors"]
    extractor_fields = rules["extractorFields"]

    structured_results = []

    for file_name, doc_type in classified_docs.items():
        doc_path = os.path.join(process_instance_dir_path, file_name)
        if not os.path.exists(doc_path):
            print(f"⚠️ File not found: {file_name}")
            log_to_mongo(process_instance_id, message = f"File not found: {file_name}", node_name = "Extraction", log_type=3)
            continue

        doc_type_lower = doc_type.lower()
        doc_type_id = categories.get(doc_type_lower)
        if not doc_type_id or str(doc_type_id) not in extractor_fields:
            print(f"⚠️ No extraction rules for {doc_type}")
            log_to_mongo(process_instance_id, message = f"No extraction rules for {doc_type}", node_name = "Extraction", log_type=3)
            continue

        ocr_text = extract_text_from_pdf(doc_path)
        field_prompts = extractor_fields[str(doc_type_id)]
        extracted = {}

        for field in field_prompts:
            field_name = field["variableName"]
            extracted_value = "N/A"
            for page_num in range(1, MAX_PAGES_TO_SCAN + 1):
                try:
                    images = convert_from_path(doc_path, first_page=page_num, last_page=page_num)
                    if not images:
                        continue
                    page_image = images[0]
                    page_text = pytesseract.image_to_string(page_image)

                    prompt = f"""
                            The following is OCR-extracted text (Page {page_num} of the document). 
                            Extract the value for field: "{field_name}".
                            Return only the value without any additional text or explanation. If not found, return "N/A".

                            Text:
                            {page_text[:1500]}
                                            """

                    log.info(f"🔍 Searching {field_name} from page {page_num} of {file_name}")
                    response = openai.ChatCompletion.create(
                        model="gpt-4o",
                        messages=[{"role": "user", "content": prompt}],
                        temperature=0.2,
                        timeout=30
                    )

                    value = response["choices"][0]["message"]["content"].strip()
                    extracted[field_name] = value

                    if value and value.upper() != "N/A":
                        break  # ✅ Stop once value is found

                except Exception as e:
                    print(f"⚠️ Error extracting {field_name} from page {page_num}: {e}")
                    log_to_mongo(process_instance_id, message = f"Error extracting {field_name} from page {page_num}: {e}", node_name = "Extraction", log_type=1)
                    extracted[field_name] = f"Error: {e}"
                    break

        # Compose final JSON structure
        structured_results.append({
            "documentDetails": {
                "documentName": file_name,
                "documentType": doc_type
            },
            "extractedFields": extracted,
            "processInstanceId": process_instance_id
        }) 

    # Save raw extraction result
    with open(EXTRACTED_FIELDS_PATH, "w") as f:
        json.dump(structured_results, f, indent=2)
    print(f"✅ Saved raw extracted_fields.json")
    log_to_mongo(process_instance_id, message = f"Saved raw extracted_fields.json", node_name = "Extraction", log_type=2)

    # Run GenAI-based typo correction
    cleaned_data = correct_typos_with_genai(structured_results)
    with open(CLEANED_FIELDS_PATH, "w") as f:
        json.dump(cleaned_data, f, indent=2)
    print(f"✅ Saved cleaned_extracted_fields.json")
    log_to_mongo(process_instance_id, message = f"Saved cleaned_extracted_fields.json", node_name = "Extraction", log_type=2)

    # Trigger validate_documents_dag
    if AUTO_EXECUTE_NEXT_NODE == 1:
        print("🚀 Triggering validate_fields_dag...")
        token = get_auth_token()
        trigger_url = f"{AIRFLOW_API_URL}/dags/highlight_extracted_fields_dag/dagRuns"
        run_id = f"triggered_by_extraction_{process_instance_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
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
        print(f"✅ Successfully triggered validate_fields_dag with ID {process_instance_id}")
        log_to_mongo(process_instance_id, message = f"Successfully triggered validate_fields_dag with ID {process_instance_id}", node_name = "Extraction", log_type=2)


# === DAG DEFINITION ===
with DAG(
    dag_id="extract_documents_dag",
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    catchup=False,
    tags=["idp", "extraction"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_fields_from_documents",
        python_callable=extract_fields_from_documents
    )
