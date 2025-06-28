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

# === DAG Trigger CONFIG === #
AIRFLOW_API_URL = "http://airflow-airflow-apiserver-1:8080/api/v2"  # or localhost in local mode
AIRFLOW_USERNAME = "airflow"
AIRFLOW_PASSWORD = "airflow"
LOCAL_MODE = os.getenv("LOCAL_MODE", "false").lower() == "true"

if LOCAL_MODE:
    AIRFLOW_API_URL = "http://localhost:8080/api/v2"

# === CONFIG ===
LOCAL_DOWNLOAD_DIR = "/opt/airflow/downloaded_docs"

openai.api_key = "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"

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

def extract_text_from_pdf(pdf_path):
    try:
        images = convert_from_path(pdf_path)
        return " ".join(pytesseract.image_to_string(img) for img in images)
    except Exception as e:
        print(f"❌ OCR failed for {pdf_path}: {e}")
        return ""

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
    with open(BLUEPRINT_PATH, "r") as f:
        blueprint = json.load(f)

    # Load classification results
    if not os.path.exists(CLASSIFIED_JSON_PATH):
        raise FileNotFoundError("❌ classified_documents.json not found.")
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

    rules = extract_node["component"]
    categories = {c["documentType"].lower(): c["id"] for c in rules["categories"]}
    extractors = rules["extractors"]
    extractor_fields = rules["extractorFields"]

    structured_results = []

    for file_name, doc_type in classified_docs.items():
        doc_path = os.path.join(process_instance_dir_path, file_name)
        if not os.path.exists(doc_path):
            print(f"⚠️ File not found: {file_name}")
            continue

        doc_type_lower = doc_type.lower()
        doc_type_id = categories.get(doc_type_lower)
        if not doc_type_id or str(doc_type_id) not in extractor_fields:
            print(f"⚠️ No extraction rules for {doc_type}")
            continue

        ocr_text = extract_text_from_pdf(doc_path)
        field_prompts = extractor_fields[str(doc_type_id)]
        extracted = {}

        for field in field_prompts:
            prompt = f"""
            The following is OCR-extracted text. Extract value for field: "{field['variableName']}".
            Return only the value without any additional text or explanation. If not found, return "N/A".

            Text:
            {ocr_text[:1500]}
            """
            try:
                print(f"🔍 Extracting {field['variableName']} from {file_name}")
                response = openai.ChatCompletion.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2,
                    timeout=30
                )
                value = response["choices"][0]["message"]["content"].strip()
                extracted[field["variableName"]] = value
            except Exception as e:
                extracted[field["variableName"]] = f"Error: {e}"

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

    # Run GenAI-based typo correction
    cleaned_data = correct_typos_with_genai(structured_results)
    with open(CLEANED_FIELDS_PATH, "w") as f:
        json.dump(cleaned_data, f, indent=2)
    print(f"✅ Saved cleaned_extracted_fields.json")

    # Trigger validate_documents_dag
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
