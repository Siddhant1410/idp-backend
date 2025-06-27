from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import openai
import os
import json
import pytesseract
from pdf2image import convert_from_path

# === CONFIG ===
LOCAL_DOWNLOAD_DIR = "/opt/airflow/downloaded_docs"
EXTRACTED_FIELDS_PATH = os.path.join(LOCAL_DOWNLOAD_DIR, "extracted_fields.json")
CLEANED_FIELDS_PATH = os.path.join(LOCAL_DOWNLOAD_DIR, "cleaned_extracted_fields.json")
CLASSIFIED_JSON_PATH = os.path.join(LOCAL_DOWNLOAD_DIR, "classified_documents.json")
BLUEPRINT_PATH = os.path.join(LOCAL_DOWNLOAD_DIR, "blueprint.json")

openai.api_key = "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"

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
    process_id = context["dag_run"].conf.get("process_id")
    if not process_id:
        raise ValueError("Missing process_id in dag_run.conf")

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
        WHERE processesId = %s
    """, ("Extraction", 1, process_id))
    conn.commit()

    # Fetch processInstanceId
    cursor.execute("SELECT id FROM ProcessInstances WHERE processesId = %s ORDER BY createdAt DESC LIMIT 1", (process_id,))
    instance_row = cursor.fetchone()
    if not instance_row:
        raise ValueError("❌ ProcessInstance ID not found.")
    process_instance_id = instance_row[0]

    extract_node = next((n for n in blueprint if n["nodeName"].lower() == "extract"), None)
    if not extract_node:
        raise ValueError("❌ No extract node found in blueprint.")

    rules = extract_node["component"]
    categories = {c["documentType"].lower(): c["id"] for c in rules["categories"]}
    extractors = rules["extractors"]
    extractor_fields = rules["extractorFields"]

    structured_results = []

    for file_name, doc_type in classified_docs.items():
        doc_path = os.path.join(LOCAL_DOWNLOAD_DIR, file_name)
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
