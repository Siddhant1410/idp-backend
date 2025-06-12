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
CLASSIFIED_JSON_PATH = os.path.join(LOCAL_DOWNLOAD_DIR, "classified_documents.json")

openai.api_key = "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"

def extract_text_from_pdf(pdf_path):
    try:
        images = convert_from_path(pdf_path)
        text = ""
        for img in images:
            text += pytesseract.image_to_string(img)
        return text
    except Exception as e:
        print(f"OCR failed for {pdf_path}: {e}")
        return ""

def extract_fields_from_documents(**context):
    process_id = context["dag_run"].conf.get("process_id")
    if not process_id:
        raise ValueError("Missing process_id in dag_run.conf")

    # Step 1: DB connection
    hook = MySqlHook(mysql_conn_id="idp_mysql")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Step 2: Fetch blueprint ID
    cursor.execute("SELECT bluePrintId FROM Processes WHERE id = %s", (process_id,))
    row = cursor.fetchone()
    if not row:
        raise ValueError("Process not found.")
    blueprint_id = row[0]

    # Step 3: Fetch blueprint JSON
    cursor.execute("SELECT bluePrint FROM BluePrint WHERE id = %s", (blueprint_id,))
    row = cursor.fetchone()
    blueprint = json.loads(row[0])

    # Step 4: Update currentStage in ProcessInstances
    cursor.execute("""
        UPDATE ProcessInstances
        SET currentStage = %s,
            isInstanceRunning = %s,
            updatedAt = NOW()
        WHERE processesId = %s
    """, ("Extraction", 1, process_id))
    conn.commit()
    print(f"🟢 Updated ProcessInstances to stage 'Extraction' for process_id={process_id}")

    # Step 5: Load classified document results
    if not os.path.exists(CLASSIFIED_JSON_PATH):
        raise FileNotFoundError("classified_documents.json not found. Please run classification DAG first.")

    with open(CLASSIFIED_JSON_PATH, "r") as f:
        classified_docs = json.load(f)

    # Step 6: Extract extraction rules from blueprint
    extract_node = next((n for n in blueprint if n["nodeName"] == "extract"), None)
    if not extract_node:
        raise ValueError("Extraction node not found in blueprint.")

    rules = extract_node["component"]
    extracted_results = {}

    for file_name, doc_type in classified_docs.items():
        doc_path = os.path.join(LOCAL_DOWNLOAD_DIR, file_name)
        if not os.path.exists(doc_path):
            print(f"❌ File not found: {file_name}")
            continue

        matching_rule = next((r for r in rules if r["documentCat"] == doc_type), None)
        if not matching_rule:
            print(f"❌ No extraction rule matched for document {file_name} of type {doc_type}")
            continue

        fields = matching_rule.get("extractionFields", [])
        extracted = {}
        ocr_text = extract_text_from_pdf(doc_path)

        for field in fields:
            #prompt = f"{field['prompt']}\n\nDocument text:\n{ocr_text[:1500]}\n From the given document text, extract only the value from the field. Return the result as a plain string, without any explanation or formatting. Fix any general Typos if you feel there are any."
            prompt = f"""
            The following is an OCR-scanned document text. It may contain minor spelling errors due to OCR.

            Please:
            1. Correct any obvious OCR typos internally (no need to return the corrected text), especially for address.
            2. Then, extract only the value for the field: "{field['fieldName']}".
            3. If any field value is not present, just return N/A instead of explanation.

            Respond with only the extracted value as plain text. No labels or explanation.

            Text:
            {ocr_text[:1500]}
            """
            try:
                print(f"🔍 Extracting '{field['fieldName']}' from {file_name}...")
                response = openai.ChatCompletion.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2,
                    timeout=30
                )
                result = response["choices"][0]["message"]["content"].strip()
                extracted[field["fieldName"]] = result
                print(f"✅ {field['fieldName']} → {result}")
            except Exception as e:
                extracted[field["fieldName"]] = f"Error: {str(e)}"
                print(f"❌ Error extracting {field['fieldName']}: {e}")

        extracted_results[file_name] = {
            "documentType": doc_type,
            "fields": extracted
        }

    # Step 7: Save to file
    with open(EXTRACTED_FIELDS_PATH, "w") as f:
        json.dump(extracted_results, f, indent=2)

    print(f"✅ Extraction complete. Results saved to {EXTRACTED_FIELDS_PATH}")



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
