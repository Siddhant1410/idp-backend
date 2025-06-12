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

# === OCR Extraction Function === #
def extract_text_from_pdf(pdf_path):
    images = convert_from_path(pdf_path)
    text = ""
    for img in images:
        text += pytesseract.image_to_string(img)
    return text

# === CONFIG ===
LOCAL_DOWNLOAD_DIR = "/opt/airflow/downloaded_docs"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # from .env
openai.api_key = "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"

def classify_documents(**context):
    process_id = context["dag_run"].conf.get("process_id")
    if not process_id:
        raise ValueError("Missing process_id in dag_run.conf")

    hook = MySqlHook(mysql_conn_id="idp_mysql")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Step 1: Fetch bluePrintId
    cursor.execute("SELECT bluePrintId FROM Processes WHERE id = %s", (process_id,))
    row = cursor.fetchone()
    if not row:
        raise ValueError("Process not found")
    blueprint_id = row[0]

    # Step 2: Fetch blueprint JSON
    cursor.execute("SELECT bluePrint FROM BluePrint WHERE id = %s", (blueprint_id,))
    row = cursor.fetchone()
    blueprint = json.loads(row[0])

    # Step 3: Update ProcessInstances table to reflect current stage
    cursor.execute(
        """
        UPDATE ProcessInstances
        SET currentStage = %s,
            isInstanceRunning = %s,
            updatedAt = NOW()
        WHERE processesId = %s
        """,
        ("Classification", 1, process_id)
    )
    conn.commit()
    print(f"🟢 ProcessInstances updated → currentStage='Classification', isInstanceRunning=1 for process_id={process_id}")

    # Step 4: Extract classify node
    classify_node = next((n for n in blueprint if n["nodeName"] == "classify"), None)
    if not classify_node:
        raise ValueError("Classification node not found in blueprint")

    target_labels = classify_node["component"].get("targetClassification", [])
    if not target_labels:
        raise ValueError("targetClassification not defined")

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
    for file_name in os.listdir(LOCAL_DOWNLOAD_DIR):
        if not file_name.endswith(".pdf"):
            continue

        file_path = os.path.join(LOCAL_DOWNLOAD_DIR, file_name)
        try:
            print(f"🧾 Extracting text from: {file_name}")
            extracted_text = extract_text_from_pdf(file_path)

            prompt = f"Given this document content, classify it into one of: {label_str}.\n\nContent:\n{extracted_text}. Return only the type."

            response = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                timeout=30,
            )
            classification = response["choices"][0]["message"]["content"].strip()
            results[file_name] = classification
            print(f"✅ {file_name} → {classification}")
        except Exception as e:
            results[file_name] = f"Error: {e}"
            print(f"❌ {file_name} → {e}")

    # Save classification results to JSON for next DAG
    with open(os.path.join(LOCAL_DOWNLOAD_DIR, "classified_documents.json"), "w") as f:
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
