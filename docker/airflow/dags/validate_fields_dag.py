from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
from pdf2image import convert_from_path
import pytesseract
import openai
import os
import json
import re

# === CONFIG ===
LOCAL_DOWNLOAD_DIR = "/opt/airflow/downloaded_docs"
EXTRACTED_FIELDS_PATH = os.path.join(LOCAL_DOWNLOAD_DIR, "cleaned_extracted_fields.json")
VALIDATED_FIELDS_PATH = os.path.join(LOCAL_DOWNLOAD_DIR, "validated_fields.json")

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

def validate_extracted_fields(**context):
    process_id = context["dag_run"].conf.get("process_id")
    if not process_id:
        raise ValueError("Missing process_id in dag_run.conf")

    # Update currentStage
    hook = MySqlHook(mysql_conn_id="idp_mysql")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE ProcessInstances
        SET currentStage = %s,
            isInstanceRunning = %s,
            updatedAt = NOW()
        WHERE processesId = %s
    """, ("Validation", 1, process_id))
    conn.commit()

    if not os.path.exists(EXTRACTED_FIELDS_PATH):
        raise FileNotFoundError("❌ extracted_fields.json not found.")

    with open(EXTRACTED_FIELDS_PATH, "r") as f:
        extracted_data = json.load(f)

    results = {}
    for file_name, doc_info in extracted_data.items():
        doc_path = os.path.join(LOCAL_DOWNLOAD_DIR, file_name)
        if not os.path.exists(doc_path):
            print(f"❌ File missing: {file_name}")
            continue

        ocr_text = extract_text_from_pdf(doc_path)
        doc_type = doc_info["documentType"]
        fields = doc_info["fields"]

        field_scores = {}
        score_sum = 0
        valid_score_count = 0

        for field_name, extracted_value in fields.items():
            if extracted_value.strip().upper() == "N/A":
                field_scores[field_name] = {
                    "score": 0,
                    "reason": "Value marked as N/A — skipping validation"
                }
                continue

            prompt = f"""
You are validating extracted data from a scanned document.

Document Type: {doc_type}

OCR Text:
{ocr_text[:1500]}

Field Name: {field_name}
Extracted Value: {extracted_value}

Validation Rules:
- Look for the extracted value in the OCR text.
- Minor spelling errors or OCR typos should not reduce the score.
- Only consider the format or presence of value.
- Do NOT penalize spelling mistakes unless they alter the actual meaning.

Return result in this format:
Score: <number between 0-100>. Reason: <very short explanation>.
"""

            try:
                response = openai.ChatCompletion.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2
                )
                content = response["choices"][0]["message"]["content"].strip()

                score_match = re.search(r"Score\s*:\s*(\d+)", content)
                reason_match = re.search(r"Reason\s*:\s*(.+)", content)

                if score_match and reason_match:
                    score = int(score_match.group(1))
                    reason = reason_match.group(1).strip()
                    field_scores[field_name] = {"score": score, "reason": reason}
                    if score > 0:
                        score_sum += score
                        valid_score_count += 1
                else:
                    raise ValueError(f"Invalid format: {content}")

            except Exception as e:
                print(f"❌ Validation failed for {field_name}: {e}")
                field_scores[field_name] = {"score": 0, "reason": str(e)}

        avg_score = round(score_sum / valid_score_count, 2) if valid_score_count else 0
        results[file_name] = {
            "documentType": doc_type,
            "average_score": avg_score,
            "fields": field_scores
        }

    with open(VALIDATED_FIELDS_PATH, "w") as f:
        json.dump(results, f, indent=2)

    print(f"✅ Validation complete. Results saved to {VALIDATED_FIELDS_PATH}")


# === DAG DEFINITION ===
with DAG(
    dag_id="validate_documents_dag",
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    catchup=False,
    tags=["idp", "validation"]
) as dag:

    validate_task = PythonOperator(
        task_id="validate_extracted_fields",
        python_callable=validate_extracted_fields
    )
