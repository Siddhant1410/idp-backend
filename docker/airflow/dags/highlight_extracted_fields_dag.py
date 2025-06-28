from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pytesseract
from pdf2image import convert_from_path
from PIL import Image, ImageDraw
import os
import json
import re
import requests
from airflow.providers.mysql.hooks.mysql import MySqlHook
import openai


# === DAG Trigger CONFIG === #
AIRFLOW_API_URL = "http://airflow-airflow-apiserver-1:8080/api/v2"  # or localhost in local mode
AIRFLOW_USERNAME = "airflow"
AIRFLOW_PASSWORD = "airflow"
LOCAL_MODE = os.getenv("LOCAL_MODE", "false").lower() == "true"

if LOCAL_MODE:
    AIRFLOW_API_URL = "http://localhost:8080/api/v2"

# === CONFIG ===
LOCAL_DOWNLOAD_DIR = "/opt/airflow/downloaded_docs"
UPLOAD_URL = "http://69.62.81.68:3057/files"

#OPENAI API KEY
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
        text = ""
        for img in images:
            text += pytesseract.image_to_string(img)
        return text
    except Exception as e:
        print(f"OCR failed for {pdf_path}: {e}")
        return ""

def highlight_and_upload(**context):
    # Get process instance ID from DAG run configuration
    process_instance_id = context["dag_run"].conf.get("id")
    if not process_instance_id:
        raise ValueError("Missing process_instance_id in dag_run.conf")
    process_instance_dir_path = os.path.join(LOCAL_DOWNLOAD_DIR, "process-instance-" + str(process_instance_id))
    os.makedirs(process_instance_dir_path, exist_ok=True)
    CLEANED_FIELDS_PATH = os.path.join(process_instance_dir_path, "cleaned_extracted_fields.json")
    HIGHLIGHTED_PDF_DIR = os.path.join(process_instance_dir_path, "highlighted_docs")
    os.makedirs(HIGHLIGHTED_PDF_DIR, exist_ok=True)
    RESPONSE_BODY_PATH = os.path.join(process_instance_dir_path, "response_body.json")
    EXTRACTED_FIELDS_PATH = os.path.join(process_instance_dir_path, "cleaned_extracted_fields.json")

    hook = MySqlHook(mysql_conn_id="idp_mysql")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE ProcessInstances
        SET currentStage = %s,
            isInstanceRunning = %s,
            updatedAt = NOW()
        WHERE id = %s
    """, ("Validation", 1, process_instance_id))
    conn.commit()
    print(f"🟢 Updated ProcessInstances to 'Validation' stage for process_instance_id={process_instance_id}")

    # Load extracted fields
    if not os.path.exists(EXTRACTED_FIELDS_PATH):
        raise FileNotFoundError("❌ cleaned_extracted_fields.json not found")

    with open(CLEANED_FIELDS_PATH, "r") as f:
        extracted_data = json.load(f)

    # Step 3: Validate each document
    for doc in extracted_data:
        document_meta = doc.get("documentDetails", {})
        file_name = document_meta.get("documentName")
        doc_type = document_meta.get("documentType")
        fields = doc.get("extractedFields", {})
        process_instance_id = doc.get("processInstanceId")

        if not file_name or not doc_type or not process_instance_id:
            print("❌ Missing critical data in entry, skipping...")
            continue

        doc_path = os.path.join(process_instance_dir_path, file_name)
        if not os.path.exists(doc_path):
            print(f"❌ File missing: {file_name}")
            continue

        ocr_text = extract_text_from_pdf(doc_path)
        score_sum = 0
        valid_fields = 0
        validated_fields = []

        for field_name, extracted_value in fields.items():
            if extracted_value.strip().upper() == "N/A":
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
            Score: <number between 0-100>.
            """

            try:
                response = openai.ChatCompletion.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2
                )
                content = response["choices"][0]["message"]["content"].strip()
                score_match = re.search(r"Score\s*:\s*(\d+)", content)
                if score_match:
                    score = int(score_match.group(1))
                    validated_fields.append({
                        "fieldName": field_name,
                        "fieldValue": extracted_value,
                        "fieldScore": score
                    })
                    score_sum += score
                    valid_fields += 1
                else:
                    print(f"⚠️ Score not found in response: {content}")
            except Exception as e:
                print(f"❌ Validation failed for {field_name}: {e}")

        # Replace raw dict with validated list
        doc["extractedFields"] = validated_fields
        overall_score = round(score_sum / valid_fields) if valid_fields else 0

        # Step 4: Save documentDetails + overAllScore
        document_details_json = json.dumps({
            "documentName": file_name,
            "documentType": doc_type
        })

        # Step 5: Write updated fields with fieldScore back to file
        with open(EXTRACTED_FIELDS_PATH, "w") as f:
            json.dump(extracted_data, f, indent=2)
        print(f"✅ Updated extracted_fields with fieldScore in {EXTRACTED_FIELDS_PATH}")


################################################## HIGHLIGHT DOCUMENT BELOW THIS POINT ########################################

    response_json = []

    for filename in os.listdir(process_instance_dir_path):
        if not filename.lower().endswith(".pdf"):
            continue

        pdf_path = os.path.join(process_instance_dir_path, filename)
        print(f"📄 Processing {filename}")

        # Try to get matching extracted fields
        matched = next((item for item in extracted_data if item.get("documentDetails", {}).get("documentName") == filename), None)
        extracted_fields = matched.get("extractedFields", []) if matched else []
        process_instance_id_ = matched.get("processInstanceId") if matched else process_instance_id

        # Highlight logic
        images = convert_from_path(pdf_path)
        highlighted_images = []

        for page_num, image in enumerate(images):
            overlay = Image.new('RGBA', image.size, (255, 255, 255, 0))
            overlay_draw = ImageDraw.Draw(overlay)

            if image.mode != 'RGB':
                image = image.convert('RGB')

            draw = ImageDraw.Draw(image)
            ocr_data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DICT)

            text_blocks = []
            for i in range(len(ocr_data["text"])):
                if ocr_data["text"][i].strip() and int(ocr_data["conf"][i]) > 60:
                    text_blocks.append({
                        "text": ocr_data["text"][i].lower(),
                        "left": ocr_data["left"][i],
                        "top": ocr_data["top"][i],
                        "width": ocr_data["width"][i],
                        "height": ocr_data["height"][i],
                        "conf": int(ocr_data["conf"][i])
                    })

            for field in extracted_fields:
                val = field.get("fieldValue", "")
                val_words = re.findall(r'[a-z0-9]+', val.lower())
                if not val_words:
                    continue
                for i in range(len(text_blocks) - len(val_words) + 1):
                    match = all(val_words[j] == text_blocks[i + j]["text"] for j in range(len(val_words)))
                    if match:
                        x = text_blocks[i]["left"]
                        y = text_blocks[i]["top"]
                        x_end = text_blocks[i + len(val_words) - 1]["left"] + text_blocks[i + len(val_words) - 1]["width"]
                        y_end = text_blocks[i + len(val_words) - 1]["top"] + text_blocks[i + len(val_words) - 1]["height"]
                        overlay_draw.rectangle(
                            [(x, y), (x_end, y_end)],
                            fill=(255, 255, 102, 100),
                            outline=(255, 204, 0, 200),
                            width=1
                        )
                        break

            image = Image.alpha_composite(image.convert('RGBA'), overlay).convert('RGB')
            highlighted_images.append(image)

        # Save the new PDF
        output_pdf_path = os.path.join(HIGHLIGHTED_PDF_DIR, filename)
        highlighted_images[0].save(
            output_pdf_path,
            save_all=True,
            append_images=highlighted_images[1:],
            format="PDF"
        )
        print(f"✅ Highlighted PDF saved: {output_pdf_path}")

        # Upload the highlighted PDF
        with open(output_pdf_path, "rb") as f:
            files = {"file": (filename, f, "application/pdf")}
            response = requests.post(UPLOAD_URL, files=files)

            if response.status_code == 200:
                print(f"✅ Uploaded: {filename}")
                upload_response = response.json()
                response_json.append({
                    "processInstanceId": process_instance_id_,
                    "response": upload_response
                })
            else:
                print(f"❌ Upload failed for {filename}: {response.status_code} - {response.text}")

        os.remove(output_pdf_path)
        print(f"🧹 Removed local file: {output_pdf_path}")

    # Save the response body
    with open(RESPONSE_BODY_PATH, "w") as f:
        json.dump(response_json, f, indent=2)

    # Save extractedFields and fileDetails to DB
    hook = MySqlHook(mysql_conn_id="idp_mysql")
    conn = hook.get_conn()
    cursor = conn.cursor()

    for upload in response_json:
        pid = upload.get("processInstanceId")
        file_info_list = upload.get("response", {}).get("files", [])
        file_details = json.dumps(file_info_list[0]) if file_info_list else "{}"

        # Find matching extracted fields list from new format
        extracted_fields_list = []
        for item in extracted_data:
            if item.get("processInstanceId") == pid:
                extracted_fields_list = item.get("extractedFields", [])
                break
        extracted_fields_json = json.dumps(extracted_fields_list)

        cursor.execute("""
            INSERT INTO ProcessInstanceDocuments 
            (processInstancesId, documentDetails, extractedFields, fileDetails, overAllScore, createdAt, updatedAt, isActive, isDeleted, isHumanUpdated)
            VALUES (%s, %s, %s, %s, %s, NOW(), NOW(), 1, 0, 1)
            ON DUPLICATE KEY UPDATE 
                documentDetails = VALUES(documentDetails),
                extractedFields = VALUES(extractedFields),
                fileDetails = VALUES(fileDetails),
                overAllScore = VALUES(overAllScore),
                updatedAt = NOW()
        """, (process_instance_id, document_details_json, extracted_fields_json, file_details, overall_score))

    conn.commit()
    print("✅ Saved extractedFields and fileDetails to DB")
    print(f"✅ Stored metadata for {file_name} with avg score {overall_score}%")

    # Trigger deliver_dag
    print("🚀 Triggering deliver_dag...")
    token = get_auth_token()
    trigger_url = f"{AIRFLOW_API_URL}/dags/deliver_dag/dagRuns"
    run_id = f"triggered_by_validate_human_{process_instance_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
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
    print(f"✅ Successfully triggered deliver_dag with ID {process_instance_id}")

# === DAG DEFINITION ===
with DAG(
    dag_id="highlight_extracted_fields_dag",
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    catchup=False,
    tags=["idp", "highlighting"]
) as dag:

    highlight_task = PythonOperator(
        task_id="highlight_extracted_fields",
        python_callable=highlight_and_upload,
    )
