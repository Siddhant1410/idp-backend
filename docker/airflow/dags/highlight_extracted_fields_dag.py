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

# === CONFIG ===
LOCAL_DOWNLOAD_DIR = "/opt/airflow/downloaded_docs"
CLEANED_FIELDS_PATH = os.path.join(LOCAL_DOWNLOAD_DIR, "cleaned_extracted_fields.json")
HIGHLIGHTED_PDF_DIR = os.path.join(LOCAL_DOWNLOAD_DIR, "highlighted_docs")
RESPONSE_BODY_PATH = os.path.join(LOCAL_DOWNLOAD_DIR, "response_body.json")
UPLOAD_URL = "http://69.62.81.68:3057/files"

os.makedirs(HIGHLIGHTED_PDF_DIR, exist_ok=True)

def highlight_and_upload(**context):
    # Get process instance ID from DAG run configuration
    process_instance_id = context["dag_run"].conf.get("id")
    if not process_instance_id:
        raise ValueError("Missing process_instance_id in dag_run.conf")

    # Load extracted fields
    with open(CLEANED_FIELDS_PATH, "r") as f:
        extracted_data = json.load(f)

    response_json = []

    for filename in os.listdir(LOCAL_DOWNLOAD_DIR):
        if not filename.lower().endswith(".pdf"):
            continue

        pdf_path = os.path.join(LOCAL_DOWNLOAD_DIR, filename)
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
            (id, extractedFields, fileDetails, createdAt, updatedAt, isActive, isDeleted, isHumanUpdated)
            VALUES (%s, %s, %s, NOW(), NOW(), 1, 0, 1)
            ON DUPLICATE KEY UPDATE 
                extractedFields = VALUES(extractedFields),
                fileDetails = VALUES(fileDetails),
                updatedAt = NOW()
        """, (pid, extracted_fields_json, file_details))

    conn.commit()
    print("✅ Saved extractedFields and fileDetails to DB")

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
