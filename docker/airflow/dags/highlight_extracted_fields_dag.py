from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pytesseract
from pdf2image import convert_from_path
from PIL import Image, ImageDraw
import img2pdf
import os
import json
import re

# === CONFIG ===
LOCAL_DOWNLOAD_DIR = "/opt/airflow/downloaded_docs"
EXTRACTED_FIELDS_PATH = os.path.join(LOCAL_DOWNLOAD_DIR, "extracted_fields.json")
HIGHLIGHTED_PDF_DIR = os.path.join(LOCAL_DOWNLOAD_DIR, "highlighted_docs")
os.makedirs(HIGHLIGHTED_PDF_DIR, exist_ok=True)

def highlight_fields():
    with open(EXTRACTED_FIELDS_PATH, "r") as f:
        extracted_data = json.load(f)

    for file_name, doc_info in extracted_data.items():
        pdf_path = os.path.join(LOCAL_DOWNLOAD_DIR, file_name)
        if not os.path.exists(pdf_path):
            print(f"❌ Skipping missing file: {pdf_path}")
            continue

        fields = doc_info.get("fields", {})
        values_to_highlight = list(fields.values())

        print(f"📄 Processing {file_name}...")

        # Convert PDF to images (1 image per page)
        images = convert_from_path(pdf_path)
        highlighted_images = []

        for page_num, image in enumerate(images):
            draw = ImageDraw.Draw(image)
            ocr_data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DICT)

            for val in values_to_highlight:
                if not val or val == "N/A":
                    continue

                val_words = re.findall(r'\w+', val.lower())
                ocr_words = [w.lower() for w in ocr_data["text"]]
                n = len(val_words)

                for i in range(len(ocr_words) - n + 1):
                    if ocr_words[i:i+n] == val_words:
                        x = ocr_data["left"][i]
                        y = ocr_data["top"][i]
                        x_end = ocr_data["left"][i+n-1] + ocr_data["width"][i+n-1]
                        y_end = ocr_data["top"][i+n-1] + ocr_data["height"][i+n-1]

                        draw.rectangle(
                            [(x, y), (x_end, y_end)],
                            outline="yellow",
                            width=2
                        )
                        break  # stop after the first match for this value


            highlighted_images.append(image)

        # Save images back as a new PDF
        output_pdf_path = os.path.join(HIGHLIGHTED_PDF_DIR, file_name)
        highlighted_images[0].save(
            output_pdf_path,
            save_all=True,
            append_images=highlighted_images[1:],
            format="PDF"
        )
        print(f"✅ Highlighted PDF saved: {output_pdf_path}")

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
        python_callable=highlight_fields
    )
