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
EXTRACTED_FIELDS_PATH = os.path.join(LOCAL_DOWNLOAD_DIR, "cleaned_extracted_fields.json")
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
        doc_type = doc_info.get("document_type", "").lower()
        values_to_highlight = [(k, v) for k, v in fields.items()]

        print(f"📄 Processing {file_name} (Type: {doc_type})...")

        # Convert PDF to images (1 image per page)
        images = convert_from_path(pdf_path)
        highlighted_images = []

        for page_num, image in enumerate(images):
            # Create a transparent overlay layer
            overlay = Image.new('RGBA', image.size, (255, 255, 255, 0))
            overlay_draw = ImageDraw.Draw(overlay)
            
            # Original image (convert to RGB if needed)
            if image.mode != 'RGB':
                image = image.convert('RGB')
            draw = ImageDraw.Draw(image)
            
            ocr_data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DICT)
            
            # Get all OCR text with positions and confidence
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

            text_blocks.sort(key=lambda x: (x["top"], x["left"]))

            for field_name, val in values_to_highlight:
                if not val or val == "N/A":
                    continue

                val_lower = val.lower()
                val_words = re.findall(r'[a-z0-9]+', val_lower)
                n = len(val_words)
                matched = False

                # Very transparent yellow (10% opacity)
                highlight_color = (255, 255, 102, 80)  # Light yellow with 10% opacity
                outline_color = (255, 204, 0, 200)  # Slightly transparent border

                # Strategy 1: Exact contiguous match
                for i in range(len(text_blocks) - n + 1):
                    block_text = ' '.join(b["text"] for b in text_blocks[i:i+n])
                    if block_text == ' '.join(val_words):
                        x = text_blocks[i]["left"]
                        y = text_blocks[i]["top"]
                        x_end = text_blocks[i+n-1]["left"] + text_blocks[i+n-1]["width"]
                        y_end = text_blocks[i+n-1]["top"] + text_blocks[i+n-1]["height"]
                        
                        # Draw on overlay
                        overlay_draw.rectangle(
                            [(x, y), (x_end, y_end)],
                            fill=highlight_color,
                            outline=outline_color,
                            width=1
                        )
                        matched = True
                        print(f"✅ Exact match for {field_name}")
                        break

                if matched:
                    continue

                # Strategy 2: Field-specific positional matching
                if doc_type in ["pan", "moa"]:
                    label_pos = None
                    for i, block in enumerate(text_blocks):
                        if field_name.lower() in block["text"]:
                            label_pos = (block["left"], block["top"], block["width"], block["height"])
                            break
                    
                    if label_pos:
                        for block in text_blocks:
                            if (block["left"] > label_pos[0] and 
                                abs(block["top"] - label_pos[1]) < label_pos[3] * 1.5):
                                overlay_draw.rectangle(
                                    [(block["left"], block["top"]), 
                                     (block["left"] + block["width"], block["top"] + block["height"])],
                                    fill=highlight_color,
                                    outline=(0, 102, 204, 200),  # Blue border
                                    width=1
                                )
                                matched = True
                                print(f"⚠️ Positional match for {field_name}")
                                break

                if matched:
                    continue

                # Strategy 3: Partial/fuzzy matching
                best_match = None
                best_score = 0
                min_match_length = max(3, len(val_words) // 2)

                for i in range(len(text_blocks) - min_match_length + 1):
                    match_length = min(len(val_words), len(text_blocks) - i)
                    matched_words = 0
                    bounds = {
                        "left": text_blocks[i]["left"],
                        "top": text_blocks[i]["top"],
                        "right": text_blocks[i]["left"] + text_blocks[i]["width"],
                        "bottom": text_blocks[i]["top"] + text_blocks[i]["height"]
                    }

                    for j in range(match_length):
                        if val_words[j] == text_blocks[i+j]["text"]:
                            matched_words += 1
                            bounds["left"] = min(bounds["left"], text_blocks[i+j]["left"])
                            bounds["top"] = min(bounds["top"], text_blocks[i+j]["top"])
                            bounds["right"] = max(bounds["right"], text_blocks[i+j]["left"] + text_blocks[i+j]["width"])
                            bounds["bottom"] = max(bounds["bottom"], text_blocks[i+j]["top"] + text_blocks[i+j]["height"])

                    current_score = matched_words / len(val_words)
                    if current_score > best_score and current_score > 0.5:
                        best_score = current_score
                        best_match = bounds

                if best_match:
                    overlay_draw.rectangle(
                        [(best_match["left"], best_match["top"]), 
                         (best_match["right"], best_match["bottom"])],
                        fill=highlight_color,
                        outline=(255, 153, 51, 200),  # Orange border
                        width=1
                    )
                    print(f"⚠️ Partial match ({int(best_score*100)}%) for {field_name}")
                    continue

                # Final fallback: Single word matches
                for block in text_blocks:
                    if any(word == block["text"] for word in val_words):
                        overlay_draw.rectangle(
                            [(block["left"], block["top"]), 
                             (block["left"] + block["width"], block["top"] + block["height"])],
                            fill=highlight_color,
                            outline=(204, 51, 51, 200),  # Red border
                            width=1
                        )
                        print(f"⚠️ Single word match for {field_name}")

            # Combine original image with overlay
            image = Image.alpha_composite(image.convert('RGBA'), overlay).convert('RGB')
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
