from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import json
from PyPDF2 import PdfReader
import pytesseract
from pdf2image import convert_from_path
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# === CONFIG ===
LOCAL_DOWNLOAD_DIR = "/opt/airflow/downloaded_docs"
OCR_DPI = 200
TESS_CONFIG = "--psm 6"

# -------------------------
# Text Extraction Helpers
# -------------------------
def extract_page_text(pdf_path: str, page_index: int) -> str:
    try:
        reader = PdfReader(pdf_path)
        if page_index >= len(reader.pages):
            return ""
        text = reader.pages[page_index].extract_text() or ""
        if text.strip():
            return text
    except Exception:
        pass

    # Fallback OCR
    try:
        images = convert_from_path(pdf_path, first_page=page_index+1, last_page=page_index+1, dpi=OCR_DPI)
        if images:
            return pytesseract.image_to_string(images[0], lang="eng", config=TESS_CONFIG).strip()
    except Exception as e:
        print(f"⚠️ OCR failed on {pdf_path} p{page_index+1}: {e}")
    return ""

def extract_text_for_training(pdf_path: str) -> str:
    # training uses only first page for efficiency
    return extract_page_text(pdf_path, 0)

def extract_text_for_classification(pdf_path: str, max_pages=10):
    texts = []
    try:
        reader = PdfReader(pdf_path)
        pages = min(len(reader.pages), max_pages)
    except Exception:
        pages = max_pages
    for i in range(pages):
        texts.append(extract_page_text(pdf_path, i))
    return texts

# -------------------------
# Training
# -------------------------
def train_classifier(categories, process_instance_dir):
    texts, labels = [], []
    for c in categories:
        doc_type = c.get("documentType")
        sample_doc = (c.get("sampleDocument") or {}).get("fileName")
        if not (doc_type and sample_doc):
            continue

        sample_path = os.path.join(process_instance_dir, sample_doc)
        if not os.path.exists(sample_path):
            print(f"⚠️ Sample not found: {sample_doc}")
            continue

        sample_text = extract_text_for_training(sample_path)
        if not sample_text.strip():
            print(f"⚠️ No text in sample: {sample_path}")
            continue

        texts.append(sample_text)
        labels.append(doc_type)

    if not texts:
        raise ValueError("No valid training data from blueprint samples")

    vectorizer = TfidfVectorizer(stop_words="english")
    X = vectorizer.fit_transform(texts)
    return vectorizer, X, labels

# -------------------------
# Classification
# -------------------------
def classify_pdf(file_path, vectorizer, X, labels):
    texts = extract_text_for_classification(file_path, max_pages=10)

    max_scores = {lab: 0.0 for lab in labels}
    for text in texts:
        if not text.strip():
            continue
        Y = vectorizer.transform([text])
        sims = cosine_similarity(Y, X)[0]  # similarity with each sample
        for sim, lab in zip(sims, labels):
            if sim > max_scores[lab]:
                max_scores[lab] = sim

    if not max_scores:
        return "Unknown"

    best_cat = max(max_scores, key=max_scores.get)
    best_score = max_scores[best_cat]
    sorted_scores = sorted(max_scores.values(), reverse=True)
    second_best = sorted_scores[1] if len(sorted_scores) > 1 else 0.0
    margin = best_score - second_best

    print(f"🔎 {os.path.basename(file_path)} scores: {max_scores}")

    # decision rules
    if best_score < 0.25:   # absolute cutoff
        return "Unknown"
    if margin < 0.10:       # categories too close
        return "Unknown"

    return best_cat


# -------------------------
# Main Task
# -------------------------
def ml_classify_documents(**context):
    process_instance_id = context["dag_run"].conf.get("id")
    if not process_instance_id:
        raise ValueError("Missing process_instance_id in dag_run.conf")

    process_instance_dir = os.path.join(LOCAL_DOWNLOAD_DIR, f"process-instance-{process_instance_id}")
    os.makedirs(process_instance_dir, exist_ok=True)

    blueprint_path = os.path.join(process_instance_dir, "blueprint.json")
    if not os.path.exists(blueprint_path):
        raise FileNotFoundError(f"❌ blueprint.json not found at {blueprint_path}")

    with open(blueprint_path, "r") as f:
        blueprint = json.load(f)

    classify_node = next((n for n in blueprint if n.get("nodeName", "").lower() == "classify"), None)
    if not classify_node:
        raise ValueError("Classification node missing in blueprint")

    categories = classify_node.get("component", {}).get("categories", [])
    if not categories:
        raise ValueError("No categories defined in blueprint Classify node")

    vectorizer, X, labels = train_classifier(categories, process_instance_dir)

    results = {}
    for fname in os.listdir(process_instance_dir):
        if not fname.lower().endswith(".pdf"):
            continue
        fpath = os.path.join(process_instance_dir, fname)
        results[fname] = classify_pdf(fpath, vectorizer, X, labels)

    results_path = os.path.join(process_instance_dir, "classified_documents.json")
    with open(results_path, "w") as f:
        json.dump(results, f)
    print(f"📝 Saved classification results → {results_path}")
    return results

# -------------------------
# Airflow DAG
# -------------------------
default_args = {"owner": "airflow", "depends_on_past": False}

with DAG(
    dag_id="ml_classify_documents_dag",
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    catchup=False,
    tags=["idp", "ml_classification"],
) as dag:

    classify_task = PythonOperator(
        task_id="ml_classify_documents",
        python_callable=ml_classify_documents,
    )
