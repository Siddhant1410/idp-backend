from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import joblib
import numpy as np
from PyPDF2 import PdfReader
from pdf2image import convert_from_path
import pytesseract
import re
import json
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer

# === CONFIG === #
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ---------- Helpers ----------
def normalize_text(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def extract_pdf_text(file_path, max_pages=5):
    """Extract text with OCR fallback"""
    text_content = ""
    try:
        reader = PdfReader(file_path)
        pages = min(len(reader.pages), max_pages)
        for i in range(pages):
            page_text = reader.pages[i].extract_text()
            if page_text:
                text_content += page_text + "\n"

        if not text_content.strip():  # OCR fallback
            images = convert_from_path(file_path, first_page=1, last_page=max_pages)
            for img in images:
                ocr_text = pytesseract.image_to_string(img)
                if ocr_text.strip():
                    text_content += ocr_text + "\n"

    except Exception as e:
        print(f"⚠️ Failed to process {file_path}: {e}")

    return normalize_text(text_content)


# ---------- Retraining Task ----------
def retrain_classifier(**context):
    NEW_DOCS_FOLDER = "/opt/airflow/new_documents"
    EXISTING_MODELS_DIR = "/opt/airflow/dags/ml_models"

    # Load existing models
    embeddings_pkl = os.path.join(EXISTING_MODELS_DIR, "embeddings.pkl")
    tfidf_pkl = os.path.join(EXISTING_MODELS_DIR, "tfidf_vectors.pkl")
    vectorizer_pkl = os.path.join(EXISTING_MODELS_DIR, "vectorizer.pkl")

    # Try loading existing data, else initialize fresh
    if os.path.exists(tfidf_pkl):
        tfidf_data = joblib.load(tfidf_pkl)
        X_tfidf, labels_tfidf = tfidf_data["vectors"], tfidf_data["labels"]
    else:
        X_tfidf, labels_tfidf = np.empty((0,)), []

    if os.path.exists(embeddings_pkl):
        embeddings_data = joblib.load(embeddings_pkl)
        X_emb, labels_emb = embeddings_data["vectors"], embeddings_data["labels"]
    else:
        X_emb, labels_emb = np.empty((0, 384)), []

    if os.path.exists(vectorizer_pkl):
        vectorizer = joblib.load(vectorizer_pkl)
    else:
        vectorizer = TfidfVectorizer(analyzer="char", ngram_range=(3, 5))

    # Collect new documents
    categories = [d for d in os.listdir(NEW_DOCS_FOLDER) if os.path.isdir(os.path.join(NEW_DOCS_FOLDER, d))]
    texts, labels = [], []

    for category in categories:
        cat_path = os.path.join(NEW_DOCS_FOLDER, category)
        for fname in os.listdir(cat_path):
            if not fname.lower().endswith(".pdf"):
                continue
            fpath = os.path.join(cat_path, fname)
            text = extract_pdf_text(fpath)
            if text.strip():
                texts.append(text)
                labels.append(category)

    if not texts:
        print("⚠️ No new documents found. Skipping retrain.")
        return

    print(f"📂 Found {len(texts)} new documents across {len(set(labels))} categories")

    # Update TF-IDF
    all_labels_tfidf = labels_tfidf + labels
    if X_tfidf.shape[0] > 0:
        X_new_tfidf = vectorizer.transform(texts).toarray()
        X_tfidf = np.vstack([X_tfidf, X_new_tfidf])
    else:
        X_tfidf = vectorizer.fit_transform(texts).toarray()
    joblib.dump({"vectors": X_tfidf, "labels": all_labels_tfidf}, tfidf_pkl)
    joblib.dump(vectorizer, vectorizer_pkl)
    print("✅ Updated TF-IDF vectors")

    # Update Embeddings
    model = SentenceTransformer("all-MiniLM-L6-v2")
    new_embs = model.encode(texts, convert_to_numpy=True)
    all_labels_emb = labels_emb + labels
    if X_emb.shape[0] > 0:
        X_emb = np.vstack([X_emb, new_embs])
    else:
        X_emb = new_embs
    joblib.dump({"vectors": X_emb, "labels": all_labels_emb}, embeddings_pkl)
    print("✅ Updated embeddings")

    print("🎉 Retraining complete. Models updated.")


# ---------- DAG ----------
with DAG(
    dag_id="classifier_retrain_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule=None,  # manual only
    catchup=False,
    tags=["idp", "classifier", "retrain"],
) as dag:

    retrain_task = PythonOperator(
        task_id="retrain_classifier",
        python_callable=retrain_classifier,
    )
