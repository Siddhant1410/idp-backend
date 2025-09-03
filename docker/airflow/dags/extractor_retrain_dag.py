from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import json
import re
import pickle
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
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

# ----------------------------
# Validators
# ----------------------------
def validate_aadhar(number):
    digits = re.sub(r"[^\d]", "", number)
    return len(digits) >= 10

def validate_pan(number):
    cleaned = re.sub(r"[^A-Z0-9]", "", number.upper())
    return 8 <= len(cleaned) <= 12 and bool(re.search(r"[A-Z]", cleaned)) and bool(re.search(r"\d", cleaned))

def validate_drivers_license(number):
    return (
        len(number.strip()) >= 6
        and bool(re.search(r"[A-Za-z]{2,}", number))
        and bool(re.search(r"\d{4,}", number))
    )

def validate_date(date_str):
    date_patterns = [
        r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}",
        r"\d{2,4}[/-]\d{1,2}[/-]\d{1,2}",
        r"\d{1,2}\s+[A-Za-z]+\s+\d{2,4}",
        r"\d{4}",
    ]
    return any(re.search(p, date_str) for p in date_patterns)

def validate_user_name(name):
    words = name.split()
    return len(words) >= 2 and all(re.search(r"[A-Za-z]", w) for w in words)

def validate_address(address):
    return len(address.strip()) >= 10 and bool(re.search(r"[A-Za-z]", address)) and bool(
        re.search(r"[\d\-/#]", address)
    )

FIELD_VALIDATORS = {
    "Aadhar Number": validate_aadhar,
    "PAN Number": validate_pan,
    "Driver's License Number": validate_drivers_license,
    "Date of Issue": validate_date,
    "User Name": validate_user_name,
    "Address": validate_address,
}

# ----------------------------
# Helpers
# ----------------------------
def preprocess_text(text):
    text = text.strip()
    text = re.sub(r"\s+", " ", text)
    return text

def validate_examples(examples):
    validated = {}
    for field, vals in examples.items():
        validator = FIELD_VALIDATORS.get(field)
        validated[field] = []
        for v in vals:
            processed = preprocess_text(v)
            if validator:
                try:
                    if not validator(processed):
                        continue
                except Exception:
                    continue
            validated[field].append(processed)
    return validated

def build_and_save_vectors(examples, out_file):
    tfidf_vectorizer = TfidfVectorizer(analyzer="char", ngram_range=(2, 4), min_df=1, max_features=10000)
    all_texts, labels = [], []
    for field, vals in examples.items():
        for v in vals:
            all_texts.append(v)
            labels.append(field)

    X_tfidf = tfidf_vectorizer.fit_transform(all_texts)
    sbert = SentenceTransformer("all-MiniLM-L6-v2")
    X_emb = sbert.encode(all_texts, convert_to_numpy=True, normalize_embeddings=True)

    field_embeddings = {}
    for field in examples.keys():
        indices = [i for i, lbl in enumerate(labels) if lbl == field]
        if indices:
            field_embeddings[field] = np.mean(X_emb[indices], axis=0)

    data = {
        "fields": list(examples.keys()),
        "examples": examples,
        "labels": labels,
        "tfidf_vectorizer": tfidf_vectorizer,
        "X_tfidf": X_tfidf,
        "embedding_model": "all-MiniLM-L6-v2",
        "X_emb": X_emb,
        "field_embeddings": field_embeddings,
    }

    with open(out_file, "wb") as f:
        pickle.dump(data, f)
    print(f"✅ Saved updated field vectors → {out_file}")

# ----------------------------
# Retrain Task
# ----------------------------
def retrain_field_extractor(**context):
    NEW_EXAMPLES_PATH = "/opt/airflow/retrain_ml_extractor/new_examples.json"  # JSON file with new examples
    EXISTING_MODELS_DIR = "/opt/airflow/dags/ml_models"
    VECTOR_PATH = os.path.join(EXISTING_MODELS_DIR, "field_vectors.pkl")

    # Load existing examples if available
    existing_examples = {}
    if os.path.exists(VECTOR_PATH):
        try:
            with open(VECTOR_PATH, "rb") as f:
                existing_data = pickle.load(f)
            existing_examples = existing_data.get("examples", {})
            print(f"📂 Loaded existing examples from {VECTOR_PATH}")
        except Exception as e:
            print(f"⚠️ Could not load existing vectors: {e}")

    # Load new examples
    if not os.path.exists(NEW_EXAMPLES_PATH):
        raise FileNotFoundError(f"❌ NEW_EXAMPLES_FILE not found: {NEW_EXAMPLES_PATH}")

    with open(NEW_EXAMPLES_PATH, "r") as f:
        new_examples = json.load(f)

    # Merge
    merged_examples = {}
    all_fields = set(existing_examples.keys()) | set(new_examples.keys())
    for field in all_fields:
        merged_examples[field] = list(set(existing_examples.get(field, []) + new_examples.get(field, [])))

    print(f"📊 Total fields after merge: {len(merged_examples)}")

    # Validate & retrain
    validated = validate_examples(merged_examples)
    build_and_save_vectors(validated, VECTOR_PATH)
    print("🎉 Field extractor retrain complete")

# ----------------------------
# DAG
# ----------------------------
with DAG(
    dag_id="field_extractor_retrain_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule=None,  # manual only
    catchup=False,
    tags=["idp", "field_extractor", "retrain"],
) as dag:

    retrain_task = PythonOperator(
        task_id="retrain_field_extractor",
        python_callable=retrain_field_extractor,
    )
