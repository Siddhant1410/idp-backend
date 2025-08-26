import os
import re
import joblib
import numpy as np
from PyPDF2 import PdfReader
from pdf2image import convert_from_path
import pytesseract
from sklearn.feature_extraction.text import TfidfVectorizer
from sentence_transformers import SentenceTransformer

# -----------------------------
# Text Normalization
# -----------------------------
def normalize_text(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)   # keep alphanumeric only
    text = re.sub(r"\s+", " ", text).strip()
    return text

# -----------------------------
# Extract text from PDF (OCR fallback)
# -----------------------------
def extract_pdf_text(file_path, max_pages=5):
    text_content = ""
    try:
        reader = PdfReader(file_path)
        pages = min(len(reader.pages), max_pages)

        for i in range(pages):
            page_text = reader.pages[i].extract_text()
            if page_text:
                text_content += page_text + "\n"

        # Fallback OCR if no text
        if not text_content.strip():
            print(f"🔍 No text found in {os.path.basename(file_path)}, using OCR...")
            images = convert_from_path(file_path, first_page=1, last_page=max_pages)
            for img in images:
                ocr_text = pytesseract.image_to_string(img)
                if ocr_text.strip():
                    text_content += ocr_text + "\n"

    except Exception as e:
        print(f"⚠️ Failed to process {file_path}: {e}")

    return normalize_text(text_content)

# -----------------------------
# Build Vectors
# -----------------------------
def build_vectors(data_dir="data", tfidf_pkl="tfidf_vectors.pkl",
                  embeddings_pkl="embeddings.pkl", vectorizer_pkl="vectorizer.pkl"):

    texts, labels, files = [], [], []

    # Load sample docs per category
    for category in os.listdir(data_dir):
        cpath = os.path.join(data_dir, category)
        if not os.path.isdir(cpath):
            continue
        for fname in os.listdir(cpath):
            if not fname.lower().endswith(".pdf"):
                continue
            fpath = os.path.join(cpath, fname)
            text = extract_pdf_text(fpath)
            if text.strip():
                texts.append(text)
                labels.append(category)
                files.append(fname)
            else:
                print(f"⚠️ Skipped {fname}, no text extracted.")

    if not texts:
        raise ValueError("❌ No valid training data found.")

    # -----------------------------
    # TF-IDF (char-level ngrams)
    # -----------------------------
    vectorizer = TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5))
    X_tfidf = vectorizer.fit_transform(texts)

    joblib.dump({"files": files, "labels": labels, "vectors": X_tfidf.toarray()},
                tfidf_pkl)
    joblib.dump(vectorizer, vectorizer_pkl)
    print(f"✅ Saved TF-IDF vectors to {tfidf_pkl}")

    # -----------------------------
    # Sentence-Transformer Embeddings
    # -----------------------------
    model = SentenceTransformer("all-MiniLM-L6-v2")  # lightweight + accurate
    X_embeddings = model.encode(texts, convert_to_numpy=True, show_progress_bar=True)S

    joblib.dump({"files": files, "labels": labels, "vectors": X_embeddings},
                embeddings_pkl)
    print(f"✅ Saved semantic embeddings to {embeddings_pkl}")

    print("🎉 Training complete: both TF-IDF + embeddings stored.")

# -----------------------------
# Run Script
# -----------------------------
if __name__ == "__main__":
    build_vectors(data_dir="data")
