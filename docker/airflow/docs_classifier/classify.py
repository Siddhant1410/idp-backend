import os
import re
import joblib
import numpy as np
from PyPDF2 import PdfReader
from pdf2image import convert_from_path
import pytesseract
from sklearn.metrics.pairwise import cosine_similarity
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

        # OCR fallback if nothing extracted
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
# Classify Document
# -----------------------------
def classify_document(file_path,
                      tfidf_pkl="tfidf_vectors.pkl",
                      embeddings_pkl="embeddings.pkl",
                      vectorizer_pkl="vectorizer.pkl",
                      threshold_embed=0.35,
                      threshold_tfidf=0.25):

    # Load stored vectors
    tfidf_data = joblib.load(tfidf_pkl)
    embeddings_data = joblib.load(embeddings_pkl)
    vectorizer = joblib.load(vectorizer_pkl)

    labels_embed = embeddings_data["labels"]
    vectors_embed = embeddings_data["vectors"]

    labels_tfidf = tfidf_data["labels"]
    vectors_tfidf = tfidf_data["vectors"]

    # Extract and normalize text
    text = extract_pdf_text(file_path)
    if not text.strip():
        return "Unknown"

    # -----------------------------
    # 1. Semantic Embedding Classification
    # -----------------------------
    model = SentenceTransformer("all-MiniLM-L6-v2")
    query_vec = model.encode([text], convert_to_numpy=True)

    sims = cosine_similarity(query_vec, vectors_embed)[0]
    best_idx = sims.argmax()
    best_score = sims[best_idx]

    print(f"🔹 Embedding similarity best score = {best_score:.3f}")

    if best_score >= threshold_embed:
        prediction = labels_embed[best_idx]
        print(f"✅ Classified via Embeddings as: {prediction}")
        return prediction

    # -----------------------------
    # 2. TF–IDF Fallback Classification
    # -----------------------------
    query_tfidf = vectorizer.transform([text]).toarray()
    sims_tfidf = cosine_similarity(query_tfidf, vectors_tfidf)[0]
    best_idx_tfidf = sims_tfidf.argmax()
    best_score_tfidf = sims_tfidf[best_idx_tfidf]

    print(f"🔹 TF–IDF similarity best score = {best_score_tfidf:.3f}")

    if best_score_tfidf >= threshold_tfidf:
        prediction = labels_tfidf[best_idx_tfidf]
        print(f"✅ Classified via TF–IDF as: {prediction}")
        return prediction

    print("⚠️ Could not confidently classify → Unknown")
    return "Unknown"

# -----------------------------
# Run Script
# -----------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("pdf_path", help="Path to PDF to classify")
    args = parser.parse_args()

    result = classify_document(args.pdf_path)
    print(f"📄 Final Classification: {result}")

# Example usage:
# python classify.py "sample_document.pdf"
