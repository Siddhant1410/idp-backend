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
    text = re.sub(r"[^a-z0-9\s]", " ", text)   # keep alphanumeric
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

        # OCR fallback if no text
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
# Update Vectors
# -----------------------------
def update_vectors(new_docs, new_labels,
                   tfidf_pkl="tfidf_vectors.pkl",
                   embeddings_pkl="embeddings.pkl",
                   vectorizer_pkl="vectorizer.pkl"):

    # Load existing
    tfidf_data = joblib.load(tfidf_pkl)
    embeddings_data = joblib.load(embeddings_pkl)
    vectorizer = joblib.load(vectorizer_pkl)

    X_tfidf = tfidf_data["vectors"]
    labels_tfidf = tfidf_data["labels"]

    vectors_embed = embeddings_data["vectors"]
    labels_embed = embeddings_data["labels"]

    model = SentenceTransformer("all-MiniLM-L6-v2")

    # Process new docs
    new_texts = []
    new_embed_vecs = []
    new_labels_list = []

    for doc, label in zip(new_docs, new_labels):
        text = extract_pdf_text(doc)
        if not text.strip():
            continue

        # TF–IDF vector
        new_texts.append(text)
        # Embedding vector
        embed_vec = model.encode([text], convert_to_numpy=True)
        new_embed_vecs.append(embed_vec[0])
        new_labels_list.append(label)

    if not new_texts:
        print("⚠️ No valid text extracted from new documents")
        return

    # Update TF–IDF
    X_new_tfidf = vectorizer.transform(new_texts)
    X_tfidf = np.vstack([X_tfidf.toarray(), X_new_tfidf.toarray()])
    labels_tfidf.extend(new_labels_list)

    joblib.dump({
        "vectors": X_tfidf,
        "labels": labels_tfidf
    }, tfidf_pkl)

    # Update Embeddings
    vectors_embed = np.vstack([vectors_embed, np.array(new_embed_vecs)])
    labels_embed.extend(new_labels_list)

    joblib.dump({
        "vectors": vectors_embed,
        "labels": labels_embed
    }, embeddings_pkl)

    print("✅ Vectors updated successfully")

# -----------------------------
# Run Script
# -----------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--docs", nargs="+", required=True, help="New PDF docs")
    parser.add_argument("--labels", nargs="+", required=True, help="Labels for docs")
    args = parser.parse_args()

    if len(args.docs) != len(args.labels):
        raise ValueError("Docs and labels count must match")

    update_vectors(args.docs, args.labels)

# Example usage:
# python update_vectors.py --docs "new_pan.pdf" "new_aadhar.pdf" --labels "Pan Card" "Aadhar Card"
