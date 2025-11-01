import argparse
import re
import pickle
import numpy as np
from PyPDF2 import PdfReader
from pdf2image import convert_from_path
import pytesseract
from sklearn.metrics.pairwise import cosine_similarity
import time
import signal
from contextlib import contextmanager
import json
import os
from datetime import datetime

# Timeout decorator to prevent hanging
class TimeoutException(Exception):
    pass

@contextmanager
def time_limit(seconds):
    def signal_handler(signum, frame):
        raise TimeoutException("Timed out!")
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)

# --------- Save extraction results to JSON ---------
def save_extraction_results(document_path, extracted_fields, output_dir=os.getcwd()):
    """
    Save extracted field results to a JSON file
    
    Args:
        document_path (str): Path to the source document
        extracted_fields (dict): Dictionary of field_name: extracted_value pairs
        output_dir (str): Directory to save results (default: "results")
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract document name from path
    document_name = os.path.basename(document_path)
    
    # Prepare the result structure
    result_data = {
        "document": document_name,
        # "extraction_timestamp": datetime.now().isoformat(),
        "fields": extracted_fields
    }
    
    # Generate output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"cleaned_extracted_fields.json"
    output_path = output_filename
    #os.path.join(output_dir, output_filename)
    
    # Save to JSON file
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result_data, f, indent=2, ensure_ascii=False)
        print(f"✅ Results saved to: {output_path}")
        return output_path
    except Exception as e:
        print(f"❌ Failed to save results: {e}")
        return None

# --------- Load vectors from field_vectors.pkl ---------
try:
    with open("field_vectors.pkl", "rb") as f:
        data = pickle.load(f)
    print("✅ Loaded field vectors successfully")
except Exception as e:
    print(f"❌ Failed to load field_vectors.pkl: {e}")
    exit(1)

vectorizer = data.get("tfidf_vectorizer")
X_tfidf = data.get("X_tfidf")
labels = data.get("labels", [])
examples = data.get("examples", [])

# Check if embeddings are available
X_emb = data.get("X_emb", None)
embedding_model_name = "all-MiniLM-L6-v2"

model = None
if X_emb is not None and embedding_model_name:
    try:
        from sentence_transformers import SentenceTransformer
        print(f"🧠 Loading embedding model: {embedding_model_name}")
        model = SentenceTransformer(embedding_model_name)
        print("✅ Embedding model loaded successfully")
    except ImportError:
        print("⚠️ SentenceTransformers not available, using TF-IDF only")
    except Exception as e:
        print(f"⚠️ Failed to load embedding model: {e}")

# --------- Text preprocessing ---------
def preprocess_text(text):
    """Clean and normalize text for better matching"""
    if not text:
        return ""
    text = text.strip()
    text = re.sub(r'\s+', ' ', text)
    return text.lower()

# --------- Extract text from PDF ---------
def extract_text_from_pdf(pdf_path, max_pages=5):
    text_content = []
    try:
        print(f"📄 Extracting text from {pdf_path}...")
        reader = PdfReader(pdf_path)
        num_pages = min(len(reader.pages), max_pages)
        
        for i in range(num_pages):
            text = reader.pages[i].extract_text()
            if text and text.strip():
                text_content.append(text)
            else:
                # OCR fallback with timeout
                try:
                    with time_limit(30):  # 30 second timeout for OCR
                        images = convert_from_path(pdf_path, first_page=i+1, last_page=i+1)
                        if images:
                            text = pytesseract.image_to_string(images[0])
                            if text.strip():
                                text_content.append(text)
                                print(f"🔍 Used OCR for page {i+1}")
                except TimeoutException:
                    print(f"⚠️ OCR timed out on page {i+1}")
                except Exception as e:
                    print(f"⚠️ OCR error on page {i+1}: {e}")
                    
    except Exception as e:
        print(f"⚠️ Error reading {pdf_path}: {e}")
    
    result = "\n".join(text_content)
    print(f"📝 Extracted {len(result)} characters")
    return result

# --------- Improved classification with timeout ---------
def classify_text_for_field(field_name, text, threshold=0.3):
    if not text or not text.strip():
        return None

    # Split text into meaningful chunks
    candidates = re.split(r"[\n\t:;|]", text)
    candidates = [preprocess_text(c) for c in candidates if len(c.strip()) > 3]
    
    if not candidates:
        return None

    print(f"🔍 Analyzing {len(candidates)} text chunks for '{field_name}'...")

    try:
        # Try with embeddings first (with timeout)
        if model and X_emb is not None:
            try:
                with time_limit(60):  # 60 second timeout for embeddings
                    cand_emb = model.encode(candidates, convert_to_numpy=True, normalize_embeddings=True)
                    sims = cosine_similarity(cand_emb, X_emb)
                    
                    best_val, best_score = None, -1
                    for i, cand in enumerate(candidates):
                        for j, label in enumerate(labels):
                            if label == field_name and sims[i][j] > best_score:
                                best_score = sims[i][j]
                                best_val = cand

                    if best_score >= threshold:
                        print(f"✅ Found '{field_name}' with score {best_score:.3f}")
                        return best_val
                    else:
                        print(f"❌ No good match for '{field_name}' (best score: {best_score:.3f})")
                        return None
                        
            except TimeoutException:
                print(f"⚠️ Embedding generation timed out for '{field_name}', falling back to TF-IDF")
            except Exception as e:
                print(f"⚠️ Embedding error for '{field_name}': {e}, falling back to TF-IDF")

        # Fallback to TF-IDF
        print(f"🔄 Using TF-IDF for '{field_name}'...")
        cand_tfidf = vectorizer.transform(candidates)
        sims = cosine_similarity(cand_tfidf, X_tfidf)

        best_val, best_score = None, -1
        for i, cand in enumerate(candidates):
            for j, label in enumerate(labels):
                if label == field_name and sims[i][j] > best_score:
                    best_score = sims[i][j]
                    best_val = cand

        if best_score >= threshold:
            print(f"✅ Found '{field_name}' with TF-IDF score {best_score:.3f}")
            return best_val
        else:
            print(f"❌ No TF-IDF match for '{field_name}' (best score: {best_score:.3f})")
            return None

    except Exception as e:
        print(f"⚠️ Classification error for '{field_name}': {e}")
        return None

# --------- Main ---------
def main():
    parser = argparse.ArgumentParser(description="Classify & extract fields from a PDF")
    parser.add_argument("pdf_path", help="Path to PDF document")
    parser.add_argument("--fields", nargs="+", required=True, help="Target fields to extract")
    parser.add_argument("--threshold", type=float, default=0.3, help="Similarity threshold (0.0-1.0)")
    parser.add_argument("--max-pages", type=int, default=5, help="Maximum pages to process")
    parser.add_argument("--output-dir", type=str, default="results", help="Directory to save JSON results")
    args = parser.parse_args()

    print(f"🚀 Starting extraction from {args.pdf_path}")
    print(f"🎯 Target fields: {args.fields}")
    print(f"📊 Threshold: {args.threshold}")

    text = extract_text_from_pdf(args.pdf_path, args.max_pages)
    if not text or not text.strip():
        print("❌ No text extracted from PDF")
        return

    results = {}
    for field in args.fields:
        print(f"\n--- Processing {field} ---")
        start_time = time.time()
        val = classify_text_for_field(field, text, args.threshold)
        elapsed = time.time() - start_time
        print(f"⏰ Time taken: {elapsed:.2f}s")
        results[field] = val if val else "Not Found"

    # Save results to JSON file
    json_path = save_extraction_results(args.pdf_path, results, args.output_dir)

    print("\n" + "="*50)
    print("📝 EXTRACTION RESULTS:")
    print("="*50)
    for k, v in results.items():
        print(f"  {k}: {v}")
    
    if json_path:
        print(f"\n💾 Results saved to: {json_path}")

if __name__ == "__main__":
    main()

# Example Usage:
# python classify_fields.py "/path/to/doc.pdf" --fields "Aadhar Number" "User Name" "Date of Issue"
# python classify_fields.py "/path/to/doc.pdf" --fields "Aadhar Number" "PAN Number" --output-dir "extraction_results"