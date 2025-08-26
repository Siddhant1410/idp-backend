import os
import json
import re
import pickle
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sentence_transformers import SentenceTransformer

# ----------------------------
# Very permissive regex validation rules - focus on pattern recognition, not strict validation
# ----------------------------
FIELD_PATTERNS = {
    "Aadhar Number": r".*\d{4}.*\d{4}.*\d{4}.*",  # Just look for 3 groups of 4 digits
    "PAN Number": r".*[A-Z]{5}.*[0-9]{4}.*[A-Z].*",  # Look for pattern: 5 letters, 4 digits, 1 letter
    "Driver's License Number": r".*[A-Z]{2}.*\d{2}.*\d{4,}.*",  # Look for state code + numbers
    "Date of Issue": r".*(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{2,4}[/-]\d{1,2}[/-]\d{1,2}|\d{1,2}\s+[A-Za-z]+\s+\d{2,4}).*"  # Very flexible date patterns
}

# ----------------------------
# Field-specific validation functions - much more permissive
# ----------------------------
def validate_aadhar(number):
    """Very permissive Aadhar validation"""
    # Just check if it contains 12 digits (with optional spaces/dashes)
    digits = re.sub(r'[^\d]', '', number)
    return len(digits) >= 10  # Allow some flexibility

def validate_pan(number):
    """Very permissive PAN validation"""
    # Remove all non-alphanumeric characters
    cleaned = re.sub(r'[^A-Z0-9]', '', number.upper())
    # Should be approximately 10 characters with mix of letters and numbers
    if len(cleaned) < 8 or len(cleaned) > 12:
        return False
    # Should contain both letters and numbers
    has_letters = bool(re.search(r'[A-Z]', cleaned))
    has_digits = bool(re.search(r'\d', cleaned))
    return has_letters and has_digits

def validate_drivers_license(number):
    """Very permissive driver's license validation"""
    # Should contain at least 2 letters (state code) and some numbers
    if len(number.strip()) < 6:
        return False
    has_letters = bool(re.search(r'[A-Za-z]{2,}', number))
    has_digits = bool(re.search(r'\d{4,}', number))  # At least 4 consecutive digits
    return has_letters and has_digits

def validate_date(date_str):
    """Very permissive date validation"""
    # Look for any date-like patterns
    date_patterns = [
        r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',
        r'\d{2,4}[/-]\d{1,2}[/-]\d{1,2}',
        r'\d{1,2}\s+[A-Za-z]+\s+\d{2,4}',
        r'\d{4}'
    ]
    for pattern in date_patterns:
        if re.search(pattern, date_str):
            return True
    return False

def validate_user_name(name):
    """Basic name validation"""
    # Should contain at least two words with letters
    words = name.split()
    if len(words) < 2:
        return False
    # Each word should contain letters
    for word in words:
        if not re.search(r'[A-Za-z]', word):
            return False
    return True

def validate_address(address):
    """Basic address validation"""
    # Should be reasonably long and contain alphanumeric characters
    if len(address.strip()) < 10:
        return False
    has_letters = bool(re.search(r'[A-Za-z]', address))
    has_digits_or_special = bool(re.search(r'[\d\-/#]', address))
    return has_letters and has_digits_or_special

FIELD_VALIDATORS = {
    "Aadhar Number": validate_aadhar,
    "PAN Number": validate_pan,
    "Driver's License Number": validate_drivers_license,
    "Date of Issue": validate_date,
    "User Name": validate_user_name,
    "Address": validate_address
}

# ----------------------------
# Text preprocessing
# ----------------------------
def preprocess_text(text):
    """Clean and normalize text for better matching"""
    text = text.strip()
    text = re.sub(r'\s+', ' ', text)  # Remove extra whitespace
    return text

# ----------------------------
# Load training examples
# ----------------------------
def load_examples(example_file="examples.json"):
    with open(example_file, "r") as f:
        examples = json.load(f)
    return examples

# ----------------------------
# Very permissive validation - only filter obviously bad examples
# ----------------------------
def validate_examples(examples):
    validated = {}
    validation_stats = {}
    
    for field, vals in examples.items():
        validated[field] = []
        validation_stats[field] = {"total": len(vals), "valid": 0, "invalid": 0}
        
        validator = FIELD_VALIDATORS.get(field)
        
        for v in vals:
            processed = preprocess_text(v)
            
            if validator:
                try:
                    if not validator(processed):
                        print(f"⚠️ Skipping invalid {field} example: '{v}'")
                        validation_stats[field]["invalid"] += 1
                        continue
                except Exception as e:
                    print(f"⚠️ Validation error for {field} '{v}': {e}")
                    validation_stats[field]["invalid"] += 1
                    continue
            
            validated[field].append(processed)
            validation_stats[field]["valid"] += 1
    
    # Print validation statistics
    print("\n📊 Validation Statistics:")
    for field, stats in validation_stats.items():
        print(f"  {field}: {stats['valid']}/{stats['total']} valid examples")
    
    return validated

# ----------------------------
# Build vectorizers
# ----------------------------
def build_and_save_vectors(examples, out_file="field_vectors.pkl"):
    # TF-IDF vectorizer
    tfidf_vectorizer = TfidfVectorizer(
        analyzer="char", 
        ngram_range=(2, 4),
        min_df=1,
        max_features=10000
    )
    
    all_texts = []
    labels = []
    field_indices = {}
    
    # Prepare data
    for field, vals in examples.items():
        field_indices[field] = len(all_texts)
        for v in vals:
            all_texts.append(v)
            labels.append(field)

    # Fit TF-IDF
    X_tfidf = tfidf_vectorizer.fit_transform(all_texts)

    # Sentence embeddings
    sbert = SentenceTransformer("all-MiniLM-L6-v2") # lightweight + accurate
    X_emb = sbert.encode(all_texts, convert_to_numpy=True, normalize_embeddings=True)

    # Calculate average embeddings per field
    field_embeddings = {}
    for field in examples.keys():
        indices = [i for i, label in enumerate(labels) if label == field]
        if indices:
            field_embeddings[field] = np.mean(X_emb[indices], axis=0)

    # Save all data
    data = {
        "fields": list(examples.keys()),
        "examples": examples,
        "labels": labels,
        "tfidf_vectorizer": tfidf_vectorizer,
        "X_tfidf": X_tfidf,
        "embedding_model": "all-mpnet-base-v2",
        "X_emb": X_emb,
        "field_embeddings": field_embeddings,
        "field_indices": field_indices
    }

    with open(out_file, "wb") as f:
        pickle.dump(data, f)

    print(f"\n✅ Saved validated field vectors to {out_file}")
    print(f"📊 Total examples: {len(all_texts)}")
    print(f"🏷️  Fields: {list(examples.keys())}")

# ----------------------------
# Main - with emergency fallback
# ----------------------------
if __name__ == "__main__":
    examples = load_examples("examples.json")
    
    try:
        validated_examples = validate_examples(examples)
        
        # Emergency fallback: if too many examples are being filtered, use original
        for field, vals in validated_examples.items():
            if len(vals) < len(examples[field]) * 0.8:  # If we lost more than 20%
                print(f"⚠️ Too many {field} examples filtered, using original examples")
                validated_examples[field] = [preprocess_text(v) for v in examples[field]]
        
        build_and_save_vectors(validated_examples)
        
    except Exception as e:
        print(f"❌ Error during validation: {e}")
        print("🔄 Falling back to using all examples without validation...")
        # Use all examples without validation
        all_examples = {}
        for field, vals in examples.items():
            all_examples[field] = [preprocess_text(v) for v in vals]
        build_and_save_vectors(all_examples)