import json
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
import time
import joblib
import os

label_names = [
    "CPU exhaustion",
    "HTTP replace",
    "code error",
    "memory exhaustion",
    "pod failure"
]
label2id = {label: i for i, label in enumerate(label_names)}
id2label = {i: label for label, i in label2id.items()}

def load_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(line.strip()) for line in f]

def train_logreg_model(train_path, output_dir="./logReg_model", max_features=2000):
    train_data = load_jsonl(train_path)

    texts = [x["text"] for x in train_data]
    labels = [label2id[x["label"]] for x in train_data]

    # Split into train, val, test
    X_train, X_temp, y_train, y_temp = train_test_split(texts, labels, test_size=0.3, stratify=labels, random_state=42)
    X_val, X_test, y_val, y_test = train_test_split(X_temp, y_temp, test_size=0.666, stratify=y_temp, random_state=42)

    print(f"Train size: {len(X_train)}")
    print(f"Validation size: {len(X_val)}")
    print(f"Test size: {len(X_test)}")

    model = make_pipeline(
        TfidfVectorizer(max_features=max_features),
        LogisticRegression(max_iter=1000, class_weight="balanced")
    )

    # Train
    start_time_train = time.time()
    model.fit(X_train, y_train)
    end_time_train = time.time()
    print(f"Training time: {end_time_train - start_time_train:.5f} seconds")

    os.makedirs(output_dir, exist_ok=True)
    model_path = os.path.join(output_dir, "logreg_model.pkl")
    joblib.dump(model, model_path)
    print(f"Model saved to {model_path}")

    # Evaluate
    print("Validation results:")
    y_val_pred = model.predict(X_val)
    print(classification_report(y_val, y_val_pred, target_names=label_names))

    print("=== Test set evaluation ===")
    start_time_test = time.time()
    y_test_pred = model.predict(X_test)
    print(classification_report(y_test, y_test_pred, target_names=label_names))
    print(f"Test time: {time.time() - start_time_test:.5f} seconds")

if __name__ == "__main__":
    train_logreg_model(train_path="Dataset_B.jsonl")
