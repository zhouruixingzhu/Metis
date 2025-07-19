import json
import joblib
from sklearn.metrics import classification_report, accuracy_score

model_path = "./logReg_model/logreg_model.pkl"
data_path = "Dataset_B.jsonl"

label_names = [
    "CPU exhaustion",
    "HTTP replace",
    "code error",
    "memory exhaustion",
    "pod failure"
]
label2id = {label: i for i, label in enumerate(label_names)}
id2label = {i: label for label, i in label2id.items()}

print("Loading model...")
model = joblib.load(model_path)

def load_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(line.strip()) for line in f]

data = load_jsonl(data_path)
texts = [x["text"] for x in data]
true_labels = [label2id[x["label"]] for x in data]  # 将标签转换为 id

print("Running inference...")
pred_ids = model.predict(texts)
pred_labels = [id2label[p] for p in pred_ids]

print("\n=== Sample Predictions ===")
for i, (sample, pred_label) in enumerate(zip(data, pred_labels)):
    gt_label = sample["label"]
    print(f"[{i}] Predicted: {pred_label} | GT: {gt_label} | Text: {sample['text'][:80]}...")

print("\n=== Classification Report ===")
print(classification_report(true_labels, pred_ids, target_names=label_names, digits=3))
accuracy = accuracy_score(true_labels, pred_ids)
print(f"Accuracy: {accuracy:.4f}")
