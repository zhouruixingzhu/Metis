import os
import toml
import json
from tqdm import tqdm

label_map = {
    "cpu-exhaustion": "CPU exhaustion",
    "memory-exhaustion": "memory exhaustion",
    "jvm-return": "code error",
    "jvm-exception": "code error",
    "pod-failure": "pod failure",
    "replace": "HTTP replace"
}

def truncate(events: list, max_len=5):
    return events[:max_len] if isinstance(events, list) else []

def build_input_text1(metric_events, trace_events, log_events):
    sections = []

    # METRIC
    if metric_events:
        metric_lines = []
        for m in truncate(metric_events):
            line = f"- {m['metric_name']} changed from {m['normal_avg']} to {m['observed_avg']} (Δ={m['avg_change']})"
            metric_lines.append(line)
        sections.append("## METRIC\n" + "\n".join(metric_lines))

    # TRACE
    if trace_events:
        trace_lines = []
        for t in truncate(trace_events):
            service = t['service_name'].split("::")[0]
            span = t['span_name'].split()[-1]  # 提取核心 span 名
            line = f"- {service}::{span} latency ↑ from {t['normal_duration_per_min']} to {t['observed_duration']} ({t['pattern']})"
            trace_lines.append(line)
        sections.append("## TRACE\n" + "\n".join(trace_lines))

    # LOG
    if log_events:
        log_lines = []
        for l in truncate(log_events):
            log_line = l['log_template'].split(']')[-1].strip()  # 去除前缀
            line = f"- \"{log_line}\" freq: {l['observed_frequence']} (normal: {l['normal_frequence_per_min']})"
            log_lines.append(line)
        sections.append("## LOG\n" + "\n".join(log_lines))

    return "\n".join(sections)

def build_input_text(metric_events, trace_events, log_events, use_metric=True, use_trace=True, use_log=True):
    sections = []

    if use_metric and metric_events:
        metric_lines = []
        for m in truncate(metric_events):
            line = f"- {m['metric_name']} changed from {m['normal_avg']} to {m['observed_avg']} (Δ={m['avg_change']})"
            metric_lines.append(line)
        sections.append("## METRIC\n" + "\n".join(metric_lines))

    if use_trace and trace_events:
        trace_lines = []
        for t in truncate(trace_events):
            service = t['service_name'].split("::")[0]
            span = t['span_name'].split()[-1]
            line = f"- {service}::{span} latency ↑ from {t['normal_duration_per_min']} to {t['observed_duration']} ({t['pattern']})"
            trace_lines.append(line)
        sections.append("## TRACE\n" + "\n".join(trace_lines))

    if use_log and log_events:
        log_lines = []
        for l in truncate(log_events):
            log_line = l['log_template'].split(']')[-1].strip()
            line = f"- \"{log_line}\" freq: {l['observed_frequence']} (normal: {l['normal_frequence_per_min']})"
            log_lines.append(line)
        sections.append("## LOG\n" + "\n".join(log_lines))

    return "\n".join(sections)


def load_all_cases(data_root, use_metric=True, use_trace=True, use_log=True):
    texts, labels, case_names = [], [], []

    for case_name in tqdm(os.listdir(data_root)):
        case_path = os.path.join(data_root, case_name)
        if not os.path.isdir(case_path):
            continue

        events_path = os.path.join(case_path, "events.toml")
        fault_path = os.path.join(data_root, "fault_injection.toml")

        if not os.path.exists(events_path) or not os.path.exists(fault_path):
            continue

        try:
            fault_data = toml.load(fault_path)
            chaos_entries = fault_data.get("chaos_injection", [])

            matched = [entry for entry in chaos_entries if entry.get("case") == case_name]
            if not matched:
                print(f"[SKIP] No matching chaos_injection entry for case '{case_name}'")
                continue

            fault_type = matched[0]["chaos_type"]
            if fault_type not in label_map:
                print(f"[SKIP] Unknown chaos_type '{fault_type}' in case '{case_name}'")
                continue

            label = label_map[fault_type]

            events_data = toml.load(events_path)
            metric_events = events_data.get("metric_events", [])
            trace_events = events_data.get("trace_events", [])
            log_events = events_data.get("log_events", [])

            text = build_input_text(metric_events, trace_events, log_events,
                                    use_metric=use_metric, use_trace=use_trace, use_log=use_log)

            texts.append(text)
            labels.append(label)
            case_names.append(case_name)
        except Exception as e:
            print(f"Error processing {case_name}: {e}")

    return texts, labels, case_names


def save_to_jsonl(texts, labels, case_names, output_file):
    with open(output_file, "w", encoding="utf-8") as f:
        for text, label, case in zip(texts, labels, case_names):
            json.dump({"case": case, "text": text, "label": label}, f)
            f.write("\n")
    print(f"Saved {len(texts)} samples to {output_file}")



def build_FTI_dataset(input_path, output_path="Dataset_B.jsonl"):
    texts, labels, case_names = load_all_cases(
        input_path,
        use_metric=False,
        use_trace=False,
        use_log=True
    )
    save_to_jsonl(texts, labels, case_names, output_file=output_path)



if __name__ == "__main__":
    build_FTI_dataset("/home/nn/workspace/Metis-DataSet/Dataset-B/")






