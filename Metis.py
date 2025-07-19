import asyncio
import aiofiles
import os
import csv
from contextlib import contextmanager
from pathlib import Path
from typing import Sequence, List, Tuple, Union, Dict
from datetime import datetime, timedelta
import time

import numpy as np
import pandas as pd
import toml
from aiomultiprocess import Pool
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.cluster import KMeans

from AD.abstract import Detector, Parser
from AD.log import LogDetector, LogParser
from AD.metric import MetricParser
from AD.metric.metric_ad import MetricDetector
from AD.trace import TraceDetector, TraceParser
from RCL.log_rcl import log_rcl
from RCL.metric_rcl import metric_rcl
from RCL.trace_rcl import (
    calculate_changes,
    get_top_spans,
    read_and_merge_data,
    sort_and_filter_data,
)
from FTI.build_data import (
    build_FTI_dataset,
    train_logreg_model,
)
from utils import (
    get_overall_time_range,
    extract_log_trace_time_ranges,
    extract_metric_time_ranges,
    get_multi_score,
    f1_score,
)


@contextmanager
def pushd(path):
    prev = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev)


async def trace_rcl(base_dir, service_list):
    base_dir = Path(base_dir)
    with pushd(base_dir):
        merged_df = read_and_merge_data("trace_ad_output/normal.csv", "trace_ad_output/abnormal.csv")
        merged_df = calculate_changes(merged_df)
        weighted_change_result = sort_and_filter_data(merged_df)
        return get_top_spans("rcl_output/trace_rcl_results.csv"), weighted_change_result


def ranking(base_dir: Path, anomalous: Sequence, score: Sequence[pd.DataFrame]):
    scores = []
    for i, (anomalous, score) in enumerate(zip(anomalous, score)):
        if anomalous:
            scores.append(score)

    combined_scores = pd.concat(scores, ignore_index=True).groupby('ServiceName', as_index=False)['anomaly_score'].sum()
    combined_scores = combined_scores[combined_scores['anomaly_score'].abs() > 0]
    combined_scores = combined_scores.rename(columns={'anomaly_score': 'CombinedScore'})
    combined_scores = combined_scores.sort_values(by='CombinedScore', ascending=False)

    # Add an index column starting from 1
    combined_scores = combined_scores.reset_index(drop=True)
    combined_scores['Index'] = combined_scores.index + 1

    combined_scores.to_csv(base_dir / 'final_ranking.csv', index=False)
    # print(combined_scores)

    return combined_scores


def calculate_metrics(prediction_data, gt_list):
    AC_at_1 = 0
    AC_at_3 = 0
    AC_at_5 = 0    
    avg_at_5 = 0
    total_cases = len(gt_list)
    total_precision = 0
    total_recall = 0
    total_f1 = 0

    for gt in gt_list:
        case_name = gt['case']
        gt_service = gt['service']
        # Calculate gt_abnormal_time_range
        gt_start_time = gt['timestamp']
        if isinstance(gt_start_time, str):
            gt_start_time = datetime.fromisoformat(gt_start_time.replace('Z', '+00:00'))
        gt_end_time = gt_start_time + timedelta(minutes=5)
        gt_abnormal_time_range = (gt_start_time, gt_end_time)
        
        # Find the corresponding case in prediction_data
        for case, combined_ranking, predict_abnormal_time_range in prediction_data:
            if case == case_name:
                # Calculate F1 score using predict_abnormal_time_range and gt_abnormal_time_range
                if predict_abnormal_time_range is None:
                    # No anomlies predicted
                    precision, recall, f1 = 0, 0, 0
                    continue
                else:
                    predicted_start, predicted_end = predict_abnormal_time_range
                    precision, recall, f1 = f1_score((predicted_start, predicted_end), gt_abnormal_time_range)

                # Update total precision, recall, and f1
                total_precision += precision
                total_recall += recall
                total_f1 += f1

                # Find ground truth service index in the combined_ranking dataframe
                ranking_df = combined_ranking
                if gt_service in ranking_df['ServiceName'].values:
                    service_index = ranking_df[ranking_df['ServiceName'] == gt_service]['Index'].values[0]

                    # AC@1
                    if service_index == 1:
                        AC_at_1 += 1

                    # AC@3
                    if service_index <= 3:
                        AC_at_3 += 1

                    # AC@5
                    if service_index <= 5:
                        AC_at_5 += 1
                    
                    # Avg@5
                    if service_index <= 5:
                        avg_at_5 += (5 - service_index + 1) / 5.0

                break

    # Ranking metrics
    AC_at_1 /= total_cases
    AC_at_3 /= total_cases
    AC_at_5 /= total_cases
    avg_at_5 /= total_cases
    precision_avg = total_precision / total_cases
    recall_avg = total_recall / total_cases
    f1_avg = total_f1 / total_cases

    return {
        "AD Precision": precision_avg,
        "AD Recall": recall_avg,
        "AD F1": f1_avg,
        "AC@1": AC_at_1,
        "AC@3": AC_at_3,
        "AC@5": AC_at_5,
        "Avg@5": avg_at_5
    }

async def process_case(base_dir, pool: Pool, file_type="log"):
    system_anomalous = False
    metric_abnormal_time = None
    log_trace_abnormal_time = None
    if file_type == "metric":
        parser = MetricParser(base_dir)
        data = await parser.parse(pool)
        detector = MetricDetector(base_dir)
        system_anomalous, anomalies = await detector.detect(data, pool)
        metric_abnormal_time = extract_metric_time_ranges(anomalies)
        # print("metric_abnormal_time:",metric_abnormal_time)
        if system_anomalous:
            return system_anomalous, *metric_rcl(base_dir), metric_abnormal_time
    else:
        parser: Parser = {"log": LogParser, "trace": TraceParser}[file_type](base_dir)
        data = await parser.parse(pool)
        detector: Detector = {"log": LogDetector, "trace": TraceDetector}[file_type](base_dir, data)
        system_anomalous, anomalies = await pool.apply(detector.detect)
        log_trace_abnormal_time = extract_log_trace_time_ranges(anomalies)
        # print("log_trace_abnormal_time:",log_trace_abnormal_time)
        if system_anomalous:
            rcl_func = {"log": log_rcl, "trace": trace_rcl}[file_type]
            return system_anomalous, *await pool.apply(rcl_func, args=(base_dir, anomalies)), log_trace_abnormal_time
    return system_anomalous, [], pd.DataFrame(), None


async def evaluate(base_dir, pool):
    base_dir = Path(base_dir)
    case = base_dir.name
    # print(case)
    base_dir = base_dir.absolute()
    file_types = ["log", "trace", "metric"]
    # ablation experiments
    # file_types = ["log"]
    result = await asyncio.gather(*(process_case(base_dir, pool, file_type) for file_type in file_types))
    result = list(zip(*result))
    system_anomalous = any(result[0])
    # multi-modal anomaly score
    score_list = list(result[2])
    event_list = list(result[1])
    time_list = list(result[3])
    # print(time_list)
    predict_abnormal_time_range = get_overall_time_range(time_list)
    print(f"Predict abnormal time range for {case}: {predict_abnormal_time_range}")
    if system_anomalous:
        combined_events = {}
        for file_type, events in zip(file_types,result[1]):
            combined_events[f"{file_type}_events"] = events
        with open(base_dir / "events.toml", 'w') as toml_file:
            toml.dump(combined_events, toml_file)
        # final service ranking prediciton, calculate by multi-modal service ranking
        combined_ranking = ranking(base_dir, result[0], result[2])
        scores = get_multi_score(file_types, score_list)
        return case, combined_ranking, predict_abnormal_time_range
    else:
        print(f"No anomalies detected in case {base_dir}.")
        return case, None, None


####### run one case ########
# root_base_dir = Path(r"./Metis-DataSet/DatasetB/ts-basic-service-1027-0546")
# async def one():
#      fault_injection_file = root_base_dir / "fault_injection.toml"
#      data = toml.load(fault_injection_file)
#      gt_list = data["chaos_injection"]
#      # Define paths
#      base_dirs = [
#          R"./Metis-DataSet/DatasetB/ts-basic-service-1027-0546",
#      ]
#      async with Pool() as pool:    
#          prediction_data = await asyncio.gather(*(evaluate(base_dir, pool) for base_dir in base_dirs))


######## run all cases ########
async def all():
    root_base_dir = Path(r"/home/nn/workspace/Metis-DataSet/Dataset-B")
    fault_injection_file = root_base_dir / "fault_injection.toml"
    data = toml.load(fault_injection_file)
    gt_list = data["chaos_injection"]
    case_dirs = [p for p in root_base_dir.iterdir() if p.is_dir()]

    childconcurrency = 20
    processes = os.cpu_count()
    queuecount = processes // 4
    async with Pool(processes=processes, childconcurrency=childconcurrency, queuecount=queuecount) as pool:
        prediction_data = await asyncio.gather(*(evaluate(case_dir, pool) for case_dir in case_dirs))
    
    service_evaluation_results = calculate_metrics(prediction_data, gt_list)
    print(service_evaluation_results)
    # FTI
    output_file = root_base_dir / "Dataset_B.jsonl"
    build_FTI_dataset(input_path=root_base_dir, output_path=output_file)
    train_logreg_model(output_file)


@contextmanager
def timer():
    start_time = time.time()
    yield
    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.4f} seconds")
if __name__ == "__main__":
    with timer():
        asyncio.run(all())
