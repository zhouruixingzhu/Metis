from datetime import datetime, timedelta
import pandas as pd
from typing import List, Tuple
import re

def parse_datetime_with_nanoseconds(datetime_str):
    if '.' in datetime_str:
        main_part, fractional_part = datetime_str.split('.')
        fractional_part = fractional_part[:6].ljust(6, '0')
        datetime_str = f"{main_part}.{fractional_part}"
    try:
        return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")

def get_overall_time_range(time_ranges):
    # print("time_ranges:",time_ranges)
    earliest_start_time = None
    latest_end_time = None

    for time_range in time_ranges:
        if time_range is None or time_range == (None, None):
            continue

        start_time_str, end_time_str = time_range
        
        # Convert strings to datetime objects
        try:
            start_time = datetime.fromisoformat(start_time_str)
            end_time = datetime.fromisoformat(end_time_str)
        except ValueError:
            continue  # Skip invalid datetime format entries
        
        # Update the earliest start time
        if earliest_start_time is None or start_time < earliest_start_time:
            earliest_start_time = start_time
        
        # Update the latest end time
        if latest_end_time is None or end_time > latest_end_time:
            latest_end_time = end_time

    # If no valid time ranges found, return None, None
    if earliest_start_time is None or latest_end_time is None:
        return (None, None)
    
    # Return the times in ISO 8601 format as strings
    return (earliest_start_time.isoformat(), latest_end_time.isoformat())

def extract_log_trace_time_ranges(log_trace_anomalies):
    """
    Extract the earliest start time and latest end time from log/trace anomalies.
    Handles both (start_time, end_time) tuples and single time values.

    Args:
        log_trace_anomalies (dict): A dictionary where keys are services and values 
                                  contain time ranges (either as tuple or single value)

    Returns:
        tuple: A tuple containing the earliest start time and the latest end time 
              as strings in ISO 8601 format, or (None, None) if no valid times found
    """
    if not log_trace_anomalies:
        return None, None

    earliest_start_time = None
    latest_end_time = None

    for service, time_data in log_trace_anomalies.items():
        # Skip None or empty values
        if time_data is None:
            continue

        try:
            if isinstance(time_data, dict) and 'TimeRanges' in time_data:
                start_time, end_time = time_data['TimeRanges']
            elif isinstance(time_data, (tuple, list)) and len(time_data) == 2:
                start_time, end_time = time_data
            else:
                start_time = end_time = time_data

            # Parse times
            start_time_parsed = parse_datetime_with_nanoseconds(str(start_time))
            end_time_parsed = parse_datetime_with_nanoseconds(str(end_time))
            
            # Update earliest and latest times
            if earliest_start_time is None or start_time_parsed < earliest_start_time:
                earliest_start_time = start_time_parsed
            if latest_end_time is None or end_time_parsed > latest_end_time:
                latest_end_time = end_time_parsed

        except (ValueError, TypeError, AttributeError) as e:
            print(f"Warning: Failed to process time data for service {service}: {e}")
            continue

    # Convert to ISO format strings
    earliest_start_time_str = earliest_start_time.isoformat() if earliest_start_time else None
    latest_end_time_str = latest_end_time.isoformat() if latest_end_time else None

    return earliest_start_time_str, latest_end_time_str

def extract_metric_time_ranges(metric_anomalies):
    """
    Calculate the overall time range from metric anomalies.
    
    Args:
        metric_anomalies (list): A list where each item contains a service, score, and a time range.

    Returns:
        tuple: A tuple containing the earliest start time and the latest end time as strings in ISO 8601 format.
    """
    if not metric_anomalies:
        return None, None

    earliest_start_time = None
    latest_end_time = None

    for anomaly in metric_anomalies:
        time_range = anomaly[2].strip()
        
        try:

            pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})-(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})'
            match = re.match(pattern, time_range)

            if match:
                start_time_str, end_time_str = match.groups()
                # print(f"start_time_str = '{start_time_str}'")
                # print(f"end_time_str = '{end_time_str}'")

            # Parse the times into datetime objects
            start_time = pd.Timestamp(start_time_str).strftime("%Y-%m-%dT%H:%M:%S")
            end_time = pd.Timestamp(end_time_str).strftime("%Y-%m-%dT%H:%M:%S")

            # parse_datetime_with_nanoseconds()

            # Update the earliest start time and the latest end time
            if earliest_start_time is None or start_time < earliest_start_time:
                earliest_start_time = start_time
            if latest_end_time is None or end_time > latest_end_time:
                latest_end_time = end_time

        except Exception as e:
            print(f"Error processing time range '{time_range}': {e}")
            continue

    # print("metric time range:")
    # print(earliest_start_time, latest_end_time)
    return (earliest_start_time, latest_end_time)

def get_multi_score(file_types,score_list):
    # Define sources and initialize scores list
    # sources = ["log", "trace", "metric"]
    sources = file_types
    scores = [0, 0, 0]
    # Extract anomaly scores and most common events

    for i, source in enumerate(sources):
        score_df = score_list[i]
        if not score_df.empty:
            scores[i] = score_df["anomaly_score"][0]
            # print(f"{source} score:", scores[i])
    return scores

def calculate_overlap(start1, end1, start2, end2):
    if start1 is None or end1 is None or start2 is None or end2 is None:
        return False  # No overlap if any input is None
    return max(start1, start2) < min(end1, end2)

def f1_score(predicted_range, ground_truth_range):
    """
    Calculate the F1 score based on predicted and ground truth time ranges.
    
    Args:
        predicted_range (tuple): A tuple with predicted start and end times (datetime, datetime).
        ground_truth_range (tuple): A tuple with ground truth start and end times (datetime, datetime).
    
    Returns:
        float: precision, recall, F1 score.
    """
    predicted_start, predicted_end = predicted_range
    truth_start, truth_end = ground_truth_range

    if isinstance(predicted_start, str):
        predicted_start = datetime.fromisoformat(predicted_start)
    if isinstance(predicted_end, str):
        predicted_end = datetime.fromisoformat(predicted_end)
    if isinstance(truth_start, str):
        truth_start = datetime.fromisoformat(truth_start)
    if isinstance(truth_end, str):
        truth_end = datetime.fromisoformat(truth_end)
    
    if predicted_start is None or predicted_end is None:
        tp, fp, fn = 0, 0, 1
    else:
        has_overlap = calculate_overlap(predicted_start, predicted_end, truth_start, truth_end)
        if has_overlap:
            tp, fp, fn = 1, 0, 0
        else:
            tp, fp, fn = 0, 1, 1  # 预测了一个区间但没对上

    # Calculate Precision and Recall
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    
    # Calculate F1 score
    if (precision + recall) > 0:
        f1 = 2 * (precision * recall) / (precision + recall)
    else:
        f1 = 0  # If precision and recall are both 0, F1 is 0
    
    return precision, recall, f1

