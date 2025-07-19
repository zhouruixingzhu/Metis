import os
from typing import Sequence
from pathlib import Path
import pandas as pd


def process_log_files(log_data: Sequence[pd.DataFrame], output_file_path):

    # Combine all data into one DataFrame
    logs_df = pd.concat(log_data, ignore_index=True)

    # Calculate total number of logs
    total_logs = len(logs_df)
    total_minutes = (logs_df['Timestamp'].max() - logs_df['Timestamp'].min()).total_seconds() / 60

    # Use value_counts to count ERROR and WARN occurrences
    counts = logs_df.value_counts(subset=['ServiceName', 'temp_id', 'log_level']).unstack(fill_value=0).reset_index()

    # Ensure there are columns for ERROR and WARN counts, even if missing
    counts['ERROR'] = counts.get('ERROR', 0)
    counts['WARN'] = counts.get('WARN', 0)
    counts = counts[(counts['ERROR'] > 0) | (counts['WARN'] > 0)]
    # Calculate rates and averages
    counts['Error Rate'] = counts['ERROR'] / total_logs
    counts['Warn Rate'] = counts['WARN'] / total_logs
    counts['Avg Errors/Min'] = counts['ERROR'] / total_minutes
    counts['Avg Warns/Min'] = counts['WARN'] / total_minutes

    # Find the most common log body for each service and template ID
    most_common_bodies = (
        logs_df.groupby(['ServiceName', 'temp_id'])['Body']
        .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)
        .reset_index()
    )
    log_templates = logs_df[['ServiceName', 'temp_id', 'log_temp']].drop_duplicates()
    # Merge counts with most common bodies and log template
    summary = pd.merge(counts, most_common_bodies, on=['ServiceName', 'temp_id'], how='left')
    summary = pd.merge(summary, log_templates, on=['ServiceName', 'temp_id'], how='left')
    # Rename columns for better readability
    summary.rename(columns={'temp_id': 'Template ID', 'Body': 'Most Common Log Body'}, inplace=True)

    # Sort the results by Error Rate in descending order
    summary.sort_values(by='Error Rate', ascending=False, inplace=True)

    # Save the result to a CSV file
    summary.to_csv(output_file_path, index=False, mode='w')

    # print(f"Summary saved to {output_file_path}")
    return summary

def count_template_occurrences(service_name, template_id, directory):
    log_file_path = os.path.join(directory, f"{service_name}.csv")

    if not os.path.exists(log_file_path):
        # print(f"Log file not found: {log_file_path}")
        return pd.DataFrame(), 1

    log_data = pd.read_csv(log_file_path)
    log_data['Timestamp'] = pd.to_datetime(log_data['Timestamp'], errors='coerce')
    total_minutes = (log_data['Timestamp'].max() - log_data['Timestamp'].min()).total_seconds() / 60
    counts = log_data.value_counts(subset=['temp_id', 'log_level']).unstack(fill_value=0).reset_index()
    counts['ERROR'] = counts.get('ERROR', 0)
    counts['WARN'] = counts.get('WARN', 0)
    # count = log_data['temp_id'].value_counts().get(template_id, 0)
    return counts[(counts['temp_id'] == template_id)], max(total_minutes, 1)


def add_template_counts(df: pd.DataFrame, directory):
    df['Normal Error Count/Min'] = pd.Series(dtype=float)
    df['Normal Error Count'] = pd.Series(dtype=int)
    df['Normal Warn Count'] = pd.Series(dtype=int)

    for index, row in df.iterrows():
        service_name = row['ServiceName'].replace("_filtered", "")
        template_id = row['Template ID']
        counts, total_minutes = count_template_occurrences(service_name, template_id, directory)
        df.at[index, 'Normal Error Count'] = counts['ERROR'].values[0] if not counts.empty else 0
        df.at[index, 'Normal Warn Count'] = counts['WARN'].values[0] if not counts.empty else 0
        df.at[index, 'Normal Error Count/Min'] = df.at[index, 'Normal Error Count'] / total_minutes

    return df


def process_and_output_log_data(df, directory):
    df = add_template_counts(df, directory)
    if 'Normal Error Count/Min' not in df.columns:
        df['Normal Error Count/Min'] = [0] * len(df)

    def calculate_pattern(row):
        change_num = round((row['Avg Errors/Min'] - row['Normal Error Count/Min']), 3)
        if change_num > 0:
            return f"increase {change_num}"
        elif change_num < 0:
            return f"decrease {change_num}"
        else:
            return "no change"

    df['Pattern'] = df.apply(calculate_pattern, axis=1)
    df = df[df['Pattern'] != "no change"].reset_index(drop=True)
    log_events = []
    for index, row in df.round(3).iterrows():
        log_events.append(
            {
                "top": index + 1,
                "log_template": row['log_temp'],
                "most_common_log_body": row['Most Common Log Body'],
                "service_name": row['ServiceName'],
                "normal_frequence_per_min": row['Normal Error Count/Min'],
                "observed_frequence": row['Avg Errors/Min'],
                "pattern": row['Pattern'],
            }
        )

    df['Avg Errors/Min'] = pd.to_numeric(df['Avg Errors/Min'], errors='coerce')
    df['Normal Error Count/Min'] = pd.to_numeric(df['Normal Error Count/Min'], errors='coerce')

    df['anomaly_score'] = (df['Avg Errors/Min'] - df['Normal Error Count/Min']) * (1 / (df.index + 1))
    service_scores = df.groupby('ServiceName')['anomaly_score'].sum().reset_index()
    sorted_scores = service_scores.sort_values(by='anomaly_score', ascending=False)

    return log_events, sorted_scores


async def read_and_filter(directory, service_times):
    service_list = service_times.keys()
    df_list = []
    for service_name in service_list:
        file_path = os.path.join(directory, service_name + ".csv")
        if os.path.exists(file_path):
            start_time, end_time = service_times[service_name]
            df = pd.read_csv(file_path)
            df['ServiceName'] = service_name
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
            filtered_df = df[(df['Timestamp'] >= start_time) & (df['Timestamp'] <= end_time)]
            df_list.append(filtered_df)
    return df_list


async def log_rcl(base_dir: Path, anomalies):
    input_folder = base_dir / 'log_ad_output/parsed/abnormal'
    output_folder = base_dir / 'rcl_output'
    output_folder.mkdir(parents=True, exist_ok=True)
    output_file_path = output_folder / 'log_rcl_results.csv'
    normal_directory = base_dir / 'log_ad_output/parsed/normal'
    df_list = await read_and_filter(input_folder, anomalies)
    summary = process_log_files(df_list, output_file_path)
    return process_and_output_log_data(summary, normal_directory)


if __name__ == '__main__':
    os.chdir(R"/home/nn/workspace/RCA/1021/request-abort/ts-config-service")
    input_folder = 'log_ad_output/parsed/abnormal'
    file_path = 'rcl_output'
    csv_file_path = 'rcl_output/log_rcl_results.csv'
    directory = 'log_ad_output/parsed/normal'
    process_log_files(input_folder, file_path)
    log_events = process_and_output_log_data(csv_file_path, directory)
    combined_events = {"log_events": log_events, "metric_events": {}, "trace_events": {}}

    import toml

    # Write to TOML file
    with open("out.toml", 'w') as toml_file:
        toml.dump(combined_events, toml_file)
