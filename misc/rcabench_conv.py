import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import polars as pl
import toml
from aiomultiprocess import Pool
from tqdm.auto import tqdm

LOG_SEVERITY_MAPPING = {
    "INFO": 9,
    "info": 9,
    "WARN": 13,
    "warn": 13,
    "ERROR": 17,
    "SEVERE": 17,
    "error": 17,
    "DEBUG": 5,
    "TRACE": 1,
}


def create_log_severity_number(level: str) -> int:
    """Convert log level to a number"""
    return LOG_SEVERITY_MAPPING.get(level, 0)


def process_logs_data(df: pl.DataFrame, namespace: str = "ts") -> pl.DataFrame:
    """Process log data, applying transformations similar to database queries"""

    result_df = df.select(
        [
            pl.col("time").alias("Timestamp"),
            pl.col("trace_id").alias("TraceId"),
            pl.col("span_id").alias("SpanId"),
            pl.col("level").alias("SeverityText"),
            pl.col("SeverityNumber"),
            pl.col("service_name").alias("ServiceName"),
            pl.col("message").alias("Body"),
        ]
    )

    return result_df


def process_metrics_data(df: pl.DataFrame, namespace: str = "ts") -> pl.DataFrame:
    """Process metrics data, applying transformations similar to database queries"""

    try:
        for col in [
            "k8s_namespace_name",
            "k8s_pod_uid",
            "k8s_pod_name",
            "k8s_container_name",
            "MetricDescription",
            "MetricUnit",
            "direction",
        ]:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))

        result_df = df.select(
            [
                pl.lit("ts").alias("k8s_namespace_name"),
                pl.lit(None).alias("k8s_pod_uid"),
                pl.col("attr.k8s.pod.name").alias("k8s_pod_name"),
                pl.col("attr.k8s.container.name").alias("k8s_container_name"),
                pl.col("metric").alias("MetricName"),
                pl.lit(None).alias("MetricDescription"),
                pl.col("time").alias("TimeUnix"),
                pl.col("value").alias("Value"),
                pl.lit(None).alias("MetricUnit"),
                pl.lit(None).alias("direction"),
            ]
        )

    except Exception as e:
        print(f"Error processing metrics data: {e}")
        result_df = df

    return result_df


def process_request_metrics_data(df: pl.DataFrame, namespace: str = "ts") -> pl.DataFrame:
    """Process request metrics data, similar to request_metric queries"""
    try:
        for col in [
            "namespace_name",
            "status_code",
            "MetricDescription",
            "Count",
            "Sum",
            "BucketCounts",
            "ExplicitBounds",
        ]:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))

        result_df = df.select(
            [
                pl.lit("ts").alias("namespace_name"),
                pl.col("service_name").alias("ServiceName"),
                pl.lit(None).alias("status_code"),
                pl.col("metric").alias("MetricName"),
                pl.lit(None).alias("MetricDescription"),
                pl.col("time").alias("TimeUnix"),
                pl.col("count").alias("Count") if "count" in df.columns else pl.lit(None).alias("Count"),
                pl.col("sum").alias("Sum") if "sum" in df.columns else pl.lit(None).alias("Sum"),
                pl.lit(None).alias("BucketCounts"),
                pl.lit(None).alias("ExplicitBounds"),
            ]
        )

        result_df = result_df.filter(
            pl.col("MetricName").is_in(["http.client.request.duration", "http.server.request.duration"])
        )

    except Exception as e:
        print(f"Error processing request metrics data: {e}")
        result_df = df

    return result_df


def process_traces_data(df: pl.DataFrame, namespace: str = "ts") -> pl.DataFrame:
    """Process trace data, similar to trace queries"""
    try:
        required_cols = ["time", "trace_id", "span_id", "parent_span_id", "span_name", "service_name", "duration"]
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            print(f"Missing required columns for traces processing: {missing_cols}")
            for col in missing_cols:
                df = df.with_columns(pl.lit(None).alias(col))

        if "parent_span_id" in df.columns and "service_name" in df.columns:
            span_to_service = {
                row["span_id"]: row["service_name"]
                for row in df.select(["span_id", "service_name"]).to_dicts()
                if row["span_id"] is not None
            }

            df = df.with_columns(
                [
                    pl.col("parent_span_id")
                    .map_elements(lambda x: span_to_service.get(x) if x is not None else None, return_dtype=pl.Utf8)
                    .alias("ParentServiceName")
                ]
            )
        else:
            df = df.with_columns(pl.lit(None).alias("ParentServiceName"))

        result_df = df.select(
            [
                pl.col("time").alias("Timestamp"),
                pl.col("trace_id").alias("TraceId"),
                pl.col("span_id").alias("SpanId"),
                pl.col("parent_span_id").alias("ParentSpanId"),
                pl.col("span_name").alias("SpanName"),
                pl.col("service_name").alias("ServiceName"),
                pl.col("duration").alias("Duration"),
                pl.col("ParentServiceName"),
            ]
        )

    except Exception as e:
        print(f"Error processing traces data: {e}")
        result_df = df

    return result_df


def convert_logs_parquet_to_csv(parquet_path: Path, csv_path: Path, namespace: str = "ts"):
    """Convert log Parquet files to CSV format, add severity number mapping and apply query transformations"""
    print(f"Converting logs: {parquet_path} -> {csv_path}")

    df = pl.read_parquet(parquet_path)

    df = df.with_columns(
        [
            pl.col("level")
            .map_elements(lambda x: create_log_severity_number(x) if x is not None else 0, return_dtype=pl.Int32)
            .alias("SeverityNumber")
        ]
    )

    df = process_logs_data(df, namespace)

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(csv_path)


def convert_metrics_parquet_to_csv(parquet_path: Path, csv_path: Path, namespace: str = "ts"):
    """Convert metrics Parquet files to CSV format, apply query transformations"""
    print(f"Converting metrics: {parquet_path} -> {csv_path}")

    df = pl.read_parquet(parquet_path)

    df = process_metrics_data(df, namespace)

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(csv_path)


def convert_metrics_histogram_parquet_to_csv(parquet_path: Path, csv_path: Path, namespace: str = "ts"):
    """Convert histogram metrics Parquet files to CSV format, apply query transformations"""
    print(f"Converting histogram metrics: {parquet_path} -> {csv_path}")

    df = pl.read_parquet(parquet_path)

    df = process_request_metrics_data(df, namespace)

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(csv_path)


def convert_traces_parquet_to_csv(parquet_path: Path, csv_path: Path, namespace: str = "ts"):
    """Convert trace Parquet files to CSV format, apply query transformations"""
    print(f"Converting traces: {parquet_path} -> {csv_path}")

    df = pl.read_parquet(parquet_path)

    df = process_traces_data(df, namespace)

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(csv_path)


def convert_datapack_to_csv(datapack_path: Path, output_base_path: Path):
    """Convert a single data package from Parquet to CSV, adjust timezone and create correct folder structure"""
    datapack_name = datapack_path.name
    output_datapack_path = output_base_path / datapack_name

    print(f"Converting datapack: {datapack_name}")

    namespace = "ts"

    normal_folder = output_datapack_path / "normal"
    abnormal_folder = output_datapack_path / "abnormal"
    normal_folder.mkdir(parents=True, exist_ok=True)
    abnormal_folder.mkdir(parents=True, exist_ok=True)

    file_conversions = {
        "normal_logs.parquet": normal_folder / "logs.csv",
        "abnormal_logs.parquet": abnormal_folder / "logs.csv",
        "normal_traces.parquet": normal_folder / "traces.csv",
        "abnormal_traces.parquet": abnormal_folder / "traces.csv",
        "normal_metrics.parquet": normal_folder / "metrics.csv",
        "abnormal_metrics.parquet": abnormal_folder / "metrics.csv",
        "normal_metrics_histogram.parquet": normal_folder / "request_metrics.csv",
        "abnormal_metrics_histogram.parquet": abnormal_folder / "request_metrics.csv",
    }

    for parquet_file, csv_path in file_conversions.items():
        parquet_path = datapack_path / parquet_file

        if parquet_path.exists():
            try:
                df = pl.read_parquet(parquet_path)

                if "time" in df.columns:
                    df = df.with_columns(
                        [
                            pl.col("time")
                            .cast(pl.Datetime)
                            .dt.replace_time_zone("UTC")
                            .dt.convert_time_zone("Asia/Shanghai")
                            .dt.strftime("%Y-%m-%d %H:%M:%S.%f")
                            .alias("time")
                        ]
                    )

                if "logs" in parquet_file:
                    df = df.with_columns(
                        [
                            pl.col("level")
                            .map_elements(
                                lambda x: create_log_severity_number(x) if x is not None else 0, return_dtype=pl.Int32
                            )
                            .alias("SeverityNumber")
                        ]
                    )
                    df = process_logs_data(df, namespace)
                elif "traces" in parquet_file:
                    df = process_traces_data(df, namespace)
                elif "metrics_histogram" in parquet_file:
                    df = process_request_metrics_data(df, namespace)
                elif "metrics" in parquet_file:
                    df = process_metrics_data(df, namespace)

                if "namespace_name" in df.columns:
                    df = df.with_columns([pl.lit("ts").alias("namespace_name")])

                csv_path.parent.mkdir(parents=True, exist_ok=True)
                df.write_csv(csv_path)
                print(f"Converted and processed: {parquet_path} -> {csv_path}")
            except Exception as e:
                print(f"Error converting {parquet_path}: {e}")
        else:
            print(f"File not found: {parquet_path}")

    for item in datapack_path.iterdir():
        if item.is_file() and item.suffix in [".json"]:
            dst_path = output_datapack_path / item.name
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, dst_path)


def get_fault_type_name(fault_type: int) -> str:
    """Return the corresponding fault name based on the fault type number"""
    fault_type_map = {24: "jvm-return", 25: "jvm-exception"}
    return fault_type_map.get(fault_type, f"unknown-{fault_type}")


def parse_injection_json(json_path: Path) -> dict[str, Any]:
    """Parse injection.json file, extract injection time and fault type information"""
    try:
        with open(json_path) as f:
            data = json.load(f)

        start_time = data.get("start_time")
        fault_type = data.get("fault_type")

        service_name = None
        display_config_str = data.get("display_config")
        if display_config_str:
            try:
                display_config = json.loads(display_config_str)

                if "injection_point" in display_config and "app_name" in display_config["injection_point"]:
                    service_name = display_config["injection_point"]["app_name"]
            except json.JSONDecodeError as e:
                print(f"Error parsing display_config JSON: {e}")

        if not start_time or not service_name or fault_type is None:
            print(f"Warning: Missing required fields in {json_path}")
            return {}

        return {"start_time": start_time, "service_name": service_name, "fault_type": fault_type}
    except Exception as e:
        print(f"Error parsing {json_path}: {e}")
        return {}


def generate_folder_name(service_name: str, timestamp_str: str) -> str:
    """Generate a new folder name based on service name and timestamp: service-MMDD-HHMM"""
    try:
        dt = datetime.fromisoformat(timestamp_str)
        return f"{service_name}-{dt.month:02d}{dt.day:02d}-{dt.hour:02d}{dt.minute:02d}"
    except Exception as e:
        print(f"Error generating folder name: {e}")
        return service_name


def process_datapacks_and_generate_toml(datapacks_dir: Path, output_dir: Path) -> list[dict[str, Any]]:
    """Process data packages, rename folders and collect TOML entry information"""
    toml_entries = []

    for datapack_path in tqdm(list(datapacks_dir.iterdir()), desc="Processing datapacks for TOML"):
        if not datapack_path.is_dir():
            continue

        injection_json_path = datapack_path / "injection.json"
        if not injection_json_path.exists():
            print(f"No injection.json found in {datapack_path}")
            continue

        injection_data = parse_injection_json(injection_json_path)
        if not injection_data:
            continue

        service_name = injection_data["service_name"]
        timestamp = injection_data["start_time"]
        fault_type = injection_data["fault_type"]
        chaos_type = get_fault_type_name(fault_type)

        new_folder_name = generate_folder_name(service_name, timestamp)

        toml_entry = {
            "case": new_folder_name,
            "timestamp": timestamp,
            "namespace": "ts",
            "chaos_type": chaos_type,
            "service": service_name,
        }
        toml_entries.append(toml_entry)

        old_output_path = output_dir / datapack_path.name
        new_output_path = output_dir / new_folder_name

        if old_output_path.exists() and old_output_path != new_output_path:
            if new_output_path.exists():
                shutil.rmtree(new_output_path)
            old_output_path.rename(new_output_path)
            print(f"Renamed: {old_output_path} -> {new_output_path}")

    return toml_entries


def generate_toml_file(toml_entries: list[dict[str, Any]], output_path: Path):
    """Generate TOML file"""
    toml_data = {"chaos_injection": toml_entries}

    with open(output_path, "w") as f:
        toml.dump(toml_data, f)

    print(f"Generated TOML file: {output_path}")


async def convert_datapack_to_csv_async(datapack: str):
    source_base_path = Path("/home/nn/workspace/rcabench-platform/data/rcabench_platform_datasets/rcabench_with_issues")
    output_base_path = Path("/home/nn/workspace/rcabench-platform/data/csv_datasets/rcabench_with_issues_csv")
    datapack_path = source_base_path / datapack

    if datapack_path.exists() and datapack_path.is_dir():
        convert_datapack_to_csv(datapack_path, output_base_path)
    else:
        print(f"Datapack directory not found: {datapack_path}")


async def main():
    """Main function"""

    index_path = Path(
        "/home/nn/workspace/rcabench-platform/data/rcabench_platform_datasets/__meta__/rcabench_with_issues/index.parquet"
    )
    df = pd.read_parquet(index_path)

    filtered_df = df[
        (df["datapack"].str.contains("service-exception", regex=False, na=False, case=False))
        | (df["datapack"].str.contains("service-return", regex=False, na=False, case=False))
    ]

    source_base_path = Path("/home/nn/workspace/rcabench-platform/data/rcabench_platform_datasets/rcabench_with_issues")

    output_base_path = Path("/home/nn/workspace/rcabench-platform/data/csv_datasets/rcabench_with_issues_csv")
    output_base_path.mkdir(parents=True, exist_ok=True)

    print(f"Found {len(filtered_df)} datapacks to convert")
    print("Sample datapacks:")
    print(filtered_df["datapack"].head().tolist())

    async with Pool() as pool:
        await pool.map(convert_datapack_to_csv_async, filtered_df["datapack"].tolist())

    toml_entries = process_datapacks_and_generate_toml(output_base_path, output_base_path)

    toml_path = Path(output_base_path / "fault_injection.toml")
    generate_toml_file(toml_entries, toml_path)

    print(f"Conversion completed. Output saved to: {output_base_path}")
    print(f"TOML file generated at: {toml_path}")
    print("Folder structure reorganized with normal and abnormal subfolders")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
