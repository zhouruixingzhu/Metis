import asyncio
from pathlib import Path
from typing import Sequence

import pandas as pd

from ..abstract.detector import Detector


class LogDetector(Detector):
    def __init__(self, base_dir: Path, data: Sequence[pd.DataFrame]):
        super().__init__(base_dir)
        self.normal_df = data[0]
        self.abnormal_df = data[1]
        self.output_dir = self.base_dir / "log_ad_output"
        self.output_file = self.output_dir / "service_list.csv"
        self.system_anomalous = False
        self.anomalies = {}
        self.output_dir.mkdir(parents=True, exist_ok=True)

    async def detect(self):
        """wrapper function to detect anomalies."""
        self._detect_anomalies(error_weight=0.7, warn_weight=0.3, k=3)
        if self.system_anomalous:
            await self._save_anomalies_to_csv()
        else:
            print(f"No log-related anomalies detected in case {'_'.join(self.base_dir.parts[-2:])}.")
        return self.system_anomalous, self.anomalies

    def _detect_anomalies(self, error_weight=0.7, warn_weight=0.3, k=3):
        """Detect anomalies based on the provided normal and abnormal data."""

        for start_time, group in self.abnormal_df.groupby("StartTime"):
            start_time = pd.to_datetime(start_time)
            for _, row in group.iterrows():
                service_name = row["ServiceName"]
                error_rate = row["ErrorRate"]
                warn_rate = row["WarnRate"]
                normal_stats = self.normal_df[self.normal_df["ServiceName"] == service_name]
            try:
                error_mean, error_std, warn_mean, warn_std = normal_stats[
                    ["ErrorRateMean", "ErrorRateStd", "WarnRateMean", "WarnRateStd"]
                ].iloc[0]
            except IndexError:
                error_mean, error_std, warn_mean, warn_std = 0, 0, 0, 0

            def weighted(data: Sequence):
                return data[0] * error_weight + data[1] * warn_weight

            threshold = k * weighted([error_std, warn_std])
            rate = weighted([error_rate, warn_rate])
            mean = weighted([error_mean, warn_mean])
            # Check for anomalies
            if rate > mean + threshold:
                if service_name not in self.anomalies:
                    self.anomalies[service_name] = (start_time, start_time + pd.Timedelta(minutes=2))
                else:
                    self.anomalies[service_name] = (
                        min(self.anomalies[service_name][0], start_time),
                        max(self.anomalies[service_name][1], start_time + pd.Timedelta(minutes=2)),
                    )
        self.system_anomalous = len(self.anomalies) > 0

    async def _save_anomalies_to_csv(self):
        """Save the anomalies dictionary to a CSV file."""
        data = []
        for service, time_range in self.anomalies.items():
            start_time = time_range[0].strftime("%Y-%m-%d %H:%M:%S")
            end_time = time_range[1].strftime("%Y-%m-%d %H:%M:%S")
            data.append({"ServiceName": service, "StartTime": start_time, "EndTime": end_time})

        df = pd.DataFrame(data)
        await asyncio.to_thread(df.to_csv, self.output_file, index=False, mode="w")

        # print(f"Anomalies saved to {self.output_file}")
