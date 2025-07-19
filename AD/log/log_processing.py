import asyncio
from pathlib import Path
from typing import Sequence, Tuple

import pandas as pd
from aiomultiprocess import Pool
from drain3 import TemplateMiner
from drain3.file_persistence import FilePersistence
from drain3.template_miner_config import TemplateMinerConfig

from ..abstract import Parser
from ..utils.data_utils import LogLevelCalculator
from ..utils.parse_utils import extract_log


class LogParser(Parser):
    def __init__(self, base_dir: Path):
        super().__init__(base_dir)
        self.file_name = "logs.csv"
        self.normal_group_log_path = self.base_dir / "normal" / self.file_name
        self.abnormal_group_log_path = self.base_dir / "abnormal" / self.file_name
        self.output_dir = self.base_dir / "log_ad_output"
        self.parsed_dir = self.output_dir / "parsed"
        self.parsed_dir.mkdir(parents=True, exist_ok=True)

    async def parse(self, pool: Pool):
        """Process log data and save results to CSV file for each pod."""
        normal_groups, abnormal_groups = await asyncio.gather(
            *[self.read_and_group(normal) for normal in [True, False]]
        )
        all_services = set(normal_groups.keys()).union(set(abnormal_groups.keys()))

        result = await asyncio.gather(
            *[
                self._process_pod(
                    service,
                    normal_groups.get(service, pd.DataFrame()),
                    abnormal_groups.get(service, pd.DataFrame()),
                    pool,
                )
                for service in all_services
            ]
        )
        normal_result, abnormal_result = zip(
            *[((normal_group, pod), (abnormal_group, pod)) for normal_group, abnormal_group, pod in result]
        )
        normal_rate, abnormal_rate = await asyncio.gather(
            self._calculate_rate(normal_result, True, pool), self._calculate_rate(abnormal_result, False, pool)
        )
        return normal_rate, abnormal_rate

    async def read_and_group(self, normal):
        log_path = self.normal_group_log_path if normal else self.abnormal_group_log_path
        df = pd.read_csv(log_path).dropna(subset=["Body"])
        name_col = "ServiceName" if "ServiceName" in df.columns else "k8s_container_name"
        return {name: group for name, group in df.groupby(name_col)}

    async def _process_pod(self, pod, normal_group: pd.DataFrame, abnormal_group: pd.DataFrame, pool: Pool):
        """Process logs for a pod and save results to a CSV file."""
        template_miner = get_template_miner()
        if not normal_group.empty:
            normal_group, template_miner = await pool.apply(
                batch_process, args=(normal_group, pod, template_miner, self.parsed_dir / "normal")
            )
        if not abnormal_group.empty:
            abnormal_group, _ = await pool.apply(
                batch_process, args=(abnormal_group, pod, template_miner, self.parsed_dir / "abnormal")
            )

        return (normal_group, abnormal_group, pod)

    async def _calculate_rate(self, result: Sequence[Tuple[pd.DataFrame, str]], normal: bool, pool: Pool):
        """Calculate log level rate for each pod."""
        # filter out empty dataframes
        result = [r for r in result if not r[0].empty]
        rate = await pool.apply(LogLevelCalculator.calculate, args=(result, normal))
        await asyncio.to_thread(
            rate.to_csv, self.output_dir / ("normal_rate.csv" if normal else "abnormal_rate.csv"), index=False, mode="w"
        )
        return rate


def get_template_miner(use_persistence_handler="None", config=TemplateMinerConfig()):

    if use_persistence_handler == "redis":
        from drain3.redis_persistence import RedisPersistence

        template_miner = TemplateMiner(
            persistence_handler=RedisPersistence(
                redis_host="localhost", redis_port=6379, redis_db=1, redis_key="drain", redis_pass=None, is_ssl=False
            ),
            config=config,
        )
    elif use_persistence_handler == "file":
        template_miner = TemplateMiner(
            persistence_handler=FilePersistence("./drain_state"),
            config=config,
        )
    else:
        template_miner = TemplateMiner(config=config)
    return template_miner


def parse_with_drain(log_df: pd.DataFrame, pod, template_miner: TemplateMiner) -> pd.DataFrame:
    """parse logs with drain and add template id and template mined columns to the dataframe."""
    log_df["temp_id"] = pd.Series(dtype=str)
    log_df["log_temp"] = pd.Series(dtype=str)

    if pod == "ts-ui-dashboard":
        template_miner.config.drain_sim_th = 0.8
    for index, row in log_df.iterrows():
        result = template_miner.add_log_message(row["message"])
        log_df.at[index, "temp_id"] = result["cluster_id"]
        log_df.at[index, "log_temp"] = result["template_mined"]
    return log_df, template_miner


async def batch_process(
    batch: pd.DataFrame, pod, template_miner: TemplateMiner, output_dir: Path
) -> Tuple[pd.DataFrame, TemplateMiner]:
    """Process each batch and return the processed DataFrame."""
    batch[["Body", "message", "log_level"]] = batch.apply(extract_log, axis="columns", result_type="expand")
    batch.dropna(subset=["log_level"], inplace=True)  # Drop rows with no log level
    batch, template_miner = parse_with_drain(batch, pod, template_miner)
    final_df = batch[["Timestamp", "log_level", "temp_id", "log_temp", "Body"]]
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{pod}.csv"
    if final_df.size == 0:
        # print(f"No logs for {pod}")
        return pd.DataFrame(), template_miner
    final_df.loc[:, "Timestamp"] = pd.to_datetime(final_df["Timestamp"])
    final_df = final_df.sort_values(by="Timestamp")  # sort otherwise the rolling window will not work

    await asyncio.to_thread(final_df.to_csv, output_file, index=False, mode="w")
    # print(f"Logs for {pod} saved to {output_file}")
    return final_df, template_miner
