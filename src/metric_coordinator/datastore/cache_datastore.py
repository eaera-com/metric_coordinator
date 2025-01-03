import datetime
from typing import Dict, Type, Union, Any, List
from account_metrics import MT5DealDaily
import numpy as np
import pandas as pd
from pydantic.alias_generators import to_snake

from metric_coordinator.datastore.local_datastore import LocalDatastore
from metric_coordinator.model import BaseDatastore, MetricData
from metric_coordinator.configs import MIN_TIME


class CacheDatastore(BaseDatastore):
    def __init__(self, metric: MetricData, source_datastore: BaseDatastore, load_interval: int = 86400) -> None:
        self.metric = metric
        self.source_datastore = source_datastore
        # TODO: Support different sharding columns for source and cache
        self.sharding_columns = self.source_datastore.sharding_columns
        self.reload_interval = load_interval
        self._last_load_time = datetime.datetime.fromtimestamp(MIN_TIME)
        self.cache = LocalDatastore(metric, self.sharding_columns)

    def put(self, value: pd.Series, push_to_source: bool = False) -> None:
        self.cache.put(value)
        # TODO: consider batch writing
        if push_to_source:
            self.source_datastore.put(value)

    def get_metric(self):
        return super().get_metric()
    
    def get_latest_row(self, shard_key: Dict[str, int]) -> pd.Series:
        if self._need_reload(datetime.datetime.now()):
            self._eager_load()
        result = self._get_latest_row_from_local(shard_key)

        if result is None:
            # TODO: logging local cache miss
            #  print("Warning: No data found in local cache, loading from clickhouse")
            result = self.source_datastore.get_latest_row(shard_key)
            self.cache.put(result)
        return result

    def get_row_by_timestamp(self, shard_key: Dict[str, int], timestamp: datetime.date, timestamp_column: str) -> pd.Series:
        if self._need_reload(timestamp):
            self._eager_load()
        result = self._get_row_by_timestamp_from_local(shard_key, timestamp, timestamp_column)

        if result is None:
            # TODO: logging local cache miss
            # print("Warning: No data found in local cache, loading from clickhouse")
            result = self.source_datastore.get_row_by_timestamp(shard_key, timestamp, timestamp_column)
            if result is not None:
                # TODO: Fix potential wrong order bugs
                self.cache.put(result)

        return result

    def get_source_datastore(self) -> BaseDatastore:
        return self.source_datastore

    def _get_row_by_timestamp_from_local(self, shard_key: Dict[str, int], timestamp: datetime.date, timestamp_column: str) -> pd.Series:
        return self.cache.get_row_by_timestamp(shard_key, timestamp, timestamp_column, use_default_value=False)

    def _get_latest_row_from_local(self, shard_key: Dict[str, int]) -> pd.Series:
        return self.cache.get_latest_row(shard_key)

    def _eager_load(self, shard_key_values: tuple[Any] = None) -> pd.DataFrame:
        # TODO: support eager load from timestamp (not reload_data but concat data)
        # TODO: migrate all query to clickhouse datastore
        # TODO: check if we want to parallelize this
        if self.sharding_columns is None:
            df = self.source_datastore.client.query_df(f"SELECT * FROM {self.source_datastore.get_metric_table_name()} FINAL")
        else:
            assert len(shard_key_values) == len(self.sharding_columns)
            # TODO: validate that shard_key_values is in the correct type
            df = self.source_datastore.client.query_df(
                f"SELECT * FROM {self.source_datastore.get_metric_table_name()} FINAL {self._generate_sharding_clause(shard_key_values)}"
            )
        self.cache.reload_data(df)
        self._last_load_time = datetime.datetime.now()
        return df

    def _need_reload(self, timestamp: Union[datetime.date, datetime.datetime]) -> bool:
        if self.cache is None:
            return False
        if isinstance(timestamp, int) or isinstance(timestamp, np.int64):
            timestamp = datetime.datetime.fromtimestamp(timestamp)
        elif isinstance(timestamp, datetime.date):
            timestamp = datetime.datetime.combine(timestamp, datetime.time.max)
        time_diff = timestamp - self._last_load_time
        return time_diff.total_seconds() > self.reload_interval
