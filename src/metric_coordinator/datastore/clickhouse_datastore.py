import datetime
from typing import Dict, Type, Union, Any, List
import numpy as np
import pandas as pd
from pydantic.alias_generators import to_snake

from metric_coordinator.datastore.local_datastore import LocalDatastore
from metric_coordinator.model import BaseDatastore, MetricData
from metric_coordinator.api_client.clickhouse_client import ClickhouseClient
from metric_coordinator.configs import MIN_TIME


class ClickhouseDatastore(BaseDatastore):
    DEFAULT_SHARD_KEY_VALUES = ("ALL",)

    def __init__(self, metric: MetricData, client: ClickhouseClient, table_name: str = None, sharding_columns: tuple[str] = None) -> None:
        self.metric = metric
        self.client = client
        self.table_name = table_name
        self.sharding_columns = sharding_columns
        self._last_load_time = datetime.datetime.fromtimestamp(MIN_TIME)

    def put(self, value: Union[pd.Series, pd.DataFrame]) -> None:
        if self.metric is None:
            raise ValueError("Datastore is not initialized or deactivated")
        if isinstance(value, pd.Series):
            value = value.to_frame()
        self.client.insert_df(self.get_metric_table_name(), value)

    def get_metric(self) -> Type[MetricData]:
        return self.metric

    def get_latest_row(self, shard_key: Dict[str, int]) -> pd.Series:
        if self.metric is None:
            raise ValueError("Datastore is not initialized or deactivated")
        query = f"""
        SELECT * FROM {self.get_metric_table_name()} FINAL WHERE {self._get_metric_fields(shard_key)}
        ORDER BY timestamp_server DESC
        LIMIT 1
        """
        result = self.client.query_df(query)
        if result is None or result.empty:
            return None
        return result.iloc[0]

    def get_row_by_timestamp(
        self, shard_key: Dict[str, int], timestamp: Union[datetime.date, datetime.datetime], timestamp_column: str
    ) -> pd.Series:
        if self.metric is None:
            raise ValueError("Datastore is not initialized or deactivated")
        query = f"""
        SELECT * FROM {self.get_metric_table_name()} FINAL WHERE {self._get_metric_fields(shard_key)} AND {timestamp_column} = {self._get_value_field(timestamp)}
        ORDER BY timestamp_server DESC
        LIMIT 1
        """
        result = self.client.query_df(query)
        if result is None or result.empty:
            return None
        return result.iloc[0]

    def close(self) -> None:
        self.client.drop_tables([self.get_metric_table_name()])
        self.table_name = None
        self.metric = None
    
    def _get_metric_fields(self, shard_key: Dict[str, Any]) -> str:
        if len(shard_key) == 0:
            return ""
        elif len(shard_key) == 1:
            key, value = next(iter(shard_key.items()))
            return f"{key} = {self._get_value_field(value)}"
        else:
            return " AND ".join([f"{k} = {self._get_value_field(v)}" for k, v in shard_key.items()])

    def _get_value_field(self, value: Any) -> str:
        if isinstance(value, datetime.date) or isinstance(value, str):
            return f"'{value}'"
        return str(value)

 
    def get_metric_table_name(self) -> str:
        metric_name = to_snake(self.metric.__name__) if not self.table_name else self.table_name
        return metric_name
    
    def _validate_sharding_columns(self, sharding_columns: tuple[str]) -> None:
        for column in sharding_columns:
            if column not in self.client.get_table_columns(self.get_metric_table_name()) or column not in self.metric.model_fields:
                raise ValueError(
                    f"Column {column} not found in table {
                                 self.get_metric_table_name()}"
                )

    def _generate_sharding_clause(self, shard_key_values: tuple[Any]) -> str:
        return f"WHERE {self._get_metric_fields(dict(zip(self.sharding_columns, shard_key_values)))}" if self.sharding_columns else ""

    def _extract_shard_key_values(self, shard_key: Dict[str, Any]) -> tuple[Any]:
        if self.sharding_columns is None:
            return self.DEFAULT_SHARD_KEY_VALUES
        return tuple(shard_key[col] for col in self.sharding_columns)