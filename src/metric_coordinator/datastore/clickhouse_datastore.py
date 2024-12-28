import datetime
from typing import Dict, Type, Union, Any,List
import numpy as np
import pandas as pd
from pydantic.alias_generators import to_snake

from metric_coordinator.datastore.local_datastore import LocalDatastore
from metric_coordinator.model import Datastore, MetricData
from metric_coordinator.api_client.clickhouse_client import ClickhouseClient
from metric_coordinator.configs import MIN_TIME

class ClickhouseDatastore(Datastore):
    def __init__(self, metric:MetricData, client:ClickhouseClient, table_name:str = None, cluster_columns:tuple[str] = None) -> None:        
        self.metric = metric
        self.client = client
        self.table_name = table_name
        self.cluster_columns = cluster_columns
        self._last_load_time = datetime.datetime.fromtimestamp(MIN_TIME)

    def _validate_cluster_columns(self,cluster_columns:tuple[str]) -> None:
        for column in cluster_columns:
            if column not in self.client.get_table_columns(self.get_metric_table_name()) or column not in self.metric.model_fields:
                raise ValueError(f"Column {column} not found in table {self.get_metric_table_name()}")
    
    def _generate_cluster_clause(self,cluster_value:tuple[Any]) -> str:
        return f"WHERE {self.get_metric_fields(dict(zip(self.cluster_columns,cluster_value)))}" if self.cluster_columns else ""
    
    def get_cluster_value_tuple(self,keys:Dict[str,Any]) -> tuple[Any]:
        if self.cluster_columns is None:
            return "ALL"
        return tuple(keys[col] for col in self.cluster_columns)
    
    def get_metric_table_name(self) -> str:       
        metric_name = to_snake(self.metric.__name__) if not self.table_name else self.table_name
        return metric_name
    
    def get_metric(self) -> Type[MetricData]:
        return self.metric
    
    def get_metric_fields(self,keys:Dict[str,Any]) -> str:
        if len(keys) == 0:
            return ""
        elif len(keys) == 1:
            key,value = next(iter(keys.items()))
            return f"{key} = {self.get_value_field(value)}"
        else:
            return " AND ".join([f"{k} = {self.get_value_field(v)}" for k,v in keys.items()])
    
    def get_value_field(self,value:Any) -> str:
        if isinstance(value, datetime.date) or isinstance(value, str):
            return f"'{value}'"
        return str(value)
    
    def get_latest_row(self, keys: Dict[str, int]) -> pd.Series:
        if self.metric is None:
            raise ValueError("Datastore is not initialized or deactivated")
        query = f"""
        SELECT * FROM {self.get_metric_table_name()} FINAL WHERE {self.get_metric_fields(keys)}
        ORDER BY timestamp_server DESC
        LIMIT 1
        """
        result = self.client.query_df(query)
        if result.empty:
            return None
        return result.iloc[0]
    
    def get_row_by_timestamp(self,keys:Dict[str,int],timestamp: Union[datetime.date,datetime.datetime],timestamp_column:str) -> pd.Series:
        if self.metric is None:
            raise ValueError("Datastore is not initialized or deactivated")
        query = f"""
        SELECT * FROM {self.get_metric_table_name()} FINAL WHERE {self.get_metric_fields(keys)} AND {timestamp_column} = {self.get_value_field(timestamp)}
        ORDER BY timestamp_server DESC
        LIMIT 1
        """
        result = self.client.query_df(query)
        if result.empty:
            return None
        return result.iloc[0]

    def put(self, value:Union[pd.Series,pd.DataFrame]) -> None:
        if self.metric is None:
            raise ValueError("Datastore is not initialized or deactivated")
        if isinstance(value, pd.Series):
            value = value.to_frame()
        self.client.insert_df(self.get_metric_table_name(), value)
        
    def close(self) -> None:
        self.client.drop_tables([self.get_metric_table_name()])
        self.table_name = None
        self.metric = None