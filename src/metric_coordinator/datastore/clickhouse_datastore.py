import datetime
from typing import Dict, Type, Union, Any
import pandas as pd
from pydantic.alias_generators import to_snake

from metric_coordinator.model import Datastore, MetricData
from api_client.clickhouse_client import ClickhouseClient

class ClickhouseDatastore(Datastore):
    def __init__(self, metric:MetricData, client:ClickhouseClient, table_name:str = None) -> None:        
        self.metric = metric
        self.client = client
        self.table_name = table_name
        
    def get_metric_name(self) -> str:
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
    
    def get_latest_row(self,keys:Dict[str,Any]) -> pd.Series:
        if self.metric is None:
            raise ValueError("Datastore is not initialized or deactivated")
        query = f"""
        SELECT * FROM {self.get_metric_name()} FINAL WHERE {self.get_metric_fields(keys)}
        ORDER BY timestamp_server DESC
        LIMIT 1
        """
        result = self.client.query_df(query)
        if result.empty:
            return pd.Series(self.metric(**keys).model_dump())
        return result.iloc[0]
    
    def get_row_by_timestamp(self,keys:Dict[str,int],timestamp:datetime.date,timestamp_column:str) -> pd.Series:
        if self.metric is None:
            raise ValueError("Datastore is not initialized or deactivated")
        query = f"""
        SELECT * FROM {self.get_metric_name()} FINAL WHERE {self.get_metric_fields(keys)} AND {timestamp_column} = {self.get_value_field(timestamp)}
        ORDER BY timestamp_server DESC
        LIMIT 1
        """
        result = self.client.query_df(query)
        if result.empty:
            return pd.Series(self.metric(**keys).model_dump())
        return result.iloc[0]

    def put(self, value:Union[pd.Series,pd.DataFrame]) -> None:
        if self.metric is None:
            raise ValueError("Datastore is not initialized or deactivated")
        if isinstance(value, pd.Series):
            value = value.to_frame()
        self.client.insert_df(self.get_metric_name(), value)
        
    def close(self) -> None:
        self.client.drop_tables([self.get_metric_name()])
        self.table_name = None
        self.metric = None