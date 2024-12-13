import datetime
from typing import Dict, Type, Union, Any,List
from account_metrics import MT5DealDaily
import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal
from pydantic.alias_generators import to_snake

from metric_coordinator.datastore.local_datastore import LocalDatastore
from metric_coordinator.model import Datastore, MetricData
from metric_coordinator.api_client.clickhouse_client import ClickhouseClient
from metric_coordinator.configs import MIN_TIME

class ClickhouseDatastore(Datastore):
    def __init__(self, metric:MetricData, client:ClickhouseClient, table_name:str = None, cluster_columns:tuple[str] = None, load_interval:int = 2) -> None:        
        self.metric = metric
        self.client = client
        self.table_name = table_name
        self.cluster_columns = cluster_columns
        self.reload_interval = load_interval
        self._last_load_time = datetime.datetime.fromtimestamp(MIN_TIME)
        self.cache = LocalDatastore(metric,cluster_columns) if load_interval != 0 else None

    def _validate_cluster_columns(self,cluster_columns:tuple[str]) -> None:
        for column in cluster_columns:
            if column not in self.client.get_table_columns(self.get_metric_name()) or column not in self.metric.model_fields:
                raise ValueError(f"Column {column} not found in table {self.get_metric_name()}")
    
    def _generate_cluster_clause(self,cluster_value:tuple[Any]) -> str:
        return f"WHERE {self.get_metric_fields(dict(zip(self.cluster_columns,cluster_value)))}" if self.cluster_columns else ""
    
    def get_cluster_value_tuple(self,keys:Dict[str,Any]) -> tuple[Any]:
        if self.cluster_columns is None:
            return "ALL"
        return tuple(keys[col] for col in self.cluster_columns)

    def eager_load(self,cluster_value:tuple[Any] = None) -> pd.DataFrame:
        # TODO: check if we want to parallelize this
        if self.cluster_columns is None:
            df = self.client.query_df(f"SELECT * FROM {self.get_metric_name()} FINAL")
        else:
            assert len(cluster_value) == len(self.cluster_columns)
            # TODO: validate cluster_value in the correct type
            df = self.client.query_df(f"SELECT * FROM {self.get_metric_name()} FINAL {self._generate_cluster_clause(cluster_value)}")
        self.cache.reload_data(df)
        self._last_load_time = datetime.datetime.now()
        return df
    
    def _need_reload(self,timestamp: Union[datetime.date,datetime.datetime]) -> bool:
        if self.cache is None:
            return False
        if isinstance(timestamp,int) or isinstance(timestamp,np.int64):
            timestamp = datetime.datetime.fromtimestamp(timestamp)
        elif isinstance(timestamp,datetime.date):
            timestamp = datetime.datetime.combine(timestamp, datetime.time.max)
        time_diff = timestamp - self._last_load_time
        return time_diff.total_seconds() > self.reload_interval
    
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
    
    def get_latest_row(self, keys: Dict[str, int]) -> pd.Series:
        if self.cache is None:
            return self.get_latest_row_from_clickhouse(keys)
        if self._need_reload(datetime.datetime.now()):
            self.eager_load()
        return self.get_latest_row_from_local(keys)
    
    def _convert_date_column(self,row:pd.Series,metric:MetricData):
        # TODO: fix this problem with date columns
        # Convert date columns to datetime.date type if they exist in the row
        if 'date' in metric.model_fields and 'date' in row:
            if isinstance(row['date'], pd.Timestamp):
                row['date'] = row['date'].date()
            elif isinstance(row['date'], datetime.datetime):
                row['date'] = row['date'].date()
            # If it's already datetime.date, leave it as is
    
        if 'Date' in metric.model_fields and 'Date' in row:
            if isinstance(row['Date'], pd.Timestamp):
                row['Date'] = row['Date'].date()
            elif isinstance(row['Date'], datetime.datetime):
                row['Date'] = row['Date'].date()
            # If it's already datetime.date, leave it as is
        
        return row
    
    def get_latest_row_from_local(self, keys: Dict[str, int]) -> pd.Series:
        return self.cache.get_latest_row(keys)
    
    def get_latest_row_from_clickhouse(self,keys:Dict[str,Any]) -> pd.Series:
        # TODO: Check what to do in this case
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
    
    def get_row_by_timestamp(self,keys:Dict[str,int],timestamp: Union[datetime.date,datetime.datetime],timestamp_column:str) -> pd.Series:
        if self.cache is None:
            return self.get_row_by_timestamp_from_clickhouse(keys,timestamp,timestamp_column)
        if self._need_reload(timestamp):
            self.eager_load()
        result =  self.get_row_by_timestamp_from_clickhouse(keys,timestamp,timestamp_column)

        result = self._convert_date_column(result,self.metric)

        if result is None:
            print("Warning: No data found in local cache, loading from clickhouse")
            result = self.get_row_by_timestamp_from_clickhouse(keys,timestamp,timestamp_column)
            # print(f"result: {result}")
            # self.cache.put(result)
        return result
    
    def get_row_by_timestamp_from_local(self,keys:Dict[str,int],timestamp:datetime.date,timestamp_column:str) -> pd.Series:
        return self.cache.get_row_by_timestamp(keys,timestamp,timestamp_column,use_default_value=False)
    
    def get_row_by_timestamp_from_clickhouse(self,keys:Dict[str,int],timestamp:datetime.date,timestamp_column:str) -> pd.Series:
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