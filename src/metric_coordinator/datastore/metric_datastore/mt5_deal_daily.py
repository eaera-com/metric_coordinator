import pandas as pd
from pydantic.alias_generators import to_snake
from typing import Dict, Type, Any
import datetime
from account_metrics import MT5DealDaily,MetricData
from metric_coordinator.api_client.clickhouse_client import ClickhouseClient
from model import Datastore

class MT5DealDailyDatastore(Datastore):
    def __init__(self, client:ClickhouseClient, table_name:str = None) -> None:
        self.metric = MT5DealDaily
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
    
    def eager_load(self) -> pd.DataFrame:
        keys = {"login": login_list}
        keys = {"login": login}
        query = f"""
        SELECT * FROM {self.get_metric_name()} FINAL WHERE {self.get_metric_fields(keys)}
        ORDER BY timestamp_server DESC
        """
        result = self.client.query_df(query)
        return result
