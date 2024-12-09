import sys
import traceback
from typing import Annotated, List, Literal, Dict, Any
from pydantic.alias_generators import to_snake
import pandas as pd
from datetime import datetime, time, timezone

from account_metrics import MT5Deal, MT5DealDaily

from metric_coordinator.data_retriever.basic_data_retriever import BasicDataRetriever
from metric_coordinator.api_client.clickhouse_client import ClickhouseClient
from metric_coordinator.configs import MIN_TIME
from metric_coordinator.model import MetricData
from metric_coordinator.metric_runner import MetricRunner


class ClickhouseDataRetriever(BasicDataRetriever):
    def __init__(self,filters:Dict[str,Any],
                 client:ClickhouseClient,
                 server:str,
                 table_name:str) -> None: 
        super().__init__(filters,server)
        self.client = client
        self.table_name = table_name

    def __str__(self) -> str:
        return f"Clickhouse({self.client},{self.get_server()},{self.retriave_table})"
    
    def get_metric_name(self, metric):
        metric_name = to_snake(metric.__name__) if not self.table_name else self.table_name
        return metric_name
    
    # DataRetriever Implementation
    def run(self,metric_runner:MetricRunner) -> None:
        metric_runner.setup_clickhouse_client()
        super().run(metric_runner)     
               
    def validate(self,filters: Dict[str, Any]) -> None:
        if "group_by" in filters:
            if filters["group_by"] not in ["Group", "Login"]:
                raise ValueError("Invalid group_by value")
            if filters["group_by"] == "Group" and "group" not in filters:
                raise ValueError("group must be provided when group_by is Group")
            if filters["group_by"] == "Login" and "logins" not in filters:
                raise ValueError("logins must be provided when group_by is Login")
        if "nullable_retrieve" in filters and not isinstance(filters["nullable_retrieve"], list):
            raise ValueError("nullable_retrieve must be a list")
            
    def retrieve_data(
        self,
        from_time: Annotated[int, "timestamp time-server"],
        to_time: Annotated[int, "timestamp time-utc"]
    ) -> pd.DataFrame:
        self.validate(self.filters)
        # Convert from_time from server time to UTC time
        from_time_utc = int(datetime.fromtimestamp(from_time, tz=timezone.utc).timestamp())
        skip_retrieve = False
        data = None
        for retrieve_data, retrieve_table in self.retriave_table.items():
            if retrieve_data == "Deal":
                data = self._retrieve_deal(retrieve_table,from_time_utc,to_time,self.filters,skip_retrieve)
            elif retrieve_data == "History":
                data = self._retrieve_history(retrieve_table,to_time,self.filters,skip_retrieve)
            if  data.empty:  
                if "nullable_retrieve" not in self.filters or retrieve_data not in self.filters["nullable_retrieve"]:
                    skip_retrieve = True
        
        if not skip_retrieve:
            self.update_last_retrieve_timestamp(to_time)

        return data
        
    def _retrieve_deal(self,table_name:str,from_time:Annotated[int, "timestamp time-utc"],
                       to_time:Annotated[int, "timestamp time-utc"],
                       filters: Dict[str, Any],skip_retrieve:bool=False) -> pd.DataFrame:
        if skip_retrieve:
            return pd.DataFrame(columns=["Login", "Time", "PositionID"])
            
        if "group_by" in filters and filters["group_by"] == "Login":
            logins = filters['logins']
            deal_query = f"SELECT * FROM {table_name} FINAL WHERE TimeUTC >= {from_time} AND TimeUTC <= {to_time} AND server='{self.get_server()}' AND login IN ({','.join(map(str, logins))})"
        else:
            deal_query = f"SELECT * FROM {table_name} FINAL WHERE TimeUTC >= {from_time} AND TimeUTC <= {to_time} AND server='{self.get_server()}'"
        df_deal = self.client.query_df(deal_query)
        if  not isinstance(df_deal,pd.DataFrame)  or df_deal.empty :
            return pd.DataFrame(columns=MT5Deal.model_fields.keys())
        return df_deal
    
    def _retrieve_history(self,table_name:str,to_time:Annotated[int, "timestamp time-utc"],
                          filters: Dict[str, Any],skip_retrieve:bool=False) -> pd.DataFrame:
        if skip_retrieve:
            return pd.DataFrame(columns=MT5DealDaily.model_fields.keys())
            
        # TODO: consider add timestamp to the history data
        # TODO: fix group_by group
        if "group_by" in filters and filters["group_by"] == "Group":
            history_query = f"SELECT * FROM {table_name} FINAL WHERE timestamp_utc >= {MIN_TIME} AND timestamp_utc <= {to_time} ORDER BY timestamp_utc"
        elif "group_by" in filters and filters["group_by"] == "Login":
            logins = filters['logins']
            history_query = f"SELECT * FROM {table_name} FINAL WHERE timestamp_utc >= {MIN_TIME} AND timestamp_utc <= {to_time} AND login IN ({','.join(map(str, logins))}) ORDER BY timestamp_utc"

        df_history = self.client.query_df(history_query)
        if not isinstance(df_history,pd.DataFrame) or df_history.empty:
            return pd.DataFrame(columns=MT5DealDaily.model_fields.keys())
        df_history["Date"] = pd.to_datetime(df_history["Datetime"], unit="s").dt.date
        return df_history


    def get_last_retrieve_timestamp(self) -> Annotated[int, "timestamp"]:
        return self.last_retrieve_timestamp
    
    def drop_metric(self, metric: type[MetricData]) -> Literal[True]:
        metric_name = self.get_metric_name(metric)
        self.client.drop_tables([metric_name])
    
    def drop_metrics(self, metrics: List[MetricData]) -> Literal[True]:
        metric_names = [self.get_metric_name(metric) for metric in metrics]
        self.client.drop_tables(metric_names)
    
    def update_last_retrieve_timestamp(self,to_time:int) -> None:
        # from api_client.mt5manager_client import manager
        # self.last_retrieve_timestamp = to_time + manager.TimeGet().TimeZone * 60 + 3600 * (manager.TimeGet().DaylightState)
        # TODO: change to max deal timestamp
        self.last_retrieve_timestamp = to_time