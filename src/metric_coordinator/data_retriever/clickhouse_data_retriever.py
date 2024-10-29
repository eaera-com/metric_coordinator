from typing import Annotated, List, Literal, Dict, Any
from pydantic.alias_generators import to_snake
import pandas as pd
from datetime import datetime, timezone


from metric_coordinator.api_client.clickhouse_client import ClickhouseClient
from metric_coordinator.configs import type_map, MIN_TIME
from metric_coordinator.model import DataRetriever, MetricData

from account_metrics import MT5Deal, MT5DealDaily


class ClickhouseDataRetriever(DataRetriever):
    def __init__(self,username:str, password:str, host:str, http_port:str, database:str,
                 server:str= None, retrieve_table:Dict[str,str]= None,
                 metric_table_names:Dict[type[MetricData],str]= {}) -> None: 
        self.client = ClickhouseClient(username=username, password=password, 
                                          host=host, http_port=http_port, database=database) 
        self.server = server
        self.retriave_table = retrieve_table
        assert self.server is not None and self.retriave_table is not None, "Server and retrieve_table must be provided"
        
        self.last_retrieve_timestamp = MIN_TIME
        self.metric_table_names = metric_table_names

    def __str__(self) -> str:
        return f"Clickhouse({self.client},{self.get_server()},{self.retriave_table})"
    
    def get_metric_name(self, metric):
        metric_name = to_snake(metric.__name__) if not self.metric_table_names else self.metric_table_names[metric]
        return metric_name
        
    # DataRetriever Implementation
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
        to_time: Annotated[int, "timestamp time-utc"],
        filters: Dict[str, Any],
    ) -> Dict[str, pd.DataFrame]:
        self.validate(filters)
        # Convert from_time from server time to UTC time
        from_time_utc = int(datetime.fromtimestamp(from_time, tz=timezone.utc).timestamp())
        data = {}
        skip_retrieve = False
        for retrieve_data, retrieve_table in self.retriave_table.items():
            if retrieve_data == "Deal":
                data[retrieve_data] = self._retrieve_deal(retrieve_table,from_time_utc,to_time,filters,skip_retrieve)
            elif retrieve_data == "History":
                data[retrieve_data] = self._retrieve_history(retrieve_table,to_time,filters,skip_retrieve)
            if  data[retrieve_data].empty:
                if "nullable_retrieve" not in filters or retrieve_data not in filters["nullable_retrieve"]:
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
            deal_query = f"SELECT * FROM {table_name} FINAL WHERE TimeUTC >= {from_time} AND TimeUTC <= {to_time} AND server='{self.get_server()}' {f'AND login IN ({",".join(map(str, logins))})' if logins else ''}"
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
            history_query = f"SELECT * FROM {table_name} FINAL WHERE timestamp_utc >= {MIN_TIME} AND timestamp_utc <= {to_time} {f'AND login IN ({",".join(map(str, logins))})' if logins else ''} ORDER BY timestamp_utc"

        df_history = self.client.query_df(history_query)
        if not isinstance(df_history,pd.DataFrame) or df_history.empty:
            return pd.DataFrame(columns=MT5DealDaily.model_fields.keys())
        df_history["Date"] = pd.to_datetime(df_history["Datetime"], unit="s").dt.date
        return df_history


    def get_last_retrieve_timestamp(self) -> Annotated[int, "timestamp"]:
        return self.last_retrieve_timestamp
    
    def update_last_retrieve_timestamp(self,to_time:int) -> None:
        # from api_client.mt5manager_client import manager
        # self.last_retrieve_timestamp = to_time + manager.TimeGet().TimeZone * 60 + 3600 * (manager.TimeGet().DaylightState)
        # TODO: change to max deal timestamp
        self.last_retrieve_timestamp = to_time

    def get_current_metric(self, metric: type[MetricData], from_time: int | None = None) -> pd.DataFrame:
        metric_name = self.get_metric_name(metric)

        column_groupby = [k for k, v in metric.model_fields.items() if "groupby" in v.metadata]
        groupby = ", ".join(column_groupby)
        
        # TODO: specify a field in MetricData to specify how to get the current metric
        if metric in [MT5Deal, MT5DealDaily]:
            query = f"""
            SELECT {metric_name}.* 
            FROM {metric_name} FINAL 
            {f"WHERE timestamp_server <  {from_time}" if from_time else ""}
            """
        else:
            query = f"""
            SELECT {metric_name}.*
            FROM {metric_name} FINAL
            INNER JOIN (
                SELECT {groupby}, max(deal_id) as deal_id
                FROM {metric_name} FINAL
                {f"WHERE timestamp_server <  {from_time}" if from_time else ""}
                GROUP BY {groupby}) as max_deal
                ON {" AND ".join([f"max_deal.{c} = {metric_name}.{c}" for c in column_groupby])} AND
                    max_deal.deal_id = {metric_name}.deal_id
            """
        return self.client.query_df(query)