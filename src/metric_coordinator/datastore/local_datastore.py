
import datetime
from typing import Dict, Type, Union, Any,List
from account_metrics import MT5Deal, MT5DealDaily
import pandas as pd
from pydantic.alias_generators import to_snake

from metric_coordinator.model import Datastore, MetricData

class LocalDatastore(Datastore):
    NONE_CLUSTER_KEY_VALUE = "ALL"
    MINIMUM_VALUE_PUT_IN_BATCH = 1000
    def __init__(self, metric:MetricData, cluster_columns:tuple[str] = None, batch_size:int = 64) -> None:        
        self.metric = metric
        self.cluster_columns = sorted(cluster_columns) if cluster_columns else None
        self._batch_size = batch_size
        self.cluster_values_to_dataframe = {}
           
    def get_metric_name(self) -> str:       
        metric_name = to_snake(self.metric.__name__) if not self.table_name else self.table_name
        return metric_name
    
    def get_metric(self) -> Type[MetricData]:
        return self.metric
    
    def get_cluster_value_tuple(self,keys:Dict[str,Any]) -> tuple[Any]:
        if self.cluster_columns is None:
            return "ALL"
        return tuple(keys[col] for col in self.cluster_columns)
    
    def get_latest_row(self,keys:Dict[str,Any]) -> pd.Series:
        if self.metric is None:
            raise ValueError("Datastore is not initialized or deactivated")
        result_df = self.cluster_values_to_dataframe.get(self.get_cluster_value_tuple(keys),None)
        if result_df is None:
            return pd.Series(self.metric(**keys).model_dump())
        return result_df.iloc[-1]
    
    def get_row_by_timestamp(self,keys:Dict[str,int],timestamp:datetime.date,timestamp_column:str,use_default_value:bool = True) -> pd.Series:
        if self.metric is None:
            raise ValueError("Datastore is not initialized or deactivated")
        result_df = self.cluster_values_to_dataframe.get(self.get_cluster_value_tuple(keys),None)
        result_df = result_df[result_df[timestamp_column] == timestamp]
        if result_df.empty:
            if use_default_value:   
                return pd.Series(self.metric(**keys).model_dump())
            else:
                return None
        return result_df.iloc[-1]
    
    def reload_data(self) -> None:
        raise NotImplementedError("Not implemented")
    
    def reload_data(self, df:pd.DataFrame) -> None:
        self.clean_data()
        self.put(df)
    
    def put(self, value:Union[pd.Series,pd.DataFrame]) -> None:
        if self.metric is None:
            raise ValueError("Datastore is not initialized or deactivated")
        if isinstance(value, pd.Series):
            value = value.to_frame()
        if value.shape[0] >= self.MINIMUM_VALUE_PUT_IN_BATCH:
            self._put_in_batch(value)
        else:
            for _,row in value.iterrows():
                key_values = row[self.metric.Meta.key_columns].to_dict()
                cluster_key = self.get_cluster_value_tuple(key_values)
                pydantic_row = self.metric(**row.to_dict()).model_dump()

                if cluster_key not in self.cluster_values_to_dataframe:
                    # Initialize empty DataFrame with metric model columns
                    empty_df = pd.DataFrame(columns=list(self.metric.model_fields.keys()))
                    self.cluster_values_to_dataframe[cluster_key] = empty_df
                #TODO: implement dataframe limit
                self.cluster_values_to_dataframe[cluster_key] = pd.concat([
                    self.cluster_values_to_dataframe[cluster_key],
                    pd.DataFrame([pydantic_row])
                ], ignore_index=True)
                

    def _put_in_batch(self, value):
        batch : Dict[tuple[Any],List[pd.DataFrame]] = {}
        for _,row in value.iterrows():
            key_values = row[self.metric.Meta.key_columns].to_dict()
            batch[self.get_cluster_value_tuple(key_values)].append(row)
            # flush batch if it is full
            if len(batch[self.get_cluster_value_tuple(key_values)]) == self._batch_size:
                self.cluster_values_to_dataframe[self.get_cluster_value_tuple(key_values)] = \
                    pd.concat(batch[self.get_cluster_value_tuple(key_values)],ignore_index=True)
                batch[self.get_cluster_value_tuple(key_values)] = []
        # flush remaining batch
        for key,rows in batch.items():
            self.cluster_values_to_dataframe[key] = pd.concat(rows,ignore_index=True)
        
    def clean_data(self) -> None:
        self.cluster_values_to_dataframe = {}
    
    def close(self) -> None:
        self.table_name = None
        self.metric = None
        self.clean_data()