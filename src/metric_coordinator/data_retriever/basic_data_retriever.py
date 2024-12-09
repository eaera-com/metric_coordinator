import datetime
import time
from typing import Dict, Any, List
import pandas as pd
import abc

from account_metrics.metric_model import MetricData

from metric_coordinator.model import DataRetriever    
from metric_coordinator.metric_runner import MetricRunner

class BasicDataRetriever(DataRetriever,abc.ABC):
    _supported_filters:List[str] = ["login"] # TODO: support filters by groups
    
    def __init__(self,filters:Dict[str,Any],server:str) -> None:
        self.filters = filters
        self.server = server
        
    def get_server(self) -> str:
        return self.server
    
    @abc.abstractmethod
    def retrieve_data(self, from_time:int, to_time:int, filters:Dict[str,Any]) -> Dict[str,pd.DataFrame]:
        raise NotImplementedError()
    
    def get_last_retrieve_timestamp(self) -> int:
        return self.retriever.get_last_retrieve_timestamp()
    
    def run(self,metric_runner:MetricRunner) -> None:
        while True:
            from_time = self.retriever.get_last_retrieve_timestamp()
            to_time = int(datetime.now().timestamp())
            input_data = self.retrieve_data(from_time,to_time,self.filters)
            if not input_data['Deal'].empty:
                number_data_received = {k: v.shape[0] for k,v in input_data.items()}
                print(f"Retrieved {number_data_received} deals from: {self.retriever}, with filters: {self.filters}, from time: {from_time}, to time: {to_time}")
            
            results = metric_runner.process_metrics(input_data)
            metric_runner.emit_metrics(results)
            
            time.sleep(self.interval)
