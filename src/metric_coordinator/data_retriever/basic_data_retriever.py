import datetime
import time
from typing import Dict, Any, List
import pandas as pd
import abc

from metric_coordinator.model import DataRetriever    
from metric_coordinator.metric_runner import MetricRunner
from account_metrics import METRIC_CALCULATORS

class BasicDataRetriever(DataRetriever,abc.ABC):
    _supported_filters:List[str] = ["login"] # TODO: support filters by groups
    
    def __init__(self,filters:Dict[str,Any]) -> None:
        self.filters = filters
        self.metrics_runner: MetricRunner = None
        
    @abc.abstractmethod
    def retrieve_data(self, from_time:int, to_time:int, filters:Dict[str,Any]) -> Dict[str,pd.DataFrame]:
        raise NotImplementedError()
    
    def get_last_retrieve_timestamp(self) -> int:
        return self.retriever.get_last_retrieve_timestamp()
    
    def run(self, filters) -> None:
        # Get current metric
        for metric in self.metrics:
            # TODO: Check for better way to get current metric
            self.currents_metrics[metric] = self.get_sorted_current_metric(metric)
        
        # Calculate new metric
        while True:
            from_time = self.retriever.get_last_retrieve_timestamp()
            to_time = int(datetime.now().timestamp())
            data = self.retriever.retrieve_data(from_time,to_time,filters)
            if not data['Deal'].empty:
                number_data_received = {k: v.shape[0] for k,v in data.items()}
                print(f"Retrieved {number_data_received} deals from: {self.retriever}, with filters: {filters}, from time: {from_time}, to time: {to_time}")

            
            results = {}
            for metric in self.metrics_runner.get_metrics():
                results[metric] = METRIC_CALCULATORS[metric].calculate(data,self.currents_metrics[metric])
                if not results[metric].empty:
                    print(f"Calculated metric: {metric} with {results[metric].shape[0]} rows")

            for emiter in self.emiters:
                if emiter.emit(results):
                    print(f"Emitted to emmiter: {emiter} metrics:{results.keys()}")
            self.update_current_metrics(results)
            time.sleep(self.interval)
