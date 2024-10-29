import datetime
import logging
from typing import List, Literal, Dict, Type, Annotated
import pandas as pd

from metric_coordinator.model import DataEmiter
from account_metrics.metric_model import MetricData
class LoggingEmitter(DataEmiter):
    def __init__(self,logger:logging.Logger = logging.getLogger(__name__)) -> None:
        self.logger = logger
        self.last_emit_timestamp:Dict[Type[MetricData],int] = {}
    
    def emit(self, data: Dict[MetricData,pd.DataFrame]) -> Literal[True]:
        for metric, df in data.items():
            timestamp = int(datetime.datetime.now().timestamp())
            self.logger.info(f"Emitting data: {metric} at {timestamp} with shape: {df.shape}")
            self.last_emit_timestamp[metric] = timestamp
        return True
    
    def get_last_emit_timestamp(self, metrics:Type[MetricData]) -> Annotated[int, "timestamp"]:
        return self.last_emit_timestamp[metrics]
    
    def initialize_metric(self, metric: Type[MetricData]) -> Literal[True]:
        pass
    