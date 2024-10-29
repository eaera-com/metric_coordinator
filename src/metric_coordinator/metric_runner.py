import pandas as pd
from datetime import datetime
import warnings
from typing import List, Type, Dict

from api_client.clickhouse_client import ClickhouseClient
from account_metrics import METRIC_CALCULATORS
from metric_coordinator.datastore.clickhouse_datastore import ClickhouseDatastore
from metric_coordinator.model import DataEmiter, Datastore, MetricData
from metric_coordinator.configs import Settings

warnings.filterwarnings("ignore")

class MetricRunner:
    def __init__(self,settings:Settings,input_class:Type[MetricData]) -> None:
        self._metrics = []
        self._emiters:List[DataEmiter]= []
        self._datastores:Dict[Type[MetricData],Datastore] = {}
        self.settings = settings
        self.input_class = input_class

    def validate(self) -> None:
        pass
    
    def setup_clickhouse_client(self) -> None:
        self.client = ClickhouseClient(username=self.settings.CLICKHOUSE_USERNAME, 
                                          password=self.settings.CLICKHOUSE_PASSWORD, 
                                          host=self.settings.CLICKHOUSE_HOST, 
                                          http_port=self.settings.CLICKHOUSE_HTTP_PORT, 
                                          database=self.settings.CLICKHOUSE_DATABASE)
    
    def register_metric(self, metric_class: Type[MetricData]) -> None:
        print(f"Start registering metric {metric_class}")
        calculator = METRIC_CALCULATORS[metric_class]
        if not calculator:
            raise ValueError(f"Metric {metric_class} does not support calculator yet")
        if calculator.input_class != self.input_class:
            raise ValueError(f"Metric {metric_class} does not support input class {self.input_class}")
        self._metrics.append(metric_class)
        # TODO: uncomment when package is updated
        # metric_class.set_metric_runner(self)

        for metric in calculator.additional_data:
            self.bind_datastore(metric)
        for emiter in self._emiters:
            emiter.initialize_metric(metric_class)
        print(f"Metric {metric_class} registered")
        
    def get_metrics(self) -> List[Type[MetricData]]:
        return self._metrics
    
    def register_emitter(self, dataEmiter: DataEmiter) -> None:
        self._emiters.append(dataEmiter)
        for metric in self._metrics:
            dataEmiter.initialize_metric(metric)
    
    def get_emitters(self) -> List[DataEmiter]:
        return self._emiters
    
    def setup_datastore(self, metric_class: Type[MetricData]) -> Datastore:
        return ClickhouseDatastore(metric_class,self.client)
    
    def bind_datastore(self, metric_class: Type[MetricData]) -> None:
        datastore = self.setup_datastore(metric_class)
        self._datastores[metric_class] = datastore
    
    def get_datastore(self, metric_class: Type[MetricData]) -> Datastore:
        if metric_class not in self._datastores:
            raise ValueError(f"Datastore for {metric_class} not found")
        return self._datastores[metric_class]
    
    def get_datastores(self) -> List[Type[MetricData]]:
        # TODO: check if we should return a list of datastores instead
        return self._datastores.keys()
    
    def update_metric(self, metric: Type[MetricData], result: pd.DataFrame) -> None:
        self.get_datastore(metric).put(result)
