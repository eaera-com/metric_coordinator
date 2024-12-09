import pandas as pd
from datetime import datetime
import warnings
from typing import List, Type, Dict

from metric_coordinator.api_client.clickhouse_client import ClickhouseClient
from account_metrics import METRIC_CALCULATORS
from metric_coordinator.datastore.clickhouse_datastore import ClickhouseDatastore
from metric_coordinator.model import DataEmiter, Datastore, MetricData, MetricRunnerAPI
from metric_coordinator.configs import Settings

warnings.filterwarnings("ignore")

class MetricRunner(MetricRunnerAPI):
    def __init__(self,settings:Settings,input_class:Type[MetricData]) -> None:
        self._metrics = []
        self._emiters:List[DataEmiter]= []
        self._datastores:Dict[Type[MetricData],Datastore] = {}
        self.settings = settings
        self.input_class = input_class
        self.datastore_metric_table_names = None
        self.clickhouse_client = None
    
    def build(self,metrics:List[Type[MetricData]],emits:List[DataEmiter]):
        for metric in metrics:
            self.register_metric(metric)
        for emiter in emits:
            self.register_emitter(emiter)
        self.setup_datastore_metric_table_names()
    
    def update_metric(self, metric: Type[MetricData], result: pd.DataFrame) -> None:
            self.get_datastore(metric).put(result)

    def process_metrics(self,input_data:pd.DataFrame) -> Dict[Type[MetricData],pd.DataFrame]:  
        results = {}
        for metric in self._metrics:
            calculator = METRIC_CALCULATORS[metric]
            results[metric]  = calculator.calculate(input_data)
            print(f"Calculated metric: {metric} with {results[metric].shape[0]} rows")
        self.update_metric(metric, results[metric])
        return results

    def emit_metrics(self) -> None:
        # TODO: add logic retry
        for emiter in self._emiters:
            emiter.emit_metrics(self._metrics)

    def validate(self) -> None:
        # TODO: add logic to validate and detect schema changes
        pass
    
    def setup_clickhouse_client(self) -> None:
        if not self.clickhouse_client:
            self.clickhouse_client = ClickhouseClient(username=self.settings.CLICKHOUSE_USERNAME, 
                                          password=self.settings.CLICKHOUSE_PASSWORD, 
                                          host=self.settings.CLICKHOUSE_HOST, 
                                          http_port=self.settings.CLICKHOUSE_HTTP_PORT, 
                                          database=self.settings.CLICKHOUSE_DATABASE)
    
    def setup_datastore(self, metric_class: Type[MetricData]) -> Datastore:
        if not self.clickhouse_client:
            raise ValueError("Clickhouse client not initialized before setting up clickhouse datastore")
        table_name = None if self.datastore_metric_table_names is None or metric_class not in self.datastore_metric_table_names else self.datastore_metric_table_names.get(metric_class)
        return ClickhouseDatastore(metric_class,self.clickhouse_client,table_name=table_name)
    
    def setup_datasore_metric_table_names(self, metric_table_names:Dict[Type[MetricData],str]) -> None:
        self.datastore_metric_table_names = metric_table_names
    
    def register_metric(self, metric_class: Type[MetricData]) -> None:
        print(f"Start registering metric {metric_class}")
        calculator = METRIC_CALCULATORS[metric_class]
        if not calculator:
            raise ValueError(f"Metric {metric_class} does not support calculator yet")
        if calculator.input_class != self.input_class:
            raise ValueError(f"Metric {metric_class} does not support input class {self.input_class}")
        self._metrics.append(metric_class)
        calculator.set_metric_runner(self)

        for metric in calculator.additional_data:
            self.bind_datastore(metric)
        for emiter in self._emiters:
            emiter.initialize_metric(metric_class)
        print(f"Metric {metric_class} registered")
    
    def register_emitter(self, dataEmiter: DataEmiter) -> None:
        self._emiters.append(dataEmiter)
        for metric in self._metrics:
            dataEmiter.initialize_metric(metric)
            
    def get_metrics(self) -> List[Type[MetricData]]:
        return self._metrics
    
    def get_emitters(self) -> List[DataEmiter]:
        return self._emiters
    
    def bind_datastore(self, metric_class: Type[MetricData]) -> None:
        if metric_class in self._datastores:
            return
        datastore = self.setup_datastore(metric_class)
        self._datastores[metric_class] = datastore
        print(f"Successfully bind Datastore {metric_class}")
    
    def get_datastore(self, metric_class: Type[MetricData]) -> Datastore:
        if metric_class not in self._datastores:
            raise ValueError(f"Datastore for {metric_class} not found")
        return self._datastores[metric_class]
    
    def get_datastores(self) -> List[Type[MetricData]]:
        # TODO: check if we should return a list of datastores instead
        return self._datastores.keys()
    
    def get_clickhouse_client(self) -> ClickhouseClient:
        return self.clickhouse_client
    
    def drop_datastores(self) -> None:
        for metric in self._datastores.keys():
            self._datastores[metric].drop()
        self._datastores = {}
