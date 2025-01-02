import abc
import datetime
from typing import Annotated, Any, Dict, Type, Literal, List
import pandas as pd

from account_metrics.metric_model import MetricData


class Datastore(object):

    @abc.abstractmethod
    def get_metric(self) -> Type[MetricData]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_latest_row(self, shard_key: Dict[str, int]) -> pd.Series:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_row_by_timestamp(self, shard_key: Dict[str, int], timestamp: datetime.date, timestamp_column: str) -> pd.Series:
        raise NotImplementedError()

    @abc.abstractmethod
    def put(self, value: pd.Series) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def close(self) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def drop(self) -> None:
        raise NotImplementedError()


class DataEmiter(abc.ABC):
    @abc.abstractmethod
    def emit(self, data: Dict[MetricData, pd.DataFrame]) -> Literal[True]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_last_emit_timestamp(self, metrics: Type[MetricData]) -> Annotated[int, "timestamp"]:
        raise NotImplementedError()

    @abc.abstractmethod
    def initialize_metric(self, metric: Type[MetricData]) -> Literal[True]:
        raise NotImplementedError()


class MetricRunnerAPI(abc.ABC):

    @abc.abstractmethod
    def validate(self) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def register_metric(self, metric_class: Type[MetricData]) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_metrics(self) -> List[Type[MetricData]]:
        raise NotImplementedError()

    @abc.abstractmethod
    def register_emitter(self, metric_name: str) -> Type[MetricData]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_emitters(self) -> List[DataEmiter]:
        raise NotImplementedError()

    @abc.abstractmethod
    def setup_datastore(self, metric_class: Type[MetricData]) -> Datastore:
        raise NotImplementedError()

    @abc.abstractmethod
    def bind_datastore(self, datastore: Datastore) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_datastore(self, metric_class: Type[MetricData]) -> Datastore:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_datastores(self) -> List[Datastore]:
        raise NotImplementedError()

    @abc.abstractmethod
    def update_metric(self, metric: Type[MetricData], result: pd.DataFrame) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def process_metrics(self, input_metric: pd.DataFrame, input_metric_class: Type[MetricData]) -> None:
        raise NotImplementedError()


class DataRetriever(abc.ABC):

    @abc.abstractmethod
    def validate(filters: Dict[str, Any]) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def retrieve_data(self, to_time: Annotated[int, "timestamp time-utc"]) -> Dict[str, pd.DataFrame]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_server(self) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_last_retrieve_timestamp(self) -> Annotated[int, "timestamp"]:
        raise NotImplementedError()

    @abc.abstractmethod
    def run(self) -> None:
        raise NotImplementedError()
