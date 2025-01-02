import datetime
from typing import Dict, Literal, List, Type, Annotated
import pandas as pd
from pydantic.alias_generators import to_snake

from account_metrics.metric_model import MetricData

from metric_coordinator.api_client.clickhouse_client import ClickhouseClient
from metric_coordinator.model import DataEmiter
from metric_coordinator.configs import MIN_TIME, type_map


class ClickhouseEmitter(DataEmiter):

    def __init__(self, client: ClickhouseClient, server: str = None, metric_table_names: Dict[type[MetricData], str] = {}) -> None:
        self.client = client
        self.metric_table_names = metric_table_names
        self.last_retrieve_timestamp = MIN_TIME
        self.server = server

    def emit(self, data: Dict[type[MetricData], pd.DataFrame]) -> Literal[True]:
        is_emitted = False
        for metric, calculated_metrics in data.items():
            metric_name = self.get_metric_name(metric)

            if calculated_metrics.empty:
                continue
            self.client.insert_df(metric_name, calculated_metrics)
            print(f"Inserted {calculated_metrics.shape[0]} rows into {metric_name}")
            is_emitted = True
        if is_emitted:
            # TODO: check if we should use utc time
            self.last_retrieve_timestamp = int(datetime.datetime.now().timestamp())
        return is_emitted

    def initialize_metric(self, metric: type[MetricData]) -> Literal[True]:
        self._create_metric_if_not_exist(metric)

    def get_last_emit_timestamp(self, metric: type[MetricData], logins: list[int] = []) -> Annotated[int, "timestamp"]:
        metric_name = self.get_metric_name(metric)

        query = (
            f"SELECT max(timestamp_server) FROM {metric_name} FINAL WHERE server='{self.get_server()}' AND login IN ({','.join(map(str, logins))})"
            if logins
            else f"SELECT max(timestamp_server) FROM {metric_name} FINAL WHERE server='{self.get_server()}'"
        )

        return max(self.client.query_ddl(query).first_item.get("max(timestamp_server)"), MIN_TIME)

    def get_server(self) -> str:
        # TODO: update if more than one server are used
        return self.server

    def get_metric_name(self, metric):
        metric_name = to_snake(metric.__name__) if not self.metric_table_names else self.metric_table_names[metric]
        return metric_name

    def _create_metric_if_not_exist(self, metric: type[MetricData]) -> Literal[True]:
        for k, v in metric.model_fields.items():
            if v.annotation.__name__ not in type_map:
                raise ValueError(f"Unsupported type {v.annotation.__name__} for field {k}")

        metric_name = self.get_metric_name(metric)
        metric_fields = ", ".join([f"{k} {type_map.get(v.annotation.__name__)}" for k, v in metric.model_fields.items()])
        keys = ", ".join([k for k, v in metric.model_fields.items() if "key" in v.metadata])

        if not self.client.create_metric_if_not_exist(metric_name, metric_fields, keys):
            # TODO: do a proper logging
            print(f"Failed to create metric {metric_name}")
            return False
        return True

    def drop_metric(self, metric: type[MetricData]) -> Literal[True]:
        metric_name = self.get_metric_name(metric)
        self.client.drop_tables([metric_name])

    def drop_metrics(self, metrics: List[MetricData]) -> Literal[True]:
        metric_names = [self.get_metric_name(metric) for metric in metrics]
        self.client.drop_tables(metric_names)

    def delete_logins_from_metric(self, logins: list[int], metric: type[MetricData], from_time: int | None = None) -> Literal[True]:
        metric_name = self.get_metric_name(metric)
        query = f"""DELETE FROM {metric_name} WHERE Login IN ({','.join(map(str, logins))}) AND {f"timestamp_server >= {from_time}" if from_time else ""}
                """
        if self.client.query_dml(query):
            print(f"Deleted rows from {metric_name} where timestamp_server >= {from_time}")

    def delete_logins_metrics(self, logins: list[int], metrics: List[MetricData], from_time: int | None = None) -> Literal[True]:
        for metric in metrics:
            self.delete_logins_from_metric(logins, metric, from_time)
