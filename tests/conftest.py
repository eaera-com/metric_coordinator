from typing import Any, Dict, Union
import pytest
from pydantic.alias_generators import to_snake
import pandas as pd
import os
import datetime

from metric_coordinator.configs import Settings, MIN_TIME
from metric_coordinator.data_retriever.clickhouse_data_retriever import ClickhouseDataRetriever
from metric_coordinator.datastore.clickhouse_datastore import ClickhouseDatastore
from metric_coordinator.model import MetricData

from account_metrics import AccountMetricDaily, AccountMetricByDeal, AccountSymbolMetricByDeal, PositionMetricByDeal, MT5Deal, MT5DealDaily

METRICS = [AccountMetricDaily, AccountMetricByDeal, AccountSymbolMetricByDeal, PositionMetricByDeal, MT5Deal, MT5DealDaily]


def load_csv(metric: MetricData) -> pd.DataFrame:
    def get_type_mapping(metric: MetricData) -> Dict[str, str]:
        type_mapping = {}
        for field_name, field in metric.model_fields.items():
            if field.annotation in [int, "int32", "int64"]:
                type_mapping[field_name] = "int64"  # Use nullable integer type
            elif field.annotation == float:
                type_mapping[field_name] = "float64"
            elif field.annotation in [str, "string"]:
                type_mapping[field_name] = "string"
            else:
                type_mapping[field_name] = "object"
        return type_mapping

    csv_path = os.path.abspath(f"tests/test_data/{to_snake(metric.__name__)}.csv")
    date_columns = [field_name for field_name, field in metric.model_fields.items() if field.annotation == datetime.date]
    df = pd.read_csv(csv_path, dtype=get_type_mapping(metric), parse_dates=date_columns)

    # Convert string columns with NA values to empty strings
    string_columns = df.select_dtypes(include=["string"]).columns
    df[string_columns] = df[string_columns].fillna("")

    df = setup_date_column_type(df, metric)
    df = df[metric.model_fields.keys()]  # Reorder columns to match the metric model fields
    return df


def get_test_settings():
    return Settings(
        CLICKHOUSE_HOST="localhost",
        CLICKHOUSE_HTTP_PORT="8124",
        CLICKHOUSE_USERNAME="default",
        CLICKHOUSE_PASSWORD="",
        CLICKHOUSE_DATABASE="default",
        MT_GROUPS="test_group",
        MT_SERVER="test_server",
    )


@pytest.fixture
def get_test_name(request):
    return request.node.name


def join_metric_name_test_name(metric: MetricData, test_name: str):
    return f"{test_name}_{to_snake(metric.__name__)}"


def insert_data_into_clickhouse(ch: Union[ClickhouseDatastore, ClickhouseDataRetriever], metric: MetricData, test_name: str):
    df = load_csv(metric)
    result = ch.client.insert_df(join_metric_name_test_name(metric, test_name), df)
    assert result


def setup_string_column_type(df: pd.DataFrame, metric: MetricData):
    for field_name, field in metric.model_fields.items():
        if field.annotation in [str, "string"]:
            df[field_name] = df[field_name].astype("string")
    return df


def setup_date_column_type(df: pd.DataFrame, metric: MetricData):
    for field_name, field in metric.model_fields.items():
        if field.annotation == datetime.date:
            df[field_name] = pd.to_datetime(df[field_name]).dt.date
    return df


def strip_quotes_from_string_columns(df: pd.DataFrame):
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].apply(
            lambda x: (
                x[2:-1] if isinstance(x, str) and x.startswith("b'") and x.endswith("'") else x.strip("'") if isinstance(x, str) else x
            )
        )
        df[col] = df[col].astype("string")
    return df


def retrieve_test_data(ch, from_time: int = MIN_TIME, to_time: int = 1750000000):
    filters = {"group_by": "Group", "group": get_test_settings().MT_GROUPS, "nullable_retrieve": ["History"]}
    return ch.retrieve_data(from_time, to_time, filters)


def get_default_metric_data(metric: MetricData, key_values: Dict[str, Any]):
    metric_row = metric()
    for key, value in key_values.items():
        metric_row[key] = value
    return metric_row
