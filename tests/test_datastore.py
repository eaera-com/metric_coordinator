import datetime
import pytest
import pandas as pd
from pandas.testing import assert_series_equal

from metric_coordinator.data_retriever.clickhouse_data_retriever import ClickhouseClient
from account_metrics import AccountMetricDaily, MT5DealDaily, MetricData
from metric_coordinator.datastore.clickhouse_datastore import ClickhouseDatastore
from tests.conftest import (
    METRICS,
    join_metric_name_test_name,
    get_test_settings,
    insert_data_into_clickhouse,
    load_csv,
)
from metric_coordinator.configs import type_map
from metric_coordinator.datastore.local_datastore import LocalDatastore


@pytest.fixture
def setup_and_teardown_clickhouse_datastore(request):
    test_name = request.node.name
    settings = get_test_settings()
    datastores = {}
    for metric in METRICS:
        metric_fields = ", ".join(
            [
                f"{k} {type_map.get(
            v.annotation.__name__)}"
                for k, v in metric.model_fields.items()
            ]
        )
        keys = ", ".join([k for k, v in metric.model_fields.items() if "key" in v.metadata])

        client = ClickhouseClient(
            username=settings.CLICKHOUSE_USERNAME,
            password=settings.CLICKHOUSE_PASSWORD,
            host=settings.CLICKHOUSE_HOST,
            http_port=settings.CLICKHOUSE_HTTP_PORT,
            database=settings.CLICKHOUSE_DATABASE,
        )

        if not client.create_metric_if_not_exist(join_metric_name_test_name(metric, test_name), metric_fields, keys):
            raise ValueError(
                f"Failed to create metric {
                             join_metric_name_test_name(metric, test_name)}"
            )
        datastores[metric] = ClickhouseDatastore(metric, client, table_name=join_metric_name_test_name(metric, test_name))

    yield datastores, test_name

    for metric in METRICS:
        datastores[metric].close()


@pytest.fixture
def setup_and_teardown_local_datastore(request):
    test_name = request.node.name
    datastores = {}
    for metric in METRICS:
        datastores[metric] = LocalDatastore(metric)
    yield datastores, test_name
    for metric in METRICS:
        datastores[metric].close()


@pytest.fixture
def setup_and_teardown_local_datastore_sharded(request):
    test_name = request.node.name
    datastores = {}
    for metric in METRICS:
        datastores[metric] = LocalDatastore(metric=metric, sharding_columns=["login"])
    yield datastores, test_name
    for metric in METRICS:
        datastores[metric].close()

def sample_dataframe_of_metric(metric: MetricData, num_rows: int = 1000):
    pass


def insert_data_into_local_datastore(local_datastore: LocalDatastore, dataframe: pd.DataFrame):
    local_datastore.put(dataframe)


def convert_date_column(row: pd.Series, metric: MetricData):
    # TODO: fix this problem with date columns
    if "date" in metric.model_fields:
        row["date"] = row["date"].date()
    if "Date" in metric.model_fields:
        row["Date"] = row["Date"].date()
    return row

class TestClickhouseDatastore:
    @staticmethod
    def test_clickhouse_datastore_get_latest_row(setup_and_teardown_clickhouse_datastore):
        ch_datastores, test_name = setup_and_teardown_clickhouse_datastore

        for metric in METRICS:
            insert_data_into_clickhouse(ch_datastores[metric], metric, test_name)

            expected_df = load_csv(metric)
            expected_last_row = expected_df.iloc[-1]
            key_last_row = expected_last_row[metric.Meta.sharding_columns]
            retrieved_last_row = ch_datastores[metric].get_latest_row({k: key_last_row[k] for k in metric.Meta.sharding_columns})
            # Convert date column to datetime.date type to correct format
            retrieved_last_row = convert_date_column(retrieved_last_row, metric)

            assert_series_equal(retrieved_last_row, expected_last_row, check_index=False, check_names=False)

    @staticmethod
    def test_clickhouse_datastore_get_row_by_timestamp(setup_and_teardown_clickhouse_datastore):
        ch_datastores, test_name = setup_and_teardown_clickhouse_datastore

        for metric in METRICS:
            expected_df = load_csv(metric)
            insert_data_into_clickhouse(ch_datastores[metric], metric, test_name)

            if len(metric.Meta.sharding_columns) <= 1:
                continue
            expected_last_row = expected_df.iloc[-1]
            # remove 1 last key column
            key_last_row = expected_last_row[metric.Meta.sharding_columns]
            sharding_columns = metric.Meta.sharding_columns.copy()
            sharding_columns.pop()

            retrieved_last_row = ch_datastores[metric].get_row_by_timestamp(
                {k: key_last_row[k] for k in sharding_columns}, expected_last_row["timestamp_utc"], "timestamp_utc"
            )
            # Convert date column to datetime.date type to correct format
            retrieved_last_row = convert_date_column(retrieved_last_row, metric)
            assert_series_equal(retrieved_last_row, expected_last_row, check_index=False, check_names=False)

    @staticmethod
    def test_clickhouse_datastore_get_row_by_timestamp_2(setup_and_teardown_clickhouse_datastore):
        ch_datastores, test_name = setup_and_teardown_clickhouse_datastore
        expected_df = load_csv(MT5DealDaily)
        insert_data_into_clickhouse(ch_datastores[MT5DealDaily], MT5DealDaily, test_name)
        expected_last_row = expected_df.iloc[-1]
        retrieved_last_row = ch_datastores[MT5DealDaily].get_row_by_timestamp(
            {"Login": expected_last_row["Login"]}, expected_last_row["Date"], "Date"
        )
        # Convert date column to datetime.date type to correct format
        retrieved_last_row = convert_date_column(retrieved_last_row, MT5DealDaily)
        assert_series_equal(retrieved_last_row, expected_last_row, check_index=False, check_names=False)

        # Return None if not found
        retrieved_last_row = ch_datastores[MT5DealDaily].get_row_by_timestamp(
            {"Login": expected_last_row["Login"]}, datetime.date(1960, 1, 1), "Date"
        )
        assert retrieved_last_row is None

    @staticmethod
    def test_clickhouse_datastore_put(setup_and_teardown_clickhouse_datastore):
        ch_datastores, _ = setup_and_teardown_clickhouse_datastore

        for metric in METRICS:
            df = load_csv(metric)
            ch_datastores[metric].put(df)
            expected_last_row = df.iloc[-1]
            key_last_row = expected_last_row[metric.Meta.sharding_columns]
            retrieved_last_row = ch_datastores[metric].get_latest_row({k: key_last_row[k] for k in metric.Meta.sharding_columns})
            # Convert date column to datetime.date type to correct format
            retrieved_last_row = convert_date_column(retrieved_last_row, metric)
            assert_series_equal(retrieved_last_row, expected_last_row, check_index=False, check_names=False)


class TestLocalDatastore:
    @staticmethod
    def test_local_datastore_put(setup_and_teardown_local_datastore):
        local_datastores, _ = setup_and_teardown_local_datastore
        for metric in METRICS:
            expected_df = load_csv(metric)
            insert_data_into_local_datastore(local_datastores[metric], expected_df)
            expected_last_row = expected_df.iloc[-1]
            key_last_row = expected_last_row[metric.Meta.sharding_columns]
            retrieved_last_row = local_datastores[metric].get_latest_row({k: key_last_row[k] for k in metric.Meta.sharding_columns})
            
            assert_series_equal(retrieved_last_row, expected_last_row, check_index=False, check_names=False)
    
    @staticmethod
    def test_local_datastore_put_2(setup_and_teardown_local_datastore_sharded):
        local_datastores, _ = setup_and_teardown_local_datastore_sharded
        metric = AccountMetricDaily
        expected_df = load_csv(metric)
        insert_data_into_local_datastore(local_datastores[metric], expected_df)
        
        expected_last_row = expected_df.iloc[-1]
            
        sharding_columns = metric.Meta.sharding_columns
        shard_key = {k: expected_last_row[k] for k in sharding_columns}
        retrieved_last_row = local_datastores[metric].get_latest_row(shard_key)
        assert_series_equal(retrieved_last_row, expected_last_row, check_index=False, check_names=False)
        
        shard_key2 =  {'date': datetime.date(2024, 8, 19), 'login': 500387, 'server': 'demo'}
        retrieved_last_row2 = local_datastores[metric].get_latest_row(shard_key2)
        expected_last_row2 = expected_df.iloc[-2]
        assert_series_equal(retrieved_last_row2, expected_last_row2, check_index=False, check_names=False)

    # TODO: add more test cases (with cluster columns = logins also)
    def test_local_datastore_get(setup_and_teardown_local_datastore):
        pass
    
class TestCacheDatastore:
    @staticmethod
    def test_cache_datastore_get_row_by_timestamp():
        pass
