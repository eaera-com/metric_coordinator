import datetime
import pytest
import pandas as pd
from pandas.testing import assert_series_equal

from metric_coordinator.data_retriever.clickhouse_data_retriever import ClickhouseClient
from account_metrics import MT5DealDaily, MetricData
from metric_coordinator.datastore.clickhouse_datastore import ClickhouseDatastore
from tests.conftest import TEST_DATAFRAME_PATH, TEST_METRICS, get_metric_from_csv, get_test_metric_name, get_test_settings, insert_data, setup_date_column_type
from metric_coordinator.configs import type_map
from metric_coordinator.datastore.local_datastore import LocalDatastore

@pytest.fixture
def setup_and_teardown_clickhouse_datastore(request):
    test_name = request.node.name
    settings = get_test_settings()
    datastores = {}
    for metric in TEST_METRICS:
        metric_fields = ", ".join([f"{k} {type_map.get(v.annotation.__name__)}" for k, v in metric.model_fields.items()])
        keys = ", ".join([k for k, v in metric.model_fields.items() if "key" in v.metadata])

        client = ClickhouseClient( username=settings.CLICKHOUSE_USERNAME,
                                                password=settings.CLICKHOUSE_PASSWORD,
                                                host=settings.CLICKHOUSE_HOST,
                                                http_port=settings.CLICKHOUSE_HTTP_PORT,
                                                database=settings.CLICKHOUSE_DATABASE)

        if not client.create_metric_if_not_exist(get_test_metric_name(metric,test_name), metric_fields, keys):
            raise ValueError(f"Failed to create metric {get_test_metric_name(metric,test_name)}")
        datastores[metric] = ClickhouseDatastore(metric,
                                                 client,
                                                 table_name=get_test_metric_name(metric,test_name))
        
    yield datastores,test_name

    for metric in TEST_METRICS:
       datastores[metric].close()

@pytest.fixture
def setup_and_teardown_local_datastore(request):
    test_name = request.node.name
    datastores = {}
    for metric in TEST_METRICS:
        datastores[metric] = LocalDatastore(metric)
    yield datastores,test_name
    for metric in TEST_METRICS:
        datastores[metric].close()

def sample_dataframe_of_metric(metric:MetricData,num_rows:int=1000):
    pass

def insert_data_to_local_datastore(local_datastore:LocalDatastore,dataframe:pd.DataFrame):
    local_datastore.put(dataframe)
        
def convert_date_column(row:pd.Series,metric:MetricData):
    # TODO: fix this problem with date columns
    if 'date' in metric.model_fields:
        row['date'] = row['date'].date()
    if 'Date' in metric.model_fields:
        row['Date'] = row['Date'].date()
    return row

class TestClickhouseDatastore:
    @staticmethod
    def test_clickhouse_datastore_get_latest_row(setup_and_teardown_clickhouse_datastore):
        ch_datastores,test_name = setup_and_teardown_clickhouse_datastore
        expected_dataframes = {metric:get_metric_from_csv(metric,TEST_DATAFRAME_PATH[metric]) for metric in TEST_METRICS}
        for metric in TEST_METRICS:
            insert_data(ch_datastores[metric],metric,test_name)
        for metric in TEST_METRICS:
            expected_last_row = expected_dataframes[metric].iloc[-1]
            key_last_row = expected_last_row[metric.Meta.key_columns]
            retrieved_latest_row = ch_datastores[metric].get_latest_row({k:key_last_row[k] for k in metric.Meta.key_columns})
            # Convert date column to datetime.date type to correct format
            retrieved_latest_row = convert_date_column(retrieved_latest_row,metric)
            assert_series_equal(retrieved_latest_row,expected_last_row,check_index=False, check_names= False)

    @staticmethod
    def test_clickhouse_datastore_get_row_by_timestamp(setup_and_teardown_clickhouse_datastore):
        ch_datastores,test_name = setup_and_teardown_clickhouse_datastore
        expected_dataframes = {metric:get_metric_from_csv(metric,TEST_DATAFRAME_PATH[metric]) for metric in TEST_METRICS}
        for metric in TEST_METRICS:
            insert_data(ch_datastores[metric],metric,test_name)
        for metric in TEST_METRICS:
            if len(metric.Meta.key_columns) <= 1:
                continue
            expected_last_row = expected_dataframes[metric].iloc[-1]
            # remove 1 last key column
            key_last_row = expected_last_row[metric.Meta.key_columns]
            key_columns = metric.Meta.key_columns.copy()
            key_columns.pop()
            
            retrieved_latest_row = ch_datastores[metric].get_row_by_timestamp({k:key_last_row[k] for k in key_columns},expected_last_row['timestamp_utc'],'timestamp_utc')
            # Convert date column to datetime.date type to correct format
            retrieved_latest_row = convert_date_column(retrieved_latest_row,metric)
            assert_series_equal(retrieved_latest_row,expected_last_row,check_index=False, check_names= False)

    @staticmethod        
    def test_clickhouse_datastore_get_row_by_timestamp_2(setup_and_teardown_clickhouse_datastore):
        ch_datastores,test_name = setup_and_teardown_clickhouse_datastore
        expected_dataframe = get_metric_from_csv(MT5DealDaily,TEST_DATAFRAME_PATH[MT5DealDaily])
        insert_data(ch_datastores[MT5DealDaily],MT5DealDaily,test_name)
        expected_last_row = expected_dataframe.iloc[-1]
        retrieved_latest_row = ch_datastores[MT5DealDaily].get_row_by_timestamp({'Login':expected_last_row['Login']},expected_last_row['Date'],'Date')
        # Convert date column to datetime.date type to correct format
        retrieved_latest_row = convert_date_column(retrieved_latest_row,MT5DealDaily)
        assert_series_equal(retrieved_latest_row,expected_last_row,check_index=False, check_names= False)
        
        # Return None if not found
        retrieved_latest_row = ch_datastores[MT5DealDaily].get_row_by_timestamp({'Login':expected_last_row['Login']},datetime.date(1960,1,1),'Date')
        assert retrieved_latest_row is None

    @staticmethod
    def test_clickhouse_datastore_put(setup_and_teardown_clickhouse_datastore):
        ch_datastores,_ = setup_and_teardown_clickhouse_datastore
        expected_dataframes = {metric:get_metric_from_csv(metric,TEST_DATAFRAME_PATH[metric]) for metric in TEST_METRICS}

        for metric in TEST_METRICS:
            ch_datastores[metric].put(expected_dataframes[metric])
            expected_last_row = expected_dataframes[metric].iloc[-1]
            key_last_row = expected_last_row[metric.Meta.key_columns]
            retrieved_latest_row = ch_datastores[metric].get_latest_row({k:key_last_row[k] for k in metric.Meta.key_columns})
            # Convert date column to datetime.date type to correct format
            retrieved_latest_row = convert_date_column(retrieved_latest_row,metric)
            assert_series_equal(retrieved_latest_row,expected_last_row,check_index=False, check_names= False)

class TestLocalDatastore:
    @staticmethod
    def test_local_datastore_put(setup_and_teardown_local_datastore):
        local_datastores,_ = setup_and_teardown_local_datastore
        expected_dataframes = {metric:get_metric_from_csv(metric,TEST_DATAFRAME_PATH[metric]) for metric in TEST_METRICS}
        for metric in TEST_METRICS:
            insert_data_to_local_datastore(local_datastores[metric],expected_dataframes[metric])
            expected_last_row = expected_dataframes[metric].iloc[-1]
            key_last_row = expected_last_row[metric.Meta.key_columns]
            retrieved_latest_row = local_datastores[metric].get_latest_row({k:key_last_row[k] for k in metric.Meta.key_columns})
            assert_series_equal(retrieved_latest_row,expected_last_row,check_index=False, check_names= False)   
            
    # TODO: add more test cases (with cluster columns = logins also)
            
class TestCacheDatastore:
    @staticmethod
    def test_cache_datastore_get_row_by_timestamp():
        pass
    