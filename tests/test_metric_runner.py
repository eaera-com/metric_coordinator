import pandas as pd
import pytest

from account_metrics import AccountMetricByDeal, AccountMetricDaily, AccountSymbolMetricByDeal, MT5Deal, MT5DealDaily, PositionMetricByDeal
from account_metrics import METRIC_CALCULATORS

from metric_coordinator.configs import type_map
from metric_coordinator.data_emiter.clickhouse_data_emiter import ClickhouseEmitter
from metric_coordinator.data_emiter.logging_data_emiter import LoggingEmitter
from metric_coordinator.metric_runner import MetricRunner
from metric_coordinator.model import Datastore
from metric_coordinator.api_client.clickhouse_client import ClickhouseClient

from tests.conftest import TEST_DATAFRAME_PATH, TEST_METRICS, extract_type_mapping, get_metric_from_csv, get_test_metric_name, get_test_settings, insert_data, setup_string_column_type

@pytest.fixture
def setup_teardown_metric_runner_after_test(request):
    test_name = request.node.name
    metric_runner = MetricRunner(get_test_settings(),MT5Deal)
    setup_clickhouse_datastore_table(test_name)
    yield metric_runner,test_name
    for emiter in metric_runner.get_emitters():
        if isinstance(emiter,ClickhouseEmitter):
            emiter.drop_metrics(metric_runner.get_metrics())
            
def setup_clickhouse_datastore_table(test_name:str):
    settings = get_test_settings()
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


def test_metric_runner_initialization(setup_teardown_metric_runner_after_test):
    metric_runner,_ = setup_teardown_metric_runner_after_test
    assert metric_runner.settings == get_test_settings()
    assert metric_runner._metrics == []
    assert metric_runner._emiters == []
    assert metric_runner._datastores == {}
    metric_runner.setup_clickhouse_client()
    assert metric_runner.client is not None
    
    for metric in TEST_METRICS:
        if metric in [MT5Deal, MT5DealDaily]:
            continue
        metric_runner.register_metric(metric)
        assert METRIC_CALCULATORS[metric].get_metric_runner() == metric_runner
        for additional_metric in METRIC_CALCULATORS[metric].additional_data:
            assert metric_runner.get_datastore(additional_metric) is not None and isinstance(metric_runner.get_datastore(additional_metric), Datastore)
    metric_runner.register_emitter(ClickhouseEmitter(metric_runner.client,server=get_test_settings().SERVER_NAME))
    metric_runner.register_emitter(LoggingEmitter())
    assert len(metric_runner.get_emitters()) == 2


def test_metric_runner_process_metrics(setup_teardown_metric_runner_after_test):
    metric_runner,test_name = setup_teardown_metric_runner_after_test
    metric_runner.setup_clickhouse_client()
    metric_runner.setup_datasore_metric_table_names(
                                {
                                    MT5Deal:get_test_metric_name(MT5Deal,test_name),
                                    MT5DealDaily:get_test_metric_name(MT5DealDaily,test_name),
                                    AccountMetricDaily:get_test_metric_name(AccountMetricDaily,test_name),
                                    AccountMetricByDeal:get_test_metric_name(AccountMetricByDeal,test_name),
                                    AccountSymbolMetricByDeal:get_test_metric_name(AccountSymbolMetricByDeal,test_name),
                                    PositionMetricByDeal:get_test_metric_name(PositionMetricByDeal,test_name)
                                })
 
    for metric in TEST_METRICS:
        if metric in [MT5Deal, MT5DealDaily]:
            continue
        metric_runner.register_metric(metric)
    
    # Insert data to datastore MT5DealDaily
    insert_data(metric_runner.get_datastore(MT5DealDaily),MT5DealDaily,test_name)
    
    df_deal = get_metric_from_csv(MT5Deal,TEST_DATAFRAME_PATH[MT5Deal])
    results = metric_runner.process_metrics(df_deal)
    
    calculated_df = setup_string_column_type(results[AccountMetricDaily],AccountMetricDaily)
    
    # Load the expected data from CSV
    expected_df = pd.read_csv(TEST_DATAFRAME_PATH[AccountMetricDaily],dtype=extract_type_mapping(AccountMetricDaily))
    
    # Adopt type and adjust expected different columns
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date
    expected_df.rename(columns={"timestamp":"timestamp_utc"},inplace=True)
    
    # Ensure both dataframes have the same columns
    assert set(calculated_df.columns) == set(expected_df.columns), f"Columns do not match {calculated_df.columns} != {expected_df.columns}"
    # Verify data types of each column
    for column in expected_df.columns:
        assert calculated_df[column].dtype == expected_df[column].dtype, f"Data type mismatch for column {column}: {calculated_df[column].dtype} != {expected_df[column].dtype}"

    # Compare dataframes
    pd.testing.assert_frame_equal(calculated_df[expected_df.columns],expected_df,check_dtype=True)
    
    

