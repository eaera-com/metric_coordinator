


import datetime
import pandas as pd
import pytest

from account_metrics import AccountMetricByDeal, AccountMetricDaily, AccountSymbolMetricByDeal, MT5Deal, MT5DealDaily, PositionMetricByDeal

from api_client.clickhouse_client import ClickhouseClient
from metric_coordinator.data_emiter.clickhouse_data_emiter import ClickhouseEmitter
from tests.conftest import TEST_METRICS, get_test_metric_name
from metric_coordinator.configs import settings

@pytest.fixture
def setup_and_teardown_clickhouse_emitter(request):
    test_name = request.node.name
    ch = ClickhouseEmitter( ClickhouseClient(username=settings.CLICKHOUSE_USERNAME,
                                            password=settings.CLICKHOUSE_PASSWORD,
                                            host=settings.CLICKHOUSE_HOST,
                                            http_port=settings.CLICKHOUSE_HTTP_PORT,
                                            database=settings.CLICKHOUSE_DATABASE
                                            ),
                server=settings.SERVER_NAME,
                metric_table_names={MT5Deal:get_test_metric_name(MT5Deal,test_name),
                                    MT5DealDaily:get_test_metric_name(MT5DealDaily,test_name),
                                    AccountMetricDaily:get_test_metric_name(AccountMetricDaily,test_name),
                                    AccountMetricByDeal:get_test_metric_name(AccountMetricByDeal,test_name),
                                    AccountSymbolMetricByDeal:get_test_metric_name(AccountSymbolMetricByDeal,test_name),
                                    PositionMetricByDeal:get_test_metric_name(PositionMetricByDeal,test_name)}
                )
    for metric in TEST_METRICS:
        ch.initialize_metric(metric)
        
    yield ch,test_name

    for metric in TEST_METRICS:
        ch.drop_metric(metric)

def test_clickhouse_data_emitter_initialize(setup_and_teardown_clickhouse_emitter):
    ch,test_name = setup_and_teardown_clickhouse_emitter
    for metric in TEST_METRICS:
        metric_name = get_test_metric_name(metric,test_name)
        assert len(ch.client.query_ddl(f"DESCRIBE TABLE {metric_name}").result_set) == len(metric.model_fields)    
        

def test_clickhouse_data_emitter_emit(setup_and_teardown_clickhouse_emitter):
    metrics = [MT5Deal, MT5DealDaily]
    ch_emitter,test_name = setup_and_teardown_clickhouse_emitter
    first_timestamp = ch_emitter.get_last_emit_timestamp(MT5Deal)
    
    for metric in metrics:
        metric_name = get_test_metric_name(metric,test_name)
        assert len(ch_emitter.client.query_ddl(f"DESCRIBE TABLE {metric_name}").result_set) == len(metric.model_fields)
        assert len(ch_emitter.client.query_ddl(f"SELECT * FROM {metric_name}").result_set) == 0
    
    df_history = pd.DataFrame([MT5DealDaily(Login=1999,timestamp_utc= int(datetime.datetime.now().timestamp())).model_dump()],
                              columns=MT5DealDaily.model_fields.keys()) # get default value
    df_deal = pd.DataFrame([MT5Deal(login=1999,Deal=1502).model_dump(),
                            MT5Deal(login=1999,Deal=2708).model_dump()],
                           columns=MT5Deal.model_fields.keys())  # get default value
    data = {
            MT5Deal :df_deal,
            MT5DealDaily:df_history
            }
    ch_emitter.emit(data)
    
    # TODO: fix last emit timestamp
    # assert ch_emitter.get_last_emit_timestamp(MT5Deal) > first_timestamp
    
    mt5deal_metric_name = get_test_metric_name(MT5Deal,test_name)
    mt5dealdaily_metric_name = get_test_metric_name(MT5DealDaily,test_name)
    assert ch_emitter.client.query_df(f"SELECT * FROM {mt5deal_metric_name}").shape[0] == df_deal.shape[0]
    assert ch_emitter.client.query_df(f"SELECT * FROM {mt5dealdaily_metric_name}").shape[0] == df_history.shape[0]


def test_clickhouse_data_emitter_emit_duplicate_key(setup_and_teardown_clickhouse_emitter):
    ch,test_name = setup_and_teardown_clickhouse_emitter
    
    df_deal = pd.DataFrame([MT5Deal(login=1999,Deal=1502).model_dump(),
                            MT5Deal(login=1999,Deal=1502).model_dump()],
                           columns=MT5Deal.model_fields.keys())  # get default value
    data = {
            MT5Deal :df_deal,
            }
    ch.emit(data)
    
    mt5deal_metric_name = get_test_metric_name(MT5Deal,test_name)
    assert ch.client.query_df(f"SELECT * FROM {mt5deal_metric_name}").shape[0] == 1
