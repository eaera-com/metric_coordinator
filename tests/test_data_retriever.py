import pytest

from metric_coordinator.data_retriever.clickhouse_data_retriever import ClickhouseDataRetriever
from metric_coordinator.api_client.clickhouse_client import ClickhouseClient
from metric_coordinator.configs import MIN_TIME, type_map
from account_metrics import MT5Deal, MT5DealDaily
from metric_coordinator.metric_runner import MetricRunner
from tests.conftest import TEST_METRICS, get_test_metric_name, get_test_settings, insert_data


@pytest.fixture
def setup_and_teardown_clickhouse_retriever(request):
    """
    Setup and teardown for the ClickhouseRetriever
    """
    test_name = request.node.name
    settings = get_test_settings()
    ch = ClickhouseDataRetriever(
        filters={"group_by": "Group", "group": settings.MT_GROUPS},
        client=ClickhouseClient(
            username=settings.CLICKHOUSE_USERNAME,
            password=settings.CLICKHOUSE_PASSWORD,
            host=settings.CLICKHOUSE_HOST,
            http_port=settings.CLICKHOUSE_HTTP_PORT,
            database=settings.CLICKHOUSE_DATABASE,
        ),
        server=settings.SERVER_NAME,
        table_name=get_test_metric_name(MT5Deal, test_name),
    )
    metric_fields = ", ".join([f"{k} {type_map.get(v.annotation.__name__)}" for k, v in MT5Deal.model_fields.items()])
    keys = ", ".join([k for k, v in MT5Deal.model_fields.items() if "key" in v.metadata])
    if not ch.client.create_metric_if_not_exist(get_test_metric_name(MT5Deal, test_name), metric_fields, keys):
        raise ValueError(f"Failed to create metric {get_test_metric_name(MT5Deal,test_name)}")

    insert_data(ch, MT5Deal, test_name)
    metrics_runner = MetricRunner(get_test_settings(), MT5Deal)

    yield ch, metrics_runner, test_name

    for metric in TEST_METRICS:
        ch.drop_metric(metric)


def test_clickhouse_retriever_retrieve_data(setup_and_teardown_clickhouse_retriever):
    # ch_retriever,_,test_name = setup_and_teardown_clickhouse_retriever
    # for metric in TEST_METRICS:
    #     insert_data(ch_retriever,metric,test_name)

    # input_data = ch_retriever.retrieve_data(from_time=MIN_TIME,to_time=1750000000)

    # assert input_data.shape[0] == 61
    pass
