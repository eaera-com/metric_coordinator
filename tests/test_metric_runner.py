from account_metrics import MT5Deal, MT5DealDaily
from account_metrics import METRIC_CALCULATORS

from metric_coordinator.data_emiter.clickhouse_data_emiter import ClickhouseEmitter
from metric_coordinator.data_emiter.logging_data_emiter import LoggingEmitter
from metric_coordinator.metric_runner import MetricRunner
from metric_coordinator.model import Datastore
from tests.conftest import TEST_METRICS, get_test_settings

def test_metric_runner_initialization():
    metric_runner = MetricRunner(get_test_settings(),MT5Deal)
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
        # TODO: uncomment when package is updated
        # assert metric.get_metric_runner() == metric_runner
        for additional_metric in METRIC_CALCULATORS[metric].additional_data:
            assert metric_runner.get_datastore(additional_metric) is not None and isinstance(metric_runner.get_datastore(additional_metric), Datastore)
    metric_runner.register_emitter(ClickhouseEmitter(metric_runner.client,server=get_test_settings().SERVER_NAME))
    metric_runner.register_emitter(LoggingEmitter())
    assert len(metric_runner.get_emitters()) == 2
