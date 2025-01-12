from metric_coordinator.data_emiter.clickhouse_data_emiter import ClickhouseEmitter
from metric_coordinator.data_emiter.logging_data_emiter import LoggingEmitter
from metric_coordinator.metric_runner import MetricRunner
from metric_coordinator.configs import settings
from account_metrics import MT5Deal, MT5DealDaily, AccountMetricByDeal, AccountMetricDaily,AccountSymbolMetricByDeal, PositionMetricByDeal, METRIC_CALCULATORS

if __name__ == "__main__":
    metrics = [MT5Deal, MT5DealDaily, AccountMetricByDeal, AccountMetricDaily,AccountSymbolMetricByDeal, PositionMetricByDeal]
    metric_runner = MetricRunner(settings)
    metric_runner.register_metrics(metrics)
    for metric in metrics:
        if metric in [MT5Deal, MT5DealDaily]:
            continue
        metric_runner.register_metric(metric)
        metric_runner.register_emitter(ClickhouseEmitter(metric_runner.get_clickhouse_client(), server=settings.SERVER_NAME))
        metric_runner.register_emitter(LoggingEmitter())
        
        metric_runner.run()
