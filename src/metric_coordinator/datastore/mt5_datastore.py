import datetime
from typing import Annotated, Any, Dict, Type

from account_metrics import MT5Deal, MT5DealDaily
import pandas as pd
from metric_coordinator.configs import MIN_TIME
from metric_coordinator.model import SourceDatastore
from account_metrics.metric_model import MetricData

class MT5DataStore(SourceDatastore):
    def __init__(self, metric: MetricData, mt5_manager_client: Annotated[Any, "MT5ManagerClient"], sharding_columns: tuple[str] = None):
        self.metric = metric
        self.mt5_manager_client = mt5_manager_client
        assert self.mt5_manager_client is not None, "MT5ManagerClient is required"
        assert self.metric in [MT5Deal, MT5DealDaily], "MT5DataStore only supports MT5Deal and MT5DealDaily"
        self.sharding_columns = sharding_columns
        if len(self.sharding_columns) > 1 or self.sharding_columns[0] != "Login":
            raise ValueError("MT5DataStore only supports sharding by Login")
        self._last_load_time = datetime.datetime.fromtimestamp(MIN_TIME)
    
    def get_metric(self) -> Type[MetricData]:
        return self.metric

    def eager_load(self, shard_key_values: tuple[Any] = None, from_time: int = MIN_TIME, to_time: int = datetime.datetime.now()) -> pd.DataFrame:
        assert len(shard_key_values) == len(self.sharding_columns)
        logins = shard_key_values[0]
        mt_time = self.mt5_manager_client.TimeGet()
        offset = mt_time.TimeZone * 60
        if self.metric == MT5Deal:
            deals = self.mt5_manager_client.DealRequestByLoginsNumPy(logins, from_time + 1, to_time + offset)
        elif self.metric == MT5DealDaily:
            history = self.mt5_manager_client.DailyRequestByLogins(logins, from_time, to_time + offset)
            return self._process_data_from_mt5(deals, history, offset)
        return deals

    def _process_deal_from_mt5(self, deals: pd.DataFrame, offset: int) -> pd.DataFrame:
        df = pd.DataFrame(deals)
        df = df.drop(columns=["ObsoleteValue"])
        # TODO: remove duplicate columns use rename instead
        # TODO: check if server or server_name is needed
        df["server"] = self.mt5_manager_client.server
        df["login"] = df["Login"]
        df["TimeUTC"] = df["Time"] - offset
        df["deal_id"] = df["Deal"]
        df["timestamp_server"] = df["Time"]
        return df
    
    def _process_deal_daily_from_mt5(self, history: pd.DataFrame, offset: int) -> pd.DataFrame:
        df_history = pd.DataFrame(
            [
                {
                    "Login": x.Login,
                    "Group": x.Group,
                    "Datetime": x.Datetime,
                    "Balance": x.Balance,
                    "ProfitEquity": x.ProfitEquity,
                }
                for x in history
            ]
        )
        # TODO: define 1 deal format that works for all.
        df_history["Date"] = pd.to_datetime(df_history["Datetime"], unit="s").dt.date
        df_history["server"] = self.mt5_manager_client.server
        df_history["timestamp_server"] = df_history["Datetime"]
        df_history["timestamp_utc"] = df_history["Datetime"] + offset