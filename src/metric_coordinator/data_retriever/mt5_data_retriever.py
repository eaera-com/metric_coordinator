from typing import Annotated, List, Dict, Any
from pydantic import BaseModel
from pydantic.alias_generators import to_snake
import pandas as pd


from metric_coordinator.configs import MIN_TIME
from metric_coordinator.api_client.mt5manager_client import MT5ManagerClient
from metric_coordinator.data_retriever.basic_data_retriever import BasicDataRetriever


class MT5DataRetriever(BasicDataRetriever):
    def __init__(self, server: str, server_name: str, login: str, password: str) -> None:
        self.server = server
        self.server_name = server_name
        self.last_retrieve_timestamp = MIN_TIME

        import MT5Manager

        self.mt5_client = MT5ManagerClient(server=server, login=login, password=password)
        self.mt5_manager = self.mt5_client.manager

    def __str__(self) -> str:
        return f"MT5DataRetriever({self.server},{self.server_name})"

    def _process_data_from_mt5(
        self,
        deals: List[Annotated[Any, "MT5Deal"]],
        history: List[Annotated[Any, "MT5DealDaily"]],
        offset: Annotated[int, "timestamp offset"],
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        df = pd.DataFrame(deals)
        df = df.drop(columns=["ObsoleteValue"])
        # TODO: remove duplicate columns use rename instead
        # TODO: check if server or server_name is needed
        df["server"] = self.server
        df["login"] = df["Login"]
        df["TimeUTC"] = df["Time"] - offset
        df["deal_id"] = df["Deal"]
        df["timestamp_server"] = df["Time"]

        # TODO: this actually might make things slower
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
        df_history["server"] = self.server
        df_history["timestamp_server"] = df_history["Datetime"]
        df_history["timestamp_utc"] = df_history["Datetime"] + offset
        return df, df_history

    def validate(self, filters: Dict[str, Any]) -> None:
        assert "group_by" in filters or "group" in filters  # "group_by or group must be provided"
        if filters["group_by"] == "logins":
            assert "logins" in filters  # "logins must be provided when group_by is logins"
        elif filters["group_by"] == "group":
            assert "group" in filters

    def retrieve_data(
        self, from_time: Annotated[int, "timestamp time-server"], to_time: Annotated[int, "timestamp time-utc"], filters: Dict[str, Any]
    ) -> Dict[str, pd.DataFrame]:
        self.validate(filters)
        data = {}
        if filters["group_by"] == "Login":
            data["Deal"], data["History"] = self.retrieve_deals_by_logins(filters["logins"], from_time, to_time)
            self.last_retrieve_timestamp = to_time + self.mt5_manager.TimeGet().TimeZone * 60
        elif filters["group_by"] == "Group":
            data["Deal"], data["History"] = self.retrieve_deals_by_group(filters["group"], from_time, to_time)
            # TODO: check if we should replace to max time of the deal
            self.last_retrieve_timestamp = (
                to_time + self.mt5_manager.TimeGet().TimeZone * 60 + 3600 * (self.mt5_manager.TimeGet().DaylightState)
            )
        return data

    def retrieve_deals_by_logins(
        self,
        logins: List[int],
        from_time: Annotated[int, "timestamp time-server"],
        to_time: Annotated[int, "timestamp time-utc"],
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        mt_time = self.mt5_manager.TimeGet()
        offset = mt_time.TimeZone * 60
        deals = self.mt5_manager.DealRequestByLoginsNumPy(logins, from_time + 1, to_time + offset)
        if deals is False:
            return pd.DataFrame(columns=["Login", "Time", "PositionID"]), pd.DataFrame(
                columns=["Login", "Datetime", "Balance", "ProfitEquity"]
            )

        history: list[Annotated[Any, "MT5DealDaily"]] = self.mt5_manager.DailyRequestByLogins(logins, MIN_TIME, to_time + offset)
        return self._process_data_from_mt5(deals, history, offset)

    def retrieve_deals_by_group(
        self,
        groups: str,
        from_time: Annotated[int, "timestamp time-server"],
        to_time: Annotated[int, "timestamp time-utc"],
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        mt_time = self.mt5_manager.TimeGet()
        offset = mt_time.TimeZone * 60 + 3600 * (mt_time.DaylightState)
        deals = self.mt5_manager.DealRequestByGroupNumPy(groups, from_time + 1, to_time + offset)
        if deals is False:
            return pd.DataFrame(columns=["Login", "Time", "PositionID"]), pd.DataFrame(
                columns=["Login", "Datetime", "Balance", "ProfitEquity"]
            )
        history: list[Annotated[Any, "MT5DealDaily"]] = self.mt5_manager.DailyRequestByGroup(groups, 31536001, to_time + offset)
        return self._process_data_from_mt5(deals, history, offset)

    def get_last_retrieve_timestamp(self) -> Annotated[int, "timestamp"]:
        return self.last_retrieve_timestamp

    def get_server(self) -> str:
        return self.server
