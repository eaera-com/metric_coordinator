import contextlib
import traceback
from typing import Any, Generator, List, Literal

from account_metrics import MetricData
from clickhouse_connect.driver import Client
import clickhouse_connect

from metric_coordinator.configs import type_map
import pandas as pd


class ClickhouseClient:
    def __init__(self, username: str, password: str, host: str, http_port: str, database: str) -> None:
        self.username = username
        self.password = password
        self.host = host
        self.http_port = http_port
        self.database = database

    def __str__(self) -> str:
        dns = f"clickhouse://{self.username}:{self.password}@{self.host}:{self.http_port}/{self.database}"
        return f"ClickhouseClient({dns})"

    @contextlib.contextmanager
    def get_ch_client(self) -> Generator[Client, Any, None]:
        dns = f"clickhouse://{self.username}:{self.password}@{self.host}:{self.http_port}/{self.database}"
        ch_client: Client = clickhouse_connect.get_client(dsn=dns)
        try:
            yield ch_client
        except Exception as e:
            print(traceback.format_exc())
        finally:
            ch_client.close()

    def query_dml(self, query: str) -> bool:
        with self.get_ch_client() as client:
            client.command(query)
            return True

    def query_ddl(self, query: str) -> Any:
        with self.get_ch_client() as client:
            return client.query(query)

    def query_df(self, query: str) -> pd.DataFrame:
        with self.get_ch_client() as client:
            return client.query_df(query)

    def insert_df(self, table: str, df: pd.DataFrame) -> bool:
        with self.get_ch_client() as client:
            client.insert_df(table, df)
            return True

    def truncate_tables(self, tables: List[str]) -> Literal[True]:
        for table in tables:
            if not self.query_dml(f"TRUNCATE TABLE IF EXISTS {table}"):
                raise ValueError(f"Failed to truncate table {table}")
        return True

    def drop_tables(self, tables: List[str]) -> Literal[True]:
        for table in tables:
            if not self.query_dml(f"DROP TABLE IF EXISTS {table}"):
                raise ValueError(f"Failed to drop table {table}")
        return True

    def create_metric_if_not_exist(self, table_name: str, metric_fields: List[str], keys: List[str]) -> Literal[True]:
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({metric_fields}) ENGINE = ReplacingMergeTree ORDER BY ({keys})"

        # only log if the table doesn't exist
        if not self.query_dml(query):
            return False
        return True
