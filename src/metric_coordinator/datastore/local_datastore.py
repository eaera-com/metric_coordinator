import datetime
from typing import Dict, Type, Union, Any, List
import pandas as pd
import numpy as np

from metric_coordinator.model import BaseDatastore, MetricData


class LocalDatastore(BaseDatastore):
    DEFAULT_SHARD_KEY_VALUES = ("ALL",)
    MINIMUM_VALUE_PUT_IN_BATCH = 1000
    BATCH_PROCESSING = False

    def __init__(self, metric: MetricData, sharding_columns: tuple[str] = None, batch_size: int = 64) -> None:
        self.metric = metric
        self.sharding_columns = sorted(sharding_columns) if sharding_columns else []
        self._batch_size = batch_size
        self.shard_key_values_to_dataframe = {}

    def put(self, value: Union[pd.Series, pd.DataFrame]) -> None:
        if self.metric is None:
            raise ValueError("Datastore is not initialized or deactivated")
        if isinstance(value, pd.Series):
            value = value.to_frame()
        if value.shape[0] >= self.MINIMUM_VALUE_PUT_IN_BATCH and self.BATCH_PROCESSING:
            self._put_in_batch(value)
        else:
            for _, row in value.iterrows():
                key_values = row[self.metric.Meta.sharding_columns].to_dict()
                shard_key_values = self._extract_shard_key_values(key_values)
                pydantic_row = self.metric(**row.to_dict()).model_dump()

                if shard_key_values not in self.shard_key_values_to_dataframe:
                    # Initialize empty DataFrame with metric model columns
                    empty_df = pd.DataFrame(columns=list(self.metric.model_fields.keys()))
                    self.shard_key_values_to_dataframe[shard_key_values] = empty_df
                # TODO: implement dataframe limit
                self.shard_key_values_to_dataframe[shard_key_values] = pd.concat(
                    [self.shard_key_values_to_dataframe[shard_key_values], pd.DataFrame([pydantic_row])], ignore_index=True
                )

    def get_metric(self) -> Type[MetricData]:
        return self.metric

    def get_latest_row(self, shard_key: Dict[str, Any]) -> pd.Series:
        if self.metric is None:
            raise ValueError("Datastore is not initialized or deactivated")
        result_df = self.shard_key_values_to_dataframe.get(self._extract_shard_key_values(shard_key), None)
        if result_df is None:
            return pd.Series(self.metric(**shard_key).model_dump())
        return result_df.iloc[-1]

    def get_row_by_timestamp(
        self, shard_key: Dict[str, Any], timestamp: datetime.date, timestamp_column: str, use_default_value: bool = False
    ) -> pd.Series:
        if self.metric is None:
            raise ValueError("Datastore is not initialized or deactivated")
        result_df = self.shard_key_values_to_dataframe.get(self._extract_shard_key_values(shard_key), None)

        # TODO: make this cleaner
        if timestamp_column not in shard_key:
            shard_key[timestamp_column] = timestamp
        else:
            assert (
                shard_key[timestamp_column] == timestamp
            ), f"Timestamp {
                shard_key[timestamp_column]} != {timestamp}"

        # TODO: optimize for subset of shard_key extraction
        final_result_df = self._subset_df(result_df, shard_key)

        if final_result_df.empty:
            if use_default_value:
                return pd.Series(self.metric(**shard_key).model_dump())
            else:
                return None
        return final_result_df.iloc[-1]

    def close(self) -> None:
        self.table_name = None
        self.metric = None
        self._clean_data()

    def _subset_df(self, df: pd.DataFrame, filters: dict) -> pd.DataFrame:
        mask = pd.Series(True, index=df.index)
        for col, val in filters.items():
            mask &= df[col] == val
        return df[mask]

    def _get_df_filtered(self, df: pd.DataFrame, shard_key: Dict[str, int]) -> pd.DataFrame:
        # Start with an all-True NumPy array
        mask = np.ones(len(df), dtype=bool)
        for col, val in shard_key.items():
            # Compare directly using df[col].values to avoid Pandas overhead
            mask &= df[col].values == val
        return df[mask]

    def reload_data(self, df: pd.DataFrame) -> None:
        self._clean_data()
        self.put(df)

    def _put_in_batch(self, value):
        batch: Dict[tuple[Any], List[pd.DataFrame]] = {}
        for _, row in value.iterrows():
            key_values = row[self.metric.Meta.sharding_columns].to_dict()
            batch[self._extract_shard_key_values(key_values)].append(row)
            # flush batch if it is full
            if len(batch[self._extract_shard_key_values(key_values)]) == self._batch_size:
                self.shard_key_values_to_dataframe[self._extract_shard_key_values(key_values)] = pd.concat(
                    batch[self._extract_shard_key_values(key_values)], ignore_index=True
                )
                batch[self._extract_shard_key_values(key_values)] = []
        # flush remaining batch
        for key, rows in batch.items():
            self.shard_key_values_to_dataframe[key] = pd.concat(rows, ignore_index=True)

    def _clean_data(self) -> None:
        self.shard_key_values_to_dataframe = {}

    def _extract_shard_key_values(self, shard_key: Dict[str, Any]) -> tuple:
        """
        Extracts the values of the shard key based on the sharding columns as a tuple
        e.g. {'login': 1001, 'date': datetime.date(2021, 1, 1)} -> (1001, datetime.date(2021, 1, 1))
        """
        if not shard_key:
            return self.DEFAULT_SHARD_KEY_VALUES
        missing_columns = [col for col in self.sharding_columns if col not in shard_key]
        if missing_columns:
            raise ValueError(
                f"Missing columns in shard_key: {
                             ', '.join(missing_columns)}"
            )

        return tuple(shard_key[col] for col in self.sharding_columns)
