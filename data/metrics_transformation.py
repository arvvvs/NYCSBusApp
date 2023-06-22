import json
from io import StringIO
from typing import Literal

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytz
from pandas.io.parsers.readers import TextFileReader

from connnections.google_drive import DriveService
from data.CONSTANTS import METRICS_FINALIZED_DATA_FOLDER, METRICS_SNAPSHOT_FOLDER
from data.reference_data import get_geotab_mappings_dataframe
from data.utilities import get_csv_from_drive_as_dataframe, get_raw_data_file_ids


def get_id_from_json(x) -> str:
    """Takes columns containing strings {'id':'device_number'}
    and gets teh device number. Created for use in DataFrame
    apply function

    Args:
        x (Series): the parameter from apply

    Returns:
        str: device number
    """
    try:
        dict_data = json.loads(x.replace("'", '"'))
        return dict_data["id"]
    except Exception as e:
        print(type(x))
        print(x)
        print(e)
        return x


def generate_dataframe_for_metric_data(metric_folder_id: str) -> pd.DataFrame:
    """As currently all the raw files are split into smaller csvs
    this generates a singular large dataframe containing all the files

    Returns:
        pd.DataFrame: Raw metric data across all geotab devices
    """
    drive_service = DriveService()
    metric_file_id_list = get_raw_data_file_ids(metric_folder_id, drive_service)
    giant_metric_data_csv_df = pd.concat(
        [
            format_metric_df(
                get_csv_from_drive_as_dataframe(
                    file,
                    drive_service,
                    {"dtype": str, "index_col": 0, "chunksize": 200},
                )  # type: ignore
            )
            for file in metric_file_id_list
        ],
    )
    return giant_metric_data_csv_df


def format_metric_df(
    chunks: TextFileReader, data_dtype: Literal["float", "integer"] = "float"
) -> pd.DataFrame:
    """
    Gets chunks from the read_csv when data is passed down with chunksize

    chunks (TextFileReader): Chunks to process metric data in dataframes of x size at a time to make
    reading more memory efficient

    Returns:
        pd.DataFrame: metric data reformatted to have better types per columns and a utc datetime
    """
    pd.set_option("mode.chained_assignment", None)
    metrics_total_df = pd.concat(
        parse_metric_df(metrics_raw_df, data_dtype=data_dtype)
        if len(metrics_raw_df)
        else pd.DataFrame()
        for metrics_raw_df in chunks
    )
    pd.reset_option("mode.chained_assignment")
    print(len(metrics_total_df))
    return metrics_total_df.drop_duplicates(keep="first")


def parse_metric_df(
    metric_raw_chunk_df: pd.DataFrame, data_dtype: Literal["float", "integer"] = "float"
) -> pd.DataFrame:
    metric_raw_chunk_df = metric_raw_chunk_df[["data", "device", "dateTime"]]
    if data_dtype == "integer":
        metric_raw_chunk_df["data"] = pd.to_numeric(
            metric_raw_chunk_df["data"].astype(float).round().astype(int),
            downcast="integer",
        )
    else:
        metric_raw_chunk_df["data"] = pd.to_numeric(
            metric_raw_chunk_df["data"].astype(np.float64).round(2), downcast="float"
        )

    metric_raw_chunk_df["device"] = (
        metric_raw_chunk_df["device"].apply(get_id_from_json).astype("category")
    )
    est_tz = pytz.timezone("US/Eastern")
    try:
        metric_raw_chunk_df["dateTime"] = pd.to_datetime(
            metric_raw_chunk_df["dateTime"],
            format="%Y-%m-%d %H:%M:%S.%f%z",
        )
    except ValueError:
        metric_raw_chunk_df["dateTime"] = pd.to_datetime(
            metric_raw_chunk_df["dateTime"], format="mixed"
        )

    metric_raw_chunk_df["estDateTime"] = metric_raw_chunk_df["dateTime"].dt.tz_convert(
        est_tz
    )
    metric_raw_chunk_df["estDateTime"] = metric_raw_chunk_df["estDateTime"].dt.strftime(
        "%Y-%m-%d %H:%M:%S%z"
    )
    metric_raw_chunk_df["dateTime"] = pd.to_numeric(
        metric_raw_chunk_df["dateTime"].dt.strftime("%s"), downcast="integer"
    )

    return metric_raw_chunk_df.drop_duplicates(keep="first")


def generate_metric_view_data(
    metric_data_df: pd.DataFrame,
) -> pd.DataFrame:
    """Gets the metric data such as  battery voltage data
    including the bus # associated with the geotab.
    Includes formatting and such.

    Returns:
        pd.DataFrame: Battery voltage data including the geotab mappings
    """
    geotab_df = get_geotab_mappings_dataframe()
    metric_data_df = metric_data_df.merge(
        geotab_df, left_on=["device"], right_on=["Geotab Device"]
    )
    metric_data_df["Bus #"] = metric_data_df["Bus #"].astype("category")
    return metric_data_df[["data", "dateTime", "estDateTime", "Bus #"]].drop_duplicates(
        keep="first"
    )


def upload_metrics_view_data(
    file_id: str, file_name: str, metric_df: pd.DataFrame, overwrite: bool = False
):
    """Gets the existing metrics view file appends the new data to it and uploads the result.
    It also compares the dataframe created to the current view dataframe.  If the data is different
    then a snapshot of the view data is saved in the View Data/Metrics Snapshot folder

    Args:
        file_id (str): The alphanumeric code id of the metric view file
        file_name (str): The filename of the metric view file
        metric_df (pd.DataFrame): The data being uploaded
    """
    drive_service = DriveService()
    current_view_metrics_df = pd.DataFrame()
    if overwrite == False:
        current_view_metrics_df = pd.DataFrame(
            get_csv_from_drive_as_dataframe(
                file_id,
                drive_service=drive_service,
                pandas_read_csv_kwargs={
                    "dtype": {
                        "data": "float16[pyarrow]",
                        "Bus #": "category",
                        "estDateTime": "string[pyarrow]",
                        "dateTime": "uint32[pyarrow]",
                    },
                },
            )
        )
    # current_view_metrics_df['estDateTime'] = pd.to_datetime(current_view_metrics_df['estDateTime'], format="mixed", utc=False, errors="coerce")
    # current_view_metrics_df['estDateTime'] = current_view_metrics_df['estDateTime'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S%z'))
    # current_view_metrics_df['estDateTime'] = current_view_metrics_df['estDateTime'].astype("string")
    # current_view_metrics_df['dateTime'] = pd.to_datetime(current_view_metrics_df['dateTime'], format="mixed", utc=False, errors="coerce")
    # current_view_metrics_df['dateTime'] = current_view_metrics_df['dateTime]'].dt.strftime('%s').astype(np.int16)
    # current_view_metrics_df['dateTime'] = current_view_metrics_df['dateTime'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S%z'))
    # current_view_metrics_df['estDateTime'] = current_view_metrics_df['estDateTime'].astype("string")
    # current_view_metrics_df['dateTime'] = current_view_metrics_df['dateTime'].astype("string")

    metric_df = (
        pd.concat([current_view_metrics_df, metric_df])
        .sort_values(by=["dateTime", "Bus #"])
        .drop_duplicates(keep="first")
        .reset_index(drop=True)
    )
    # metric_df['data'] = pd.to_numeric(metric_df['data'], downcast="integer")
    # metric_df['dateTime'] = pd.to_numeric(metric_df['dateTime'], downcast="integer")
    # metric_df['estDateTime'] = metric_df['estDateTime'].convert_dtypes(dtype_backend="pyarrow")
    # metric_df['Bus #'] = metric_df['Bus #'].astype('category')
    if not current_view_metrics_df.equals(metric_df) and not overwrite:
        from datetime import datetime

        current_view_metrics_df_rowcount = len(current_view_metrics_df)
        del current_view_metrics_df
        metric_df_len = len(metric_df)
        metric_df = StringIO(metric_df.to_csv(index=False))  # type:ignore

        snapshot_file_name = (
            f"{file_name.split('.csv')[0]}"
            f"_{datetime.now().strftime('%Y-%m-%d %H:%M')}.csv"
        )
        print(
            f"old dataframe {current_view_metrics_df_rowcount} rows."
            f"New dataframe {metric_df_len} rows"
        )
        print(
            "New data is being uploaded view csv."
            f" Creating backup of new file first {snapshot_file_name}"
        )
        drive_service.upload_file(
            filename=snapshot_file_name,
            folder_id=METRICS_SNAPSHOT_FOLDER,
            file=metric_df,
            mimetype="text/csv",
        )
    else:
        del current_view_metrics_df

    drive_service.upload_file(
        filename=file_name,
        file_id=file_id,
        folder_id=METRICS_FINALIZED_DATA_FOLDER,
        file=(
            StringIO(metric_df.to_csv(index=False))
            if isinstance(metric_df, pd.DataFrame)
            else metric_df
        ),
        mimetype="text/csv",
    )
