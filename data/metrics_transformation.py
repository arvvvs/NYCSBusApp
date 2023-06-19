import json
from io import StringIO

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
        format_metric_df(
            get_csv_from_drive_as_dataframe(
                file, drive_service, {"dtype": str, "index_col": 0, "chunksize": 2000}
            )  # type: ignore
        )
        for file in metric_file_id_list
    )
    return giant_metric_data_csv_df


def format_metric_df(chunks: TextFileReader) -> pd.DataFrame:
    """
    Gets chunks from the read_csv when data is passed down with chunksize

    chunks (TextFileReader): Chunks to process metric data in dataframes of x size at a time to make
    reading more memory efficient

    Returns:
        pd.DataFrame: metric data reformatted to have better types per columns and a utc datetime
    """
    metrics_raw_df = pd.DataFrame({})
    for metrics_raw_df in chunks:
        if len(metrics_raw_df):
            metrics_raw_df = metrics_raw_df[["data", "device", "dateTime"]]
            metrics_raw_df["data"] = metrics_raw_df["data"].astype(float)
            metrics_raw_df["device"] = metrics_raw_df["device"].apply(get_id_from_json)
            est_tz = pytz.timezone("US/Eastern")
            try:
                metrics_raw_df["dateTime"] = pd.to_datetime(
                    metrics_raw_df["dateTime"],
                    format="%Y-%m-%d %H:%M:%S.%f%z",
                )
            except ValueError:
                metrics_raw_df["dateTime"] = pd.to_datetime(
                    metrics_raw_df["dateTime"], format="mixed"
                )

            metrics_raw_df["estDateTime"] = metrics_raw_df["dateTime"].dt.tz_convert(
                est_tz
            )
            metrics_raw_df[["estDateTime", "dateTime"]] = metrics_raw_df[
                ["estDateTime", "dateTime"]
            ].astype(str)
    return metrics_raw_df.drop_duplicates(keep="first")


def generate_metric_view_data(
    metric_data_df: pd.DataFrame,
) -> pd.DataFrame:
    """Gets the battery voltage data including the bus # associated with the geotab.
    Includes formatting and such.

    Returns:
        pd.DataFrame: Battery voltage data including the geotab mappings
    """
    geotab_df = get_geotab_mappings_dataframe()
    metric_data_df = metric_data_df.merge(
        geotab_df, left_on=["device"], right_on=["Geotab Device"]
    )
    return metric_data_df[["data", "dateTime", "estDateTime", "Bus #"]]


def upload_metrics_view_data(file_id: str, file_name: str, metric_df: pd.DataFrame):
    """Gets the existing metrics view file appends the new data to it and uploads the result.
    It also compares the dataframe created to the current view dataframe.  If the data is different
    then a snapshot of the view data is saved in the View Data/Metrics Snapshot folder

    Args:
        file_id (str): The alphanumeric code id of the metric view file
        file_name (str): The filename of the metric view file
        metric_df (pd.DataFrame): The data being uploaded
    """
    drive_service = DriveService()
    current_view_metrics_df = pd.DataFrame(
        get_csv_from_drive_as_dataframe(
            file_id,
            drive_service=drive_service,
            pandas_read_csv_kwargs={
                "dtype": {
                    "data": float,
                    "dateTime": str,
                    "estDateTime": str,
                    "Bus #": str,
                }
            },
        )
    )
    metric_df = (
        pd.concat([current_view_metrics_df, metric_df])
        .sort_values(by=["dateTime", "Bus #"])
        .drop_duplicates(keep="first")
        .reset_index(drop=True)
    )
    if not current_view_metrics_df.equals(metric_df):
        from datetime import datetime

        snapshot_file_name = (
            f"{file_name.split('.csv')[0]}"
            f"_{datetime.now().strftime('%Y-%m-%d %H:%M')}.csv"
        )
        print(
            f"old dataframe {len(current_view_metrics_df)} rows."
            f"New dataframe {len(metric_df)} rows"
        )
        print(
            "New data is being uploaded view csv."
            f" Creating backup of new file first {snapshot_file_name}"
        )
        drive_service.upload_file(
            filename=snapshot_file_name,
            folder_id=METRICS_SNAPSHOT_FOLDER,
            file=StringIO(metric_df.to_csv(index=False)),
            mimetype="text/csv",
        )

    drive_service.upload_file(
        filename=file_name,
        file_id=file_id,
        folder_id=METRICS_FINALIZED_DATA_FOLDER,
        file=StringIO(current_view_metrics_df.to_csv(index=False)),
        mimetype="text/csv",
    )
