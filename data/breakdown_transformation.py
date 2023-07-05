from io import StringIO
from typing import Union

import pandas as pd
import pytz
from pandas.io.parsers.readers import TextFileReader

from connnections.google_drive import DriveService
from data.CONSTANTS import BREAKDOWN_VIEW_FOLDER, BUS_BREAKDOWN_SNAPSHOT_FOLDER
from data.reference_data import get_geotab_mappings_dataframe
from data.utilities import get_csv_from_drive_as_dataframe, get_raw_data_file_ids


def generate_dataframe_for_breakdown_data(breakdown_folder_id: str) -> pd.DataFrame:
    """As currently all the raw files are split into smaller csvs
    this generates a singular large dataframe containing all the files

    Returns:
        pd.DataFrame: Raw breakdown data across all geotab devices
    """
    drive_service = DriveService()
    breakdown_file_id_list = get_raw_data_file_ids(breakdown_folder_id, drive_service)
    giant_breakdown_data_csv_df = pd.concat(
        format_breakdown_df(
            get_csv_from_drive_as_dataframe(
                file, drive_service, {"dtype": str, "chunksize": 2000}
            )  # type: ignore
        )
        for file in breakdown_file_id_list
    )
    return giant_breakdown_data_csv_df


def format_breakdown_df(chunks: TextFileReader) -> pd.DataFrame:
    """
    Gets chunks from the read_csv when data is passed down with chunksize

    chunks (TextFileReader): Chunks to process breakdown data in dataframes of x size at a time to make
    reading more memory efficient

    Returns:
        pd.DataFrame: breakdown data reformatted to have better types per columns and a utc datetime
    """
    breakdown_total_df = pd.concat(
        parse_breakdown_data(breakdown_raw_df)
        if len(breakdown_raw_df)
        else pd.DataFrame()
        for breakdown_raw_df in chunks
    )
    return breakdown_total_df.drop_duplicates(keep="first")


def parse_breakdown_data(breakdown_raw_df: pd.DataFrame) -> pd.DataFrame:
    est_tz = pytz.timezone("US/Eastern")
    breakdown_raw_df = breakdown_raw_df[["geotab", "description", "reported_at"]]
    breakdown_raw_df = breakdown_raw_df.rename(columns={"reported_at": "estReportedAt"})
    try:
        breakdown_raw_df["estReportedAt"] = pd.to_datetime(
            breakdown_raw_df["estReportedAt"],
            format="%Y-%m-%dT%H:%M:%S",
            utc=False,
        ).astype(pd.DatetimeTZDtype(tz=est_tz))
    except ValueError:
        breakdown_raw_df["estReportedAt"] = pd.to_datetime(
            breakdown_raw_df["estReportedAt"],
            format="mixed",
            utc=False,
        ).astype(pd.DatetimeTZDtype(tz=est_tz))
    breakdown_raw_df["reportedAt"] = breakdown_raw_df["estReportedAt"].dt.tz_convert(
        "UTC"
    )
    breakdown_raw_df["reportedAt"] = breakdown_raw_df["reportedAt"].dt.strftime(
        "%Y-%m-%d %H:%M:%S%z"
    )
    breakdown_raw_df["estReportedAt"] = breakdown_raw_df["estReportedAt"].dt.strftime(
        "%Y-%m-%d %H:%M:%S%z"
    )

    return breakdown_raw_df


def generate_breakdown_view_data(
    breakdown_data_df: pd.DataFrame,
) -> pd.DataFrame:
    """Gets the breakdown data such as  battery voltage data
    including the bus # associated with the geotab.
    Includes formatting and such.

    Returns:
        pd.DataFrame: Battery voltage data including the geotab mappings
    """
    geotab_df = get_geotab_mappings_dataframe()
    breakdown_data_df = breakdown_data_df.merge(
        geotab_df, left_on=["geotab"], right_on=["Geotab Device"]
    )
    return breakdown_data_df[
        ["description", "reportedAt", "estReportedAt", "Bus #"]
    ].drop_duplicates(keep="first")


def upload_breakdown_view_data(
    file_id: str, file_name: str, breakdown_df: pd.DataFrame
):
    """Gets the existing breakdown view file appends the new data to it and uploads the result.
    It also compares the dataframe created to the current view dataframe.  If the data is different
    then a snapshot of the view data is saved in the View Data/breakdown Snapshot folder

    Args:
        file_id (str): The alphanumeric code id of the metric view file
        file_name (str): The filename of the metric view file
        breakdown_df (pd.DataFrame): The data being uploaded
    """
    drive_service = DriveService()
    current_view_breakdown_df = pd.DataFrame(
        get_csv_from_drive_as_dataframe(
            file_id,
            drive_service=drive_service,
            pandas_read_csv_kwargs={"dtype": "sring[pyarrow]"},
        )
    )
    breakdown_df = (
        pd.concat([current_view_breakdown_df, breakdown_df])
        .sort_values(by=["reportedAt", "Bus #"])
        .drop_duplicates(keep="first")
        .reset_index(drop=True)
    )

    if not current_view_breakdown_df.equals(breakdown_df):
        from datetime import datetime

        current_view_breakdown_df_rowcount = len(current_view_breakdown_df)
        del current_view_breakdown_df
        breakdown_df_len = len(breakdown_df)
        breakdown_df = StringIO(
            breakdown_df.to_csv(index=False).encode("ascii", "ignore").strip().decode()
        )  # type:ignore

        snapshot_file_name = (
            f"{file_name.split('.csv')[0]}"
            f"_{datetime.now().strftime('%Y-%m-%d %H:%M')}.csv"
        )
        print(
            f"old dataframe {current_view_breakdown_df_rowcount} rows."
            f"New dataframe {breakdown_df_len} rows"
        )
        print(
            "New data is being uploaded view csv."
            f" Creating backup of new file first {snapshot_file_name}"
        )
        drive_service.upload_file(
            filename=snapshot_file_name,
            folder_id=BUS_BREAKDOWN_SNAPSHOT_FOLDER,
            file=breakdown_df,
            mimetype="text/csv",
        )
    else:
        del current_view_breakdown_df

    drive_service.upload_file(
        filename=file_name,
        file_id=file_id,
        folder_id=BREAKDOWN_VIEW_FOLDER,
        file=(StringIO(breakdown_df.to_csv(index=False).encode("ascii", "ignore").strip().decode()) if isinstance(breakdown_df, pd.DataFrame) else breakdown_df),  # type: ignore
        mimetype="text/csv",
    )
