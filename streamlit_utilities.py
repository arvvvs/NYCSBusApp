from io import BytesIO
from typing import Any, Literal

import pandas as pd
import streamlit as st

from connnections.google_drive import DriveService
from data.CONSTANTS import BATTERY_VIEW_DATA_CSV, BUS_BREAKDOWN_VIEW, RPM_VIEW_DATA_CSV
from data.utilities import get_csv_from_drive_as_dataframe, get_random_sample_of_chunks


def _downcast_data(
    df: pd.DataFrame, downcast_to: Literal["integer", "unsigned", "float"] = "integer"
) -> pd.DataFrame:
    df["data"] = pd.to_numeric(
        df["data"].astype(float),
        downcast=downcast_to,
    )
    return df


def _remove_est_tz_info(
    df: pd.DataFrame, est_tz_column_name: str = "estDateTime"
) -> pd.DataFrame:
    """Removes the -05:00 from the estDatetime

    Args:
        df (pd.DataFrame): the rpm view

    Returns:
        pd.DataFrame: RPM view
    """
    df[est_tz_column_name] = df[est_tz_column_name].str.rsplit("-", n=1, expand=True)[0]
    df["Bus #"] = df["Bus #"].astype("category")
    return df


@st.cache_data
def get_rpm_data(
    nrows: Literal["All", "Random"] | int = "All",
    usecols: list[str] = ["data", "Bus #", "estDateTime"],
) -> pd.DataFrame:
    """Returns RPM data optimized with options allowing for nrows or a random sampling of data, as well
    as not returning dateTime

    Args:
        nrows (Literal['All','Random'] | int, optional): All returns all rows, Random will return a sampling
        of 70% of the rows randomly chosen and a number n will return top n rows. Defaults to "All".
        usecols (list[str], optional): Which cols you want returned. Defaults to ['data','Bus #', 'estDateTime'].

    Returns:
        pd.DataFrame: RPM data returned with a tad more optimization
    """
    pandas_read_csv_kwargs = {
        "dtype": {
            "data": "int16[pyarrow]",
            "Bus #": "category",
            "estDateTime": "string[pyarrow]",
            "dateTime": "uint32[pyarrow]",
        },
        "usecols": usecols,
        # "parse_dates":['estDateTime'],
        # "infer_datetime_format":True,
        "dtype_backend": "pyarrow",
    }

    drive_service = DriveService()
    if isinstance(nrows, int):
        pandas_read_csv_kwargs = {
            "nrows": nrows,
            **pandas_read_csv_kwargs,
        }
    else:
        pandas_read_csv_kwargs = {
            "chunksize": 2000,
            **pandas_read_csv_kwargs,
        }

    if nrows == "Random":
        rpm_view_df = _remove_est_tz_info(
            get_random_sample_of_chunks(
                get_csv_from_drive_as_dataframe(
                    RPM_VIEW_DATA_CSV,
                    drive_service=drive_service,
                    pandas_read_csv_kwargs=pandas_read_csv_kwargs,
                ),  # type: ignore
                sample_percent=0.7,
            )
        )  # type: ignore
        rpm_view_df["Bus #"] = rpm_view_df["Bus #"].astype("category")
        return rpm_view_df

    rpm_view_chunks = get_csv_from_drive_as_dataframe(
        RPM_VIEW_DATA_CSV,
        drive_service=drive_service,
        pandas_read_csv_kwargs=pandas_read_csv_kwargs,
    )
    return pd.concat((_downcast_data(_remove_est_tz_info(rpm_view_df), "integer") for rpm_view_df in rpm_view_chunks), ignore_index=True)  # type: ignore


@st.cache_data(persist=True)
def get_battery_data(
    nrows: Literal["All", "Random"] | int = "All",
    usecols: list[str] = ["data", "Bus #", "estDateTime"],
) -> pd.DataFrame:
    """Returns Battery data optimized with options allowing for nrows or a random sampling of data, as well
    as not returning dateTime

    Args:
        nrows (Literal['All','Random'] | int, optional): All returns all rows, Random will return a sampling
        of 70% of the rows randomly chosen and a number n will return top n rows. Defaults to "All".
        usecols (list[str], optional): Which cols you want returned. Defaults to ['data','Bus #', 'estDateTime'].

    Returns:
        pd.DataFrame: battery data returned with a tad more optimization
    """
    pandas_read_csv_kwargs = {
        "dtype": {
            "data": "float32",
            "Bus #": "category",
            "estDateTime": "string[pyarrow]",
            "dateTime": "uint32[pyarrow]",
        },
        "usecols": usecols,
        # "parse_dates":['estDateTime'],
        # "infer_datetime_format":True,
        "dtype_backend": "pyarrow",
    }

    drive_service = DriveService()
    if isinstance(nrows, int):
        pandas_read_csv_kwargs = {
            "nrows": nrows,
            **pandas_read_csv_kwargs,
        }
    else:
        pandas_read_csv_kwargs = {
            "chunksize": 2000,
            **pandas_read_csv_kwargs,
        }

    if nrows == "Random":
        battery_view_df = _remove_est_tz_info(
            get_random_sample_of_chunks(
                get_csv_from_drive_as_dataframe(
                    BATTERY_VIEW_DATA_CSV,
                    drive_service=drive_service,
                    pandas_read_csv_kwargs=pandas_read_csv_kwargs,
                ),  # type: ignore
                sample_percent=0.7,
            )
        )  # type: ignore
        battery_view_df["Bus #"] = battery_view_df["Bus #"].astype("category")
        return battery_view_df

    battery_view_chunks = get_csv_from_drive_as_dataframe(
        BATTERY_VIEW_DATA_CSV,
        drive_service=drive_service,
        pandas_read_csv_kwargs=pandas_read_csv_kwargs,
    )
    return pd.concat((_downcast_data(_remove_est_tz_info(battery_view_df), "float") for battery_view_df in battery_view_chunks), ignore_index=True)  # type: ignore


@st.cache_data(persist=True)
def get_breakdown_data() -> pd.DataFrame:
    file_id = BUS_BREAKDOWN_VIEW
    bus_breakdown_view_df = get_csv_from_drive_as_dataframe(file_id)
    return bus_breakdown_view_df.drop_duplicates(keep="first")


@st.cache_data(persist=True)
def get_breakdown_timeline() -> pd.DataFrame:
    breakdown_df = get_breakdown_data()
    breakdown_df = _remove_est_tz_info(breakdown_df, "estReportedAt")
    breakdown_df = breakdown_df.rename(columns={"estReportedAt": "Reported At"})
    return breakdown_df[["Bus #", "Reported At"]].drop_duplicates(keep="first")


def get_breakdown_count_by_bus() -> pd.DataFrame:
    breakdown_df = get_breakdown_timeline()
    breakdown_df = (
        breakdown_df.groupby(by=["Bus #"])["Reported At"].count().reset_index()
    )
    breakdown_df = breakdown_df.rename(columns={"Reported At": "Breakdowns"})
    return breakdown_df


def format_breakdown_for_chart(selected_bus: str) -> pd.DataFrame:
    breakdown_df = get_breakdown_timeline()
    breakdown_df = breakdown_df.loc[breakdown_df["Bus #"] == selected_bus]
    breakdown_df = breakdown_df.rename(columns={"Reported At": "estDateTime"})
    return breakdown_df
