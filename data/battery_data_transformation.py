
import pandas as pd
import pytz
from io import StringIO

from data.CONSTANTS import BATTERY_RAW_DATA_FOLDER, BATTERY_VIEW_DATA_CSV,METRICS_FINALIZED_DATA_FOLDER
from data.reference_data import get_geotab_mappings_dataframe
from data.utilities import (
    get_csv_from_drive_as_dataframe,
    get_id_from_json,
    get_raw_data_file_ids,
)



def generate_dataframe_for_battery_raw_data() -> pd.DataFrame:
    """As currently all the raw files are split into smaller csvs this generates a singular large dataframe
    containing all the files

    Returns:
        pd.DataFrame: Raw battery data across all geotab devices
    """
    battery_raw_data_file_ids = get_raw_data_file_ids(BATTERY_RAW_DATA_FOLDER)
    print(battery_raw_data_file_ids)
    giant_battery_data_csv_df = pd.concat(
        format_battery_df(
            get_csv_from_drive_as_dataframe(file, {"dtype": str, "index_col": 0})
        )
        for file in battery_raw_data_file_ids
    )
    return giant_battery_data_csv_df


def generate_battery_view_data(
    battery_voltage_raw_data_df: pd.DataFrame,
) -> pd.DataFrame:
    """Gets the battery voltage data including the bus # associated with the geotab.
    Includes formatting and such.

    Returns:
        pd.DataFrame: Battery voltage data including the geotab mappings
    """
    geotab_df = get_geotab_mappings_dataframe()
    battery_voltage_raw_data_df = battery_voltage_raw_data_df.merge(
        geotab_df, left_on=["device"], right_on=["Geotab Device"]
    )
    return battery_voltage_raw_data_df[
        ["data", "dateTime", "estDateTime", "Bus #"]
    ]


def format_battery_df(battery_raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Get the ID from the device/diagnostic columns, adds datetimes and a column containing EST.

    Args:
        battery_raw_df (pd.DataFrame): DataFrame containing raw battery data

    Returns:
        pd.DataFrame: Battery data reformatted to have better types per columns and a utc datetime
    """
    if len(battery_raw_df):
        battery_raw_df["data"] = battery_raw_df["data"].astype(float)
        battery_raw_df["device"] = battery_raw_df["device"].apply(get_id_from_json)
        battery_raw_df["diagnostic"] = battery_raw_df["diagnostic"].apply(
            get_id_from_json
        )
        est_tz = pytz.timezone("US/Eastern")
        battery_raw_df["dateTime"] = pd.to_datetime(
            battery_raw_df["dateTime"], format="mixed"
        )
        battery_raw_df["estDateTime"] = battery_raw_df["dateTime"].dt.tz_convert(est_tz)
    return battery_raw_df



def upload_battery_view_data():
    """Generates and uploads the battery data in a format that inlcudes the view
    """
    from connnections.google_drive import DriveService
    battery_df = generate_battery_view_data(generate_dataframe_for_battery_raw_data())
    DriveService().upload_file(
        filename="battery_view_data.csv",
        file_id=BATTERY_VIEW_DATA_CSV,
        folder_id=METRICS_FINALIZED_DATA_FOLDER,
        file=StringIO(battery_df.to_csv(index=False)),
        mimetype="text/csv",
    )


