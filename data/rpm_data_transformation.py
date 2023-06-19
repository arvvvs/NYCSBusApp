 #import pandas as pd
 #import pytz
 #
 #from connnections.google_drive import DriveService
 #from data.CONSTANTS import METRICS_FINALIZED_DATA_FOLDER, RPM_RAW_DATA_FOLDER
 #from data.reference_data import get_geotab_mappings_dataframe
 #from data.utilities import (
 #    get_csv_from_drive_as_dataframe,
 #    get_raw_data_file_ids,
 #)
 #
 #
 #def generate_dataframe_for_rpm_raw_data() -> pd.DataFrame:
 #    """As currently all the raw files are split into smaller csvs this generates a singular large dataframe
 #    containing all the files
 #
 #    Returns:
 #        pd.DataFrame: Raw battery data across all geotab devices
 #    """
 #    drive_service = DriveService()
 #    battery_raw_data_file_ids = get_raw_data_file_ids(RPM_RAW_DATA_FOLDER)
 #    giant_battery_data_csv_df = pd.concat(
 #        format_rpm_df(
 #            get_csv_from_drive_as_dataframe(
 #                file, drive_service, {"dtype": str, "index_col": 0, "chunksize": 2000}
 #            )
 #        )
 #        for file in battery_raw_data_file_ids
 #    )
 #    return giant_battery_data_csv_df
 #
 #
 #def format_rpm_df(chunks) -> pd.DataFrame:
 #    """
 #    Get the ID from the device/diagnostic columns, adds datetimes and a column containing EST.
 #
 #    Args:
 #        rpm_raw_df (pd.DataFrame): DataFrame containing raw battery data
 #
 #    Returns:
 #        pd.DataFrame: Battery data reformatted to have better types per columns and a utc datetime
 #    """
 #    rpm_raw_df = pd.DataFrame({})
 #    for rpm_raw_df in chunks:
 #        if len(rpm_raw_df):
 #            rpm_raw_df = rpm_raw_df[["data", "device", "dateTime"]]
 #            rpm_raw_df["data"] = rpm_raw_df["data"].astype(float)
 #            rpm_raw_df["device"] = rpm_raw_df["device"].apply(get_id_from_json)
 #            # rpm_raw_df["diagnostic"] = rpm_raw_df["diagnostic"].apply(
 #            #     get_id_from_json
 #            # )
 #            est_tz = pytz.timezone("US/Eastern")
 #
 #            try:
 #                rpm_raw_df["dateTime"] = pd.to_datetime(
 #                    rpm_raw_df["dateTime"],
 #                    format="%Y-%m-%d %H:%M:%S.%f%z",
 #                )
 #            except:
 #                rpm_raw_df["dateTime"] = pd.to_datetime(
 #                    rpm_raw_df["dateTime"], format="mixed"
 #                )
 #
 #            rpm_raw_df["estDateTime"] = rpm_raw_df["dateTime"].dt.tz_convert(est_tz)
 #            rpm_raw_df = rpm_raw_df
 #    return rpm_raw_df.drop_duplicates(keep="first")
 #
 #
 #def generate_rpm_view_data(
 #    rpm_voltage_raw_data_df: pd.DataFrame,
 #) -> pd.DataFrame:
 #    """Gets the battery voltage data including the bus # associated with the geotab.
 #    Includes formatting and such.
 #
 #    Returns:
 #        pd.DataFrame: Battery voltage data including the geotab mappings
 #    """
 #    geotab_df = get_geotab_mappings_dataframe()
 #    rpm_voltage_raw_data_df = rpm_voltage_raw_data_df.merge(
 #        geotab_df, left_on=["device"], right_on=["Geotab Device"]
 #    )
 #    return rpm_voltage_raw_data_df
 #
 #
 ## def upload_battery_view_data():
 ##     """Generates and uploads the battery data in a format that inlcudes the view
 ##     """
 ##     from connnections.google_drive import DriveService
 ##     rpm_df = generate_rpm_view_data(generate_dataframe_for_rpm_raw_data())
 ##     DriveService().upload_file(
 ##         filename="rpm_view_data.csv",
 ##         file_id=RPM_VIEW_DATA_CSV,
 ##         folder_id=METRICS_FINALIZED_DATA_FOLDER,
 ##         file=StringIO(rpm_df.to_csv(index=False)),
 ##         mimetype="text/csv",
 ##     )
 #