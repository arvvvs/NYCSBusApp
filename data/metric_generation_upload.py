from typing import Callable

import pandas as pd

from connnections.google_drive import DriveService
from data.breakdown_transformation import (
    generate_breakdown_view_data,
    generate_dataframe_for_breakdown_data,
    upload_breakdown_view_data,
)
from data.CONSTANTS import (
    BATTERY_RAW_DATA_FOLDER,
    BATTERY_VIEW_DATA_CSV,
    BREAKDOWN_RAW_DATA_FOLDER,
    BUS_BREAKDOWN_VIEW,
    RPM_RAW_DATA_FOLDER,
    RPM_VIEW_DATA_CSV,
)
from data.metrics_transformation import (
    format_metric_df,
    generate_dataframe_for_metric_data,
    generate_metric_view_data,
    upload_metrics_view_data,
)
from data.utilities import (
    chunk_list,
    get_csv_from_drive_as_dataframe,
    get_raw_data_file_ids,
)


def generate_and_upload_breakdown_view():
    breakdown_df = generate_breakdown_view_data(
        generate_dataframe_for_breakdown_data(BREAKDOWN_RAW_DATA_FOLDER)
    )
    upload_breakdown_view_data(
        BUS_BREAKDOWN_VIEW, "bus_breakdown_view.csv", breakdown_df
    )


def generate_and_upload_battery_view():
    """Function to generate and upload metric data for batteries"""
    battery_df = generate_metric_view_data(
        generate_dataframe_for_metric_data(BATTERY_RAW_DATA_FOLDER)
    )
    upload_metrics_view_data(
        BATTERY_VIEW_DATA_CSV, "battery_view_data.csv", battery_df, overwrite=True
    )


def generate_and_upload_rpm_view():
    """Function to generate and upload metric data for rpm"""
    metric_folder_id = RPM_RAW_DATA_FOLDER
    drive_service = DriveService()
    metric_file_id_list = get_raw_data_file_ids(metric_folder_id, drive_service)
    metric_file_id_chunk_list = chunk_list(metric_file_id_list, 3)
    for metric_file_id_chunked_list in metric_file_id_chunk_list:
        giant_metric_data_csv_df = pd.concat(
            [
                format_metric_df(
                    get_csv_from_drive_as_dataframe(
                        file,
                        drive_service,
                        {"dtype": str, "index_col": 0, "chunksize": 200},
                    ),  # type: ignore
                    data_dtype="integer",
                )
                for file in metric_file_id_chunked_list
            ]
        )
        giant_metric_data_csv_df = giant_metric_data_csv_df.drop_duplicates(
            keep="first"
        ).sample(n=int(len(giant_metric_data_csv_df) * 0.45))
        giant_metric_data_csv_df = generate_metric_view_data(giant_metric_data_csv_df)
        upload_metrics_view_data(
            RPM_VIEW_DATA_CSV, "rpm_view_data.csv", giant_metric_data_csv_df
        )


BREAKDOWN_GENERATION_UPLOAD: list[Callable] = [generate_and_upload_breakdown_view]
METRIC_GENERATION_UPLOAD: list[Callable] = [
    generate_and_upload_rpm_view,
    generate_and_upload_battery_view,
]
