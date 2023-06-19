from typing import Callable

from data.CONSTANTS import (
    BATTERY_RAW_DATA_FOLDER,
    BATTERY_VIEW_DATA_CSV,
    RPM_RAW_DATA_FOLDER,
    RPM_VIEW_DATA_CSV,
)
from data.metrics_transformation import (
    generate_dataframe_for_metric_data,
    generate_metric_view_data,
    upload_metrics_view_data,
)


def generate_and_upload_battery_view():
    """Function to generate and upload metric data for batteries"""
    battery_df = generate_metric_view_data(
        generate_dataframe_for_metric_data(BATTERY_RAW_DATA_FOLDER)
    )
    upload_metrics_view_data(BATTERY_VIEW_DATA_CSV, "battery_view_data.csv", battery_df)


def generate_and_upload_rpm_view():
    """Function to generate and upload metric data for rpm"""
    rpm_df = generate_metric_view_data(
        generate_dataframe_for_metric_data(RPM_RAW_DATA_FOLDER)
    )
    upload_metrics_view_data(RPM_VIEW_DATA_CSV, "rpm_view_data.csv", rpm_df)


METRIC_GENERATION_UPLOAD: list[Callable] = [
    generate_and_upload_battery_view,
    generate_and_upload_rpm_view,
]
