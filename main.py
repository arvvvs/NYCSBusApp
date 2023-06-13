from data.battery_data_transformation import (
    generate_dataframe_for_battery_raw_data,
    generate_battery_view_data,
)
from connnections.google_drive import DriveService
from data.CONSTANTS import BATTERY_VIEW_DATA_CSV, METRICS_FINALIZED_DATA_FOLDER
from io import StringIO


def main():
    battery_df = generate_battery_view_data(generate_dataframe_for_battery_raw_data())
    DriveService().upload_file(
        filename="battery_view_data.csv",
        file_id=BATTERY_VIEW_DATA_CSV,
        folder_id=METRICS_FINALIZED_DATA_FOLDER,
        file=StringIO(battery_df.to_csv(index=False)),
        mimetype="text/csv",
    )


if __name__ == "__main__":
    main()
