from io import StringIO

from connnections.google_drive import DriveService
from data.rpm_data_transformation import (
    generate_dataframe_for_rpm_raw_data,
    generate_rpm_view_data
)
from data.CONSTANTS import METRICS_FINALIZED_DATA_FOLDER


def main():
    rpm_df = generate_rpm_view_data(generate_dataframe_for_rpm_raw_data())
    DriveService().upload_file(
        filename="rpm_view_data.csv",
        # file_id=BATTERY_VIEW_DATA_CSV,
        folder_id=METRICS_FINALIZED_DATA_FOLDER,
        file=StringIO(rpm_df.to_csv(index=False)),
        mimetype="text/csv",
    )


if __name__ == "__main__":
    main()
