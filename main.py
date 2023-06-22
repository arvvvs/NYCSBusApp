from collections import deque

from data.breakdown_transformation import (
    generate_breakdown_view_data,
    generate_dataframe_for_breakdown_data,
)
from data.CONSTANTS import BREAKDOWN_RAW_DATA_FOLDER
from data.metric_generation_upload import (
    BREAKDOWN_GENERATION_UPLOAD,
    METRIC_GENERATION_UPLOAD,
)


def main():
    # generate_breakdown_view_data(generate_dataframe_for_breakdown_data((BREAKDOWN_RAW_DATA_FOLDER))) # type: ignore
    deque((metric_upload() for metric_upload in METRIC_GENERATION_UPLOAD), maxlen=0)


if __name__ == "__main__":
    main()
