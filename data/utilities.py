import json
from io import BytesIO
from typing import Optional
from pandas.io.parsers.readers import (
    TextFileReader,
)
import pandas as pd

from connnections.google_drive import DriveService


def chunk_list(lst: list, n: int) -> list[list]:
    """Generates a list of lists from the list passed in to parameter
    lst, where each list is of n size.

    Args:
        lst (list): The list you want to chunk
        n (int): How big you want each list to be

    Returns:
        list[list]: A list of of n-sized lists derived from the original lst
    """
    return [lst[i : i + n] for i in range(0, len(lst), n)]


def get_csv_from_drive_as_dataframe(
    file_id: str,
    drive_service: DriveService = DriveService(),
    pandas_read_csv_kwargs: dict = {},
    drive_kwargs: dict = {},
) -> pd.DataFrame | TextFileReader:
    """Downloads a csv file and converts it into a dataframe making it easy to use using pandas read_csv attribute.

    Args:
        file_id (str): Alphanumeric ID of the file you're trying to retrieve
        pandas_read_csv_kwargs (dict): any arguments you want to pass to the pandas read_csv call.
        drive_kwargs (dict): Any arguments passing to the drive call

    Returns:
        Union[pd.DataFrame, TextFileReader]: DataFrame containing the data.  If chunksize is used then TextFileReader returned
    """
    return pd.read_csv(
        BytesIO(drive_service.get_file(file_id, **drive_kwargs)),
        **pandas_read_csv_kwargs,
    )



def get_raw_data_file_ids(
    folder_id: str, drive_service: DriveService = DriveService()
) -> list[str]:
    """Gets a list of all battery raw file ids
    Args:
        folder_id (pd.DataFrame): Folder containing csvs to pull

    Returns:
        list[str]: list of ids containing the csvs of the different raw files
    """
    # TODO: update to ensure only csvs are pulled
    return [
        files["id"]
        for files in drive_service.list_files_in_shared_drive_folder(folder_id)
    ]
