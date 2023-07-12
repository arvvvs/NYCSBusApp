from io import BytesIO
from typing import Any

import pandas as pd
from pandas.io.parsers.readers import TextFileReader

from connnections.google_drive import DriveService


def chunk_list(lst: list[Any], n: int) -> list[list[Any]]:
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
    """Downloads a csv file and converts it into a dataframe
    making it easy to use using pandas read_csv attribute.

    Args:
        file_id (str): Alphanumeric ID of the file you're trying to retrieve
        pandas_read_csv_kwargs (dict): any arguments you want to pass to the pandas read_csv call.
        drive_kwargs (dict): Any arguments passing to the drive call

    Returns:
        Union[pd.DataFrame, TextFileReader]: DataFrame containing the data.
        If chunksize is used then TextFileReader returned
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
    return list(
        sorted(
            set(
                [
                    files["id"]
                    for files in drive_service.list_files_in_shared_drive_folder(
                        folder_id
                    )
                ]
            )
        )
    )


def get_random_sample_of_chunks(
    chunks: TextFileReader, sample_percent: float = 0.50
) -> pd.DataFrame:
    """If using the chunksize parameter to read csv and wanting to return a sample of the dataframe
    csv use this function to help drive down memory usage while returninga random sampleof data

    Args:
        chunks (TextFileReader): pass in pd.read_csv with chunksize parameter here
        sample_percent (float, optional): The percent of data you want returned
        between 0 and 1. Defaults to 0.50.

    Returns:
        pd.DataFrame: The dataframe with the sample chosen.
    """
    total_df = pd.concat((df.sample(n=int(len(df) * sample_percent)) for df in chunks))
    return total_df
