from io import BytesIO

import pandas as pd


def list_files_in_shared_drive_folder(
    folder_id: str,
) -> list:
    from connnections.google_drive import DriveService

    return (
        DriveService().list_files(
            **{
                "supportsAllDrives": True,
                "includeItemsFromAllDrives": True,
                "q": f"'{folder_id}' in parents and trashed = false",
                "pageSize": 1000,
            }
        )
        or []
    )


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
    file_id: str, pandas_read_csv_kwargs: dict = {}, drive_kwargs: dict = {}
) -> pd.DataFrame:
    """Downloads a csv file and converts it into a dataframe making it easy to use using pandas read_csv attribute.

    Args:
        file_id (str): Alphanumeric ID of the file you're trying to retrieve
        pandas_read_csv_kwargs (dict): any arguments you want to pass to the pandas read_csv call.
        drive_kwargs (dict): Any arguments passing to the drive call

    Returns:
        pd.DataFrame: DataFrame containing the data
    """
    from connnections.google_drive import DriveService

    return pd.read_csv(
        BytesIO(DriveService().get_file(file_id, **drive_kwargs)),
        **pandas_read_csv_kwargs,
    )
