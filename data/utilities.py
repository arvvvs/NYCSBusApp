from io import BytesIO

import json

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
def get_id_from_json(x) -> str:
    """Takes columns containing strings {'id':'device_number'}
    and gets teh device number. Created for use in DataFrame
    apply function

    Args:
        x (_type_): the parameter from apply

    Returns:
        str: device number
    """
    try:
        dict_data = json.loads(x.replace("'", '"'))
        return dict_data["id"]
    except Exception as e:
        print(type(x))
        print(x)
        print(e)
        return x


def get_raw_data_file_ids(folder_id: str) -> list[str]:
    """Gets a list of all battery raw file ids
    Args:
        folder_id (pd.DataFrame): Folder containing csvs to pull

    Returns:
        list[str]: list of ids containing the csvs of the different raw files
    """
    # TODO: update to ensure only csvs are pulled
    return [
        files["id"]
        for files in list_files_in_shared_drive_folder(folder_id)
    ]

