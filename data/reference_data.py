from io import BytesIO

import pandas as pd

from connnections.google_drive import DriveService
from data.CONSTANTS import GEOTAB_MAPPINGS_CSV


def get_geotab_mappings_raw_file() -> BytesIO:
    """Downloads the geotab-mappings.csv file from the reference
    data folder

    Returns:
        BytesIO: Raw csv data as pulled by the Drive API
    """
    geotab_bytes = BytesIO(DriveService().get_file(GEOTAB_MAPPINGS_CSV))
    return geotab_bytes


def get_geotab_mappings_dataframe() -> pd.DataFrame:
    """Gets Geotab device -> bus mappings as a dataframe, as well
    as relabelling Geotab Name to Bus # for easier understanding.

    Returns:
        pd.DataFrame: Dataframe with two columns: 'Geotab Device' and 'Bus #'
    """
    geotab_df = pd.read_csv(get_geotab_mappings_raw_file(), dtype=str)
    geotab_df = geotab_df.rename(columns={"Geotab Name": "Bus #"})
    return geotab_df
