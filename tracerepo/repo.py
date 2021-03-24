"""
Main repo handlers.
"""
import tracerepo.rules as rules
import tracerepo.utils as utils
from pathlib import Path
from typing import List, Dict
import pandas as pd


def scaffold():
    """
    Make scaffold start for a repo.
    """
    Path(rules.FolderNames.UNORGANIZED.value).mkdir(exist_ok=True)
    for path in rules.folder_structure():
        path.mkdir(exist_ok=True, parents=True)
    df = pd.DataFrame(columns=[col.value for col in rules.ColumnNames]).set_index(
        rules.ColumnNames.AREA.value
    )
    assert df is not None
    df = rules.database_schema().validate(df)
    return df
