"""
Main repo handlers.
"""
from pathlib import Path

import pandas as pd

from tracerepo import rules


def write_database_csv(path: Path, database: pd.DataFrame):
    """
    Write database.csv to disk.
    """
    database = rules.database_schema().validate(database)
    database.to_csv(
        path_or_buf=path,
        sep=rules.DATABASE_CSV_SEP,
        index=True,
        index_label=rules.ColumnNames.AREA.value,
    )


def read_database_csv(path: Path) -> pd.DataFrame:
    """
    Read database csv.
    """
    csv = pd.read_csv(path, index_col=0, sep=rules.DATABASE_CSV_SEP, dtype=str)
    assert isinstance(csv, pd.DataFrame)
    csv = rules.database_schema().validate(csv)
    return csv


def scaffold_database():
    """
    Make scaffold database.
    """
    return pd.DataFrame(columns=[col.value for col in rules.ColumnNames]).set_index(
        rules.ColumnNames.AREA.value
    )


def scaffold(tracerepository_path: Path) -> pd.DataFrame:
    """
    Make scaffold start for a repo.
    """
    unorganized_path = tracerepository_path / rules.PathNames.UNORGANIZED.value
    data_path = tracerepository_path / rules.PathNames.DATA.value
    # Make unorganized folder
    unorganized_path.mkdir(exist_ok=True)
    data_path.mkdir(exist_ok=True)

    # # Make other default folders

    # for path in rules.folder_structure():
    #     path.mkdir(exist_ok=True, parents=True)

    # Create dataframe for relations between datasets
    df = scaffold_database()
    assert df is not None

    # Validate dataframe with pandera
    df = rules.database_schema().validate(df)
    return df
