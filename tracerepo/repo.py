"""
Main repo handlers.
"""
from pathlib import Path

import pandas as pd

import tracerepo.rules as rules


def write_database_csv(path: Path, database: pd.DataFrame):
    """
    Write database.csv to disk.
    """
    database.to_csv(
        path_or_buf=path,
        sep=rules.DATABASE_CSV_SEP,
        index=True,
    )


def read_database_csv(path: Path) -> pd.DataFrame:
    """
    Read database csv.
    """
    csv = pd.read_csv(
        path, index_col=rules.ColumnNames.AREA.value, sep=rules.DATABASE_CSV_SEP
    )
    assert isinstance(csv, pd.DataFrame)
    return csv


def scaffold_database():
    """
    Make scaffold database.
    """
    return pd.DataFrame(columns=[col.value for col in rules.ColumnNames]).set_index(
        rules.ColumnNames.AREA.value
    )


def scaffold() -> pd.DataFrame:
    """
    Make scaffold start for a repo.
    """
    Path(rules.FolderNames.UNORGANIZED.value).mkdir(exist_ok=True)
    for path in rules.folder_structure():
        path.mkdir(exist_ok=True, parents=True)
    df = scaffold_database()
    assert df is not None
    df = rules.database_schema().validate(df)
    return df
