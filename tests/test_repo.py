"""
Tests for repo.py.
"""
from pathlib import Path

import pandas as pd
import pytest

import tests
from tracerepo import repo, rules


@pytest.fixture
def database_csv():
    """
    Set up database.csv file with contents.
    """
    df = repo.scaffold_database()
    df, *_ = tests.df_with_row(df=df)
    path = Path(rules.DATABASE_CSV)
    repo.write_database_csv(path=path, database=df)
    yield path
    if path.exists():
        path.unlink()


def test_read_database_csv(database_csv):
    """
    Test read_database_csv.
    """
    path = database_csv
    database_csv = repo.read_database_csv(path=path)
    validated = rules.database_schema().validate(database_csv)
    assert isinstance(validated, pd.DataFrame)
