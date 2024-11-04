"""
Tests for rules.py.
"""

from contextlib import nullcontext

import pandas as pd
import pandera as pa
import pytest
from hypothesis import given

import tests
from tracerepo import rules


@given(tests.name_regex(geom_type=None))
def test_filename_regex(name: str):
    """
    Test tests name_regex.
    """
    assert isinstance(name, str)
    assert len(name) >= 2


@pytest.mark.parametrize(
    "df,raises",
    [
        (pd.DataFrame({}), pytest.raises(pa.errors.SchemaError)),
        (tests.df_with_row(None)[0], nullcontext()),
        (pd.concat([tests.df_with_row(None)[0]] * 2), nullcontext()),
    ],
)
def test_database_schema(df, raises):
    """
    Test database_schema.
    """
    result = None
    with raises:
        result = rules.database_schema().validate(df)
    if result is None:
        return
    assert isinstance(result, pd.DataFrame)
