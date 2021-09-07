"""
Tests for traces_schema.py.
"""
import geopandas as gpd
import pandas as pd
import pandera as pa
import pytest

import tests
from tracerepo import trace_schema


@pytest.mark.parametrize("gdf,will_fail,geom_test", tests.test_traces_schema_params())
def test_traces_schema(gdf: gpd.GeoDataFrame, will_fail: bool, geom_test: bool):
    """
    Test traces_schema.
    """
    schema = trace_schema.traces_schema(tests.metadata_loaded())

    assert isinstance(schema, pa.DataFrameSchema)

    if not geom_test:
        schema.remove_columns(["geometry"])

    try:
        validated = schema.validate(gdf)
    except Exception:
        if will_fail:
            return
        raise
    if will_fail:
        assert False
    assert isinstance(validated, pd.DataFrame)
