"""
Tests for traces_schema.py.
"""
import tests
import pytest
import geopandas as gpd
import pandas as pd

from tracerepo import trace_schema
import pandera as pa


@pytest.mark.parametrize("gdf,will_fail,geom_test", tests.test_traces_schema_params())
def test_traces_schema(gdf: gpd.GeoDataFrame, will_fail: bool, geom_test: bool):
    """
    Test traces_schema.
    """
    schema = trace_schema.traces_schema()

    assert isinstance(schema, pa.DataFrameSchema)

    if not geom_test:
        schema.remove_columns(["geometry"])

    try:
        validated = schema.validate(gdf)
    except Exception:
        if will_fail:
            return
        else:
            raise
    assert isinstance(validated, pd.DataFrame)
