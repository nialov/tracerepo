"""
Tests for spatial.py.
"""

import geopandas as gpd
import pytest

import tests
import tracerepo.spatial as spatial


@pytest.mark.parametrize(
    "traces,area,name,snap_threshold,assume_result_validity",
    tests.test_validate_params(),
)
def test_validate(traces, area, name, snap_threshold, assume_result_validity):
    """
    Test validate.
    """
    validated, validation_results = spatial.validate(traces, area, name, snap_threshold)

    assert isinstance(validated, gpd.GeoDataFrame)

    assert validation_results == assume_result_validity
