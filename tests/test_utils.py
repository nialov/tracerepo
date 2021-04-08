"""
Tests for utils.py.
"""

import pytest
from pandera.errors import SchemaError

import tests
import tracerepo.utils as utils


@pytest.mark.parametrize("gdf,assume_error", tests.test_write_geodata_params())
def test_write_geodata(gdf, assume_error, tmp_path):
    """
    Test write_geodata.
    """
    save_path = tmp_path / "temp.geojson"

    try:
        utils.write_geodata(gdf, path=save_path)
    except SchemaError:
        assert assume_error
