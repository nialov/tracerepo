"""
Tests for utils.py.
"""

from pathlib import Path

import pytest
from pandera.errors import SchemaError

import tests
from tracerepo import utils


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


@pytest.mark.parametrize("path, rename_to", tests.test_rename_data_path_params())
def test_rename_data_path(path, rename_to):
    """
    Test rename_data_path.
    """
    result = utils.rename_data_path(path, rename_to)

    assert isinstance(result, Path)

    assert rename_to in str(result)
    assert rename_to in str(list(result.parents)[0])


@pytest.mark.parametrize("driver", tests.test_compile_export_dir_params())
def test_compile_export_dir(driver):
    """
    Test compile_export_dir.
    """
    assert isinstance(driver, str)
    result = utils.compile_export_dir(driver)
    assert isinstance(result, str)
    assert len(result) != 0
    assert utils.export_dir_prefix in result
    assert " " not in result
