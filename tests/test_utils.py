"""
Tests for utils.py.
"""

import time
from pathlib import Path

import pandas as pd
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
    assert utils.EXPORT_DIR_PREFIX in result
    assert " " not in result


@pytest.mark.parametrize("traces,will_fail", tests.test_perform_pandera_check_params())
def test_perform_pandera_check(traces, will_fail):
    """
    Test perform_pandera_check.
    """
    pandera_report = utils.perform_pandera_check(
        traces, metadata=tests.metadata_loaded()
    )
    assert isinstance(pandera_report, pd.DataFrame)

    if will_fail:
        assert not pandera_report.empty
        assert pandera_report.shape[0] > 0
    else:
        assert pandera_report.empty


def test_filename_friendly_datetime_string_manual():
    """
    Test filename_friendly_datetime_string.
    """
    curr_year = str(time.localtime().tm_year)
    result = utils.filename_friendly_datetime_string()
    assert curr_year in result


@pytest.mark.parametrize(
    "update_tuple,empty_dict,empty_df,update_values",
    tests.test_pandera_reporting_params(),
)
def test_pandera_reporting(update_tuple, empty_dict, empty_df, update_values):
    """
    Test pandera_reporting.
    """
    pandera_update_values, pandera_report = utils.pandera_reporting(
        update_tuple=update_tuple,
        metadata=tests.metadata_loaded(),
    )

    if empty_dict:
        assert len(pandera_update_values) == 0
    if empty_df:
        assert pandera_report.empty

    assert update_values == pandera_update_values
