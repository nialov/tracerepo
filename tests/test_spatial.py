"""
Tests for spatial.py.
"""
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest
from hypothesis import given, settings
from pytest import TempPathFactory

import tests
from tracerepo import rules
from tracerepo import spatial
from tracerepo import utils


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


@settings(max_examples=5, deadline=5000)
@given(
    database=tests.database_schema_strategy(),
)
@pytest.mark.parametrize(
    "trace_gdf,assume_error",
    [
        (tests.kb11_traces_cut, rules.ValidationResults.VALID),
        (tests.kb11_traces_cut_dislocated, rules.ValidationResults.EMPTY),
    ],
)
def test_validate_invalids_with_full_setup(
    database: pd.DataFrame,
    tmp_path_factory: TempPathFactory,
    trace_gdf: gpd.GeoDataFrame,
    assume_error: rules.ValidationResults,
):
    """
    Test validate_invalids.
    """
    area_gdf: gpd.GeoDataFrame = tests.kb11_area
    tmp_path = tmp_path_factory.mktemp(basename="test_validate_invalids", numbered=True)

    with tests.setup_scaffold_context(tmp_path):

        organizer = tests.set_up_repo_with_invalids_organized(
            database=database, trace_gdf=trace_gdf, area_gdf=area_gdf
        )

        # Query for invalid traces
        invalids = organizer.query(
            validity=[rules.ValidationResults.INVALID],
        )

        for invalid in invalids:
            assert invalid.traces_path.exists()
            assert invalid.area_path.exists()

        update_tuples = spatial.validate_invalids(invalids)

        assert isinstance(update_tuples, list)
        assert all(isinstance(val, utils.UpdateTuple) for val in update_tuples)

        for update_tuple in update_tuples:
            assert (
                update_tuple.update_values[rules.ColumnNames.VALIDITY]
                == assume_error.value
            )


@pytest.mark.parametrize(
    "dataset_tuples,export_destination,driver", tests.test_convert_trace_tuples_params()
)
def test_convert_trace_tuples(dataset_tuples, export_destination, driver):
    """
    Test convert_trace_tuples.
    """
    result = spatial.convert_trace_tuples(
        dataset_tuples, export_destination=export_destination, driver=driver
    )

    assert isinstance(result, list)
    assert isinstance(result[0], tuple)
    assert isinstance(result[0][0], Path)


@pytest.mark.parametrize("invalids,will_fail", tests.test_validate_invalids_params())
def test_validate_invalids(invalids, will_fail):
    """
    Test validate_invalids.
    """
    result = spatial.validate_invalids(invalids)

    assert len(result) == len(invalids)

    failed = any(invalid.error for invalid in result)
    if will_fail:
        assert failed
    else:
        assert not failed
