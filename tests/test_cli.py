"""
Tests for cli.py.
"""
from pathlib import Path
from typing import Optional

import geopandas as gpd
import pandas as pd
import pytest
from hypothesis import given, settings
from hypothesis.strategies import composite, from_regex, integers, lists, sampled_from
from pytest import TempPathFactory

import tests
import tracerepo.cli as cli
import tracerepo.rules as rules
import tracerepo.utils as utils
from tracerepo.organize import Organizer


def name_regex(geom_type: Optional[rules.ColumnNames]):
    """
    Compile regex strat.
    """
    return from_regex(rules.filename_regex(geom_type=geom_type), fullmatch=True)


@composite
def database_schema_strategy(draw):
    """
    Create sensible database schema stragegy.
    """
    size = draw(integers(min_value=1, max_value=5))
    size_kwargs = dict(min_size=size, max_size=size)
    area_index = draw(
        lists(elements=name_regex(rules.ColumnNames.AREA), **size_kwargs, unique=True)
    )
    traces = draw(lists(elements=name_regex(rules.ColumnNames.TRACES), **size_kwargs))
    thematic = draw(lists(elements=name_regex(None), **size_kwargs))
    scale = draw(lists(elements=name_regex(None), **size_kwargs))
    area_shape = draw(
        lists(
            sampled_from([area_shape.value for area_shape in rules.AreaShapes]),
            **size_kwargs,
        )
    )
    validity = draw(
        lists(
            sampled_from([area_shape.value for area_shape in rules.ValidationResults]),
            **size_kwargs,
        )
    )
    snap_threshold = [0.001] * size

    df = pd.DataFrame(
        index=area_index,
        data={
            rules.ColumnNames.TRACES.value: traces,
            rules.ColumnNames.THEMATIC.value: thematic,
            rules.ColumnNames.SCALE.value: scale,
            rules.ColumnNames.AREA_SHAPE.value: area_shape,
            rules.ColumnNames.VALIDITY.value: validity,
            rules.ColumnNames.SNAP_THRESHOLD.value: snap_threshold,
        },
    )

    assert [
        col.value in df.columns
        for col in rules.ColumnNames
        if col != rules.ColumnNames.AREA
    ]

    return df


@settings(max_examples=5, deadline=5000)
@given(
    database=database_schema_strategy(),
)
@pytest.mark.parametrize(
    "trace_gdf,assume_error",
    [
        (tests.kb11_traces_cut, rules.ValidationResults.VALID),
        (tests.kb11_traces_cut_dislocated, rules.ValidationResults.EMPTY),
    ],
)
def test_validate_invalids(
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

    organizer = Organizer(database)

    with tests.setup_scaffold_context(tmp_path):
        for trace_name, area_name in zip(
            organizer.columns[rules.ColumnNames.TRACES.value],
            organizer.columns[rules.ColumnNames.AREA.value],
        ):
            save_path = (
                lambda name: Path(rules.FolderNames.UNORGANIZED.value)
                / f"{name}.{rules.FILETYPE}"
            )
            trace_path = save_path(name=trace_name)
            area_path = save_path(name=area_name)
            for path, gdf in zip((trace_path, area_path), (trace_gdf, area_gdf)):
                if not path.exists():
                    gdf.to_file(path, driver="GeoJSON")
            assert trace_path.exists()
            assert area_path.exists()
        try:
            organizer.check()
            assert False
        except FileNotFoundError:
            pass

        organizer.organize(simulate=False)

        organizer.check()

        # Query for invalid traces
        invalids = organizer.query(
            validity=rules.ValidationResults.INVALID,
        )

        for invalid in invalids:
            assert invalid.traces_path.exists()
            assert invalid.area_path.exists()

        update_tuples = cli.validate_invalids(invalids)

        assert isinstance(update_tuples, list)
        assert all([isinstance(val, utils.UpdateTuple) for val in update_tuples])

        for update_tuple in update_tuples:
            assert (
                update_tuple.update_values[rules.ColumnNames.VALIDITY]
                == assume_error.value
            )
