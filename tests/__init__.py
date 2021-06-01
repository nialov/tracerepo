"""
Parameters for tests.
"""
import os
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from pickle import loads
from traceback import print_tb
from typing import Callable, Iterator, Optional

import geopandas as gpd
import pandas as pd
from click.testing import Result
from fractopo.general import read_geofile
from hypothesis.strategies import composite, from_regex, integers, lists, sampled_from
from shapely.geometry import LineString, MultiLineString, Point

import tracerepo.repo as repo
import tracerepo.rules as rules
from tracerepo.organize import Organizer


def cut(
    dataset: gpd.GeoDataFrame,
    end: int,
    start: int = 0,
) -> gpd.GeoDataFrame:
    """
    Cut a dataset.
    """
    part = dataset.iloc[start:end]
    assert isinstance(part, gpd.GeoDataFrame)
    return part


def dislocate(dataset: gpd.GeoDataFrame, xoff: int = 100000):
    """
    Dislocate traces dataset.
    """
    dislocated = gpd.GeoDataFrame(geometry=dataset.translate(xoff=xoff))
    return dislocated


def cached_sample(
    path: Path, create_dataset: Callable[..., gpd.GeoDataFrame]
) -> gpd.GeoDataFrame:
    """
    Cache a sample or load already existing.
    """
    if path.exists():
        loaded = loads(path.read_bytes())
        assert isinstance(loaded, gpd.GeoDataFrame)
        assert loaded.crs is not None
        return loaded
    else:
        loaded = create_dataset()
        loaded.to_pickle(path=path)
    return loaded


def df_data():
    """
    Set up a row of some database df data.
    """
    traces_name = "some_traces"
    area_name = "some_area"
    thematic = "some"
    scale = "wide"
    area_shape = "circle"
    validity = "invalid"
    snap_threshold = 0.001

    return (
        traces_name,
        area_name,
        thematic,
        scale,
        area_shape,
        validity,
        snap_threshold,
    )


def df_with_row(
    df,
):
    """
    Set up a database df with a row of data.
    """
    (
        traces_name,
        area_name,
        thematic,
        scale,
        area_shape,
        validity,
        snap_threshold,
    ) = df_data()

    row = {
        # rules.ColumnNames.AREA.value: area_name,
        rules.ColumnNames.TRACES.value: traces_name,
        rules.ColumnNames.THEMATIC.value: thematic,
        rules.ColumnNames.SCALE.value: scale,
        rules.ColumnNames.AREA_SHAPE.value: area_shape,
        rules.ColumnNames.VALIDITY.value: validity,
        rules.ColumnNames.SNAP_THRESHOLD.value: snap_threshold,
    }

    srs = pd.Series(data=row.values(), index=row.keys(), name=area_name)

    df = df.append(srs)

    traces_path = (
        Path(rules.FolderNames.UNORGANIZED.value) / f"{traces_name}.{rules.FILETYPE}"
    )
    area_path = (
        Path(rules.FolderNames.UNORGANIZED.value) / f"{area_name}.{rules.FILETYPE}"
    )

    return df, traces_path, area_path


@contextmanager
def setup_scaffold_context(tmp_path: Path):
    """
    Set up a repo scaffold at a temporary directory.
    """
    current_dir = Path(".").resolve()
    os.chdir(tmp_path)
    try:
        yield repo.scaffold()
    finally:
        os.chdir(current_dir)


def set_up_repo_with_invalids_organized(
    database: pd.DataFrame,
    trace_gdf: gpd.GeoDataFrame,
    area_gdf: gpd.GeoDataFrame,
    organized=True,
) -> Organizer:
    """
    Set up repo with organized traces and areas in current directory.

    Areas will be all marked as invalid.
    """
    organizer = Organizer(database)
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

    if organized:
        organizer.organize(simulate=False)
        organizer.check()

    return organizer


kb11_traces_path = Path("tests/sample_data/KB11/KB11_traces.geojson")
kb11_area_path = Path("tests/sample_data/KB11/KB11_area.geojson")
kb11_traces = read_geofile(kb11_traces_path)
kb11_area = read_geofile(kb11_area_path)


kb11_traces_cut_dislocated_path = Path(
    "tests/sample_data/KB11/KB11_traces_cut_dislocated.pickle"
)

kb11_traces_cut_path = Path("tests/sample_data/KB11/KB11_traces_cut.pickle")

kb11_traces_cut = cached_sample(
    path=kb11_traces_cut_path,
    create_dataset=lambda: cut(dataset=kb11_traces, start=0, end=50),
)


kb11_traces_cut_dislocated = cached_sample(
    path=kb11_traces_cut_dislocated_path,
    create_dataset=lambda: dislocate(dataset=kb11_traces_cut),
)


@lru_cache(maxsize=None)
def test_validate_params() -> Iterator[tuple]:
    """
    Test validate params.
    """
    traces = (kb11_traces_cut, kb11_traces_cut_dislocated)
    area = (kb11_area, kb11_area)
    name = (kb11_area_path.name, kb11_area_path.name)
    snap_threshold = (0.001, 0.001)
    assume_result_validity = (
        rules.ValidationResults.VALID,
        rules.ValidationResults.EMPTY,
    )

    return zip(traces, area, name, snap_threshold, assume_result_validity)


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


def name_regex(geom_type: Optional[rules.ColumnNames]):
    """
    Compile regex strat.
    """
    return from_regex(rules.filename_regex(geom_type=geom_type), fullmatch=True)


def click_error_print(result: Result):
    """
    Print click result traceback.
    """
    if result.exit_code == 0:
        return
    assert result.exc_info is not None
    _, _, tb = result.exc_info
    # print(err_class, err)
    print_tb(tb)
    print(result.output)
    raise Exception(result.exception)


@lru_cache(maxsize=None)
def test_write_geodata_params():
    """
    Params for test_write_geodata.
    """
    return [
        (
            gpd.GeoDataFrame(geometry=[LineString([(0, 0), (1, 1)])]),  # gdf
            False,  # assume_error
        ),
        (
            gpd.GeoDataFrame(
                geometry=[MultiLineString([LineString([(0, 0), (1, 1)])])]
            ),  # gdf
            False,  # assume_error
        ),
        (
            gpd.GeoDataFrame(geometry=[Point(1, 1)]),  # gdf
            True,  # assume_error
        ),
        (
            gpd.GeoDataFrame(
                geometry=[LineString([(0, 0), (1, 1)])], index=["hello"]
            ),  # gdf
            True,  # assume_error
        ),
    ]


@lru_cache(maxsize=None)
def test_rename_data_path_params():
    """
    Params for test_rename_data_path.
    """
    return [
        (
            Path("basedir/hello/somedir/anoterdir/yay.txt"),
            "renamed",
        ),
        (
            Path("basedir/hello/somedir/anoterdir/yay.txt"),
            "sadfsdfsadfsadfsdafsadfsadf",
        ),
    ]
