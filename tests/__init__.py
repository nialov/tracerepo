"""
Parameters for tests.
"""
import os
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from pickle import loads
from typing import Callable, Iterator

import geopandas as gpd
import pandas as pd
from fractopo.general import read_geofile

import tracerepo.repo as repo
import tracerepo.rules as rules


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
