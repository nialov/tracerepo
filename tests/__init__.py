"""
Parameters for tests.
"""
from pathlib import Path
from functools import lru_cache
from fractopo.general import read_geofile

import tracerepo.rules as rules
from typing import Iterator, Optional, Callable
import geopandas as gpd
from pickle import loads


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
