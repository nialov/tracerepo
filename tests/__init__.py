"""
Parameters for tests.
"""
import re
from functools import lru_cache
from pathlib import Path
from traceback import print_tb
from typing import Any, Callable, Iterator, List, Optional, Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from click.testing import Result
from fractopo.general import read_geofile
from hypothesis.strategies import composite, from_regex, integers, lists, sampled_from
from json5 import loads

# Setup nialog logging
from nialog.logger import setup_module_logging
from shapely.geometry import LineString, MultiLineString, Point

from tracerepo import rules, trace_schema, utils
from tracerepo.organize import Organizer
from tracerepo.utils import TraceTuple

setup_module_logging()

READY_TRACEREPOSITORY_PATH = Path("tests/sample_data/tracerepository/")
METADATA_JSON_PATH = READY_TRACEREPOSITORY_PATH / rules.PathNames.METADATA.value


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


def create_and_package(path: Path, create_dataset: Callable):
    """
    Create dataset and save as GeoPackage.
    """
    loaded: gpd.GeoDataFrame = create_dataset()
    assert isinstance(loaded, gpd.GeoDataFrame)

    # Make sure tmp directory exists
    path.parent.mkdir(parents=True, exist_ok=True)
    loaded.to_file(path, driver="GPKG")
    return loaded


def cached_sample(
    path: Path, create_dataset: Callable[..., gpd.GeoDataFrame]
) -> gpd.GeoDataFrame:
    """
    Cache a sample or load already existing.
    """
    if path.exists():
        loaded = gpd.read_file(path)
        assert isinstance(loaded, gpd.GeoDataFrame)
        assert loaded.crs is not None
        return loaded
    return create_and_package(path=path, create_dataset=create_dataset)


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
        Path(rules.PathNames.UNORGANIZED.value) / f"{traces_name}.{rules.FILETYPE}"
    )
    area_path = (
        Path(rules.PathNames.UNORGANIZED.value) / f"{area_name}.{rules.FILETYPE}"
    )

    return df, traces_path, area_path


# @contextmanager
# def setup_scaffold_context(tmp_path: Path):
#     """
#     Set up a repo scaffold at a temporary directory.
#     """
#     current_dir = Path(".").resolve()
#     os.chdir(tmp_path)
#     try:
#         yield repo.scaffold()
#     finally:
#         os.chdir(current_dir)


# @contextmanager
# def change_dir_context(path: Path):
#     """
#     Temporarily change directory context to ``path``.
#     """
#     current_dir = Path(".").resolve()
#     os.chdir(path)
#     try:
#         yield path
#     finally:
#         os.chdir(current_dir)


def set_up_repo_with_invalids_organized(
    database: pd.DataFrame,
    trace_gdf: gpd.GeoDataFrame,
    area_gdf: gpd.GeoDataFrame,
    tracerepository_path: Path,
    organized=True,
) -> Organizer:
    """
    Set up repo with organized traces and areas in current directory.

    Areas will be all marked as invalid.
    """
    organizer = Organizer(database, tracerepository_path=tracerepository_path)
    for trace_name, area_name in zip(
        organizer.columns[rules.ColumnNames.TRACES.value],
        organizer.columns[rules.ColumnNames.AREA.value],
    ):
        save_path = (
            lambda name: tracerepository_path
            / Path(rules.PathNames.UNORGANIZED.value)
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
    "tests/sample_data/tmp/KB11_traces_cut_dislocated.gpkg"
)

kb11_traces_cut_path = Path("tests/sample_data/tmp/KB11_traces_cut.gpkg")

kb11_traces_cut = cached_sample(
    path=kb11_traces_cut_path,
    create_dataset=lambda: cut(dataset=kb11_traces, start=0, end=50),
)
kb11_traces_cut_length = kb11_traces_cut.shape[0]


kb11_traces_cut_dislocated = cached_sample(
    path=kb11_traces_cut_dislocated_path,
    create_dataset=lambda: dislocate(dataset=kb11_traces_cut),
)
kb11_unfit_traces_path = (
    READY_TRACEREPOSITORY_PATH
    / "tracerepository_data/loviisa/traces/20m/kb10_unfit_traces.geojson"
)


def kb11_traces_cut_invalid_dips():
    """
    Make GeoDataFrame with invalid dip column data.
    """
    traces = kb11_traces_cut.copy()
    traces[trace_schema.DIP_COLUMN] = [-1 for _ in range(traces.shape[0])]
    return traces


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
    # traces also has to be unique as some tests put the traces files into
    # unorganized directory during test set up
    traces = draw(
        lists(elements=name_regex(rules.ColumnNames.TRACES), **size_kwargs, unique=True)
    )
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
    return from_regex(
        re.compile(rules.filename_regex(geom_type=geom_type)), fullmatch=True
    )


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
        (
            gpd.GeoDataFrame(geometry=[], index=[]),  # gdf
            False,  # assume_error
        ),
    ]


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


def test_convert_trace_tuples_params():
    """
    Params for test_convert_trace_tuples.
    """
    return [
        (
            [
                TraceTuple(
                    traces_path=Path(
                        f"{rules.PathNames.DATA.value}"
                        "/loviisa/traces/20m/hello_traces.geojson"
                    ),
                    area_path=Path(
                        f"{rules.PathNames.DATA.value}"
                        "/loviisa/area/20m/hello_area.geojson"
                    ),
                    validity=rules.ValidationResults.VALID.value,
                )
            ],
            "newdata",
            "GPKG",
        ),
    ]


def test_compile_export_dir_params():
    """
    Params for test_compile_export_dir.
    """
    return [
        "ESRI Shapefile",
        "GPKG",
    ]


def test_validate_invalids_params():
    """
    Params for test_validate_invalids.
    """
    return [
        (
            [
                utils.TraceTuple(
                    traces_path=Path(
                        "./tests/sample_data/critical_validation/"
                        "getaberget_20m_6_traces.geojson"
                    ),
                    area_path=Path(
                        "tests/sample_data/critical_validation/"
                        "getaberget_20m_6_1_area.geojson"
                    ),
                    validity=rules.ValidationResults.CRITICAL.value,
                )
            ],
            False,
        ),
        (
            [
                utils.TraceTuple(
                    traces_path=Path(
                        "./tests/sample_data/critical_validation/"
                        "getaberget_20m_6_traces_corrupted.geojson"
                    ),
                    area_path=Path(
                        "tests/sample_data/critical_validation/"
                        "getaberget_20m_6_1_area.geojson"
                    ),
                    validity=rules.ValidationResults.CRITICAL.value,
                )
            ],
            True,
        ),
    ]


def test__filter_enums_params():
    """
    Params for test__filter_enums.
    """
    return [
        (
            ["circle", "circle"],
            ["valid", "valid"],
            [True, True],
            [],
            [],
            [True, True],
        ),
        (
            ["circle", "circle"],
            ["valid", "valid"],
            [True, True],
            [rules.AreaShapes.OTHER],
            [rules.ValidationResults.VALID],
            [False, False],
        ),
        (
            ["circle", "circle"],
            ["valid", "valid"],
            [True, True],
            [rules.AreaShapes.CIRCLE, rules.AreaShapes.OTHER],
            [rules.ValidationResults.INVALID],
            [False, False],
        ),
        (
            ["circle", "circle"],
            ["valid", "valid"],
            [True, True],
            [rules.AreaShapes.CIRCLE, rules.AreaShapes.OTHER],
            [rules.ValidationResults.VALID],
            [True, True],
        ),
        (
            ["circle", "other"],
            ["valid", "valid"],
            [True, True],
            [rules.AreaShapes.OTHER],
            [rules.ValidationResults.VALID],
            [False, True],
        ),
    ]


def lines_in_file(path_str: str) -> List[str]:
    """
    Extract each line in file at ``path_str`` as a list of strings.
    """
    path = Path(path_str)
    if not path.exists():
        raise FileNotFoundError(f"Expected file to exist at {path}.")
    return path.read_text().splitlines(keepends=False)


def empty_linestrings(how_many: int) -> List[LineString]:
    """
    Make list of empty linestring geometries.
    """
    return [LineString()] * how_many


def make_example_geodataframe(column: str, values, geometries: Optional[Any] = None):
    """
    Make GeoDataFrame with ``values`` in ``column``.
    """
    gdf = gpd.GeoDataFrame(
        {
            "geometry": empty_linestrings(len(values))
            if geometries is None
            else geometries,
            column: values,
        }
    )
    return gdf


def make_example_param(
    column: str,
    values,
    will_fail: bool,
    geometries: Optional[Any] = None,
) -> Tuple[gpd.GeoDataFrame, bool, bool]:
    """
    Make example pytest param tuple.
    """
    gdf = make_example_geodataframe(column=column, values=values, geometries=geometries)
    return (
        gdf,  # gdf
        will_fail,  # will_fail
        geometries is not None,  # geom_test
    )


data_source_good_examples = lines_in_file(
    "tests/sample_data/data_source_good_examples.txt"
)

data_source_bad_examples = lines_in_file(
    "tests/sample_data/data_source_bad_examples.txt"
)

date_good_examples = lines_in_file("tests/sample_data/unique_dates.txt")
date_bad_examples = lines_in_file("tests/sample_data/unique_dates_bad.txt")
operator_good_examples = lines_in_file("tests/sample_data/operator_good_examples.txt")
operator_bad_examples = lines_in_file("tests/sample_data/operator_bad_examples.txt")
scale_good_examples = lines_in_file("tests/sample_data/unique_scales.txt")
certainty_good_examples = lines_in_file("tests/sample_data/unique_certainty.txt")
lineament_id_good_examples = lines_in_file("tests/sample_data/unique_lineament_ids.txt")


data_source_gdf_good_param = make_example_param(
    column=trace_schema.DATA_SOURCE_COLUMN,
    values=data_source_good_examples,
    will_fail=False,
)
data_source_gdf_bad_param = make_example_param(
    column=trace_schema.DATA_SOURCE_COLUMN,
    values=data_source_bad_examples,
    will_fail=True,
)
date_gdf_good_param = make_example_param(
    column=trace_schema.DATE_COLUMN, values=date_good_examples, will_fail=False
)
date_gdf_bad_param = make_example_param(
    column=trace_schema.DATE_COLUMN, values=date_bad_examples, will_fail=True
)
operator_gdf_good_param = make_example_param(
    column=trace_schema.OPERATOR_COLUMN, values=operator_good_examples, will_fail=False
)
operator_gdf_bad_param = make_example_param(
    column=trace_schema.OPERATOR_COLUMN, values=operator_bad_examples, will_fail=True
)

scale_gdf_good_param = make_example_param(
    column=trace_schema.SCALE_COLUMN, values=scale_good_examples, will_fail=False
)
certainty_gdf_good_param = make_example_param(
    column=trace_schema.CERTAINTY_COLUMN,
    values=certainty_good_examples,
    will_fail=False,
)
lineament_id_gdf_good_param = make_example_param(
    column=trace_schema.LINEAMENT_ID_COLUMN,
    values=lineament_id_good_examples,
    will_fail=False,
)


def test_traces_schema_params() -> list:
    """
    Params for test_traces_schema.
    """
    all_params = [
        (data_source_gdf_good_param, "data_source_gdf_good_param"),
        (data_source_gdf_bad_param, "data_source_gdf_bad_param"),
        (date_gdf_good_param, "date_gdf_good_param"),
        (date_gdf_bad_param, "date_gdf_bad_param"),
        (operator_gdf_good_param, "operator_gdf_good_param"),
        (operator_gdf_bad_param, "operator_gdf_bad_param"),
        (scale_gdf_good_param, "scale_gdf_good_param"),
        (certainty_gdf_good_param, "certainty_gdf_good_param"),
        (lineament_id_gdf_good_param, "lineament_id_gdf_good_param"),
    ]

    for params in all_params:
        actual_params = params[0]
        assert isinstance(actual_params[0], gpd.GeoDataFrame)
        assert isinstance(actual_params[1], bool)
        assert isinstance(actual_params[2], bool)
        assert len(actual_params) == 3

    return [pytest.param(*params[0], id=params[1]) for params in all_params]


def test_data_source_regex_check_params():
    """
    Params for test_data_source_regex_check.
    """
    return [
        *[
            (example, False, metadata_loaded().data_source.order)
            for example in data_source_good_examples
        ],
        *[
            (example, True, metadata_loaded().data_source.order)
            for example in data_source_bad_examples
        ],
    ]


def test_date_datetime_check_params():
    """
    Params for test_date_datetime_check.
    """
    return [
        *[(example, False) for example in date_good_examples],
        *[(example, True) for example in date_bad_examples],
    ]


def test_sort_update_tuples_to_match_invalids_params():
    """
    Params for test_sort_update_tuples_to_match_invalids.
    """
    return [
        (
            [
                utils.UpdateTuple(
                    area_name="name_3",
                    update_values=dict(),
                    traces_path=Path(),
                ),
                utils.UpdateTuple(
                    area_name="name_1",
                    update_values=dict(),
                    traces_path=Path(),
                ),
                utils.UpdateTuple(
                    area_name="name_2",
                    update_values=dict(),
                    traces_path=Path(),
                ),
            ],
            [
                utils.TraceTuple(
                    traces_path=Path(),
                    area_path=Path("name_1.file"),
                    validity=rules.ValidationResults.INVALID.value,
                ),
                utils.TraceTuple(
                    traces_path=Path(),
                    area_path=Path("name_2.file"),
                    validity=rules.ValidationResults.INVALID.value,
                ),
                utils.TraceTuple(
                    traces_path=Path(),
                    area_path=Path("name_3.file"),
                    validity=rules.ValidationResults.INVALID.value,
                ),
            ],
        ),
    ]


def test_perform_pandera_check_params():
    """
    Params for test_perform_pandera_check.
    """
    return [
        (kb11_traces_cut, False),
        (kb11_traces_cut.drop(columns=["geometry"]), True),
        (
            kb11_traces_cut.assign(
                **{
                    trace_schema.SCALE_COLUMN: np.array(
                        ["this is not correct scale..."] * kb11_traces_cut_length
                    )
                }
            ),
            True,
        ),
    ]


def test_pandera_reporting_params():
    """
    Params for test_pandera_reporting.
    """
    return [
        (
            utils.UpdateTuple(
                area_name="param",
                update_values={
                    rules.ColumnNames.VALIDITY: rules.ValidationResults.EMPTY.value
                },
                traces_path=Path(),
            ),
            True,
            True,
            dict(),
        ),
        (
            utils.UpdateTuple(
                area_name="param",
                update_values={
                    rules.ColumnNames.VALIDITY: rules.ValidationResults.CRITICAL.value
                },
                traces_path=Path(),
            ),
            True,
            True,
            dict(),
        ),
        (
            utils.UpdateTuple(
                area_name="kb11_traces_cut",
                update_values={
                    rules.ColumnNames.VALIDITY: rules.ValidationResults.VALID.value
                },
                traces_path=kb11_traces_cut_path,
            ),
            True,
            True,
            dict(),
        ),
        (
            utils.UpdateTuple(
                area_name="kb11_traces_unfit",
                update_values={
                    rules.ColumnNames.VALIDITY: rules.ValidationResults.VALID.value
                },
                traces_path=kb11_unfit_traces_path,
            ),
            False,
            False,
            {rules.ColumnNames.VALIDITY: rules.ValidationResults.UNFIT.value},
        ),
    ]


@lru_cache(maxsize=None)
def metadata_loaded() -> rules.Metadata:
    """
    Load and return json metadata of traces schema.
    """
    loaded_metadata = loads(METADATA_JSON_PATH.read_text())
    assert isinstance(loaded_metadata, dict)
    return rules.Metadata(**loaded_metadata, filepath=METADATA_JSON_PATH)


@lru_cache(maxsize=None)
def test_create_initial_validation_table_params():
    """
    Params for test_report_validation_table.
    """
    return [
        [
            utils.TraceTuple(
                traces_path=Path("traces.gpkg"),
                area_path=Path("area.gpkg"),
                snap_threshold=0.001,
                validity=rules.ValidationResults.VALID.value,
            )
        ],
    ]
