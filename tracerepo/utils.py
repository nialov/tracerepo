"""
General utilities.
"""
import json
import logging
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Sequence, Type

import geopandas as gpd
import pandas as pd
import pandera as pa

from tracerepo import rules, trace_schema

geojson_driver = "GeoJSON"
export_dir_prefix = "data-exported-"


class TraceTuple(NamedTuple):

    """
    Named tuple of traces and area paths and snap_threshold.
    """

    traces_path: Path
    area_path: Path
    snap_threshold: float = 0.001


@dataclass
class UpdateTuple:

    """
    Tuple with information of validity for trace-area-pair.
    """

    area_name: str
    update_values: Dict[rules.ColumnNames, str]
    traces_path: Path
    error: bool = False


def dataframe_column_to_python(
    dataframe: pd.DataFrame, column: str, python_type: Type[Any]
) -> List[Any]:
    """
    Get dataframe column as Python typed list.

    E.g.

    >>> df = pd.DataFrame({"data": [1.0, 52.2, 1.0]})
    >>> column = "data"
    >>> python_type = float
    >>> as_list = dataframe_column_to_python(df, column, python_type)
    >>> as_list
    [1.0, 52.2, 1.0]
    >>> hasattr(as_list[0], "item")
    False

    """
    # area name is used as index
    python_list: List[Any]
    if column == rules.ColumnNames.AREA.value:
        python_list = dataframe.index.to_list()
    else:
        python_list = dataframe[column].to_list()
    if not isinstance(python_list, list):
        raise TypeError("Expected Python list.")
    python_list_pythoned = [
        val.item() if hasattr(val, "item") else val for val in python_list
    ]
    python_list_checked = [
        val for val in python_list_pythoned if isinstance(val, python_type)
    ]
    if len(python_list) != len(python_list_checked):
        raise TypeError(
            f"Expected database to contain types convertable to Python {python_type}."
        )

    return python_list_checked


def compiled_path(
    thematic: str,
    geometry: str,
    scale: str,
    name: str,
    root: str = rules.FolderNames.DATA.value,
) -> Path:
    r"""
    Compile Path.

    E.g.

    >>> path = str(compiled_path("inkoo", "traces", "drone_20m", "geta_20m_1_traces"))
    >>> path.replace("\\", "/")
    'tracerepository_data/inkoo/traces/drone_20m/geta_20m_1_traces.geojson'

    """
    return Path(root) / thematic / geometry / scale / f"{name}.{rules.FILETYPE}"


def check_database_row_files(
    thematic: str,
    geometry: str,
    scale: str,
    name: str,
):
    """
    Check that a row in database actually corresponds to trace and area files.
    """
    path = compiled_path(thematic=thematic, geometry=geometry, scale=scale, name=name)
    if not path.exists():
        raise FileNotFoundError(f"Expected {name} file to exist at {path}.")


def identify_geom_type(filename_stem: str) -> rules.ColumnNames:
    """
    Naively identify if file contains traces or areas based on filename.

    E.g.

    >>> identify_geom_type("geta_20m_1_traces")
    <ColumnNames.TRACES: 'traces'>

    """
    assert "." not in filename_stem
    if filename_stem.endswith("_area"):
        return rules.ColumnNames.AREA
    if filename_stem.endswith("_traces"):
        return rules.ColumnNames.TRACES
    raise ValueError(
        f"Expected filename_stem {filename_stem} to end in _area or _traces."
    )


def multi_string_filter(
    strings: Sequence[str], list_to_filter: Sequence[str]
) -> Sequence[bool]:
    """
    Filter list for any matching string in strings.

    E.g.

    >>> multi_string_filter(["geta"], ["geta", "heta"])
    [True, False]

    >>> multi_string_filter(["geta", "leta"], ["geta", "heta", "geta"])
    [True, False, True]

    >>> multi_string_filter([], ["geta", "heta", "geta"])
    [True, True, True]

    """
    if len(strings) == 0:
        return [True] * len(list_to_filter)

    string_filtered = [string_filter(string, list_to_filter) for string in strings]

    any_filtered = [any(vals) for vals in zip(*string_filtered)]

    assert len(any_filtered) == len(list_to_filter)

    return any_filtered


def string_filter(string: str, list_to_filter: Sequence[str]) -> Sequence[bool]:
    """
    Filter list for strings that contain string.
    """
    bools = [string in val for val in list_to_filter]
    return bools


def join_bools(*bool_sequences) -> Sequence[bool]:
    """
    Perform logical OR on sequences of bools of equal length.
    """
    joined = [all(vals) for vals in zip(*bool_sequences)]
    return joined


def query_result_tuple(
    thematic_val: str,
    scale_val: str,
    traces_val: str,
    area_val: str,
    snap_threshold: float,
) -> TraceTuple:
    """
    Compile TraceTuple with trace path, area path and snap_threshold.

    Some of paths might be None based on geometry_filter.
    """
    traces_path = compiled_path(
        thematic=thematic_val,
        scale=scale_val,
        name=traces_val,
        geometry=rules.ColumnNames.TRACES.value,
    )
    area_path = compiled_path(
        thematic=thematic_val,
        scale=scale_val,
        name=area_val,
        geometry=rules.ColumnNames.AREA.value,
    )

    return TraceTuple(
        traces_path=traces_path, area_path=area_path, snap_threshold=snap_threshold
    )


def convert_list_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Convert list type columns to string.
    """
    gdf = gdf.copy()
    if gdf.empty:
        return gdf
    for column in gdf.columns.values:
        if isinstance(gdf[column].values[0], list):
            logging.info(f"Converting {column} from list to str.")
            gdf[column] = [tuple(item) for item in gdf[column].values]
            gdf[column] = gdf[column].astype(str)
    return gdf


def write_geojson(gdf: gpd.GeoDataFrame, path: Path):
    """
    Write geodata as GeoJSON with 1 space delimition.
    """
    as_json = gdf.to_json(indent=1)
    path.write_text(as_json)


def write_geodata(gdf: gpd.GeoDataFrame, path: Path, driver: str = geojson_driver):
    """
    Write geodata with driver.

    Default is GeoJSON.
    """
    if gdf.empty:
        # Handle empty GeoDataFrames
        path.write_text(gdf.to_json())
    else:
        gdf = convert_list_columns(gdf)

        gdf.to_file(path, driver=driver)

    if driver != geojson_driver:
        return

    # Format geojson with indent of 1
    read_json = path.read_text()
    loaded_json = json.loads(read_json)
    dumped_json = json.dumps(loaded_json, indent=1)
    path.write_text(dumped_json)


def rename_data_path(path: Path, rename_to: str) -> Path:
    """
    Rename the first directory of path to rename_to.

    >>> str(rename_data_path(Path("data/loviisa/traces/20m/file.txt"), "hey"))
    'hey/loviisa/traces/20m/file.txt'

    """
    splitter = "/" if "/" in str(path) else r"\\"

    return Path(f"{rename_to}/" + "/".join(str(path).split(splitter)[1:]))


def compile_export_dir(driver: str) -> str:
    """
    Compile directory name for data exporting.

    E.g.

    >>> compile_export_dir("ESRI Shapefile")
    'data-exported-ESRI-Shapefile'
    """
    dash_replaced_driver = driver.replace(" ", "-")
    return f"{export_dir_prefix}{dash_replaced_driver}"


def remove_from_dict_if_in(key: str, dict_to_check: Dict[str, Path]):
    """
    Remove value from dict if it exists.

    E.g.

    >>> remove_from_dict_if_in("some_key", dict(some_key=Path(".")))
    {}

    >>> remove_from_dict_if_in("some_key", dict(other_key=Path(".")))
    {'other_key': PosixPath('.')}
    """
    with suppress(KeyError):
        dict_to_check.pop(key)
    return dict_to_check


def perform_pandera_check(
    traces: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """
    Validate the column data in ``traces`` ``GeoDataFrame``.
    """
    pandera_report: pd.DataFrame = pd.DataFrame()
    assert pandera_report.empty
    try:
        trace_schema.traces_schema().validate(traces, lazy=True)
    except pa.errors.SchemaErrors as exc:
        pandera_report = exc.failure_cases
        assert isinstance(pandera_report, pd.DataFrame)

    return pandera_report


def filename_friendly_datetime_string() -> str:
    """
    Get filename friendly datetime string.
    """
    return datetime.now().strftime("%Y%m%d_%H%M")


def report_pandera_errors(
    pandera_report: pd.DataFrame, report_directory: Path, area_name: str
) -> str:
    """
    Report pandera errors as html files saved to a reports directory.
    """
    report_directory.mkdir(exist_ok=True)
    current_time = filename_friendly_datetime_string()
    report_name = f"{area_name}_report_{current_time}.html"
    report_path = report_directory / report_name
    pandera_report.to_html(report_path)
    return f"Reported {area_name} traces pandera errors to {report_path}."


def otherwise_valid(update_tuple: UpdateTuple) -> bool:
    """
    Is dataset otherwise valid.
    """
    assert rules.ColumnNames.VALIDITY in update_tuple.update_values
    if (
        update_tuple.update_values[rules.ColumnNames.VALIDITY]
        == rules.ValidationResults.VALID.value
    ):
        return True
    return False
