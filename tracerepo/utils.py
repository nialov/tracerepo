"""
General utilities.
"""
import json
import logging
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Sequence, Tuple, Type

import geopandas as gpd
import pandas as pd
import pandera as pa
from fractopo import general
from rich.table import Table
from rich.text import Text

from tracerepo import rules, trace_schema

GEOJSON_DRIVER = "GeoJSON"
EXPORT_DIR_PREFIX = "data-exported-"


class TraceTuple(NamedTuple):

    """
    Named tuple of traces and area paths and snap_threshold.
    """

    traces_path: Path
    area_path: Path
    validity: str
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
            f"Expected database to contain types convertible to Python {python_type}."
        )

    return python_list_checked


def compiled_path(
    thematic: str,
    geometry: str,
    scale: str,
    name: str,
    root: Path,
    data_root: str = rules.PathNames.DATA.value,
) -> Path:
    r"""
    Compile Path.

    E.g.

    >>> path = str(
    ...     compiled_path(
    ...         "inkoo", "traces", "drone_20m", "geta_20m_1_traces", root=Path(".")
    ...     )
    ... )
    >>> path.replace("\\", "/")
    'tracerepository_data/inkoo/traces/drone_20m/geta_20m_1_traces.geojson'

    """
    return root / data_root / thematic / geometry / scale / f"{name}.{rules.FILETYPE}"


def check_database_row_files(
    thematic: str,
    geometry: str,
    scale: str,
    name: str,
    root: Path,
):
    """
    Check that a row in database actually corresponds to trace and area files.
    """
    path = compiled_path(
        root=root, thematic=thematic, geometry=geometry, scale=scale, name=name
    )
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
    validity_val: str,
    snap_threshold: float,
    tracerepository_path: Path,
) -> TraceTuple:
    """
    Compile TraceTuple with trace path, area path and snap_threshold.

    Some of paths might be None based on geometry_filter.
    """
    traces_path = compiled_path(
        root=tracerepository_path,
        thematic=thematic_val,
        scale=scale_val,
        name=traces_val,
        geometry=rules.ColumnNames.TRACES.value,
    )
    area_path = compiled_path(
        root=tracerepository_path,
        thematic=thematic_val,
        scale=scale_val,
        name=area_val,
        geometry=rules.ColumnNames.AREA.value,
    )

    return TraceTuple(
        traces_path=traces_path,
        area_path=area_path,
        snap_threshold=snap_threshold,
        validity=validity_val,
    )


def convert_sequence_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Convert sequence (list/tuple) type columns to string.
    """
    gdf = gdf.copy()
    if gdf.empty:
        return gdf
    for column in gdf.columns.values:
        column_data = gdf[column]
        assert isinstance(column_data, pd.Series)
        first_val = column_data.values[0]
        if isinstance(first_val, (list, tuple)):
            logging.info(f"Converting {column} from {type(first_val)} to str.")
            gdf[column] = [str(tuple(item)) for item in column_data.values]
    return gdf


def write_geojson(gdf: gpd.GeoDataFrame, path: Path):
    """
    Write geodata as GeoJSON with 1 space delimitation.
    """
    as_json = gdf.to_json(indent=1)
    path.write_text(as_json)


def write_geodata(gdf: gpd.GeoDataFrame, path: Path, driver: str = GEOJSON_DRIVER):
    """
    Write geodata with driver.

    Default is GeoJSON.
    """
    if gdf.empty:
        # Handle empty GeoDataFrames
        path.write_text(gdf.to_json())
    else:
        gdf = convert_sequence_columns(gdf)

        gdf.to_file(path, driver=driver)

    if driver != GEOJSON_DRIVER:
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
    return f"{EXPORT_DIR_PREFIX}{dash_replaced_driver}"


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
    traces: gpd.GeoDataFrame, metadata: rules.Metadata
) -> pd.DataFrame:
    """
    Validate the column data in ``traces`` ``GeoDataFrame``.
    """
    pandera_report: pd.DataFrame = pd.DataFrame()
    assert pandera_report.empty
    try:
        trace_schema.traces_schema(metadata=metadata).validate(traces, lazy=True)
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


def pandera_reporting(
    update_tuple: UpdateTuple, metadata: rules.Metadata
) -> Tuple[Dict[rules.ColumnNames, str], pd.DataFrame]:
    """
    Check traces GeoDataFrame column data against schema and report if needed.
    """
    if update_tuple.update_values[rules.ColumnNames.VALIDITY] in (
        rules.ValidationResults.EMPTY.value,
        rules.ValidationResults.CRITICAL.value,
    ):
        return dict(), pd.DataFrame()
    # Read traces from disk.
    # (Alternative is to keep GeoDataFrame in memory from multiprocessing
    # but that is risky.)
    traces = general.read_geofile(update_tuple.traces_path)
    if traces.empty:
        logging.error(f"Empty traces uncaught by validation for {update_tuple}.")
        return dict(), pd.DataFrame()
    try:
        pandera_report = perform_pandera_check(traces, metadata=metadata)
    except Exception as exc:
        logging.error(
            f"GeoDataFrame validation critically failed with {update_tuple} traces.",
            exc_info=True,
        )
        pandera_report = pd.DataFrame(
            {"ERROR": ["Column validation critically failed...", str(exc)]}
        )
    if pandera_report.empty:
        return dict(), pd.DataFrame()

    if otherwise_valid(update_tuple=update_tuple):
        # If the dataset is otherwise marked valid mark it as unfit due
        # to pandera schema error
        update_values = {
            rules.ColumnNames.VALIDITY: rules.ValidationResults.UNFIT.value
        }
        return update_values, pandera_report
    return dict(), pandera_report


def create_validation_table(
    invalids: List[TraceTuple], validity_changes: Optional[List[Text]] = None
) -> Table:
    """
    Generate a rich Table from invalids.
    """
    if validity_changes is not None:
        assert len(validity_changes) == len(invalids)
        title = "Validation Results"
    else:
        title = "Validation Targets"
    table = Table(title=title)
    table.add_column("Traces", header_style="bold")
    table.add_column("Area", header_style="bold")
    table.add_column("Snap Threshold", header_style="bold", style="bold blue")
    # table.add_column("Validity", header_style="bold", style="bold blue")
    table.add_column("Validity")

    for idx, trace_tuple in enumerate(invalids):
        validity = (
            enrich_color_validity_value(trace_tuple.validity)
            if validity_changes is None
            else validity_changes[idx]
        )
        table.add_row(
            trace_tuple.traces_path.name,
            trace_tuple.area_path.name,
            str(trace_tuple.snap_threshold),
            validity,
        )
    return table


def enrich_color_validity_value(validity_value: str) -> Text:
    """
    Wrap validity value with approppriate color.
    """
    try:
        color = rules.ValidationResults.color_dict()[validity_value]
    except KeyError:
        logging.error(
            "validity_value was not found in rules.ValidationResults.color_dict()",
            extra=dict(validity_value=validity_value),
            exc_info=True,
        )
        color = "blue"
    return Text(text=validity_value, style=color)


def create_validation_results_table(
    invalids: List[TraceTuple], update_tuples: List[UpdateTuple]
) -> Table:
    """
    Create table for validation results.
    """
    assert len(invalids) == len(update_tuples)
    new_validities = [
        update_tuple.update_values[rules.ColumnNames.VALIDITY]
        for update_tuple in update_tuples
    ]
    old_validities = [invalid.validity for invalid in invalids]

    validity_changes = [
        Text.assemble(
            enrich_color_validity_value(old), " -> ", enrich_color_validity_value(new)
        )
        for old, new in zip(old_validities, new_validities)
    ]

    table = create_validation_table(
        invalids=invalids, validity_changes=validity_changes
    )
    return table
