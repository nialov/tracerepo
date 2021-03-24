"""
General utilities.
"""
from pathlib import Path
from typing import Any, List, Sequence, Type

import pandas as pd

import tracerepo.rules as rules


def dataframe_column_to_python(
    dataframe: pd.DataFrame, column: str, python_type: Type
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
    if column == rules.ColumnNames.AREA.value:
        python_list: List[Any] = dataframe.index.to_list()
    else:
        python_list: List[Any] = dataframe[column].to_list()
    if not isinstance(python_list, list):
        raise TypeError("Expected Python list.")
    python_list_pythoned = [
        val.item() if hasattr(val, "item") else val for val in python_list
    ]
    python_list_checked = [
        val for val in python_list_pythoned if isinstance(val, python_type)
    ]
    if len(python_list) != len(python_list_checked):
        raise TypeError("Expected database to contain types convertable to Python.")

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
    'data/inkoo/traces/drone_20m/geta_20m_1_traces.geojson'

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
    elif filename_stem.endswith("_traces"):
        return rules.ColumnNames.TRACES
    else:
        raise ValueError("Expected filenames to end in _area or _traces.")


def multi_string_filter(
    strings: Sequence[str], list_to_filter: Sequence[str]
) -> Sequence[bool]:
    """
    Filter list for any matching string in strings.
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
