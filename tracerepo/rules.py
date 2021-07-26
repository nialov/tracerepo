"""
trace-repository rules.
"""
from __future__ import annotations

from enum import Enum, unique
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union

import pandera as pa

FILETYPE = "geojson"
DATABASE_CSV = "database.csv"
DATABASE_CSV_SEP = ","


@unique
class ColumnNames(Enum):

    """
    Column names for database.csv.
    """

    AREA = "area"
    TRACES = "traces"
    THEMATIC = "thematic"
    SCALE = "scale"
    AREA_SHAPE = "area-shape"
    VALIDITY = "validity"
    SNAP_THRESHOLD = "snap-threshold"

    @classmethod
    def column_type(
        cls,
    ) -> Dict[ColumnNames, Union[Type[str], Type[float], Type[bool]]]:
        """
        Get python type for each column.
        """
        return {
            cls.AREA: str,
            cls.TRACES: str,
            cls.THEMATIC: str,
            cls.SCALE: str,
            cls.AREA_SHAPE: str,
            cls.VALIDITY: str,
            cls.SNAP_THRESHOLD: float,
        }


@unique
class AreaShapes(Enum):

    """
    area-shape options in database.csv.
    """

    CIRCLE = "circle"
    OTHER = "other"


@unique
class FolderNames(Enum):

    """
    Folder names.

    TODO: Add filenames as well.
    """

    DATA = "tracerepository_data"
    TRACES = "traces"
    AREA = "area"
    UNORGANIZED = "unorganized"
    REPORTS = "tracerepository_reports"


@unique
class Geometry(Enum):

    """
    Folder names.
    """

    TRACES = ColumnNames.TRACES.value
    AREAS = ColumnNames.AREA.value
    BOTH = "both"


@unique
class ValidationResults(Enum):

    """
    Validation result description enums.
    """

    EMPTY = "empty"
    VALID = "valid"
    INVALID = "invalid"
    CRITICAL = "critical"


def name_column_kwargs(
    geom_type: Optional[ColumnNames],
    allow_duplicates=True,
) -> Dict[str, Any]:
    """
    Get kwargs for a string/name column.
    """
    return dict(
        pandas_dtype=pa.String,
        checks=[
            pa.Check.str_matches(filename_regex(geom_type=geom_type)),
        ],
        allow_duplicates=allow_duplicates,
    )


def enum_column_kwargs(enum_class: Type[Enum]) -> Dict[str, Any]:
    """
    Get kwargs for an enum column.
    """
    return dict(
        pandas_dtype=pa.String,
        checks=pa.Check.isin([member.value for member in enum_class]),
    )


def filename_regex(geom_type: Optional[ColumnNames] = None) -> str:
    """
    Get general, trace or area filename regex.

    E.g.

    >>> filename_regex()
    '^[a-z0-9_]{2,49}$'

    >>> filename_regex(ColumnNames.AREA)
    '^[a-z0-9_]{2,49}_area$'

    >>> filename_regex(ColumnNames.TRACES)
    '^[a-z0-9_]{2,49}_traces$'

    """
    base = r"^[a-z0-9_]{2,49}"

    if geom_type is None:
        return base + r"$"
    if geom_type == ColumnNames.AREA:
        return base + r"_area$"
    if geom_type == ColumnNames.TRACES:
        return base + r"_traces$"
    raise TypeError(f"Expected {geom_type=} to be None or TRACES or AREA enum.")


def database_schema() -> pa.DataFrameSchema:
    """
    Get pandera DataFrame schema for database.csv.
    """
    return pa.DataFrameSchema(
        # Index is the area name
        index=pa.Index(
            **name_column_kwargs(allow_duplicates=False, geom_type=ColumnNames.AREA)
        ),
        coerce=True,
        # Columns
        columns={
            # traces, thematic and scale columns are strings
            ColumnNames.TRACES.value: pa.Column(
                **name_column_kwargs(
                    allow_duplicates=True, geom_type=ColumnNames.TRACES
                )
            ),
            ColumnNames.THEMATIC.value: pa.Column(
                **name_column_kwargs(allow_duplicates=True, geom_type=None)
            ),
            ColumnNames.SCALE.value: pa.Column(
                **name_column_kwargs(allow_duplicates=True, geom_type=None)
            ),
            # area-shape must be one of the enum values
            ColumnNames.AREA_SHAPE.value: pa.Column(
                **enum_column_kwargs(enum_class=AreaShapes)
            ),
            # validated must be one of the enum values
            ColumnNames.VALIDITY.value: pa.Column(
                **enum_column_kwargs(enum_class=ValidationResults)
            ),
            ColumnNames.SNAP_THRESHOLD.value: pa.Column(
                pa.Float,
                checks=[
                    pa.Check.greater_than_or_equal_to(1e-8),
                    pa.Check.less_than_or_equal_to(1e8),
                ],
                coerce=True,
                nullable=False,
            ),
        },
    )


def folder_structure() -> List[Path]:
    """
    Get the default data folder structure.
    """
    return [Path(FolderNames.DATA.value)]
