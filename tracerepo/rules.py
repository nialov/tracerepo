"""
trace-repository rules.
"""
from __future__ import annotations

from enum import Enum, unique
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union

import pandera as pa
from fractopo.tval.trace_validation import Validation
from shapely.geometry import LineString, MultiLineString

FILETYPE = "geojson"
DATABASE_CSV = "database.csv"
DATABASE_CSV_SEP = ","

VALIDATION_ERROR_COLUMN = Validation.ERROR_COLUMN


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

    DATA = "data"
    TRACES = "traces"
    AREA = "area"
    UNORGANIZED = "unorganized"


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


@lru_cache(maxsize=None)
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
            pa.Check.str_length(min_value=2, max_value=50),
            pa.Check.str_matches(filename_regex(geom_type=geom_type)),
        ],
        allow_duplicates=allow_duplicates,
    )


@lru_cache(maxsize=None)
def enum_column_kwargs(enum_class: Type[Enum]) -> Dict[str, Any]:
    """
    Get kwargs for an enum column.
    """
    return dict(
        pandas_dtype=pa.String,
        checks=pa.Check.isin([member.value for member in enum_class]),
    )


@lru_cache(maxsize=None)
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


def filename_regex(geom_type: Optional[ColumnNames] = None) -> str:
    """
    Get general, trace or area filename regex.

    E.g.

    >>> filename_regex()
    '^[a-z0-9_]{2,50}$'

    >>> filename_regex(ColumnNames.AREA)
    '^[a-z0-9_]{2,50}_area$'

    >>> filename_regex(ColumnNames.TRACES)
    '^[a-z0-9_]{2,50}_traces$'

    """
    base = r"^[a-z0-9_]{2,50}"

    if geom_type is None:
        return base + r"$"
    elif geom_type == ColumnNames.AREA:
        return base + r"_area$"
    elif geom_type == ColumnNames.TRACES:
        return base + r"_traces$"
    else:
        raise TypeError(f"Expected {geom_type=} to be None or TRACES or AREA enum.")


@lru_cache(maxsize=None)
def traces_schema():
    """
    Get pandera schema for traces GeoDataFrame.
    """
    return pa.DataFrameSchema(
        index=pa.Index(pa.Int),
        columns={
            "geometry": pa.Column(
                checks=[
                    pa.Check(
                        lambda geoms: [
                            isinstance(geom, (LineString, MultiLineString))
                            for geom in geoms
                        ],
                    )
                ],
            ),
            VALIDATION_ERROR_COLUMN: pa.Column(
                pa.String,
                required=False,
            ),
        },
    )
