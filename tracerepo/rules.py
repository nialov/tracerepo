"""
trace-repository rules.
"""
from __future__ import annotations
from enum import Enum, unique
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Type, Union

import pandera as pa

FILETYPE = "geojson"
DATABASE_CSV = "database.csv"


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
    EMPTY = "empty"
    VALIDATED = "validated"
    SNAP_THRESHOLD = "snap-threshold"

    @classmethod
    def column_type(cls) -> Dict[ColumnNames, Union[Type[str], Type[float]]]:
        """
        Get python type for each column.
        """
        return {
            cls.AREA: str,
            cls.TRACES: str,
            cls.THEMATIC: str,
            cls.SCALE: str,
            cls.AREA_SHAPE: str,
            cls.EMPTY: str,
            cls.VALIDATED: str,
            cls.SNAP_THRESHOLD: float,
        }


@unique
class BooleanChoices(Enum):

    """
    Boolean chocies in database.csv.
    """

    TRUE = "true"
    FALSE = "false"


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


@lru_cache(maxsize=None)
def name_column_kwargs() -> Dict[str, Any]:
    """
    Get kwargs for a string/name column.
    """
    return dict(
        pandas_dtype=pa.String,
        checks=[
            pa.Check.str_length(min_value=3, max_value=30),
        ],
        allow_duplicates=False,
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
        index=pa.Index(**name_column_kwargs()),
        # Columns
        columns={
            # traces, thematic and scale columns are strings
            ColumnNames.TRACES.value: pa.Column(**name_column_kwargs()),
            ColumnNames.THEMATIC.value: pa.Column(**name_column_kwargs()),
            ColumnNames.SCALE.value: pa.Column(**name_column_kwargs()),
            # area-shape must be one of the enum values
            ColumnNames.AREA_SHAPE.value: pa.Column(
                **enum_column_kwargs(enum_class=AreaShapes)
            ),
            # empty must be one of the boolean enum values
            ColumnNames.EMPTY.value: pa.Column(
                **enum_column_kwargs(enum_class=BooleanChoices)
            ),
            # validated must be one of the boolean enum values
            ColumnNames.VALIDATED.value: pa.Column(
                **enum_column_kwargs(enum_class=BooleanChoices)
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
    root = FolderNames.DATA.value
    geometry = [FolderNames.TRACES.value, FolderNames.AREA.value]
    return [Path(root) / geom for geom in geometry]
