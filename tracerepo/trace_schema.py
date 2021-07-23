"""
Traces pandera scheming.
"""
from functools import lru_cache
from typing import Dict

import pandera as pa
from fractopo.tval.trace_validation import Validation
from shapely.geometry import LineString, MultiLineString

from tracerepo import schema_checks

VALIDATION_ERROR_COLUMN = Validation.ERROR_COLUMN
DIP_COLUMN = "DIP"
DIP_DIR_COLUMN = "DIP_DIR"
DATA_SOURCE_COLUMN = "Data_Source"
DATE_COLUMN = "Date"
OPERATOR_COLUMN = "Operator"
SCALE_COLUMN = "Scale"
CERTAINTY_COLUMN = "Certainty"


def default_non_required_kwargs(
    required: bool = False, coerce: bool = True, nullable: bool = True
) -> Dict[str, bool]:
    """
    Get default non required, coercable and nullable column kwargs.
    """
    return dict(required=required, coerce=coerce, nullable=nullable)


trace_columns: Dict[str, pa.Column] = {
    VALIDATION_ERROR_COLUMN: pa.Column(pa.String, **default_non_required_kwargs()),
    DIP_COLUMN: pa.Column(
        pa.Float,
        **default_non_required_kwargs(),
        checks=[pa.checks.Check.in_range(min_value=0.0, max_value=90.0)]
    ),
    DIP_DIR_COLUMN: pa.Column(
        pa.Float,
        **default_non_required_kwargs(),
        checks=[pa.checks.Check.in_range(min_value=0.0, max_value=360.0)]
    ),
    DATA_SOURCE_COLUMN: pa.Column(
        pa.String,
        **default_non_required_kwargs(nullable=False),
        checks=[pa.Check(schema_checks.data_source_regex_check, element_wise=True)]
    ),
    DATE_COLUMN: pa.Column(
        pa.DateTime,
        **default_non_required_kwargs(nullable=False),
        checks=[pa.Check(schema_checks.date_datetime_check, element_wise=True)]
    ),
    OPERATOR_COLUMN: pa.Column(
        pa.String,
        **default_non_required_kwargs(nullable=False),
        checks=[pa.Check(schema_checks.operator_regex_check, element_wise=True)]
    ),
    SCALE_COLUMN: pa.Column(
        pa.String,
        **default_non_required_kwargs(nullable=False),
        checks=[pa.Check(schema_checks.scale_regex_check, element_wise=True)]
    ),
    CERTAINTY_COLUMN: pa.Column(
        pa.String,
        **default_non_required_kwargs(nullable=False),
        checks=[pa.Check(schema_checks.certainty_check, element_wise=True)]
    ),
}


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
                required=True,
            ),
            **trace_columns,
        },
    )
