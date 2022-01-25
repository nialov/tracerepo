"""
Traces pandera scheming.
"""
from functools import lru_cache
from typing import Dict

import pandera as pa
from fractopo.tval.trace_validation import Validation

from tracerepo import rules, schema_checks

VALIDATION_ERROR_COLUMN = Validation.ERROR_COLUMN
DIP_COLUMN = "DIP"
DIP_DIR_COLUMN = "DIP_DIR"
DATA_SOURCE_COLUMN = "Data_Source"
DATE_COLUMN = "Date"
OPERATOR_COLUMN = "Operator"
SCALE_COLUMN = "Scale"
CERTAINTY_COLUMN = "Certainty"
LINEAMENT_ID_COLUMN = "Lineament_ID"


def default_non_required_kwargs(
    required: bool = False, coerce: bool = True, nullable: bool = True
) -> Dict[str, bool]:
    """
    Get default non required, coercible and nullable column kwargs.
    """
    return dict(required=required, coerce=coerce, nullable=nullable)


def prioritized_values_check(
    named_priorities: Dict[str, int], separator: str, name: str
) -> pa.checks.Check:
    """
    Construct check for e.g. data source and scale columns.

    Both have fixed values and fixed order of the values.
    """
    return pa.Check(
        lambda value: schema_checks.named_priority_check(
            value,
            named_priorities=named_priorities,
            separator=separator,
        ),
        element_wise=True,
        name=name,
    )


@lru_cache(maxsize=None)
def traces_schema(metadata: rules.Metadata):
    """
    Get pandera schema for traces GeoDataFrame.
    """
    trace_columns: Dict[str, pa.Column] = {
        VALIDATION_ERROR_COLUMN: pa.Column(pa.String, **default_non_required_kwargs()),
        DIP_COLUMN: pa.Column(
            pa.Float,
            **default_non_required_kwargs(),
            checks=[pa.checks.Check.in_range(min_value=0.0, max_value=90.0)],
        ),
        DIP_DIR_COLUMN: pa.Column(
            pa.Float,
            **default_non_required_kwargs(),
            checks=[pa.checks.Check.in_range(min_value=0.0, max_value=360.0)],
        ),
        DATA_SOURCE_COLUMN: pa.Column(
            pa.String,
            **default_non_required_kwargs(nullable=False),
            checks=[
                prioritized_values_check(
                    named_priorities=metadata.data_source.order,
                    separator=metadata.data_source.separator,
                    name=f"Value and priority order check for {DATA_SOURCE_COLUMN}.",
                )
            ],
        ),
        DATE_COLUMN: pa.Column(
            pa.DateTime,
            **default_non_required_kwargs(nullable=False),
            checks=[pa.Check(schema_checks.date_datetime_check, element_wise=True)],
        ),
        OPERATOR_COLUMN: pa.Column(
            pa.String,
            **default_non_required_kwargs(nullable=False),
            checks=[pa.Check.isin(metadata.operators)],
        ),
        SCALE_COLUMN: pa.Column(
            pa.String,
            **default_non_required_kwargs(nullable=False),
            checks=[
                prioritized_values_check(
                    named_priorities=metadata.scale.order,
                    separator=metadata.scale.separator,
                    name=f"Value and priority order check for {SCALE_COLUMN}.",
                )
            ],
        ),
        CERTAINTY_COLUMN: pa.Column(
            pa.String,
            **default_non_required_kwargs(nullable=False),
            checks=[pa.Check.isin(metadata.certainty)],
        ),
        LINEAMENT_ID_COLUMN: pa.Column(
            pa.String,
            **default_non_required_kwargs(nullable=False),
            checks=[
                pa.Check(
                    lambda raw_value: schema_checks.lineament_id_check(
                        raw_value=raw_value,
                        lineament_id_prefixes=metadata.lineament_id_prefixes,
                    ),
                    element_wise=True,
                    name=f"{LINEAMENT_ID_COLUMN} check.",
                )
            ],
            allow_duplicates=False,
        ),
    }
    return pa.DataFrameSchema(
        index=pa.Index(pa.Int),
        columns={
            "geometry": pa.Column(
                required=True,
            ),
            **trace_columns,
        },
    )
