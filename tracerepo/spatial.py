"""
Spatial data validation.
"""

import logging
from typing import List, Tuple, Type, Union

import geopandas as gpd
from fractopo.tval.trace_validation import Validation
from fractopo.tval.trace_validators import (
    ALL_VALIDATORS,
    BaseValidator,
    EmptyTargetAreaValidator,
    SharpCornerValidator,
)

from tracerepo.rules import ValidationResults

ANY_VALIDATOR = Union[Type[BaseValidator], Type[EmptyTargetAreaValidator]]


def check_for_validator_error(
    errors: List[str], validators: Tuple[ANY_VALIDATOR, ...] = ALL_VALIDATORS
) -> bool:
    """
    Check if validator errors are in list of validation errors.
    """
    return any([validator.ERROR in errors for validator in validators])


def validate(
    traces: gpd.GeoDataFrame, area: gpd.GeoDataFrame, name: str, snap_threshold: float
) -> Tuple[gpd.GeoDataFrame, ValidationResults]:
    """
    Validate trace GeoDataFrame.
    """
    # Create Validation instance
    validation = Validation(
        name=name,
        traces=traces,
        area=area,
        allow_fix=True,
        SNAP_THRESHOLD=snap_threshold,
    )

    try:

        # Run validation
        validated = validation.run_validation(allow_empty_area=False)

    except Exception:
        logging.critical(f"Validation critically failed for dataset ({name}).")
        return traces, ValidationResults.CRITICAL

    # Get the error column values
    validated_error_column_values = validated[validation.ERROR_COLUMN].values

    # Convert to list and make sure the error values are lists themselves
    validated_error_lists: List[List[str]] = [
        errors for errors in validated_error_column_values if isinstance(errors, list)
    ]

    # Make sure all were lists
    assert len(validated_error_column_values) == len(validated_error_lists)

    # Check for empty target area
    if any(
        [
            check_for_validator_error(errors, validators=(EmptyTargetAreaValidator,))
            for errors in validated_error_lists
        ]
    ):
        return validated, ValidationResults.EMPTY

    # Check for critical errors that require user fix
    elif any(
        [
            check_for_validator_error(
                errors,
                validators=tuple(
                    validator
                    for validator in ALL_VALIDATORS
                    if validator is not SharpCornerValidator
                ),
            )
            for errors in validated_error_lists
        ]
    ):
        return validated, ValidationResults.INVALID

    # Traces are valid
    else:

        return validated, ValidationResults.VALID
