"""
Spatial data validation.
"""

import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import groupby
from pathlib import Path
from typing import List, Sequence, Tuple, Type, Union

import geopandas as gpd
from fractopo.general import read_geofile
from fractopo.tval.trace_validation import Validation
from fractopo.tval.trace_validators import (
    ALL_VALIDATORS,
    BaseValidator,
    EmptyTargetAreaValidator,
    SharpCornerValidator,
)

import tracerepo.rules as rules
import tracerepo.spatial as spatial
import tracerepo.utils as utils
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


def unique_invalids(invalids: Sequence[utils.TraceTuple]) -> Sequence[utils.TraceTuple]:
    """
    Return invalids that are unique by traces_path.
    """

    def keyfunc(invalid: utils.TraceTuple) -> Path:
        """
        Return traces_path from invalid.
        """
        traces_path = invalid.traces_path
        assert isinstance(traces_path, Path)
        return traces_path

    unique = []
    sorted_invalids = sorted(invalids, key=keyfunc)
    for _, group in groupby(sorted_invalids, key=keyfunc):
        unique.append(next(group))
    return unique


def validate_invalids(invalids: Sequence[utils.TraceTuple]) -> List[utils.UpdateTuple]:
    """
    Validate a sequence of invalids with multiprocessing support.

    Will not validate the same trace dataset twice.
    """
    update_tuples: List[utils.UpdateTuple] = []

    with ProcessPoolExecutor(max_workers=8) as executor:
        # Iterate over invalids
        futures = {
            executor.submit(validate_invalid, invalid): invalid
            for invalid in unique_invalids(invalids=invalids)
        }

        for future in as_completed(futures):
            try:
                update_tuple = future.result()
                update_tuples.append(update_tuple)
            except Exception as exc:
                logging.error(
                    f"Validation exception with {futures[future]}."
                    f"\n\nException: {exc}"
                )
                update_tuples.append(
                    utils.UpdateTuple(area_name="", update_values=dict(), error=True)
                )
    assert len(invalids) == len(update_tuples)
    return update_tuples


def validate_invalid(invalid: utils.TraceTuple) -> utils.UpdateTuple:
    """
    Validate a given trace dataset.
    """
    # invalid is a TraceTuple which has named path attributes
    traces_path = invalid.traces_path
    area_path = invalid.area_path

    # Both should be Paths
    assert isinstance(traces_path, Path)
    assert isinstance(area_path, Path)

    # Read traces GeoDataFrame
    traces = read_geofile(traces_path)

    # Validate with fractopo trace validation
    validated, validation_results = spatial.validate(
        traces=traces,
        area=read_geofile(area_path),
        snap_threshold=invalid.snap_threshold,
        name=area_path.name,
    )

    assert validated.crs == traces.crs

    # Save the validated (overwrites old)
    utils.write_geodata(gdf=validated, path=traces_path)

    # Create dict with information on validity for trace-area-combo
    update_tuple = utils.UpdateTuple(
        area_name=area_path.stem,
        update_values={rules.ColumnNames.VALIDITY: validation_results.value},
    )

    return update_tuple
