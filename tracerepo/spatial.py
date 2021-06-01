"""
Spatial data validation.
"""

import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import groupby
from pathlib import Path
from typing import List, Sequence, Tuple, Type, Union

import geopandas as gpd
from fractopo.general import read_geofile, is_empty_area
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
    # Check for empty target area
    if is_empty_area(area=area, traces=traces):
        return traces.copy(), ValidationResults.EMPTY

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

    except Exception as exc:
        logging.critical(
            f"Validation critically failed for dataset ({name}) with exception: {exc}"
        )
        return traces, ValidationResults.CRITICAL

    # Get the error column values
    validated_error_column_values = validated[validation.ERROR_COLUMN].values

    # Convert to list and make sure the error values are lists themselves
    validated_error_lists: List[List[str]] = [
        errors for errors in validated_error_column_values if isinstance(errors, list)
    ]

    # Make sure all were lists
    assert len(validated_error_column_values) == len(validated_error_lists)

    # Check for critical errors that require user fix
    if any(
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


def sort_update_tuples_to_match_invalids(
    update_tuples: List[utils.UpdateTuple], invalids: Sequence[utils.TraceTuple]
) -> List[utils.UpdateTuple]:
    """
    Sort update_tuples to match the order in invalids based on area_name.

    >>> from pprint import pprint
    >>> update_tuples = [
    ... utils.UpdateTuple(area_name="name_3", update_values=dict()),
    ... utils.UpdateTuple(area_name="name_1", update_values=dict()),
    ... utils.UpdateTuple(area_name="name_2", update_values=dict()),
    ... ]
    >>> invalids = [
    ... utils.TraceTuple(traces_path=None, area_path=Path("name_1.file")),
    ... utils.TraceTuple(traces_path=None, area_path=Path("name_2.file")),
    ... utils.TraceTuple(traces_path=None, area_path=Path("name_3.file")),
    ... ]
    >>> pprint(sort_update_tuples_to_match_invalids(update_tuples, invalids))
    [UpdateTuple(area_name='name_1', update_values={}, error=False),
     UpdateTuple(area_name='name_2', update_values={}, error=False),
     UpdateTuple(area_name='name_3', update_values={}, error=False)]

    """
    # Resolve the matching area name from area_path variable
    invalids_area_names = [invalid.area_path.stem for invalid in invalids]

    # Get the matching indexes for update_tuples
    idxs = [
        invalids_area_names.index(update_tuple.area_name)
        for update_tuple in update_tuples
    ]

    # Sort update_tuples with the idxs
    sorted_with_idx = sorted(zip(update_tuples, idxs), key=lambda vals: vals[1])

    # Extract only the update_tuples that should now be ordered
    sorted_update_tuples = [ut[0] for ut in sorted_with_idx]

    return sorted_update_tuples


def validate_invalids(invalids: Sequence[utils.TraceTuple]) -> List[utils.UpdateTuple]:
    """
    Validate a sequence of invalids with multiprocessing support.

    Will not validate the same trace dataset twice.
    """
    # Collect validated
    update_tuples: List[utils.UpdateTuple] = []

    # multiprocessing!
    with ProcessPoolExecutor(max_workers=8) as executor:
        # Iterate over invalids. submit as tasks
        futures = {
            executor.submit(validate_invalid, invalid): invalid for invalid in invalids
        }

        # Collect all tasks as they complete
        # Will not be in same order submitted!?
        for future in as_completed(futures):

            # If validation critically fails for a dataset
            # we can still proceed with other validations
            try:

                # Get result from Future
                # This will throw an error (if it happened in process)
                update_tuple = future.result()

                # Collect result
                update_tuples.append(update_tuple)
            except Exception as exc:

                # Catch and log critical failures
                logging.error(
                    f"Validation exception with {futures[future]}."
                    f"\n\nException: {exc}"
                )
                update_tuples.append(
                    utils.UpdateTuple(area_name="", update_values=dict(), error=True)
                )
    return sort_update_tuples_to_match_invalids(
        update_tuples=update_tuples, invalids=invalids
    )


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

def convert_trace_tuples():
    """
    Convert between geodata filetypes.
    """
    # Iterate over datasets and save
    for dataset_tuple in dataset_tuples:
        for path in (dataset_tuple.traces_path, dataset_tuple.area_path):

            # Rename the base data directory to export_destination
            renamed = utils.rename_data_path(path=path, rename_to=export_destination)


