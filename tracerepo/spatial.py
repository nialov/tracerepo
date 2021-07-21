"""
Spatial data validation.
"""

import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import groupby
from pathlib import Path
from typing import List, Sequence, Tuple, Type, Union

import geopandas as gpd
from fractopo.general import is_empty_area, read_geofile
from fractopo.tval.trace_validation import Validation
from fractopo.tval.trace_validators import (
    ALL_VALIDATORS,
    BaseValidator,
    EmptyTargetAreaValidator,
    SharpCornerValidator,
)

from tracerepo import rules
from tracerepo import utils
from tracerepo.rules import ValidationResults
from tracerepo.utils import TraceTuple

ANY_VALIDATOR = Union[Type[BaseValidator], Type[EmptyTargetAreaValidator]]

DRIVER_EXTENSIONS = {"ESRI Shapefile": ".shp", "GPKG": ".gpkg"}


def check_for_validator_error(
    errors: List[str], validators: Tuple[ANY_VALIDATOR, ...] = ALL_VALIDATORS
) -> bool:
    """
    Check if validator errors are in list of validation errors.
    """
    return any(validator.ERROR in errors for validator in validators)


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
        check_for_validator_error(
            errors,
            validators=tuple(
                validator
                for validator in ALL_VALIDATORS
                if validator is not SharpCornerValidator
            ),
        )
        for errors in validated_error_lists
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
    with ProcessPoolExecutor() as executor:
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
                    utils.UpdateTuple(
                        area_name=futures[future].area_path.stem,
                        update_values=dict(),
                        error=True,
                    )
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

    if traces.empty:
        return utils.UpdateTuple(
            area_name=area_path.stem,
            update_values={
                rules.ColumnNames.VALIDITY: rules.ValidationResults.EMPTY.value
            },
        )

    # Validate with fractopo trace validation
    validated, validation_results = validate(
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


def rename_trace_tuple_paths(
    trace_tuple: TraceTuple, export_destination: str, driver: str
) -> Tuple[Path, Path]:
    """
    Rename TraceTuple paths with new base data directory and extension.
    """
    return rename_path(
        trace_tuple.traces_path, export_destination, driver
    ), rename_path(trace_tuple.area_path, export_destination, driver)


def rename_path(path: Path, export_destination: str, driver: str) -> Path:
    """
    Rename path to new base data dir and extension.

    >>> str(rename_path(
    ... Path("data/loviisa/traces/20m/hey.geojson"), "newdata", "ESRI Shapefile"
    ... ))
     'newdata/loviisa/traces/20m/hey.shp'
    """
    renamed = utils.rename_data_path(path=path, rename_to=export_destination)

    # Convert suffix
    fully_renamed = renamed.with_suffix(DRIVER_EXTENSIONS[driver])

    assert DRIVER_EXTENSIONS[driver] in str(fully_renamed)
    assert export_destination in str(fully_renamed)

    return fully_renamed


def convert_trace_tuples(
    trace_tuples: Sequence[TraceTuple], export_destination: str, driver: str
) -> List[Tuple[Path, Path]]:
    """
    Make paths for converting between geodata filetypes.

    >>> from pprint import pprint
    >>> trace_tuple = TraceTuple(
    ... traces_path=Path("data/loviisa/traces/20m/traces.geojson"),
    ... area_path=Path("data/loviisa/traces/20m/area.geojson"),
    ... )
    >>> pprint(convert_trace_tuples([trace_tuple], "exported", "GPKG") )
    [(PosixPath('exported/loviisa/traces/20m/traces.gpkg'),
      PosixPath('exported/loviisa/traces/20m/area.gpkg'))]

    """
    convert_paths = []
    # Iterate over datasets and save
    trace_tuple: TraceTuple
    for trace_tuple in trace_tuples:
        convert_paths.append(
            rename_trace_tuple_paths(
                trace_tuple=trace_tuple,
                export_destination=export_destination,
                driver=driver,
            )
        )

    return convert_paths


def save_converted_paths(
    trace_tuples: Sequence[TraceTuple],
    convert_paths: Sequence[Tuple[Path, Path]],
    driver: str,
    destination: Path,
):
    """
    Save transformed geodata files to new paths.
    """
    for trace_tuple, convert_path_tuple in zip(trace_tuples, convert_paths):

        for original_path, convert_path in zip(
            (trace_tuple.traces_path, trace_tuple.area_path), convert_path_tuple
        ):
            convert_filetype(original_path, destination / convert_path, driver=driver)


def convert_filetype(original_path: Path, convert_path: Path, driver: str):
    """
    Convert from original_path to convert_path with driver.
    """
    if not original_path.exists():
        raise FileNotFoundError(
            f"Expected {original_path.name} to exist at {original_path}."
        )

    # If already saved same trace dataset no need to overwrite
    # If exporting to directory that you've already previously exported to
    # it is removed before exporting
    if convert_path.exists():
        logging.info(f"Dataset already exists at {convert_path}.")
        return

    # Read from path
    gdf = read_geofile(original_path)

    # Make parent directories as needed
    convert_path.parent.mkdir(exist_ok=True, parents=True)

    # Save with new extension and type
    logging.info(f"Saving to {convert_path} with driver {driver}.")
    try:
        gdf.to_file(convert_path, driver=driver)
    except Exception as err:
        logging.error(
            f"Failed to save {original_path} to {convert_path}"
            f" with driver {driver} due to error:\n{err}."
        )
