"""
Command line api for tracerepo.
"""

import typer
import logging
from itertools import groupby
from pathlib import Path
from tracerepo.organize import Organizer
from fractopo.general import read_geofile
import tracerepo.rules as rules
import tracerepo.repo as repo
import tracerepo.spatial as spatial
import tracerepo.utils as utils
from typing import List, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed


app = typer.Typer()


@app.command()
def validate(
    database: Path = typer.Option(
        Path(rules.DATABASE_CSV),
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=True,
        readable=True,
    ),
    area_filter: List[str] = typer.Option(default=[]),
    thematic_filter: List[str] = typer.Option(default=[]),
    traces_filter: List[str] = typer.Option(default=[]),
    scale_filter: List[str] = typer.Option(default=[]),
):
    """
    Validate trace datasets.

    Only validates if the dataset has been marked as invalid in database.csv.
    """
    # Initialize Organizer
    organizer = Organizer(database=repo.read_database_csv(path=database))

    # Query for invalid traces
    invalids = organizer.query(
        area=area_filter,
        thematic=thematic_filter,
        traces=traces_filter,
        scale=scale_filter,
        validity=rules.ValidationResults.INVALID,
    )

    update_tuples = validate_invalids(invalids=invalids)

    for update_tuple, invalid in zip(update_tuples, invalids):

        organizer.update(
            area_name=invalid.area_path.stem, update_values=update_tuple.update_values
        )

        repo.write_database_csv(path=database, database=organizer.database)


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
                raise
                # logging.error(
                #     f"Validation exception with {futures[future]}."
                #     f"\n\nException: {exc}"
                # )
                # update_tuples.append(dict())
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
    # validated.to_file(traces_path, driver="GeoJSON")

    # Create dict with information on validity for trace-area-combo
    update_tuple = utils.UpdateTuple(
        area_name=area_path.stem,
        update_values={rules.ColumnNames.VALIDITY: validation_results.value},
    )
    # update_dict = dict(
    #     area_name=area_path.stem,
    #     update_values={rules.ColumnNames.VALIDITY: validation_results.value},
    # )

    return update_tuple
