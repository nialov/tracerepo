"""
Command line api for tracerepo.
"""

import logging
from pathlib import Path
from pprint import pprint
from typing import List

import typer

import tracerepo.repo as repo
import tracerepo.rules as rules
import tracerepo.spatial as spatial
import tracerepo.utils as utils
from tracerepo.organize import Organizer

app = typer.Typer()

DATABASE_OPTION = typer.Option(
    rules.DATABASE_CSV,
    exists=True,
    file_okay=True,
    dir_okay=False,
    writable=True,
    readable=True,
)

DATA_FILTER = typer.Option(default=[])


@app.command()
def validate(
    database: Path = DATABASE_OPTION,
    area_filter: List[str] = DATA_FILTER,
    thematic_filter: List[str] = DATA_FILTER,
    traces_filter: List[str] = DATA_FILTER,
    scale_filter: List[str] = DATA_FILTER,
    report: bool = typer.Option(False),
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

    # Only validate a single trace dataset once
    # Means you might have to validate for each area dataset
    # that uses the traces.
    unique_invalids_only = spatial.unique_invalids(invalids=invalids)

    # Validate the invalids
    update_tuples = spatial.validate_invalids(invalids=unique_invalids_only)

    # Exit with error code 1 if there's errors in updating the database.csv
    database_error = False

    assert len(update_tuples) == len(unique_invalids_only)
    # Iterate over results
    for update_tuple, invalid in zip(update_tuples, unique_invalids_only):

        try:
            # Update Organizer database.csv
            organizer.update(
                area_name=invalid.area_path.stem,
                update_values=update_tuple.update_values,
            )

            # Write the database.csv
            repo.write_database_csv(path=database, database=organizer.database)
        except Exception as exc:

            # Error in updating database.csv
            database_error = True

            # Log exception
            logging.error(
                f"Error when updating Organizer database.csv: {exc}\n"
                f"update_tuple: {update_tuple}\n"
                f"invalid: {invalid}\n"
            )

        if report:
            pprint(update_tuple)

    if database_error:

        # Exit with error code 1 (not successful)
        raise typer.Exit(code=1)


@app.command()
def organize(
    database: Path = DATABASE_OPTION,
    simulate: bool = typer.Option(False),
    report: bool = typer.Option(True),
):
    """
    Organize repo.
    """
    organizer = Organizer(database=repo.read_database_csv(path=database))

    move_descriptions = organizer.organize(simulate=simulate)

    if report:
        pprint("\n".join(move_descriptions))

    if not simulate:
        organizer.check()


@app.command()
def check(
    database: Path = DATABASE_OPTION,
):
    """
    Check repo.
    """
    organizer = Organizer(database=repo.read_database_csv(path=database))

    organizer.check()


@app.command()
def init(path: Path = typer.Argument(rules.DATABASE_CSV)):
    """
    Initialize tracerepo in current directory.
    """
    df = repo.scaffold()
    repo.write_database_csv(path=path, database=df)


def export_data(destination: Path, driver: str, database: Path):
    """
    Export datasets into another format.
    """
    # Initialize Organizer
    organizer = Organizer(database=repo.read_database_csv(path=database))

    # Query for all datasets
    trace_tuples = organizer.query()

    # Compile the export destination folder
    export_destination = utils.compile_export_dir(driver)

    convert_paths = spatial.convert_trace_tuples(
        trace_tuples, export_destination=export_destination, driver=driver
    )

    spatial.save_converted_paths(
        trace_tuples=trace_tuples,
        convert_paths=convert_paths,
        driver=driver,
        destination=destination,
    )


@app.command()
def export(
    destination: Path = typer.Argument(".", file_okay=False),
    driver: str = typer.Option("ESRI Shapefile"),
    database: Path = DATABASE_OPTION,
):
    """
    Export datasets into another format from command line.
    """
    export_data(destination=destination, driver=driver, database=database)
