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
from tracerepo.organize import Organizer

app = typer.Typer()


@app.command()
def validate(
    database: Path = typer.Option(
        rules.DATABASE_CSV,
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

    # Validate the invalids
    update_tuples = spatial.validate_invalids(invalids=invalids)

    # Exit with error code 1 if there's errors in updating the database.csv
    database_error = False

    # Iterate over results
    for update_tuple, invalid in zip(update_tuples, invalids):

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

    if database_error:

        # Exit with error code 1 (not successful)
        raise typer.Exit(code=1)


@app.command()
def organize(
    database: Path = typer.Option(
        rules.DATABASE_CSV,
        exists=True,
        file_okay=True,
        dir_okay=False,
        writable=True,
        readable=True,
    ),
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
