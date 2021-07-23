"""
Command line api for tracerepo.
"""

import logging
from pathlib import Path
from pprint import pprint
from shutil import rmtree
from typing import List, Sequence

import pandera as pa
import typer

from tracerepo import repo, rules, spatial, trace_schema, utils
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


def logging_level(level: str):
    """
    Make logging level string.
    """
    return f"Set logging level to {level}."


@app.callback()
def app_callback(
    verbose: bool = typer.Option(False, help=logging_level("INFO")),
    debug: bool = typer.Option(False, help=logging_level("DEBUG")),
):
    """
    Run before app execution.

    Sets logging level.
    """
    logging_level_str = "WARNING"
    logging.basicConfig(level=logging.WARNING, force=True)
    if verbose and not debug:
        logging.basicConfig(level=logging.INFO, force=True)
        logging_level_str = "INFO"
    if debug:
        logging.basicConfig(level=logging.DEBUG, force=True)
        logging_level_str = "DEBUG"
    logging.info(f"Logging verbosity set to {logging_level_str}")


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
        validity=[rules.ValidationResults.INVALID, rules.ValidationResults.EMPTY],
    )

    # Only validate a single trace dataset once
    # Means you might have to validate for each area dataset
    # that uses the traces.
    unique_invalids_only = spatial.unique_invalids(invalids=invalids)

    # Validate the invalids
    update_tuples = spatial.validate_invalids(invalids=unique_invalids_only)

    # Exit with error code 1 if there's errors in updating the database.csv
    database_error, write_error = False, False

    assert len(update_tuples) == len(unique_invalids_only)
    # Iterate over results
    for update_tuple, invalid in zip(update_tuples, unique_invalids_only):

        pandera_pass = True
        try:
            trace_schema.traces_schema().validate(update_tuple.traces, lazy=True)
        except pa.errors.SchemaErrors as exc:
            pandera_pass = False
            print(exc.failure_cases)
        if not pandera_pass:
            update_tuple.update_values = {
                rules.ColumnNames.VALIDITY: rules.ValidationResults.INVALID.value
            }
        try:
            # Update Organizer database.csv
            organizer.update(
                area_name=invalid.area_path.stem,
                update_values=update_tuple.update_values,
            )

            # Write the database.csv
            repo.write_database_csv(path=database, database=organizer.database)

        except Exception:

            # Error in updating database.csv
            database_error = True

            # Log exception
            logging.error(
                f"Error when updating Organizer database.csv.\n"
                f"update_tuple: {update_tuple}\n"
                f"invalid: {invalid}\n",
                exc_info=True,
            )

        if report:
            pprint(update_tuple)

    if database_error or write_error:

        # Exit with error code 1 (not successful)
        raise typer.Exit(code=1)


@app.command()
def format_geojson(
    database: Path = DATABASE_OPTION,
):
    """
    Format all dataset GeoJSON from cli.
    """
    format_repo_geojson(database=database)


def format_repo_geojson(
    database: Path,
):
    """
    Format all dataset GeoJSON.
    """
    # Initialize Organizer
    organizer = Organizer(database=repo.read_database_csv(path=database))

    # Query for invalid traces
    trace_tuples = organizer.query()

    # Do not load and save same paths multiple times
    formatted_paths = set()

    # Iterate over all area trace tuples

    for trace_tuple in trace_tuples:

        for path in (trace_tuple.traces_path, trace_tuple.area_path):
            if path in formatted_paths:
                continue

            # Read GeoDataFrame
            gdf = spatial.read_geofile(path)

            # Write as GeoJSON
            utils.write_geodata(gdf=gdf, path=path)

            # Add formatted paths
            formatted_paths.add(path)


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


def export_data(
    destination: Path,
    driver: str,
    database: Path,
    area_filter: Sequence[str] = (),
    thematic_filter: Sequence[str] = (),
    traces_filter: Sequence[str] = (),
    scale_filter: Sequence[str] = (),
    overwrite: bool = True,
) -> str:
    """
    Export datasets into another format.
    """
    # Initialize Organizer
    organizer = Organizer(database=repo.read_database_csv(path=database))

    # Query for datasets based on filters
    # By default filters are empty i.e. all are selected
    trace_tuples = organizer.query(
        area=area_filter,
        thematic=thematic_filter,
        traces=traces_filter,
        scale=scale_filter,
    )

    # Resolve the export destination folder
    export_destination = utils.compile_export_dir(driver)

    # Does destination already exist
    destination_exists = Path(export_destination).exists()

    if destination_exists and overwrite:

        logging.info(f"Removing directory ({export_destination}) recursively.")
        rmtree(export_destination)

    elif destination_exists:
        raise FileExistsError(
            f"Directory already exists at {export_destination} "
            "and overwrite is not allowed (--no-overwrite given)."
        )

    # Compile from trace tuples into paths
    # TODO: Could just return TraceTuples again as they are just paths...
    convert_paths = spatial.convert_trace_tuples(
        trace_tuples, export_destination=export_destination, driver=driver
    )

    # Export to disk
    spatial.save_converted_paths(
        trace_tuples=trace_tuples,
        convert_paths=convert_paths,
        driver=driver,
        destination=destination,
    )

    return export_destination


@app.command()
def export(
    destination: Path = typer.Argument(".", file_okay=False),
    driver: str = typer.Option("ESRI Shapefile"),
    database: Path = DATABASE_OPTION,
    area_filter: List[str] = DATA_FILTER,
    thematic_filter: List[str] = DATA_FILTER,
    traces_filter: List[str] = DATA_FILTER,
    scale_filter: List[str] = DATA_FILTER,
    overwrite: bool = typer.Option(True),
):
    """
    Export datasets into another format from command line.
    """
    export_destination = export_data(
        destination=destination,
        driver=driver,
        database=database,
        area_filter=area_filter,
        thematic_filter=thematic_filter,
        traces_filter=traces_filter,
        scale_filter=scale_filter,
        overwrite=overwrite,
    )

    print(f"Saved datasets to {export_destination} with driver {driver}.")
