"""
Command line api for tracerepo.
"""

import logging
from pathlib import Path
from shutil import rmtree
from typing import List, Optional, Sequence

import typer
from json5 import loads
from nialog.logger import setup_module_logging
from rich.console import Console
from rich.table import Table

from tracerepo import repo, rules, spatial, utils
from tracerepo.organize import Organizer

app = typer.Typer()

DATABASE_OPTION = typer.Option(
    rules.DATABASE_CSV,
)

TRACEREPOSITORY_PATH_OPTION = typer.Option(
    ".",
    exists=True,
    file_okay=False,
    dir_okay=True,
)

DATA_FILTER = typer.Option(default=())


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
    Use tracerepo to manage and validate fracture & lineament trace data.
    """
    logging_level_int = logging.WARNING
    # logging.basicConfig(level=logging.WARNING, force=True)
    if verbose:
        # logging.basicConfig(level=logging.INFO, force=True)
        logging_level_int = logging.INFO
    if debug:
        # logging.basicConfig(level=logging.DEBUG, force=True)
        logging_level_int = logging.DEBUG
    setup_module_logging(logging_level_int=logging_level_int)
    logging.info(
        "Logging verbosity set and nialog initialized.",
        extra=dict(logging_level_int=logging_level_int, verbose=verbose, debug=debug),
    )


def load_metadata_from_json(metadata_json_path: Path) -> rules.Metadata:
    """
    Load and parse json metadata of trace columns.
    """
    # Load metadata of column restrictions
    loaded_metadata = loads(metadata_json_path.read_text())
    if not isinstance(loaded_metadata, dict):
        raise TypeError(
            f"Expected {metadata_json_path} to parse as a dict."
            f" Got {type(loaded_metadata)}."
        )
    return rules.Metadata(**loaded_metadata, filepath=metadata_json_path)


def report_validation_table(invalids: Sequence[utils.TraceTuple]) -> Table:
    """
    Generate a rich Table from invalids.
    """
    table = Table(title="Selected Validation Targets")
    table.add_column("Traces", header_style="bold", style="bold green")
    table.add_column("Area", header_style="bold", style="bold green")
    table.add_column("Snap Threshold", header_style="bold", style="bold blue")
    table.add_column("Current Validity", header_style="bold", style="bold blue")

    for trace_tuple in invalids:
        table.add_row(
            trace_tuple.traces_path.name,
            trace_tuple.area_path.name,
            str(trace_tuple.snap_threshold),
            trace_tuple.validity,
        )
    return table


@app.command()
def validate(
    tracerepository_path: Path = TRACEREPOSITORY_PATH_OPTION,
    database_name: str = DATABASE_OPTION,
    area_filter: List[str] = DATA_FILTER,
    thematic_filter: List[str] = DATA_FILTER,
    traces_filter: List[str] = DATA_FILTER,
    scale_filter: List[str] = DATA_FILTER,
    report: bool = typer.Option(False),
    # report_directory: Optional[Path] = typer.Option(rules.PathNames.REPORTS.value),
    report_directory: Optional[Path] = typer.Option(
        None,
        help=(
            "Defaults to directory in tracerepository_path "
            f"with name: {rules.PathNames.REPORTS.value}."
        ),
    ),
    metadata_json: Path = typer.Option(
        rules.PathNames.METADATA.value, exists=True, dir_okay=False
    ),
):
    """
    Validate trace datasets.

    Only validates if the dataset has been marked as invalid in database.csv.
    """
    console = Console()
    database = tracerepository_path / database_name
    # Initialize Organizer
    organizer = Organizer(
        tracerepository_path=tracerepository_path,
        database=repo.read_database_csv(path=database),
    )

    # Load metadata of traces column restrictions
    metadata = load_metadata_from_json(metadata_json_path=metadata_json)

    # Query for invalid traces
    invalids = organizer.query(
        area=area_filter,
        thematic=thematic_filter,
        traces=traces_filter,
        scale=scale_filter,
        validity=[
            rules.ValidationResults.INVALID,
            rules.ValidationResults.EMPTY,
            rules.ValidationResults.UNFIT,
        ],
    )

    # Only validate a single trace dataset once
    # Means you might have to validate for each area dataset
    # that uses the traces.
    unique_invalids_only = spatial.unique_invalids(invalids=invalids)

    # Report which data are validated
    if report:
        console.print(report_validation_table(unique_invalids_only))

    # Validate the invalids
    update_tuples = spatial.validate_invalids(invalids=unique_invalids_only)

    # Exit with error code 1 if there's errors in updating the database.csv
    database_error, write_error = False, False

    assert len(update_tuples) == len(unique_invalids_only)
    # Iterate over results

    for update_tuple, invalid in zip(update_tuples, unique_invalids_only):

        # Validate and gather pandera reporting
        pandera_update_values, pandera_report = utils.pandera_reporting(
            update_tuple=update_tuple,
            metadata=metadata,
        )

        # If the geodataset is otherwise valid but fails pandera checks it will
        # be marked as unfit
        if len(pandera_update_values) > 0:
            update_tuple.update_values = pandera_update_values
        try:
            # Update Organizer database.csv
            organizer.update(
                area_name=invalid.area_path.stem,
                update_values=update_tuple.update_values,
            )

            # Write the database.csv
            repo.write_database_csv(path=database, database=organizer.database)

        except Exception:

            # Error in updating or writing database.csv
            database_error = True

            # Log exception
            logging.error(
                f"Error when updating or writing Organizer database.csv.\n"
                f"update_tuple: {update_tuple}\n"
                f"invalid: {invalid}\n",
                exc_info=True,
            )

        if report:
            # UpdateTuple(area_name='finland_em_lineaments_1_500000_area',
            # update_values={<ColumnNames.VALIDITY: 'validity'>: 'unfit'},
            # traces_path=PosixPath('tracerepository_data/finland/traces/500000/
            # finland_em_lineaments_1_500000_traces.geojson'),
            # error=False)
            typer.echo("")
            typer.secho(
                f"Validation results for area: { update_tuple.area_name }",
                fg=typer.colors.BRIGHT_WHITE,
            )
            typer.secho(
                f"(Traces: {update_tuple.traces_path.name})", fg=typer.colors.CYAN
            )

            validation_result = update_tuple.update_values[rules.ColumnNames.VALIDITY]
            typer.secho(
                f"Validation result: {validation_result}",
                fg=rules.ValidationResults.color_dict()[validation_result],
            )
            # if update_tuple.update_values[rules.ColumnNames.VALIDITY.value]
            # == rules.ValidationResults.CRITICAL:

            if not pandera_report.empty:
                report_directory = (
                    tracerepository_path / Path(rules.PathNames.REPORTS.value)
                    if report_directory is None
                    else report_directory
                )
                str_report = utils.report_pandera_errors(
                    pandera_report=pandera_report,
                    report_directory=report_directory,
                    area_name=update_tuple.area_name,
                )
                typer.echo(str_report)

    if database_error or write_error:

        # Exit with error code 1 (not successful)
        raise typer.Exit(code=1)


@app.command()
def format_geojson(
    tracerepository_path: Path = TRACEREPOSITORY_PATH_OPTION,
    database_name: str = DATABASE_OPTION,
):
    """
    Format all dataset GeoJSON from cli.
    """
    database = tracerepository_path / database_name
    format_repo_geojson(database=database, tracerepository_path=tracerepository_path)


def format_repo_geojson(tracerepository_path: Path, database: Path):
    """
    Format all dataset GeoJSON.
    """
    # Initialize Organizer
    organizer = Organizer(
        tracerepository_path=tracerepository_path,
        database=repo.read_database_csv(path=database),
    )

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
    tracerepository_path: Path = TRACEREPOSITORY_PATH_OPTION,
    database_name: str = DATABASE_OPTION,
    simulate: bool = typer.Option(False),
    report: bool = typer.Option(True),
):
    """
    Organize repo.
    """
    database = tracerepository_path / database_name
    organizer = Organizer(
        tracerepository_path=tracerepository_path,
        database=repo.read_database_csv(path=database),
    )

    move_descriptions = organizer.organize(simulate=simulate)

    if report:
        typer.echo("\n".join(move_descriptions))

    if not simulate:
        organizer.check()


@app.command()
def check(
    tracerepository_path: Path = TRACEREPOSITORY_PATH_OPTION,
    database_name: str = DATABASE_OPTION,
):
    """
    Check repo.
    """
    database = tracerepository_path / database_name
    organizer = Organizer(
        tracerepository_path=tracerepository_path,
        database=repo.read_database_csv(path=database),
    )

    organizer.check()


@app.command()
def init(
    tracerepository_path: Path = TRACEREPOSITORY_PATH_OPTION,
    database_name: str = DATABASE_OPTION,
):
    """
    Initialize tracerepo in current directory.
    """
    df = repo.scaffold(tracerepository_path=tracerepository_path)
    repo.write_database_csv(path=tracerepository_path / database_name, database=df)


def export_data(
    destination: Path,
    driver: str,
    database: Path,
    tracerepository_path: Path,
    area_filter: Sequence[str] = (),
    thematic_filter: Sequence[str] = (),
    traces_filter: Sequence[str] = (),
    scale_filter: Sequence[str] = (),
    overwrite: bool = True,
) -> Path:
    """
    Export datasets into another format.
    """
    assert destination.is_dir()
    # Initialize Organizer
    organizer = Organizer(
        tracerepository_path=tracerepository_path,
        database=repo.read_database_csv(path=database),
    )

    # Query for datasets based on filters
    # By default filters are empty i.e. all are selected
    trace_tuples = organizer.query(
        area=area_filter,
        thematic=thematic_filter,
        traces=traces_filter,
        scale=scale_filter,
    )

    # Resolve the export destination folder
    export_destination_dir = utils.compile_export_dir(driver)
    export_destination_path = destination / export_destination_dir

    # Does destination already exist
    destination_exists = export_destination_path.exists()

    if destination_exists and overwrite:

        logging.info(f"Removing directory ({export_destination_path}) recursively.")
        rmtree(export_destination_path)

    elif destination_exists:
        raise FileExistsError(
            f"Directory already exists at {export_destination_path} "
            "and overwrite is not allowed (--no-overwrite given)."
        )

    # Compile from trace tuples into paths
    dest_trace_tuples = spatial.convert_trace_tuples(
        trace_tuples, export_destination=export_destination_dir, driver=driver
    )

    # Export to disk
    spatial.save_converted_paths(
        src_trace_tuples=trace_tuples,
        dest_trace_tuples=dest_trace_tuples,
        driver=driver,
        destination=destination,
    )

    return export_destination_path


@app.command()
def export(
    destination: Path = typer.Argument(".", file_okay=False),
    driver: str = typer.Option("ESRI Shapefile"),
    tracerepository_path: Path = TRACEREPOSITORY_PATH_OPTION,
    database_name: str = DATABASE_OPTION,
    area_filter: List[str] = DATA_FILTER,
    thematic_filter: List[str] = DATA_FILTER,
    traces_filter: List[str] = DATA_FILTER,
    scale_filter: List[str] = DATA_FILTER,
    overwrite: bool = typer.Option(True),
):
    """
    Export datasets into another format from command line.
    """
    database = tracerepository_path / database_name
    export_destination = export_data(
        tracerepository_path=tracerepository_path,
        destination=destination,
        driver=driver,
        database=database,
        area_filter=area_filter,
        thematic_filter=thematic_filter,
        traces_filter=traces_filter,
        scale_filter=scale_filter,
        overwrite=overwrite,
    )

    typer.echo(f"Saved datasets to {export_destination} with driver {driver}.")
