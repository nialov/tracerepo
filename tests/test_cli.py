"""
Tests for cli.py.
"""

import os
from pathlib import Path
from shutil import rmtree
from warnings import warn

import geopandas as gpd
import pytest
from click.testing import Result
from hypothesis import given, settings
from shapely.geometry import LineString
from typer.testing import CliRunner

import tests
from tracerepo import repo, rules, spatial, utils
from tracerepo.cli import app, export_data, format_geojson

runner = CliRunner()


@pytest.mark.parametrize("subcommand", ["", "validate", "organize"])
def test_cli_app_help(subcommand: str):
    """
    Test tracerepo cli help cmd.
    """
    args = ["--help"]
    if len(subcommand) > 0:
        args.insert(0, subcommand)
    assert isinstance(subcommand, str)
    result: Result = runner.invoke(app=app, args=args)

    tests.click_error_print(result=result)


@settings(max_examples=2, deadline=10000)
@given(
    database=tests.database_schema_strategy(),
)
@pytest.mark.parametrize(
    "trace_gdf,assume_error,pandera_valid",
    [
        (tests.kb11_traces_cut, rules.ValidationResults.VALID, True),
        (tests.kb11_traces_cut_invalid_dips(), rules.ValidationResults.INVALID, False),
        (
            gpd.GeoDataFrame(
                {"geometry": [LineString([(10e10, 10e10), (10e10, 1 + 10e10)])]}
            ),
            rules.ValidationResults.EMPTY,
            True,
        ),
    ],
)
@pytest.mark.parametrize(
    "metadata_json",
    [Path("tests/sample_data/metadata_rules.json").absolute()],
)
def test_cli_validate_exec(
    trace_gdf,
    assume_error: rules.ValidationResults,
    pandera_valid: bool,
    database,
    tmp_path_factory,
    metadata_json: Path,
):
    """
    Test tracerepo validate command with a set up of invalidated data.
    """
    area_gdf: gpd.GeoDataFrame = tests.kb11_area
    tmp_path = tmp_path_factory.mktemp(basename="test_cli_validate_exec", numbered=True)

    with tests.setup_scaffold_context(tmp_path):

        organizer = tests.set_up_repo_with_invalids_organized(
            database=database, trace_gdf=trace_gdf, area_gdf=area_gdf
        )
        database_csv_path: Path = tmp_path / rules.DATABASE_CSV
        database_csv_path.unlink(missing_ok=True)
        repo.write_database_csv(path=database_csv_path, database=organizer.database)

        result = runner.invoke(
            app=app, args=["validate", "--report", f"--metadata-json={metadata_json}"]
        )

        reports_path = Path(rules.PathNames.REPORTS.value)
        if not pandera_valid:
            assert reports_path.exists()
            assert reports_path.is_dir()
            assert len(list(reports_path.glob("*.html"))) > 0
            assert "html" in result.output

        tests.click_error_print(result)

        # TODO: Inconsistent results here
        if assume_error.value not in database_csv_path.read_text():
            warn(f"Expected {assume_error.value} to be in {database_csv_path}.")


@settings(max_examples=5, deadline=5000)
@given(
    database=tests.database_schema_strategy(),
)
@pytest.mark.parametrize(
    "trace_gdf,other_args",
    [
        (tests.kb11_traces_cut, ["--report", "--simulate"]),
        (tests.kb11_traces_cut, ["--report"]),
        (tests.kb11_traces_cut, ["--no-report"]),
    ],
)
def test_cli_organize(database, trace_gdf, other_args, tmp_path_factory):
    """
    Test cli_organize click entrypoint.
    """
    area_gdf: gpd.GeoDataFrame = tests.kb11_area
    tmp_path = tmp_path_factory.mktemp(basename="test_cli_organize", numbered=True)

    args = ["organize"] + other_args

    with tests.setup_scaffold_context(tmp_path):

        organizer = tests.set_up_repo_with_invalids_organized(
            database=database, trace_gdf=trace_gdf, area_gdf=area_gdf, organized=False
        )

        repo.write_database_csv(
            path=tmp_path / rules.DATABASE_CSV, database=organizer.database
        )

        result = runner.invoke(app=app, args=args)

        if "--simulate" not in args:
            organizer.check()

        if "--report" in args:
            assert len(result.stdout) > 0
        elif "--no-report" in args:
            assert len(result.stdout) == 0

        tests.click_error_print(result)


@pytest.mark.parametrize("driver", ["ESRI Shapefile", "GPKG"])
def test_export_data(tmp_path, driver):
    """
    Test export_data.
    """
    cur_dir = Path(".").absolute()
    os.chdir(tmp_path)

    try:
        database_lines = (
            "area,traces,thematic,scale,area-shape,validity,snap-threshold",
            (
                "getaberget_20m_1_1_area,getaberget_20m_1_traces,"
                "ahvenanmaa,20m,circle,valid,0.001"
            ),
        )

        database_path = Path("database.csv")
        database_path.write_text("\n".join(database_lines))

        area_path = Path(
            f"{rules.PathNames.DATA.value}"
            "/ahvenanmaa/area/20m/getaberget_20m_1_1_area.geojson"
        )
        traces_path = Path(
            f"{rules.PathNames.DATA.value}"
            "/ahvenanmaa/traces/20m/getaberget_20m_1_traces.geojson"
        )

        assert not area_path.parent.exists()
        assert not traces_path.parent.exists()

        area_path.parent.mkdir(exist_ok=True, parents=True)
        traces_path.parent.mkdir(exist_ok=True, parents=True)

        tests.kb11_traces_cut.to_file(traces_path, driver="GeoJSON")
        tests.kb11_area.to_file(area_path, driver="GeoJSON")

        Path("unorganized").mkdir()

        file_count = len(list(Path(".").iterdir()))

        export_data(Path("."), driver=driver, database=database_path)

        assert len(list(Path(".").iterdir())) > file_count

        export_dir_path = Path(utils.compile_export_dir(driver=driver))
        assert export_dir_path.exists()

        for shp in export_dir_path.rglob(f"*{spatial.DRIVER_EXTENSIONS[driver]}"):
            assert isinstance(gpd.read_file(shp), gpd.GeoDataFrame)

    except Exception:
        os.chdir(cur_dir)
        raise
    finally:
        os.chdir(cur_dir)


def test_format_geojson(tmp_path):
    """
    Test format_geojson.
    """
    cur_dir = Path(".").absolute()
    os.chdir(tmp_path)

    try:
        database_lines = (
            "area,traces,thematic,scale,area-shape,validity,snap-threshold",
            (
                "getaberget_20m_1_1_area,getaberget_20m_1_traces,"
                "ahvenanmaa,20m,circle,valid,0.001"
            ),
        )

        database_path = Path("database.csv")
        database_path.write_text("\n".join(database_lines))

        area_path = Path(
            f"{rules.PathNames.DATA.value}"
            "/ahvenanmaa/area/20m/getaberget_20m_1_1_area.geojson"
        )
        traces_path = Path(
            f"{rules.PathNames.DATA.value}"
            "/ahvenanmaa/traces/20m/getaberget_20m_1_traces.geojson"
        )

        assert not area_path.parent.exists()
        assert not traces_path.parent.exists()

        area_path.parent.mkdir(exist_ok=True, parents=True)
        traces_path.parent.mkdir(exist_ok=True, parents=True)

        tests.kb11_traces_cut.to_file(traces_path, driver="GeoJSON")
        tests.kb11_area.to_file(area_path, driver="GeoJSON")

        original_traces_geojson = traces_path.read_text()
        original_area_geojson = area_path.read_text()

        Path("unorganized").mkdir()

        format_geojson(database=database_path)

        # The original files have unindented geojson by default from
        # gpd.to_file
        assert len(original_traces_geojson) != len(traces_path.read_text())
        assert len(original_area_geojson) != len(area_path.read_text())

    except Exception:
        os.chdir(cur_dir)
        raise
    finally:
        os.chdir(cur_dir)


def test_all_cli():
    """
    Test all cli tools in ready-made tracerepository.
    """
    metadata_json = Path("tests/sample_data/metadata_rules.json").absolute()
    database_csv_path = Path(rules.DATABASE_CSV)
    with tests.change_dir_context(tests.READY_TRACEREPOSITORY_PATH):

        # Read database.csv before execution
        csv_text_before = database_csv_path.read_text()

        # Assert that that what we expect is in database
        assert rules.ValidationResults.VALID.value in csv_text_before
        assert rules.ValidationResults.INVALID.value in csv_text_before
        assert rules.ValidationResults.CRITICAL.value not in csv_text_before

        # Run tracerepo --help
        help_result = runner.invoke(app=app, args=["--help"])
        tests.click_error_print(help_result)

        # Run tracerepo check
        check_result = runner.invoke(app=app, args=["check"])
        tests.click_error_print(check_result)

        # Run tracerepo organize
        organize_result = runner.invoke(app=app, args=["organize"])
        tests.click_error_print(organize_result)

        # Run tracerepo organize
        format_result = runner.invoke(app=app, args=["format-geojson"])
        tests.click_error_print(format_result)

        # Run tracerepo validate
        # Validate kb* and hastholmen infinity traces
        validate_result = runner.invoke(
            app=app,
            args=[
                "validate",
                "--traces-filter=kb",
                "--traces-filter=hastholmen",
                "--report",
                f"--metadata-json={metadata_json}",
            ],
        )

        # Make sure pandera error was caught
        assert "Reported" in validate_result.stdout
        assert "html" in validate_result.stdout
        reports_path = Path(rules.PathNames.REPORTS.value)
        assert reports_path.exists()
        assert len(list(reports_path.glob("*.html"))) > 0
        rmtree(reports_path)

        # Test that there we no changes to database
        csv_text_after = database_csv_path.read_text()
        assert csv_text_after == csv_text_before
        assert rules.ValidationResults.VALID.value in csv_text_after
        assert rules.ValidationResults.INVALID.value in csv_text_after
        assert rules.ValidationResults.CRITICAL.value not in csv_text_after

        # Run tracerepo export
        export_result = runner.invoke(app=app, args=["export"])
        tests.click_error_print(export_result)

        # Find export directory
        for directory in Path(".").glob("data-exported-*"):
            if directory.is_dir():
                # Verify contents
                assert len(list(directory.rglob("*.shp"))) > 0
                rmtree(directory)
