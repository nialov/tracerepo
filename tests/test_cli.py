"""
Tests for cli.py.
"""

import os
from pathlib import Path

import geopandas as gpd
import pytest
from click.testing import Result
from hypothesis import given, settings
from typer.testing import CliRunner

import tests
import tracerepo.repo as repo
import tracerepo.rules as rules
import tracerepo.spatial as spatial
import tracerepo.utils as utils
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


@settings(max_examples=5, deadline=5000)
@given(
    database=tests.database_schema_strategy(),
)
@pytest.mark.parametrize(
    "trace_gdf,assume_error",
    [
        (tests.kb11_traces_cut, rules.ValidationResults.VALID),
        (tests.kb11_traces_cut_dislocated, rules.ValidationResults.EMPTY),
    ],
)
def test_cli_validate_exec(trace_gdf, assume_error, database, tmp_path_factory):
    """
    Test tracerepo validate command with a set up of invalidated data.
    """
    area_gdf: gpd.GeoDataFrame = tests.kb11_area
    tmp_path = tmp_path_factory.mktemp(basename="test_cli_validate_exec", numbered=True)

    with tests.setup_scaffold_context(tmp_path):

        organizer = tests.set_up_repo_with_invalids_organized(
            database=database, trace_gdf=trace_gdf, area_gdf=area_gdf
        )

        repo.write_database_csv(
            path=tmp_path / rules.DATABASE_CSV, database=organizer.database
        )

        result = runner.invoke(app=app, args=["validate"])

        tests.click_error_print(result)


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
            f"{rules.FolderNames.DATA.value}/ahvenanmaa/area/20m/getaberget_20m_1_1_area.geojson"
        )
        traces_path = Path(
            f"{rules.FolderNames.DATA.value}/ahvenanmaa/traces/20m/getaberget_20m_1_traces.geojson"
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
            f"{rules.FolderNames.DATA.value}/ahvenanmaa/area/20m/getaberget_20m_1_1_area.geojson"
        )
        traces_path = Path(
            f"{rules.FolderNames.DATA.value}/ahvenanmaa/traces/20m/getaberget_20m_1_traces.geojson"
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
