"""
Tests for cli.py.
"""

import geopandas as gpd
import pytest
from click.testing import Result
from hypothesis import given, settings
from typer.testing import CliRunner

import tests
import tracerepo.repo as repo
import tracerepo.rules as rules
from tracerepo.cli import app

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
