"""
Tests for organize.py.
"""
from tracerepo.organize import Organizer
import pytest
from pandas.testing import assert_frame_equal
import tracerepo.repo as repo
import tracerepo.rules as rules
import os
from pathlib import Path
from typing import List, Any
import pandas as pd
from itertools import combinations
from hypothesis import given, example
from hypothesis.strategies import sampled_from, lists, none
from tempfile import tempdir
from contextlib import contextmanager
from pytest import TempPathFactory


@contextmanager
def setup_scaffold_context(tmp_path: Path):
    """
    Set up a repo scaffold at a temporary directory.
    """
    current_dir = Path(".").resolve()
    os.chdir(tmp_path)
    try:
        yield repo.scaffold()
    finally:
        os.chdir(current_dir)


def df_data():
    """
    Set up a row of some database df data.
    """
    traces_name = "some_traces"
    area_name = "some_area"
    thematic = "some"
    scale = "wide"
    area_shape = "circle"
    empty = "false"
    validated = "false"

    return traces_name, area_name, thematic, scale, area_shape, empty, validated


def df_with_row(
    df,
):
    """
    Set up a database df with a row of data.
    """
    traces_name, area_name, thematic, scale, area_shape, empty, validated = df_data()

    row = {
        # rules.ColumnNames.AREA.value: area_name,
        rules.ColumnNames.TRACES.value: traces_name,
        rules.ColumnNames.THEMATIC.value: thematic,
        rules.ColumnNames.SCALE.value: scale,
        rules.ColumnNames.AREA_SHAPE.value: area_shape,
        rules.ColumnNames.EMPTY.value: empty,
        rules.ColumnNames.VALIDATED.value: validated,
    }

    srs = pd.Series(data=row.values(), index=row.keys(), name=area_name)

    df = df.append(srs)

    traces_path = (
        Path(rules.FolderNames.UNORGANIZED.value) / f"{traces_name}.{rules.FILETYPE}"
    )
    area_path = (
        Path(rules.FolderNames.UNORGANIZED.value) / f"{area_name}.{rules.FILETYPE}"
    )

    return df, traces_path, area_path


def setup_df_with_traces_and_area(df):
    """
    Set up df with associated data.
    """
    df, traces_path, area_path = df_with_row(df=df)

    traces_path.touch()
    area_path.touch()

    return df


@pytest.fixture
def df_with_traces_and_area(tmp_path):
    """
    Populate scaffold and database with a single traces+area row.

    traces and area file will be in unorganized.
    """
    with setup_scaffold_context(tmp_path) as df:

        df = setup_df_with_traces_and_area(df=df)

        yield df


@pytest.fixture
def organizer_unorganized(df_with_traces_and_area):
    """
    Fix up an Organizer with files in unorganized.
    """
    df = df_with_traces_and_area
    yield Organizer(database=df)


@pytest.fixture(scope="module")
def organizer_organized(tmp_path_factory: TempPathFactory):
    """
    Fix up an Organizer with files organized.
    """
    tmp_path = tmp_path_factory.mktemp(basename="organizer_organized")
    with setup_scaffold_context(tmp_path) as df:

        df = setup_df_with_traces_and_area(df=df)
        organizer = Organizer(database=df)
        organizer.organize(simulate=False)
        organizer.check()
        yield organizer


def test_organizer_check_unorganized(organizer_unorganized):
    """
    Tests for Organizer check.
    """
    try:
        # Should have no existing files in data folder
        organizer_unorganized.check()
        assert False
    except FileNotFoundError:
        pass


def test_organizer_organize_unorganized(organizer_unorganized: Organizer):
    """
    Tests for Organizer check.
    """
    file_count = len(list(organizer_unorganized.unorganized_folder.glob("*")))
    assert file_count > 0
    move_descriptions = organizer_unorganized.organize(simulate=True)
    assert len(move_descriptions) > 0
    after_simulation = len(list(organizer_unorganized.unorganized_folder.glob("*")))
    assert file_count == after_simulation
    move_descriptions = organizer_unorganized.organize(simulate=False)
    assert len(move_descriptions) > 0
    after_simulation = len(list(organizer_unorganized.unorganized_folder.glob("*")))
    assert after_simulation == 0


def test_organizer_columns_unorganized(organizer_unorganized: Organizer):
    """
    Test Organizer columns.
    """
    columns = organizer_unorganized.columns
    assert isinstance(columns, dict)
    assert "some_traces" in columns[rules.ColumnNames.TRACES.value]


def test_organizer_organize_then_check_unorganized(organizer_unorganized: Organizer):
    """
    Tests for Organizer check.
    """
    test_organizer_organize_unorganized(organizer_unorganized=organizer_unorganized)

    organizer_unorganized.check()


def test_organizer_unorganized_unorganized(organizer_unorganized: Organizer):
    """
    Tests for Organizer unorganized.
    """
    unorganized = organizer_unorganized.unorganized
    assert len(unorganized) == 2


def query_strategy():
    """
    Create organizer query test strategy.
    """
    traces_name, area_name, thematic, scale, *_ = df_data()

    possible_areas = [area_name]
    possible_traces = [traces_name]
    possible_thematic = [thematic]
    possible_scale = [scale]
    possible_area_shape = [enum for enum in rules.AreaShapes] + [None]
    possible_empty = [enum for enum in rules.BooleanChoices] + [None]
    possible_validated = [enum for enum in rules.BooleanChoices] + [None]
    possible_geometry = [rules.ColumnNames.AREA, rules.ColumnNames.TRACES] + [None]

    return dict(
        area=lists(sampled_from(possible_areas), unique=True),
        traces=lists(sampled_from(possible_traces), unique=True),
        thematic=lists(sampled_from(possible_thematic), unique=True),
        scale=lists(sampled_from(possible_scale), unique=True),
        area_shape=sampled_from(possible_area_shape),
        empty=sampled_from(possible_empty),
        validated=sampled_from(possible_validated),
        geometry=sampled_from(possible_geometry),
    )


def query_example(geometry, assumed_result: int):
    """
    Create query example.
    """
    traces_name, area_name, thematic, scale, area_shape, empty, validated = df_data()

    assert area_shape == rules.AreaShapes.CIRCLE.value
    assert empty == rules.BooleanChoices.FALSE.value
    assert validated == rules.BooleanChoices.FALSE.value

    return dict(
        area=[area_name],
        traces=[traces_name],
        thematic=[thematic],
        scale=[scale],
        area_shape=rules.AreaShapes.CIRCLE,
        empty=rules.BooleanChoices.FALSE,
        validated=rules.BooleanChoices.FALSE,
        assumed_result=assumed_result,
        geometry=geometry,
    )


@example(**query_example(geometry=None, assumed_result=2))
@example(**query_example(geometry=rules.Geometry.AREAS, assumed_result=1))
@example(**query_example(geometry=rules.Geometry.TRACES, assumed_result=1))
@given(**query_strategy(), assumed_result=none())
def test_organizer_query_organized(
    organizer_organized: Organizer,
    area,
    traces,
    thematic,
    scale,
    area_shape,
    empty,
    validated,
    geometry,
    assumed_result,
):
    """
    Tests for Organizer query organized.
    """
    query_results = organizer_organized.query(
        area=area,
        traces=traces,
        thematic=thematic,
        scale=scale,
        area_shape=area_shape,
        empty=empty,
        validated=validated,
        geometry=geometry,
    )
    assert isinstance(query_results, list)
    assert all([isinstance(val, Path) for val in query_results])
    assert all([val.exists() for val in query_results])

    if isinstance(assumed_result, int):
        assert len(query_results) == assumed_result


def test_organizer_update_organized(organizer_unorganized: Organizer):
    """
    Test Organizer update.
    """
    _, area_name, _, _, *_ = df_data()
    current_db = organizer_unorganized.database.copy()
    assert organizer_unorganized.columns[rules.ColumnNames.EMPTY.value][0] == "false"
    organizer_unorganized.update(
        area_name=area_name, update_values={rules.ColumnNames.EMPTY: "true"}
    )
    assert organizer_unorganized.columns[rules.ColumnNames.EMPTY.value][0] == "true"
    try:
        assert_frame_equal(current_db, organizer_unorganized.database)
    except AssertionError:
        pass

    try:
        organizer_unorganized.check()
    except FileNotFoundError:
        pass

    assert len(organizer_unorganized.unorganized) == 2
