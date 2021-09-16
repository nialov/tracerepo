"""
Tests for organize.py.
"""
from pathlib import Path
from typing import Sequence

import pytest
from hypothesis import example, given
from hypothesis.strategies import lists, none, sampled_from
from pandas.testing import assert_frame_equal
from pytest import TempPathFactory

import tests
from tracerepo import repo, rules, utils
from tracerepo.organize import Organizer


def setup_df_with_traces_and_area(df, tracerepository_path: Path):
    """
    Set up df with associated data.
    """
    df, traces_path, area_path = tests.df_with_row(df=df)

    (tracerepository_path / traces_path).touch()
    (tracerepository_path / area_path).touch()

    return df


@pytest.fixture
def df_with_traces_and_area(tmp_path):
    """
    Populate scaffold and database with a single traces+area row.

    traces and area file will be in unorganized.
    """
    df = setup_df_with_traces_and_area(
        repo.scaffold(tmp_path), tracerepository_path=tmp_path
    )
    yield df


@pytest.fixture
def organizer_unorganized(tmp_path):
    """
    Fix up an Organizer with files in unorganized.
    """
    df = setup_df_with_traces_and_area(
        repo.scaffold(tracerepository_path=tmp_path), tracerepository_path=tmp_path
    )
    repo.scaffold(tmp_path)
    yield Organizer(database=df, tracerepository_path=tmp_path)


@pytest.fixture(scope="module")
def organizer_organized(tmp_path_factory: TempPathFactory):
    """
    Fix up an Organizer with files organized.
    """
    tmp_path = tmp_path_factory.mktemp(basename="organizer_organized")
    df = setup_df_with_traces_and_area(
        repo.scaffold(tracerepository_path=tmp_path), tracerepository_path=tmp_path
    )
    # with tests.setup_scaffold_context(tmp_path) as df:

    #     df = setup_df_with_traces_and_area(df=df)
    organizer = Organizer(database=df, tracerepository_path=tmp_path)
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
    traces_name, area_name, thematic, scale, *_ = tests.df_data()

    possible_areas = [area_name]
    possible_traces = [traces_name]
    possible_thematic = [thematic]
    possible_scale = [scale]
    possible_area_shape = list(rules.AreaShapes)
    possible_validity = list(rules.ValidationResults)

    return dict(
        area=lists(sampled_from(possible_areas), unique=True),
        traces=lists(sampled_from(possible_traces), unique=True),
        thematic=lists(sampled_from(possible_thematic), unique=True),
        scale=lists(sampled_from(possible_scale), unique=True),
        area_shape=sampled_from(possible_area_shape),
        validity=sampled_from(possible_validity),
    )


def query_example(assumed_result: int):
    """
    Create query example.
    """
    (
        traces_name,
        area_name,
        thematic,
        scale,
        area_shape,
        validity,
        snap_threshold,
    ) = tests.df_data()

    assert isinstance(validity, str)
    assert isinstance(snap_threshold, float)

    assert area_shape == rules.AreaShapes.CIRCLE.value

    return dict(
        area=[area_name],
        traces=[traces_name],
        thematic=[thematic],
        scale=[scale],
        area_shape=rules.AreaShapes.CIRCLE,
        validity=rules.ValidationResults.INVALID,
        assumed_result=assumed_result,
    )


@example(**query_example(assumed_result=1))
@example(**query_example(assumed_result=1))
@example(**query_example(assumed_result=1))
@given(**query_strategy(), assumed_result=none())
def test_organizer_query_organized(
    organizer_organized: Organizer,
    area,
    traces,
    thematic,
    scale,
    area_shape,
    validity,
    assumed_result,
):
    """
    Tests for Organizer query organized.
    """
    assert None not in (area_shape, validity)
    query_results = organizer_organized.query(
        area=area,
        traces=traces,
        thematic=thematic,
        scale=scale,
        area_shape=[area_shape],
        validity=[validity],
    )
    assert isinstance(query_results, list)

    trace_tuple: utils.TraceTuple
    for trace_tuple in query_results:
        assert isinstance(trace_tuple.snap_threshold, float)
        assert (
            isinstance(trace_tuple.traces_path, Path) or trace_tuple.traces_path is None
        )
        assert isinstance(trace_tuple.area_path, Path) or trace_tuple.area_path is None

    assert all(isinstance(val, utils.TraceTuple) for val in query_results)

    if isinstance(assumed_result, int):
        assert len(query_results) == assumed_result


def test_organizer_update_organized(organizer_unorganized: Organizer):
    """
    Test Organizer update.
    """
    _, area_name, _, _, *_ = tests.df_data()
    current_db = organizer_unorganized.database.copy()
    assert (
        organizer_unorganized.columns[rules.ColumnNames.VALIDITY.value][0]
        == rules.ValidationResults.INVALID.value
    )
    organizer_unorganized.update(
        area_name=area_name,
        update_values={
            rules.ColumnNames.VALIDITY: rules.ValidationResults.CRITICAL.value
        },
    )
    assert (
        organizer_unorganized.columns[rules.ColumnNames.VALIDITY.value][0]
        == rules.ValidationResults.CRITICAL.value
    )
    try:
        assert_frame_equal(current_db, organizer_unorganized.database)
    except AssertionError:
        pass

    try:
        organizer_unorganized.check()
    except FileNotFoundError:
        pass

    assert len(organizer_unorganized.unorganized) == 2


@pytest.mark.parametrize(
    "area_shape_values,validity_values,query_bools,area_shape,validity,assume_result",
    tests.test__filter_enums_params(),
)
def test__filter_enums(
    area_shape_values, validity_values, query_bools, area_shape, validity, assume_result
):
    """
    Test _filter_enums.
    """
    result = Organizer._filter_enums(
        area_shape_values, validity_values, query_bools, area_shape, validity
    )
    assert isinstance(result, Sequence)

    assert result == assume_result
