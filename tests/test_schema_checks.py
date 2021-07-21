"""
Tests for schema checks.
"""
import tests
import pytest

from tracerepo import schema_checks


@pytest.mark.parametrize("value,will_fail", tests.test_data_source_regex_check_params())
def test_data_source_regex_check(value: str, will_fail: bool):
    """
    Test data_source_regex_check.
    """
    result = schema_checks.data_source_regex_check(value)

    assert result or will_fail
