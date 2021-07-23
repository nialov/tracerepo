"""
Tests for schema checks.
"""
import numpy as np
import pytest

import tests
from tracerepo import schema_checks


@pytest.mark.parametrize("value,will_fail", tests.test_data_source_regex_check_params())
def test_data_source_regex_check(value: str, will_fail: bool):
    """
    Test data_source_regex_check.
    """
    result = schema_checks.data_source_regex_check(value)

    assert result or will_fail


@pytest.mark.parametrize("value,will_fail", tests.test_date_datetime_check_params())
def test_date_datetime_check(value: str, will_fail: bool):
    """
    Test date_datetime_check.
    """
    value_as_datetime = np.datetime64(value)

    result = schema_checks.date_datetime_check(value_as_datetime)

    assert result or will_fail
