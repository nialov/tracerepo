"""
Tests for rules.py.
"""

from hypothesis import given

import tests


@given(tests.name_regex(geom_type=None))
def test_filename_regex(name: str):
    """
    Test tests name_regex.
    """
    assert isinstance(name, str)
    assert len(name) >= 3
