"""
Trace schema checks used in validation.
"""
import re
from typing import Any, Dict, Tuple

import numpy as np

LINEAMENT_ID_PATTERN_UNFORMATTED = r"^({})_\w\d+$"


def pattern_matcher(value: str, pattern_str: str) -> bool:
    """
    Check if string matches regex pattern.
    """
    pattern = re.compile(pattern_str)
    try:
        pattern_match = pattern.match(value)
    except TypeError:
        return False
    return pattern_match is not None


def named_priority_check(value: Any, named_priorities: Dict[str, int], separator: str):
    """
    Check that value matches a predetermined name and priority order of names.
    """
    if len(value) == 0 or not isinstance(value, str):
        return False
    # Check if value matches one of the named
    if value in named_priorities:
        return True
    # Check if any of the named are actually in the value string
    if (not any(name in value for name in named_priorities)) or separator not in value:
        return False
    # Check if separator is within the value string
    # if separator not in value:
    #     return False
    # Split by separator
    split_value = value.split(sep=separator)

    # if not all(val in named_priorities for val in split_value):
    #     return False

    previous = 0
    for part in split_value:
        # Check that all in split value are in named_priorities
        if part not in named_priorities:
            return False
        current = named_priorities[part]
        if current > previous:
            previous = current
        else:
            return False
    return True


def date_datetime_check(raw_value: Any) -> bool:
    """
    Check that date column has valid datetime values.

    >>> date_datetime_check(np.datetime64("2021-07-23"))
    True

    >>> date_datetime_check(np.datetime64("1995-07-23"))
    False

    """
    try:
        if not isinstance(raw_value, np.datetime64):
            value = np.datetime64(raw_value)
        else:
            value = raw_value
        after_2000 = value > np.datetime64("2000-01-01")
        before_2100 = value < np.datetime64("2100-01-01")

        return all((after_2000, before_2100))
    except (ValueError, TypeError):
        return False


def lineament_id_check(raw_value: Any, lineament_id_prefixes: Tuple[str, ...]) -> bool:
    """
    Check that Lineament_ID column value matches predefined pattern.

    >>> lineament_id_check("M_A1", ("M",))
    True

    >>> lineament_id_check("M_A1", ("M", "L", "EM"))
    True

    >>> lineament_id_check("L_A1", ("M", "L", "EM"))
    True

    >>> lineament_id_check("H_A1", ("M", "L", "EM"))
    False
    """
    if not isinstance(raw_value, str) or len(raw_value) == 0:
        return False
    compiled_pattern = re.compile(
        LINEAMENT_ID_PATTERN_UNFORMATTED.format("|".join(lineament_id_prefixes))
    )
    return compiled_pattern.match(raw_value) is not None
