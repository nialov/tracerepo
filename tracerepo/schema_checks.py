"""
Trace schema checks used in validation.
"""
import re
from typing import Any, Tuple

import numpy as np


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


def data_source_regex_check(value: str) -> bool:
    """
    Regex check for data source column.

    E.g.

    >>> data_source_regex_check("LiDAR")
    True

    Order matters, EM must come after LiDAR.

    >>> data_source_regex_check("EM+LiDAR")
    False

    >>> data_source_regex_check("LiDAR+EM")
    True

    TODO: Could get sources from json where priority is indicated by number.
    """
    pattern_str = r"^(?:LiDAR(?:\+(?:Mag(?:\+EM)?|EM))?|Mag(?:\+EM)?|nan|EM)$"
    return pattern_matcher(pattern_str=pattern_str, value=value)


def date_datetime_check(raw_value: Any) -> bool:
    """
    Check that date column has valid datetime values.

    >>> date_datetime_check(np.datetime64("2021-07-23"))
    True

    >>> date_datetime_check(np.datetime64("1995-07-23"))
    False

    """
    if not isinstance(raw_value, np.datetime64):
        value = np.datetime64(raw_value)
    else:
        value = raw_value
    after_2000 = value > np.datetime64("2000-01-01")
    before_2100 = value < np.datetime64("2100-01-01")

    return all((after_2000, before_2100))


def operator_regex_check(value: str) -> bool:
    """
    Check operator column value with regex.

    >>> operator_regex_check("Ovaskainen N.")
    True

    >>> operator_regex_check("Ovaskainen Nn.")
    False

    TODO: Regex check not needed if the name strings are predetermined.
    TODO: Could get names from e.g. json config. Dot should be required there.
    """
    names: Tuple[str, ...] = (
        r"Markovaara-Koivisto M\.",
        r"Martinkauppi A\.",
        r"Ovaskainen N\.",
        r"Aaltonen I\.",
        r"Engström J\.",
        r"Laxström H\.",
        r"Nordbäck N\.",
        r"Hietava J\.",
        r"Wik H\.",
    )
    pattern_str_start = r"^("
    pattern_str_end = r")$"
    joined = "|".join(names)
    pattern_str = f"{pattern_str_start}{joined}{pattern_str_end}"
    return pattern_matcher(pattern_str=pattern_str, value=value)


def scale_regex_check(value: str) -> bool:
    """
    Check scale column value with regex.

    >>> scale_regex_check("1:200 000")
    True

    >>> scale_regex_check("1:200 000/Infinity")
    True

    Order matters.

    >>> scale_regex_check("1:200 000/Infinity/1:500 000")
    False

    """
    pattern_str = (
        r"^(?:1(?::(?:500 000(?:/(?:1:200 000(?:/Infinity)?|"
        r"Infinity))?|200 000(?:/Infinity)?)|\.[25]00 000)|Infinity)$"
    )
    return pattern_matcher(pattern_str=pattern_str, value=value)


def certainty_check(value: str) -> bool:
    """
    Check certainty column value.

    >>> certainty_check("1_Geoph_OR_LiDAR")
    True

    >>> certainty_check("anything else")
    False
    """
    options = {
        "1_Geoph_OR_LiDAR",
        "2_Geoph_AND_LiDAR",
    }
    return value in options
