"""
Trace schema checks used in validation.
"""
import re
from typing import Any, Dict, Tuple

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


# def data_source_regex_check(value: str, data_source_priorities: Dict[str, int]) -> bool:
#     """
#     Regex check for data source column.

#     E.g.

#     >>> data_source_regex_check("LiDAR")
#     True

#     Order matters, EM must come after LiDAR.

#     >>> data_source_regex_check("EM+LiDAR")
#     False

#     >>> data_source_regex_check("LiDAR+EM")
#     True

#     """
#     return named_priority_check(
#         value,
#     )
#     # data_source_names = set
# (data_source.name for data_source in data_source_priorities)
#     # if len(value) == 0:
#     #     return False
#     # if value in data_source_names:
#     #     return True
#     # if not any(data_source_name in value for data_source_name in data_source_names):
#     #     return False

#     # pattern_str = r"^(?:LiDAR(?:\+(?:Mag(?:\+EM)?|EM))?|Mag(?:\+EM)?|nan|EM)$"
#     # return pattern_matcher(pattern_str=pattern_str, value=value)


def named_priority_check(value: str, named_priorities: Dict[str, int], separator: str):
    """
    Check that value matches a predetermined name and priority order of names.
    """
    if len(value) == 0:
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
