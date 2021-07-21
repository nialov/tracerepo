"""
Trace schema checks used in validation.
"""
import re


def data_source_regex_check(value: str):
    """
    Regex check for data source column.
    """
    pattern = re.compile(r"^(?:LiDAR(?:\+(?:Mag(?:\+EM)?|EM))?|Mag(?:\+EM)?|nan|EM)$")
    try:
        pattern_match = pattern.match(value)
    except TypeError:
        return False
    return pattern_match is not None
