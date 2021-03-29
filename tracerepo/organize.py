"""
Organize trace data.
"""

from dataclasses import dataclass
from functools import cached_property
from itertools import compress
from pathlib import Path
from shutil import move
from typing import Dict, List, Optional, Sequence

import pandas as pd

import tracerepo.rules as rules
import tracerepo.utils as utils


@dataclass
class Organizer:

    """
    Organize trace data files with Organizer.
    """

    database: pd.DataFrame

    def __post_init__(self):
        """
        Post initialization steps.
        """
        self.database = rules.database_schema().validate(self.database)
        self.unorganized_folder = Path(rules.FolderNames.UNORGANIZED.value)

    @property
    def unorganized(self) -> List[Path]:
        """
        Find unorganized files in unorganized_folder.
        """
        return list(self.unorganized_folder.glob(f"*.{rules.FILETYPE}"))

    def organize(self, simulate=False) -> List[str]:
        """
        Organize files from unorganized_folder.
        """
        move_descriptions = []

        for filepath in self.unorganized:
            filename_stem = filepath.stem
            geometry = utils.identify_geom_type(filename_stem).value
            idx_in_database = self.columns[geometry].index(filename_stem)
            thematic = self.columns[rules.ColumnNames.THEMATIC.value][idx_in_database]
            scale = self.columns[rules.ColumnNames.SCALE.value][idx_in_database]
            destination = utils.compiled_path(
                thematic=thematic, geometry=geometry, scale=scale, name=filename_stem
            )
            if not simulate:
                destination.parent.mkdir(parents=True, exist_ok=True)
                move(filepath, destination)
            move_descriptions.append(
                f"Moving {filepath} to {destination}."
                + (" --SIMULATION--" if simulate else "")
            )
        return move_descriptions

    @cached_property
    def columns(self) -> Dict[str, List[str]]:
        """
        Get database columns and index as Python typed values.

        Accesible as a dict with column names as keys. Includes the area name
        values that were originally the dataframe index.
        """
        cols: Dict[str, List[str]] = dict()
        for column in rules.ColumnNames:
            cols[column.value] = utils.dataframe_column_to_python(
                dataframe=self.database,
                column=column.value,
                python_type=rules.ColumnNames.column_type()[column],
            )
        return cols

    def check(self):
        """
        Check if all rows in database correspond to files.
        """
        for area, traces, thematic, scale in zip(
            self.columns[rules.ColumnNames.AREA.value],
            self.columns[rules.ColumnNames.TRACES.value],
            self.columns[rules.ColumnNames.THEMATIC.value],
            self.columns[rules.ColumnNames.SCALE.value],
        ):
            for geom_filename, geometry in zip(
                (area, traces),
                (rules.ColumnNames.AREA.value, rules.ColumnNames.TRACES.value),
            ):
                utils.check_database_row_files(
                    thematic=thematic,
                    geometry=geometry,
                    scale=scale,
                    name=geom_filename,
                )

    @staticmethod
    def _filter_strings(
        area_values: Sequence[str],
        traces_values: Sequence[str],
        thematic_values: Sequence[str],
        scale_values: Sequence[str],
        query_bools: Sequence[bool],
        area: Sequence[str] = [],
        traces: Sequence[str] = [],
        thematic: Sequence[str] = [],
        scale: Sequence[str] = [],
    ) -> Sequence[bool]:
        """
        Filter database traces and areas based on given strings.
        """
        for filterer, list_to_filter in zip(
            (area, traces, thematic, scale),
            (area_values, traces_values, thematic_values, scale_values),
        ):
            filtered = utils.multi_string_filter(
                strings=filterer, list_to_filter=list_to_filter
            )

            query_bools = utils.join_bools(filtered, query_bools)
        return query_bools

    @staticmethod
    def _filter_enums(
        area_shape_values: Sequence[str],
        empty_values: Sequence[str],
        validated_values: Sequence[str],
        query_bools: Sequence[bool],
        area_shape: Optional[rules.AreaShapes] = None,
        empty: Optional[rules.BooleanChoices] = rules.BooleanChoices.FALSE,
        validated: Optional[rules.BooleanChoices] = rules.BooleanChoices.TRUE,
    ) -> Sequence[bool]:
        """
        Filter database traces and areas based on given enum choices.
        """
        for filterer, list_to_filter in zip(
            (area_shape, empty, validated),
            (
                area_shape_values,
                empty_values,
                validated_values,
            ),
        ):
            if filterer is None:
                continue
            query_bools = [
                all((val == filterer.value, query_bool_val))
                for val, query_bool_val in zip(list_to_filter, query_bools)
            ]
        return query_bools

    def query(
        self,
        area: Sequence[str] = [],
        traces: Sequence[str] = [],
        thematic: Sequence[str] = [],
        scale: Sequence[str] = [],
        area_shape: Optional[rules.AreaShapes] = None,
        empty: Optional[rules.BooleanChoices] = rules.BooleanChoices.FALSE,
        validated: Optional[rules.BooleanChoices] = rules.BooleanChoices.TRUE,
        geometry: Optional[rules.ColumnNames] = None,
    ) -> Sequence[Path]:
        """
        Query for trace and area data.
        """
        # default value, all accepted
        query_bools = [True] * len(self.columns[rules.ColumnNames.AREA.value])

        # Check area, traces, thematic and scale filters
        query_bools = self._filter_strings(
            area_values=self.columns[rules.ColumnNames.AREA.value],
            traces_values=self.columns[rules.ColumnNames.TRACES.value],
            thematic_values=self.columns[rules.ColumnNames.THEMATIC.value],
            scale_values=self.columns[rules.ColumnNames.SCALE.value],
            query_bools=query_bools,
            area=area,
            traces=traces,
            thematic=thematic,
            scale=scale,
        )

        # Return empty if none accepted already
        if not any(query_bools):
            return []

        # Check area_shape, empty and validated filters
        query_bools = self._filter_enums(
            query_bools=query_bools,
            area_shape_values=self.columns[rules.ColumnNames.AREA_SHAPE.value],
            empty_values=self.columns[rules.ColumnNames.EMPTY.value],
            validated_values=self.columns[rules.ColumnNames.VALIDATED.value],
            area_shape=area_shape,
            empty=empty,
            validated=validated,
        )

        # Return if no accepted
        if not any(query_bools):
            return []

        # Solve which geometries are wanted based on input
        geometries = (
            [rules.ColumnNames.TRACES.value, rules.ColumnNames.AREA.value]
            if geometry is None
            else [geometry.value]
        )

        # Collect the columns that are needed for path solving
        columns_needed = [
            rules.ColumnNames.THEMATIC.value,
            rules.ColumnNames.SCALE.value,
        ] + geometries

        # Compress the values corresponding to columns_needed based on
        # query_bools boolean list
        all_value_lists: List[Sequence[str]] = [
            list(compress(data=self.columns[col], selectors=query_bools))
            for col in columns_needed
        ]

        # Collect the accepted paths
        paths = []

        # Iterate over the values of columns_needed
        for vals in zip(*all_value_lists):

            assert len(vals) >= 3

            # Unpack values based on the order in columns_needed
            thematic_val = vals[0]
            scale_val = vals[1]
            geom_vals = vals[2:]

            # We might want traces, areas or both so geom_vals is a list
            # Iterate over the geom types and collect paths
            for geom_val, geom_type in zip(geom_vals, geometries):

                # Compile the path to the wanted traces or area dataset
                path = utils.compiled_path(
                    thematic=thematic_val,
                    geometry=geom_type,
                    scale=scale_val,
                    name=geom_val,
                )
                paths.append(path)

        # Return the wanted paths that pass all filters
        return paths

    def update(self, area_name: str, update_values: Dict[rules.ColumnNames, str]):
        """
        Change a value in the database.
        """
        # Get index for area_name
        index_for_area = self.columns[rules.ColumnNames.AREA.value].index(area_name)

        # Iterate over keys in update_values dict
        for key in update_values:

            # Get the current values
            column_values = self.columns[key.value]

            # Update value at index
            column_values[index_for_area] = update_values[key]

            # Make copy of instance dataframe
            database = self.database.copy()

            # Update column in dataframe with updated value
            database[key.value] = column_values

            # Validate
            database = rules.database_schema().validate(database)

            # Set new database
            self.database = database

            # Reset columns cached property
            del self.__dict__["columns"]
