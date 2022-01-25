"""
Organize trace data.
"""

from dataclasses import dataclass
from itertools import compress
from pathlib import Path
from shutil import move
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

from tracerepo import rules, utils


@dataclass
class Organizer:

    """
    Organize trace data files with Organizer.
    """

    database: pd.DataFrame
    tracerepository_path: Path

    _columns: Optional[Dict[str, List[Any]]] = None

    def __post_init__(self):
        """
        Post initialization steps.
        """
        assert self.tracerepository_path.exists()
        self.database = rules.database_schema().validate(self.database)
        self.unorganized_folder = (
            self.tracerepository_path / rules.PathNames.UNORGANIZED.value
        )

    @property
    def unorganized(self) -> List[Path]:
        """
        Find unorganized files in unorganized_folder.
        """
        return list(self.unorganized_folder.glob(f"*.{rules.FILETYPE}"))

    def organize(self, simulate: bool = False) -> List[str]:
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
                root=self.tracerepository_path,
                thematic=thematic,
                geometry=geometry,
                scale=scale,
                name=filename_stem,
            )
            if not simulate:
                destination.parent.mkdir(parents=True, exist_ok=True)
                move(str(filepath.absolute().expanduser()), destination)
            move_description = f"Moving {filepath} to {destination}." + (
                " --SIMULATION--" if simulate else ""
            )
            assert isinstance(move_description, str)
            move_descriptions.append(move_description)
        return move_descriptions

    @property
    def columns(self) -> Dict[str, List[Any]]:
        """
        Get database columns and index as Python typed values.

        Accessible as a dict with column names as keys. Includes the area name
        values that were originally the dataframe index.
        """
        if self._columns is None:
            cols: Dict[str, List[Any]] = dict()
            for column in rules.ColumnNames:
                cols[column.value] = utils.dataframe_column_to_python(
                    dataframe=self.database,
                    column=column.value,
                    python_type=rules.ColumnNames.column_type()[column],
                )
            self._columns = cols
        return self._columns

    def check(self):
        """
        Check if all rows in database correspond to files.

        Furthermore check if all files in data dir correspond to rows.
        """
        data_path = self.tracerepository_path / Path(rules.PathNames.DATA.value)
        all_files_and_dirs = list(data_path.rglob("*"))
        all_files = {path.stem: path for path in all_files_and_dirs if path.is_file()}
        all_dirs = {path.stem: path for path in all_files_and_dirs if path.is_dir()}

        for value in (rules.PathNames.AREA.value, rules.PathNames.TRACES.value):
            all_dirs = utils.remove_from_dict_if_in(key=value, dict_to_check=all_dirs)

        for area, traces, thematic, scale in zip(
            self.columns[rules.ColumnNames.AREA.value],
            self.columns[rules.ColumnNames.TRACES.value],
            self.columns[rules.ColumnNames.THEMATIC.value],
            self.columns[rules.ColumnNames.SCALE.value],
        ):

            for value in (thematic, scale):
                all_dirs = utils.remove_from_dict_if_in(
                    key=value, dict_to_check=all_dirs
                )
            for value in (area, traces):
                all_files = utils.remove_from_dict_if_in(
                    key=value, dict_to_check=all_files
                )

            for geom_filename, geometry in zip(
                (area, traces),
                (rules.ColumnNames.AREA.value, rules.ColumnNames.TRACES.value),
            ):
                utils.check_database_row_files(
                    root=self.tracerepository_path,
                    thematic=thematic,
                    geometry=geometry,
                    scale=scale,
                    name=geom_filename,
                )
        orphan_files = len(all_files) != 0
        orphan_dirs = len(all_dirs) != 0

        if orphan_files or orphan_dirs:
            orphan_file_error_str = f"Found orphan files: {list( all_files.values() )}."
            orphan_dir_error_str = (
                f"Found orphan directories: {list( all_dirs.values() )}."
            )

            raise FileExistsError(
                f"Expected all files and directories in "
                f"{data_path.absolute()}"
                " to correspond to row values in database.\n"
                f"{orphan_file_error_str}\n"
                f"{orphan_dir_error_str}\n"
            )

    @staticmethod
    def _filter_strings(
        area_values: Sequence[str],
        traces_values: Sequence[str],
        thematic_values: Sequence[str],
        scale_values: Sequence[str],
        query_bools: Sequence[bool],
        area: Sequence[str] = (),
        traces: Sequence[str] = (),
        thematic: Sequence[str] = (),
        scale: Sequence[str] = (),
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
        validity_values: Sequence[str],
        query_bools: Sequence[bool],
        area_shape: Sequence[rules.AreaShapes] = (),
        validity: Sequence[rules.ValidationResults] = (),
    ) -> Sequence[bool]:
        """
        Filter database traces and areas based on given enum choices.
        """
        for filterer, list_to_filter in zip(
            (area_shape, validity),
            (
                area_shape_values,
                validity_values,
            ),
        ):
            assert isinstance(filterer, Sequence)
            if filterer is None or len(filterer) == 0:
                continue

            assert isinstance(filterer, Sequence)
            assert None not in filterer
            # filterer_vals = [filterer_val.value for filterer_val in filterer]
            # boolean = lambda val: val in filterer_vals or len(filterer_vals) == 0
            # else:
            # boolean = lambda val: val == filterer.value

            filterer_value_set = set(filt.value for filt in filterer)

            query_bools = [
                all(
                    (
                        val in filterer_value_set,
                        query_bool_val,
                    )
                )
                for val, query_bool_val in zip(list_to_filter, query_bools)
            ]
        return query_bools

    def query(
        self,
        area: Sequence[str] = (),
        traces: Sequence[str] = (),
        thematic: Sequence[str] = (),
        scale: Sequence[str] = (),
        area_shape: Sequence[rules.AreaShapes] = (),
        validity: Sequence[rules.ValidationResults] = (),
    ) -> List[utils.TraceTuple]:
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
            validity_values=self.columns[rules.ColumnNames.VALIDITY.value],
            area_shape=area_shape,
            validity=validity,
        )

        # Return if no accepted
        if not any(query_bools):
            return []

        # Collect the columns that are needed for path solving
        columns_needed = [
            rules.ColumnNames.THEMATIC.value,
            rules.ColumnNames.SCALE.value,
            rules.ColumnNames.TRACES.value,
            rules.ColumnNames.AREA.value,
            rules.ColumnNames.SNAP_THRESHOLD.value,
            rules.ColumnNames.VALIDITY.value,
        ]

        # Compress the values corresponding to columns_needed based on
        # query_bools boolean list
        (
            thematic_vals,
            scale_vals,
            trace_vals,
            area_vals,
            snap_vals,
            validity_vals,
        ) = tuple(
            list(compress(data=self.columns[col], selectors=query_bools))
            for col in columns_needed
        )

        # Collect trace and area paths (both or one of depending on geometry
        # filter) into named tuples.
        paths: List[utils.TraceTuple] = [
            utils.query_result_tuple(
                tracerepository_path=self.tracerepository_path,
                thematic_val=thematic_val,
                scale_val=scale_val,
                traces_val=traces_val,
                area_val=area_val,
                snap_threshold=snap_val,
                validity_val=validity_val,
            )
            for (
                thematic_val,
                scale_val,
                traces_val,
                area_val,
                snap_val,
                validity_val,
            ) in zip(
                thematic_vals,
                scale_vals,
                trace_vals,
                area_vals,
                snap_vals,
                validity_vals,
            )
        ]

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
            self._columns = None
