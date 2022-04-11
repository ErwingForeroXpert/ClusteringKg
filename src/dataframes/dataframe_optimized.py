#  -*- coding: utf-8 -*-
#    Created on 04/04/2022 13:28:12
#    @author: ErwingForero 
# 

from functools import reduce
import os
import re
from sre_compile import isstring
from time import time
from utils import feature_flags
from utils import constants as const
from utils import index as utils
from typing import Any
import pandas as pd
import numpy as np


class DataFrameOptimized():

    def __init__(self, table=None, **kargs) -> None:
        self.table = None
        self.__alerts = None
        # methods
        if table is not None:
            self.__process_table(table, **kargs)
            self.create_alerts()

    def __len__(self) -> int:
        return len(self.table) if self.table is not None else 0

    def __process_table(self, table: 'Any', **kargs) -> None:
        """Processes a table from a file or a str - like object .

        Args:
            table (Any): table to be processed

        Raises:
            FileNotFoundError: if table is a path not found
            TypeError: if type is invalid
        """
        if isinstance(table, str):
            if not os.path.exists(table):
                raise FileNotFoundError(f"file not found {table}")
            if "csv" in table or "txt" in table:
                self.table = pd.read_csv(table, **kargs)
            elif "xls" in table:
                self.table = pd.read_excel(table, **kargs)

        elif isinstance(table, (tuple, list)) or type(table).__module__ == np.__name__:
            self.table = pd.DataFrame(table, **kargs)
        elif isinstance(table, (pd.DataFrame)):
            self.table = table
        else:
            raise TypeError(f"Invalid permisible type of {table}")

    def delete_rows(self, criteria: 'np.array') -> 'DataFrameOptimized':
        """Delete rows from the dataframe .
        Args:
            criteria ([numpy.array]): mask of registers, ej: np.alert([True, False, True])

        Raises:
            ValueError: if actual instance hasn't table
            Exception: Generic Exception

        Returns:
            ['DataFrameOptimized']: actual instance of DataFrameOptimized
        """
        try:

            if self.table is not None:
                _df = self.table
            else:
                raise ValueError("delete_rows - instance need table")

            self.table = _df[criteria]
            return self

        except Exception as e:
            raise Exception(f"delete_rows {e}")

    def create_alerts(self) -> None:

        if self.table is not None:
            _columns = [*self.table.columns.to_list(), "description"]
            self.__alerts = pd.DataFrame(columns=_columns)
        else:
            raise Exception("Required table of DataFrameOptimized")

    def insert_alert(self, alert: 'Any', description: str) -> None:
        """Inserts an alert into the alert list .

        Args:
            alert ([Any]): Register with alert
            description (str): description of alert
            
        Raises:
            Exception: Generic Exception 
        """

        try:

            alert["description"] = description
            _alerts_columns = self.table.columns.tolist()
            # get only the columns that exist in the alerts
            _required_of_alert = alert[[*_alerts_columns, "description"]]

            self.__alerts = pd.concat(
                [self.__alerts, _required_of_alert], ignore_index=True)

            
        except Exception as e:
            raise Exception(f"insert_alert {e}")

    def get_alerts(self):
        return self.__alerts

    # def validate_alert(self, mask: bool, description: str, type: str, exception: bool=False, exception_description: str = "", aux_table: 'pd.DataFrame'=None):
    #     """Validate an alert .

    #     Args:
    #         mask (bool): [description]
    #         description (str): [description]
    #         exception (bool, optional): [description]. Defaults to False.
    #         exception_description (str, optional): [description]. Defaults to "".
    #     """
    #     if mask.sum() > 0:
    #         if aux_table is None:
    #             self.insert_alert(
    #                 alert=self.table[mask],
    #                 description=description
    #             )
    #         if exception:
    #             table = self.get_alerts() if aux_table is None else aux_table[mask]
    #             table.to_csv(
    #                 os.path.normpath(os.path.join(const.ALERTS_DIR, f"{afo_types.AFO_TYPES[type].value}_alerts.csv")), 
    #                 index=False, 
    #                 encoding="latin-1", 
    #                 sep=";")
    #             if feature_flags == "PROD":
    #                 raise Exception(exception_description)

    def get_rows(self, criteria: 'np.array') -> 'DataFrameOptimized':
        """Get rows from the dataframe .
        Args:
            criteria ([numpy.array]): mask of registers, ej: np.alert([True, False, True])

        Raises:
            ValueError: if actual instance hasn't table
            Exception: Generic Exception

        Returns:
            ['DataFrameOptimized']: actual instance of DataFrameOptimized
        """
        try:

            if self.table is not None:
                _df = self.table
            else:
                raise ValueError("delete_rows - instance need table")

            return _df[criteria]

        except Exception as e:
            raise Exception(f"delete_rows {e}")

    def replace_by(
        self, 
        dataframe_right: 'pd.DataFrame', 
        type_replace: str="all", 
        mask: list[bool]=None, 
        on: 'str|list'=None, 
        left_on: 'str|list'=None, 
        right_on: 'str|list'=None, 
        how: str="left", 
        left_replace: 'list|str'=None, 
        right_replacer: 'list|str'=None, 
        create_columns: bool=False, 
        **kargs) -> 'pd.DataFrame':
        """Replace values in the dataframe with the values in the given dataframe_right.

        first merge two dataframes by key (on or (left_on, right_on)), before replace the values 

        Args:
            dataframe_right ([pd.DataFrame]): dataframe that contains key to merge with actual table
            type_replace ([str]): type of replace, valid: 
                all: all values be reaplaced 
                not_nan: only the values found that have not been NaN in "dataframe_right" will be replaced
                mask: reaplace values by mask
                invert_mask: replace values by invert mask
            mask (bool, optional): mask for reaplace values, expected same length that given dataframe_right, Defaults to None.
            on (str|list, optional): key-column in both dataframes, Defaults to None.
            left_on (str|list, optional): key-column in left dataframe, Defaults to None.
            right_on (str|list, optional): key-column in right dataframe, Defaults to None.
            how (str, optional): type of merge dataframes (it's recomended to leave the default value), Defaults to left.
            left_replace (str|list, optional): column to be replaced by (right_on or right_replacer), Defaults to None.
            right_replacer (str|list, optional): column to replace left column, Defaults to None.
            create_columns (bool, optional): if left columns not exist is created, Defaults to False.
        Returns:
            pd.DataFrame: actual table updated
        """
        if on is None and right_on is None:
            raise ValueError("Required a value key in dataframe_right")

        if mask is None and type_replace not in ["not_nan", "all"]:
            raise ValueError("mask is required")

        _temp_table = self.table.merge(
            right=dataframe_right,
            on=on,
            left_on=left_on,
            right_on=right_on,
            how=how,
            **kargs
        )

        key_right = (
            on if on is not None else right_on) if right_replacer is None else right_replacer
        key_left = (
            on if on is not None else left_on) if left_replace is None else left_replace

        if isinstance(key_right, (list, tuple)):
            if len(key_left) != len(key_right):
                raise ValueError(f"Invalid size of keys list, left length {len(key_left)}, right length {len(key_right)}")

            for idx, key_r in enumerate(key_right):
                self.replace_by(
                    dataframe_right=dataframe_right,
                    on=on,
                    left_on=left_on,
                    right_on=right_on,
                    how=how, 
                    type_replace=type_replace, 
                    mask=mask, 
                    left_replace=key_left[idx], 
                    right_replacer=key_r, 
                    create_columns=create_columns, 
                    **kargs
                )
        else:
            if create_columns:
                self.table[key_left] = np.nan

            if type_replace == "mask":
                pass
            elif type_replace == "invert_mask":
                mask = ~mask
            elif type_replace == "not_nan":
                mask = ~pd.isna(_temp_table[key_right])
            elif type_replace == "all":
                mask = np.full(len(self.table), True)

            self.table.loc[mask, key_left] = _temp_table.loc[mask, key_right]

        return self.table

    def replace_many_by(
        self,
        dataframe_right: 'pd.DataFrame|list', 
        on=None, 
        left_on=None, 
        right_on=None, 
        how="left",
        merge=True,
        mask=None, 
        mask_idx=0,
        columns_right=None, 
        columns_left=None, 
        type="change", 
        type_replace="not_nan", 
        def_value=np.nan, **kargs):
        """Replace values in the dataframe with the values in the given dataframe_right.

        first merge two dataframes by key (on or (left_on, right_on)), before replace column by column 

        Args:
            dataframe_right ([pd.DataFrame]): dataframe that contains key to merge with actual table
            mask (bool, optional): mask for reaplace values, expected same length that given dataframe_right, Defaults to None.
            on (str|list, optional): key-column in both dataframes, Defaults to None.
            left_on (str|list, optional): key-column in left dataframe, Defaults to None.
            right_on (str|list, optional): key-column in right dataframe, Defaults to None.
            how (str, optional): type of merge dataframes (it's recomended to leave the default value), Defaults to left.
            merge (bool, optional): merge dataframes or not, Defaults to True.
            mask (list, optional): mask of columns, Defaults to None.
            mask_idx (inst, optional): if mask not exist found in dataframe_right index 0 or 1, for create mask, Defaults to 0.
            columns_right (str|list, optional): columns of dataframe_right to replace values, for create mask, Defaults to None.
            columns_left (str|list, optional): columns of dataframe_right to replace values, for create mask, Defaults to None.
            type (str, optional): type of replace columns, Defaults to change, valid:
                change: update exist values
                add_news: add new columns
            type_replace ([str]): type of replace values, valid: 
                all: all values be reaplaced 
                not_nan: only the values found that have not been NaN in "dataframe_right" will be replaced
                mask: reaplace values by mask
                invert_mask: replace values by invert mask
            def_value (Any, optional): optional value for columns added news, Defaults to NaN. 
        Returns:
            pd.DataFrame: actual table updated
        """

        if on is None and right_on is None and merge:
            raise ValueError("Required a value key in dataframe_right")

        if merge == False:
            _temp_table = dataframe_right
        elif isinstance(dataframe_right, (list, tuple)):
            if len(dataframe_right) > 2:
                raise ValueError("Invalid size for dataframe_right")

            _temp_table = [
                    self.table.merge(
                    right=data,
                    on=on,
                    left_on=left_on,
                    right_on=right_on,
                    how=how,
                    **kargs) for data in dataframe_right
            ]
        else:
            _temp_table = self.table.merge(
                right=dataframe_right,
                on=on,
                left_on=left_on,
                right_on=right_on,
                how=how,
                **kargs
            )

        for idx, _column in enumerate(columns_left):

            if type == "add_news" and _column not in self.table.columns.tolist():
                self.table[_column] = np.full((len(self.table), ), def_value)
            
            if type_replace == "mask":
                pass
            elif type_replace == "not_nan":
                mask = ~pd.isna(_temp_table[mask_idx][columns_right[mask_idx][idx]]) \
                if isinstance(_temp_table, (list, tuple)) \
                else ~pd.isna(_temp_table[columns_right[idx]])
                
            elif type_replace == "all":
                mask = np.full(len(self.table), True)

            if isinstance(_temp_table, (list, tuple)):
                self.table.loc[mask, _column] = _temp_table[0].loc[mask,
                                                                columns_right[0][idx]]
                self.table.loc[~mask, _column] = _temp_table[1].loc[~mask,
                                                                columns_right[1][idx]]
            else:
                self.table.loc[mask, _column] = _temp_table.loc[mask,
                                                                columns_right[idx]]

        return self.table

    def save_csv(self, folder_path: str, name: str = None, sep=";", **kargs) -> str:
        """Save the table to a CSV file .

        Args:
            folder_path (str): folder
            name (str, optional): name of csv file. Defaults to None.
            sep (str, optional): separator. Defaults to ";".
        """
        if name is None:
            name = f"{time.time()}.csv"

        route = os.path.normpath(os.path.join(folder_path, name))
        self.table.to_csv(path_or_buf=route, sep=sep, **kargs)

        return route

    @staticmethod
    def mask_by(data: 'pd.DataFrame', filter: 'object', replace: bool = False, aux_func: 'func' = None) ->'tuple[pd.DataFrame, pd.Series]':
        """Mask column with a given filter.

        Args:
            data (pd.DataFrame): [description]
            filter (Object): {
                "column": name of column of data to be filter,
                "<<TYPE>>":"<<VALUE>>"
                <<TYPE>>, permisible values: "more", "less", "equal", "diff", "contains" 
                    and valid merge 
                    "<<TYPE>>_and_<<TYPE>>" or "<<TYPE>>_or_<<TYPE>>", example:
                        "more_and_equals", "more_or_less"
                <<Value>>, permisible Any
                
                Examples:

                    {"column": "first_col", "equal": 0}
                    {"column": "first_col", "equal_or_less": 0}
                    {"column": "first_col", "contains": "(?i)_final"}
            }
            replace (bool, optional): replace dataframe by dataframe filtered. Defaults to False.
            aux_func (Any, optional): Function to filter column. Defaults to None.
        Raises:
            ValueError: column is required
            ValueError: column not found in data

        Returns:
            pd.DataFrame, pd.Series: dataframe, mask of fil
        """
        _keys = list(filter.keys())
        if "column" not in _keys:
            raise ValueError("column is required")
        
        if isinstance(filter["column"], (tuple, list)) and len(filter["column"]) > 1:
            if reduce(lambda a,b: (a not in data.columns.tolist()) & (b not in data.columns.tolist()), filter["column"]): #reduce list 
                raise ValueError("column not found in data")
            column = filter["column"]
        else:
            column = filter["column"][0] if isinstance(filter["column"], (tuple, list)) else filter["column"]
            if column not in data.columns.tolist():
                raise ValueError("column not found in data")

        if aux_func is not None:
            mask = aux_func(data[column])
        elif (intersec:=re.search(r'_and_|_or_', _keys[1])) is not None:

            filter_str, value = _keys[1], filter[_keys[1]]

            _filters = filter_str.split(intersec)
            for _filter in _filters:
                if "_and_" == intersec:
                    mask = DataFrameOptimized.mask_by(data, {"column": column, f"{_filter}": value}) if mask is None  \
                        else mask & DataFrameOptimized.mask_by(data, {"column": column, f"{_filter}": value})[1]
                elif "_or_" == intersec:
                    mask = DataFrameOptimized.mask_by(data, {"column": column, f"{_filter}": value}) if mask is None  \
                        else mask | DataFrameOptimized.mask_by(data, {"column": column, f"{_filter}": value})[1]
        else:
            filter_str, value = _keys[1], filter[_keys[1]]

            if "equal" in filter_str:
                mask = data[column] == value
            elif "diff" in filter_str:
                mask = data[column] != value
            elif "less" in filter_str:
                mask = data[column] < value
            elif "more" in filter_str:
                mask = data[column] > value
            elif "contains" in filter_str:
                mask = data[column].str.contains(value)

        return (data[mask], mask) if replace is True else (data, mask)

    @staticmethod
    def combine_columns(data: 'tuple[pd.DataFrame]', suffixes: 'tuple(str)', on: 'str' = None, left_on: 'str' = None, right_on: 'str' = None, how: 'str'= None, **kargs) -> pd.DataFrame:

        if len(data) != 2:
            raise ValueError("Invalid size for data")

        _temp_table = data[0].merge(
                right=data[1],
                on=on,
                left_on=left_on,
                right_on=right_on,
                how=how,
                suffixes= suffixes,
                indicator=True,
                **kargs
            )

        columns_with_suffixes = filter(lambda column: reduce(lambda a,b: (a in column) | (b in column), suffixes), _temp_table.columns.tolist())

        for idx, left_column in enumerate(columns_with_suffixes[::2]):
            right_column = columns_with_suffixes[idx+1]
            mask = pd.isna(left_column)
            column_without_suffixes = columns_with_suffixes[0]
            #delete suffixes
            for suffix in suffixes:
                column_without_suffixes.replace(suffix, "")

            _temp_table.loc[~mask, column_without_suffixes] =  _temp_table[left_column]
            _temp_table.loc[mask, column_without_suffixes] =  _temp_table[right_column]
            

        _temp_table.drop([*columns_with_suffixes, "_merge"], axis = 1, inplace = True)

        return _temp_table

    @staticmethod
    def get_table_excel(path: str, sheet: str, header_idx: 'list' = None, skiprows: 'list' = None, converters: 'list' = None, *args, **kargs) -> 'DataFrameOptimized':
        """Returns a DataFrame instance that will be used to parse the table at the given path .

        Args:
            path [str]: path of file
            sheet [str]: sheet of data
            header_idx [list]: list of each starting and ending column, example: [0,5]
            skiprows [list]: list of each starting and ending row, max_len = 2, example: [0,1000]
            converters [list]: list of columns converters, same size that columns.

        Returns:
            [DataFrameOptimized]: instance of DataFrameOptimized
        """
        try:
            _data = utils.get_data_of_excel_sheet(
                file_path=path, sheet=sheet, header_idx=header_idx, skiprows=skiprows, converters=converters)
            _dt = DataFrameOptimized(_data, *args, **kargs)
            return _dt

        except Exception as e:
            raise Exception(f"get_table_excel - {e}")

    @staticmethod
    def get_table_csv(path: str, *args, **kargs) -> 'DataFrameOptimized':
        """Returns a DataFrame instance that will be used to parse the table at the given path .

        Raises:
            Exception: [description]

        Returns:
            [type]: [description]

        Examples
        --------
        DataFrameOptimized.get_table_csv(((1,2), (3,4)), columns=["col1", "col2"])

        DataFrame 
            col1    col2
        0   1       2   
        1   3       4
        """
        try:

            _dt = DataFrameOptimized(path, *args, **kargs)
            return _dt

        except Exception as e:
            raise Exception(f"get_table_csv - {e}")

    @staticmethod
    def from_tuple(values: tuple, columns: tuple) -> 'Any':
        """Convert a tuple of values and columns to a DataFrameOptimized.

        Raises:
            Exception: if num of columns not is the same

        Returns:
            [vx.DataFrame]: DataFrame

        Examples
        --------
        DataFrameOptimized.from_tuple(((1,2), (3,4)), columns=["col1", "col2"])

        DataFrame 
            col1    col2
        0   1       2   
        1   3       4

        """
        try:
            if len(values[0]) != len(columns):  # if num of columns not is the same
                raise Exception("values in row are different that columns")
            _dt = DataFrameOptimized(pd.DataFrame(values, columns=columns))
            return _dt
        except Exception as e:
            raise Exception(f"from_tuple - {e}")

    @staticmethod
    def combine_str_columns(dataframe: 'pd.DataFrame', columns: list[str], name_res: str) -> 'pd.DataFrame':
        first_column = columns[0]
        other_columns = columns[1:]
        dataframe[name_res] = dataframe[first_column]

        for column in other_columns:
            dataframe[name_res] = dataframe[name_res] + dataframe[column]

        return dataframe

    @staticmethod
    def get_header_names_of(dataframe: 'pd.DataFrame', idx_cols: list[int], drop_duplicates: bool = False, subset: 'str|list[str]' = None, **kargs) -> tuple[pd.DataFrame, list[str]]:
        _headers = dataframe.columns.tolist()
        _columns_name = [_headers[i] for i in idx_cols]  # get the column names

        if drop_duplicates:
            if subset is None:
                raise ValueError(
                    "subset is required with drop_duplicates true")

            dataframe = dataframe.drop_duplicates(subset=subset, ignore_index=True, **kargs)

        return dataframe, _columns_name

    @staticmethod
    def get_from(dataframe: 'pd.DataFrame', start: 'str') -> 'pd.DataFrame':
        cols = dataframe.columns.tolist()
        if start not in cols:
            raise ValueError(f"{start} not found in dataframe")

        return dataframe[cols[cols.index(start):]]

    @staticmethod
    def make_criteria(dataframe: 'DataFrameOptimized', validator: 'dict[str:function]', limit: 'Any' = None) -> 'np.array':
        """AI is creating summary for make_criteria

        Raises:
            IndexError: Limit more bigger that dataframe

        Returns:
            numpy.Array: mask of values found with validator

        Examples
        --------
        dataframe = DataFrameOptimized({
            "column":["first", "second", "estr", "car", "ert", "eft"]
            })
        >>> mask = make_criteria(dataframe, {
            "column": lambda x: str(x).start_with("e")
        })

        array([False, False, True, False, True, True])

        dataframe = DataFrameOptimized({
            "column":["first", "second", "estr", "car", "ert", "eft"]
            })
        >>> mask = make_criteria(dataframe, {
            "column": lambda x: str(x).start_with("e")
        }, limit = 2)

        array([False, False, True, False, True, False])

        dataframe = DataFrameOptimized({
            "column1":["first", None, "estr", "car", "ert", "eft"]
            "column2":[1,2,3,4,5,None]
            })

        >>> mask = make_criteria(dataframe, 
        {
            "column1;column2": lambda x: x != None
        })

        array([True, False, True, True, True, False])

        """
        mask = None

        for column, validation in validator.items():  # str, function
            column = column.split(";") if ";" in column else column
            len_column = 1 if isstring(column) else len(column)

            if utils.is_iterable(validation):
                validated = None
                for sub_val in validation:
                    temp_validation = np.apply_along_axis(func1d=sub_val,
                                                          axis=1,
                                                          arr=np.array(
                                                              dataframe[column]).reshape(-1, len_column)
                                                          )
                    validated = temp_validation if validated is None else validated & temp_validation

            else:
                validated = np.apply_along_axis(func1d=validation,
                                                axis=1,
                                                arr=np.array(
                                                    dataframe[column]).reshape(-1, len_column)
                                                )
            mask = validated if mask is None else mask | validated

        if limit != None and limit > len(dataframe):
            raise IndexError("make_criteria - limit is more bigger that table")
        elif limit is not None:
            _index = 0
            _iterator = 0
            while _index < limit and _iterator < len(mask):
                if mask[_index] == True:
                    _index += 1
                _iterator += 1

            mask[_index + 1:] = False

        return mask
