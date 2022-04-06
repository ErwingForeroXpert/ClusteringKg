#  -*- coding: utf-8 -*-
#    Created on 04/04/2022 13:28:12
#    @author: ErwingForero 
# 

import tkinter
import os
import time
import yaml
import pymsgbox 
import pyxlsb as pyx
import xlwings as xw
import numpy as np
from typing import Any
from tkinter import filedialog
from . import constants as const

def get_config(path: str) -> dict:

    """Get configuration file .yml

    Args:
        path (str): path of file

    Raises:
        Exception: if file isn't found

    Returns:
        dict: config load
    """

    if os.path.exists(path):
        _path = path
    else:
        raise Exception("config.yml file is required")
    
    with open(_path, "r") as ymlfile:
        cfg = yaml.load(ymlfile)
    
    return cfg

def search_for_file_path (message: str, required: bool = False, types: 'tuple|str' = "*")-> 'str|None':
    """Search for a file path.
    Args:
        message (str):  the title of the window
        required (bool, optional): if file is required. Defaults to be False.
        types (tuple|str, optional): a sequence of (label, pattern) tuples, Defaults to be ‘*’ wildcard is allowed
    Returns:
        str|None: path of file
    """
    root = tkinter.Tk()
    root.withdraw() #use to hide tkinter window

    file_found = False
    while not file_found:
        currdir = os.getcwd()
        tempdir = filedialog.askopenfilename(parent=root, initialdir=currdir, title=message, filetypes=types)
        if os.path.exists(tempdir):
            file_found = True
        elif required:
            pymsgbox.alert("Se requiere el archivo, por favor intentelo de nuevo", message)
        else:
            file_found = True
    
    return tempdir

def wait_book_disable(mybook):
    """Wait until the book is closed

    Args:
        mybook (String): path of the excel book
    """
    while is_iterable(xw.books):
        time.sleep(1)
        for book in xw.books:
            if book.fullname == mybook:
                book.save()
        book.app.quit()

def validate_or_create_folder(path: str) -> bool:
    if os.path.exists(path):
        return True
    else:
        os.mkdir(path)
    return False

def create_necesary_folders(path: str, folders: list[str]) -> None:
    """Create folders

    Args:
        path (String): Folder parent
        folders (List[str]): Folder childrens
    """
    for folder in folders:
        validate_or_create_folder(os.path.join(path, folder))

def is_iterable(posibleList):
    """Validate if element is iterable

    Args:
        posibleList (Any): posible iterable element

    Returns:
        bool: if element is iterable
    """
    try:
        if isinstance(posibleList, (tuple, list)) or hasattr(posibleList, "__iter__"):
            _ = posibleList[0]
            return True

        return False
    except Exception as e:
        return False

def if_error_false(cb, *args, **kargs):
    """if generate error return false

    Args:
        cb (function): function

    Returns:
        Any: function return or False
    """
    try:
        return cb(*args, **kargs)
    except Exception as e:
        return False

def is_empty(value: 'Any')-> bool:
    if value is None:
        return True
    elif if_error_false(str, value):
        return str(value).strip().__len__ == 0
    elif if_error_false(int, value):
        return int(value) == 0
    elif if_error_false(bool, value):
        return True

    return False

def all_is_empty(iterable: 'Any') -> bool:
    if not is_iterable(iterable):
        raise ValueError("all_is_empty - invalid type of iterable")

    empties = 0
    size = 0
    for v in iterable:
        if is_empty(v):
            empties += 1
        size += 1
    
    return empties == size

def get_diff_list(lists: 'tuple(list)',   _type: 'str' = 'all') -> list:
    """Get difference between two list

    Args:
        lists (tuple): two list to be compared
        _type (str, optional): _type of get diff:

        all - get all list values different
        left - get only left different values
        right - get only right different values
        
        Defaults to 'all'.

    Raises:
        ValueError: Invalid size of lists, expected: __len__ 2
        ValueError: Invalid _type of lists

    Returns:
        list: difference
    """
    if len(lists) != 2:
        raise ValueError("Invalid size of lists, expected: __len__ 2")

    if not is_iterable(lists[0]) or not is_iterable(lists[1]):
        raise ValueError("Invalid _type of lists")

    diff = list(set(lists[0]) ^ set(lists[1]))

    if _type == "left":
        diff = [column for column in diff if column in lists[0]]
    
    elif _type == "right":
        diff = [column for column in diff if column in lists[1]]

    elif _type == "left":
        pass
    
    return diff

def get_same_list(lists: 'tuple(list)') -> list:
    """Get same values between two list

    Args:
        lists (tuple): two list to be compared

    Raises:
        ValueError: Invalid size of lists, expected: __len__ 2
        ValueError: Invalid _type of lists

    Returns:
        list: same values
    """
    if len(lists) != 2:
        raise ValueError("Invalid size of lists, expected: __len__ 2")

    if not is_iterable(lists[0]) or not is_iterable(lists[1]):
        raise ValueError("Invalid _type of lists")

    same = [item for item in lists[0] if item in lists[1]]

    return same

def convert_row(row: 'np.array', converter: 'list') -> 'np.array':
    
    if row.shape[-1] != len(converter):
        raise ValueError(f"convert_row - invalid size of converter array length {row.shape[-1]}")
    
    for i, _ in enumerate(converter):
        if len(row.shape) > 1:
            row[:, i] = converter[i](row[:, i][0]) #2D Array [:, 1,2,3...] all axis 0, but get axis 1
        else:
            row[i] = converter[i](row[i]) #1D Array
    return row

def get_data_of_excel_sheet(file_path: str, sheet: str, header_idx: 'list'= None, skiprows: 'list'= None, row_converter: 'list' = None) -> 'np.array':
    """Get data of sheet in Excel File

    Returns:
        Any: tuple
    """
    try:
        data = None
        if "xlsm" in file_path.lower():
            wb = xw.Book(file_path)
            ws = wb.sheets(sheet)
            tbl = ws.api.ListObjects(1) # or .ListObjects('MyTable')
            rng = ws.range(tbl.range.address) # get range from table address

            data = rng.options(np.array, header=True).value

        elif "xlsb" in file_path.lower() or "xlsx" in file_path.lower():
            with pyx.open_workbook(file_path) as wb:
                with wb.get_sheet(sheet) as _sheet:
                    data = np.array(list(_sheet))[:,:,2]

        if header_idx is not None:
            data = data[:, header_idx[0]:header_idx[1]]

        if row_converter is not None:
            data = np.apply_along_axis(convert_row, 1, data, row_converter)

        if skiprows is not None:
            data = data[skiprows[0]:skiprows[1],:]

        return data

    except Exception as e:
        raise Exception(f"get_data_of_excel_sheet - {e}")


def get_data_of_csv(file_path: str, header_idx: 'list'= None, index_idx: 'list'= None, row_converter: 'list' = None, **kargs) -> 'np.array':

    try:
        data = np.genfromtxt(file_path, **kargs)

        if header_idx is not None:
            data = data[:, header_idx[0]:header_idx[1]]

        if row_converter is not None:
            data = np.apply_along_axis(convert_row, 1, data, row_converter)

        if index_idx is not None:
            data = data[index_idx[0]:index_idx[1],:]

        return data
    
    except Exception as e:
        raise Exception(f"get_data_of_csv - {e}")
