#  -*- coding: utf-8 -*-
#    Created on 04/04/2022 13:28:12
#    @author: ErwingForero 
# 
import re
from typing import Any
from numpy import float64, number, isnan, __name__, array, int64, NaN
from pandas import isnull, isna
from utils.index import is_iterable
from unidecode import unidecode

def remove_accents(input_str: str) -> str:
    return unidecode(input_str)

def validate_empty_or_cero(value):
    try:
        if is_iterable(value) and not isinstance(value, (str)):
            _res = None
            for v in value:
                validated = validate_empty_or_cero(v)
                _res = validated  if _res is None else _res & validated
            return _res

        elif isinstance(value, (str, int)):
            if str(value).strip().__len__ == 0 or str(value).strip() == '0':
                return True
        elif value is None:
            return True
        elif isinstance(value, (float)):
            return isnan(value)
        elif type(value).__module__ == __name__:
            if value == 0:
                return True
            if value == array(['']) or value == array([None]) or value == array([0]) or value == array(['0']):
                return True
            if isnull(value) or isna(value):
                return True

        return False

    except ValueError:
        return True

def validate_not_empty_or_cero(value):
    return not validate_empty_or_cero(value)

def validate_start_with(__prefix: 'str | tuple[str, ...]'):
    def __validator(value):
        return str(value).strip().startswith(__prefix)

    return __validator

def validate_if_contains(__prefix: 'str | tuple[str, ...]'):
    def __validator(value):
        if isinstance(__prefix, str):
            return str(__prefix).lower() in str(value).strip().lower()
        elif is_iterable(__prefix):
            for _pre in __prefix:
                if str(_pre).lower() in str(value).strip().lower():
                    return True
            return False

    return __validator

def validate_if_not_contains(__prefix: 'str | tuple[str, ...]'):
    return lambda _prefix: not validate_if_contains(_prefix)


def mask_string(value: 'Any') -> str:
    try:
        if isnull(value) or isna(value):
            return ""
        else:
            return str(value)
    except ValueError:
        return ""

def mask_price(value: 'Any') -> str:
    if isnull(value) or isna(value) or str(value) == "":
        return int64(0)
    else:
        try:
            found = str(value).replace(".", "").replace(",", "")
            if (res:=re.search(r'\-*\d+',found)) is not None:
                found = res.group(0)
            else:
                found = 0

            return int64(found)
        except AttributeError:
            return NaN

def mask_number(value: 'Any') -> int64:
    if isnull(value) or str(value) == "":
        return NaN
    else:
        try:
            if (res:=re.search(r'\-*\d+',str(value))) is not None:
                found = res.group(0)
            else:
                found = 0
            return int64(found)
        except ValueError:
            return NaN

def mask_float(value: 'Any') -> int64:
    if isnull(value) or str(value) == "":
        return NaN
    else:
        try:
            if (res:=re.search(r'\-*\d+(\,*|\.*)\d+',str(value))) is not None:
                found = res.group(0).replace(",", ".")
            else:
                found = 0
            return float64(found)
        except ValueError:
            return NaN