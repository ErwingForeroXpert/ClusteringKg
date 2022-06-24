import os
import re
import asyncio
import functools
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from dataframes.dataframe_optimized import DataFrameOptimized
from cluster.cluster import Cluster
from utils import constants as const, index as utils

async def get_bases(path: str = None, sources: dict[str, str] = None, files: list[str] = None, cached_data: bool = False) -> dict:
    """Get bases of path

    Args:
        path (str): folder that have the files
        sources (dict[str, str]): key of base and their path
        cached_data (bool): used cached data, Default is False
    Returns:
        dict[str, list[DataFrameOptimized]|DataFrameOptimized]: key: key of base  with their source
    """ 
    bases = None

    if cached_data is True:
        bases = {
            f"{key}": [] for key in sources.keys()
        }
        for file in filter(lambda v: re.match(r'.*.csv',v) , os.listdir(path)):
            source = {}
            actual_key = None
            for key, s in sources.items():
                if key in file:
                    actual_key = key
                    source = s
            
            converters = Cluster.process_converters(source["converters"], source["converters"].keys()) if "converters" in source.keys(
            ) else None
            bases[actual_key].append(DataFrameOptimized.get_table_csv(f"{path}/{file}", 
                    encoding="utf-8",
                    converters=converters))

        bases = {
            **bases,
            **{f"{key}": values[0] for key, values in bases.items() if "consulta" not in key}
        }

    else:
        loop = asyncio.get_event_loop()
        
        with ThreadPoolExecutor() as executor:

            futures = []
            keys = []

            for key, source in sources.items():
                _path = files[key].split("|") #list[base, ...] - key is a name of base
                if len(_path) == 1:
                    _path = _path[0]
                keys.append(key)
                futures.append(loop.run_in_executor(executor, functools.partial(Cluster.preprocess_base, **{"path": _path, "properties": source})))

            results = await asyncio.gather(*futures)

        bases = dict(zip(keys, results))

    return bases

def get_predeterminated_files(_path: str) -> dict[str, str]:
    """Get predeterminated files inside a route

    Args:
        _path (str): folder with files

    Returns:
        dict[str, str]: dictionary with files and key 
    """
    found = {
        "base_socios": "",
        "base_coordenadas": "",
        "base_universo_directa": "",
        "base_universo_indirecta": "",
        "base_consulta_directa": "",
        "base_consulta_indirecta": ""
    }
    for (dirpath, dirnames, filenames) in os.walk(_path):
        for file in filenames:
            if re.search("socio", file, re.IGNORECASE):
                found["base_socios"] = os.path.join(_path, file)
            elif re.search("coord", file, re.IGNORECASE):
                found["base_coordenadas"] = os.path.join(_path, file)
            elif re.search(r"universo\s+direc", file, re.IGNORECASE):
                found["base_universo_directa"] = os.path.join(_path, file)
            elif re.search(r"universo\s+indirec", file, re.IGNORECASE):
                found["base_universo_indirecta"] = os.path.join(_path, file)

        for dir in dirnames:
            files = []
            root_dir = os.path.join(_path, dir)
            for _file in os.listdir(root_dir):
                files.append(os.path.normpath(os.path.join(root_dir, _file)))
            if re.search("indirecta", dir, re.IGNORECASE):
                found["base_consulta_indirecta"] = "|".join(files)
            elif re.search("directa", dir, re.IGNORECASE):
                found["base_consulta_directa"] = "|".join(files)
    return found

def async_test(f):
    """Decorator for async functions

    Args:
        f (Function): Async function
    """
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(f(*args, **kwargs))
    return wrapper
