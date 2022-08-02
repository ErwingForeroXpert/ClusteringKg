import os
import re
import asyncio
import functools
import shutil
import pandas as pd
import cProfile
import pstats

from cluster.cluster import Cluster
from concurrent.futures import ThreadPoolExecutor
from dataframes.dataframe_optimized import DataFrameOptimized
from utils import constants as const, index as utils, feature_flags as ft

async def get_bases(sources: dict[str, str], files: list[str], cached_data: bool = False) -> 'tuple(list[str], list[DataFrameOptimized])':
    """Get DataFrames of sources

    Args:
        sources (dict[str, str]): dict of sources 

    Returns:
        list[DataFrameOptimized]: [description]
    """
    if cached_data is True:
        #only for test with csv value - delete
        bases = {}
        for file in os.listdir(os.path.join(const.ROOT_DIR, f"files/temp")):
            source = {}
            for key, s in sources.items():
                if key in file:
                    source = s
            
            converters = Cluster.process_converters(source["converters"], source["converters"].keys()) if "converters" in source.keys(
            ) else None
            bases[f"{file.split('.')[0]}"] = DataFrameOptimized.get_table_csv(os.path.join(const.ROOT_DIR, f"files/temp/{file}"), 
                    encoding="utf-8",
                    converters=converters)

        bases["base_consulta_directa"] = [bases.pop('base_consulta_directa_0')]
        bases["base_consulta_indirecta"] = [bases.pop('base_consulta_indirecta_0'), bases.pop('base_consulta_indirecta_1')]

        return bases
    else:
        loop = asyncio.get_event_loop()
        
        with ThreadPoolExecutor() as executor:

            futures = []
            keys = []

            for key, source in sources.items():
                path = files[key].split("|") #list[base, ...] - key is a name of base
                if len(path) == 1:
                    path = path[0]
                keys.append(key)
                futures.append(loop.run_in_executor(executor, functools.partial(Cluster.preprocess_base, **{"path": path, "properties": source})))

            results = await asyncio.gather(*futures)

        route = os.path.join(const.ROOT_DIR, f'test/files/bases_{ft.ACTUAL_BASES}')
        utils.validate_or_create_folder(route)

        for key, base in zip(keys, results): 
            if isinstance(base, (list, tuple)):
                if "_directa" in key:
                    print("stop")
                for idx in range(len(base)):
                    base[idx].table.to_csv(f"{os.path.join(const.ROOT_DIR, 'files/temp')}/{key}_{idx}.csv", encoding="utf-8", index = None)
                    if ft.ENVIROMENT == "DEV":
                        base[idx].table.to_csv(f"{route}/{key}_{idx}.csv", encoding="utf-8", index = None)
            else:
                base.table.to_csv(f"{os.path.join(const.ROOT_DIR, 'files/temp')}/{key}.csv", encoding="utf-8", index = None)
                if ft.ENVIROMENT == "DEV":
                    base.table.to_csv(f"{route}/{key}.csv", encoding="utf-8", index = None)

        return dict(zip(keys, results))

def get_predeterminated_files(_path: str):
    
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

utils.create_necesary_folders(os.path.join(const.ROOT_DIR, "files"), ["utils", "temp", "alerts"])

#load config, paths and structure of files
config = utils.get_config(os.path.join(const.ROOT_DIR, "config.yml"))
files_found = get_predeterminated_files(os.path.join(const.ROOT_DIR, "files/Bases"))
sources = config["sources"]

if ft.ENVIROMENT == "DEV": #save config for actual group of bases
    utils.validate_or_create_folder(os.path.join(const.ROOT_DIR, f"test/files/bases_{ft.ACTUAL_BASES}"))
    shutil.copyfile(os.path.join(const.ROOT_DIR, "config.yml"), os.path.join(const.ROOT_DIR, f"test/files/bases_{ft.ACTUAL_BASES}/config.yml"))

# delete
# loop.run_until_complete(get_bases({'base_universo_indirecta':sources['base_universo_indirecta']}, files_found, cached_data=False))
# end delete

#actual event loop
loop = asyncio.get_event_loop()

print("processing bases...")
bases = loop.run_until_complete(get_bases(sources, files_found, cached_data=True))  

final_base = Cluster()
with cProfile.Profile() as pr:
    loop.run_until_complete(final_base.merge_all(bases, config)) 

stats = pstats.Stats(pr)
stats.sort_stats(pstats.SortKey.TIME)
stats.print_stats()
stats.dump_stats(filename='needs_profiling.prof')


final_base.table.to_csv("base_final.csv", index=False, encoding="utf-8", float_format='%.10f')
# final_base.table.to_excel("base_final.xlsx", index=False, encoding="utf-8", float_format='%.10f') #more slow that to_csv