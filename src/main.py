#  -*- coding: utf-8 -*-
#    Created on 04/04/2022 13:28:12
#    @author: ErwingForero 
# 

#  -*- coding: utf-8 -*-
#    Created on 07/01/2022 15:51:23
#    @author: ErwingForero 
# 

from concurrent.futures import ThreadPoolExecutor
import os
import tkinter as tk
import re
import asyncio
import functools

import pandas as pd
from dataframes.dataframe_optimized import DataFrameOptimized

from utils import constants as const
from gui.application import Application
from gui.func import decorator_exception_message
from utils import constants as const, index as utils, feature_flags as ft
from cluster.cluster import Cluster

config = utils.get_config(os.path.join(const.ROOT_DIR, "config.yml"))

def load_cache():
        for key, base in bases.items(): 
            if isinstance(base, (list, tuple)):
                for ind, b in enumerate(base):
                    b.table.to_csv(os.path.join(const.ROOT_DIR, f"files/temp/{key}_{ind}.csv"), encoding='utf-8', index=False)
            else:
                base.table.to_csv(os.path.join(const.ROOT_DIR, f"files/temp/{key}.csv"), encoding='utf-8', index=False)
            
@decorator_exception_message(title=const.PROCESS_NAME)
async def principal_process(app: 'Application'):
    """Main process for process

    Args:
        app (Application): actual instance of application
    """
    loop = asyncio.get_event_loop()

    app.buttons["btn_execute"]['state'] = tk.DISABLED
    app.execution_seconds = 0 # seconds of process
    # pas = app.validate_results() #validate if all insertions are correct
    
    # if pas is True:
    if True:
        app.prog_bars["pb_process"].start()
        app.update_label(label="lbl_status", label_text="status_project", text="Procesando...") # status
        app.update_progress() #update progress of process

        # results = app.results
        # sources = config["sources"]

        # bases = {}

        # with ThreadPoolExecutor(max_workers=4) as executor:

        #     futures = []
        #     keys = []

        #     for key, source in sources.items():
        #         path = results[key].split("|") #list[base, ...] - key is a name of base
        #         if len(path) == 1:
        #             path = path[0]
        #         keys.append(key)
        #         futures.append(loop.run_in_executor(executor, functools.partial(Cluster.preprocess_base, **{"path": path, "properties": source})))

        #     results = await asyncio.gather(*futures)
        
        for key, base in bases.items(): 
            if isinstance(base, (list, tuple)):
                for idx in range(len(base)):
                    base[idx].table.to_csv(f"{os.path.join(const.ROOT_DIR, 'files/temp')}/{key}_{idx}.csv", encoding="utf-8", index = None)
            else:
                base.table.to_csv(f"{os.path.join(const.ROOT_DIR, 'files/temp')}/{key}.csv", encoding="utf-8", index = None)

        bases = {f"{file.split('.')[0]}": DataFrameOptimized(pd.read_csv(os.path.join(const.ROOT_DIR, f"files/temp/{file}"))) for file in os.listdir(os.path.join(const.ROOT_DIR, f"files/temp"))}
        # bases = {f"{keys[idx]}": result for idx, result in enumerate(results)}

        final_base = Cluster()
        await final_base.merge_all(bases, config["order_base"])

        app.prog_bars["pb_process"].stop()
        app.update_label(label="lbl_status", label_text="status_project", text="Terminado") # status

if __name__ == "__main__":
    
    utils.create_necesary_folders(const.ROOT_DIR, ["files", "resultado"])
    utils.create_necesary_folders(os.path.join(const.ROOT_DIR, "files"), ["temp", "alerts"])

    async_loop = asyncio.get_event_loop()

    App = Application(
        title=const.PROCESS_NAME,
        size ="500x400",
        sources = config["sources"]
    )

    App.insert_action("button", "btn_execute", principal_process, event_loop=async_loop)
    App.run()



