#  -*- coding: utf-8 -*-
#    Created on 04/04/2022 13:28:12
#    @author: ErwingForero 
# 

#  -*- coding: utf-8 -*-
#    Created on 07/01/2022 15:51:23
#    @author: ErwingForero 
# 

import os
from posixpath import split
import tkinter as tk
import re
import asyncio
import functools

from concurrent.futures import ThreadPoolExecutor

from utils import constants as const
from gui.application import Application
from gui.func import decorator_exception_message
from utils import constants as const, index as utils, feature_flags as ft
from cluster.cluster import Cluster

config = (decorator_exception_message(title=const.PROCESS_NAME))(utils.get_config(os.path.join(const.ROOT_DIR, "/config.yml"))) 

@decorator_exception_message(title=const.PROCESS_NAME)
async def principal_process(app: 'Application'):
    """Main process for process

    Args:
        app (Application): actual instance of application
    """
    app.execution_seconds = 0 # seconds of process
    app.update_label(label="lbl_status", label_text="status_project", text="Procesando...") # status
    app.update_progress() #update progress of process
    app.validate_results() #validate if all insertions are correct
    results = app.results
    sources = config["sources"]

    bases = {}
    for key, source in sources.items():
        path = results[key].split("|") #list[base, ...] - key is a name of base
        if len(path) == 1:
            path = path[0]
        bases[key] = Cluster.preprocess_base(path, source)

    final_base = Cluster()
    final_base.merge_all(bases)

    
    
if __name__ == "__main__":
    
    utils.create_necesary_folders(const.ROOT_DIR, ["files", "resultado"])
    utils.create_necesary_folders(os.path.join(const.ROOT_DIR, "files"), ["temp", "alerts"])

    async_loop = asyncio.get_event_loop()

    App = Application(
        title=const.PROCESS_NAME,
        divisions=[2,2],
        size ="500x600",
        sources = config["sources"]
    )

    App.insert_action("button", "btn_execute", principal_process, event_loop=async_loop)
    App.run()
    



