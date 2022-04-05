#  -*- coding: utf-8 -*-
#    Created on 04/04/2022 13:28:12
#    @author: ErwingForero 
# 

#  -*- coding: utf-8 -*-
#    Created on 07/01/2022 15:51:23
#    @author: ErwingForero 
# 

import os
import tkinter as tk
import re
import asyncio
import functools

from concurrent.futures import ThreadPoolExecutor

from utils import constants as const
from gui.application import Application
from gui.func import decorator_exception_message
from utils import constants as const, index as utils, feature_flags as ft

config = utils.get_config(os.path.join(const.ROOT_DIR, "/config.yml")) 

@decorator_exception_message(title=const.PROCESS_NAME)
async def principal_process(app: 'Application'):
    """Main process for process

    Args:
        app (Application): actual instance of application
    """
    app.validate_results()
    results = app.results

    for result in results:
        pass
    
    
if __name__ == "__main__":
    
    utils.create_necesary_folders(const.ROOT_DIR, ["files", "resultado"])
    utils.create_necesary_folders(os.path.join(const.ROOT_DIR, "files"), ["temp", "alerts"])

    async_loop = asyncio.get_event_loop()

    App = Application(
        title=const.PROCESS_NAME,
        divisions=[2,2],
        size ="300x200"
    )

    App.insert_action("button", "btn_execute", principal_process, event_loop=async_loop)
    App.run()
    



