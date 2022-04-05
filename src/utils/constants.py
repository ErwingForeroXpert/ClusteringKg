#  -*- coding: utf-8 -*-
#    Created on 04/04/2022 13:28:12
#    @author: ErwingForero 
# 

from os import getcwd, path
from .feature_flags import ENVIROMENT 

# Project constants
LOG_NAME = "aut_clutering"
PROCESS_NAME = "Proceso Automatización Agrupacion"
ICON_IMAGE = "icon.ico"

# routes
PRINCIPAL_FILE_SOURCE = ""
ROOT_DIR = path.abspath(
    path.join(__file__, "../../..")
) if ENVIROMENT == "DEV" else getcwd()
