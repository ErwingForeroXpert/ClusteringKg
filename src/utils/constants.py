#  -*- coding: utf-8 -*-
#    Created on 07/01/2022 15:51:23
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
