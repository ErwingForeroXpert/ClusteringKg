#  -*- coding: utf-8 -*-
#    Created on 04/04/2022 13:28:12
#    @author: ErwingForero 
# 
from tkinter import messagebox
from utils import log 
import tkinter as tk

def decorator_exception_message(title: str):
    """Create exception message for the decorated function.

    Args:
        func (function): middle function
        title (str): title of project
    """
    def parent_wrapper(func):
        async def function_wrapper(*args, **kargs):
            try:
                await func(*args, **kargs)
            except Exception as e:
                args[0].buttons["btn_execute"]['state'] = tk.NORMAL
                log.insertInLog(message=e, type="error")
                messagebox.showerror(message=f"Ha ocurrido un error en el proceso, intentelo de nuevo \n si el error persiste contacte a soporte.", title=title)
        return function_wrapper
        
    return parent_wrapper