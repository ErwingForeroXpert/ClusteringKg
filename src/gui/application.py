#  -*- coding: utf-8 -*-
#    Created on 04/04/2022 13:28:12
#    @author: ErwingForero 
# 
import time
import tkinter as tk
import numpy as np
import threading
import re
from os import getcwd,path, listdir
from utils.constants import ROOT_DIR, ICON_IMAGE
from utils.index import validate_or_create_folder


class Application():

    def __init__(self, title: str, hide: bool = False, size: str="600x600", sources: dict = None) -> None:
        """Initialize the GUI Aplicacion budget.

        Args:
            title (str): title of project
            divisions (list): number of vertical sections in root window
            hide (bool, optional): hide root window. Defaults to False.
            size (str, optional): size of root window. Defaults to "600x600".
        """
        self.root = tk.Tk()
        self.root.title(title)
        self.root.geometry(size)
        self.files = []
        self.buttons = {}
        self.inputs = {}
        self.labels = {}
        self.prog_bars = {}
        self.sources = sources
        source_text = {
            f"type_selector_{name}": tk.StringVar() for name in sources.keys()
        }
        self.labels_text = {
            "status_project": tk.StringVar(),
            "progress_time": tk.StringVar(),
            **source_text
        }
        self.results = {}

        self.execution_seconds = 0
        self.extensions = [
            ("Excel files", ".xlsx .xls .xlsb .xlsm"),
            ("Csv Files", ".csv .txt")
        ]
        
        _path = path.normpath(path.join(ROOT_DIR, "files/img"))
        validate_or_create_folder(_path)

        if path.exists(path.join(_path, ICON_IMAGE)):
            self.root.iconbitmap(path.join(_path, ICON_IMAGE))

        if hide:
            self.root.withdraw()
        
        self.__design()

    def __design(self) -> None:
        """Create the layout for the given root window.
        """
        # configure the grid

        cols = [0,1,2,3]
        cols_w = [1,5,1,1]
        for idx, col in enumerate(cols):
            self.root.columnconfigure(col, weight=cols_w[idx])

        rows_to_insert = [
            {
                "name": key,
                "message": prop["message"],
                "btn_message": "Añadir",
                "command": lambda: self.insert_result(name=prop['name'], type=prop['type'])
            } for key, prop in self.sources
        ]
        
        end_row = self.create_row_insert(rows_to_insert, init_row=0)


        self.buttons[f"btn_execute"] = tk.Button(self.root, text="Iniciar")
        self.buttons[f"btn_execute"].grid(row=end_row+1, column=0, columnspan=4, sticky=tk.NS, padx=5, pady=5)

        self.labels_text["status_project"].set("Sin procesar")
        self.labels["lbl_status"] = tk.Label(self.root, textvariable=self.labels_text["status_project"])
        self.labels["lbl_status"].grid(row=end_row+2, column=1, columnspan=4, sticky=tk.NS, padx=5, pady=5)

        self.labels_text["progress_time"].set("00:00:00")
        self.labels["lbl_prog_time"] = tk.Label(self.root, textvariable=self.labels_text["progress_time"],)
        self.labels["lbl_prog_time"].grid(row=end_row+3, column=0, sticky=tk.NS, padx=5, pady=5)

        self.prog_bars["pb_process"] = tk.ttk.Progressbar(
            self.root,
            orient = tk.HORIZONTAL,
            length = 100,
            mode = 'indeterminate'
        )
        self.prog_bars["pb_process"].grid(row=end_row+3, column=1, columnspan=3, sticky=tk.NS, padx=5, pady=5)

    def create_row_insert(self, rows: list, init_row: int = 0) -> None:
        """Insert a new row to insert files or folders

        Args:
            rows (list): list with next rows structure:
                [{
                    "name": str,
                    "message": str,
                    "btn_message": str,
                    "command": func
                }, ...]
            init_row (int, optional): row number from where to start inserting rows. Defaults to 0.

        Returns:
            int: end row number
        """
        end_row = init_row

        for idx, row in enumerate(rows):
            idx = idx+init_row
            end_row = idx

            self.labels_text[f"type_selector_{row['name']}"].set(row["message"])
            self.labels[f"lbl_type_selector_{row['name']}"] = tk.Label(self.root, textvariable=self.labels_text[f"type_selector_{row['name']}"]).grid(row=idx, column=0, sticky=tk.W, padx=5, pady=5)

            self.inputs[f"path_{row['name']}"] = tk.Entry(self.root)
            self.inputs[f"path_{row['name']}"].grid(row=idx, column=1, sticky=tk.E, padx=5, pady=5)
            
            self.buttons[f"btn_insert_file_{row['name']}"] = tk.Button(self.root, text=row["btn_message"], command=row["command"])
            self.buttons[f"btn_insert_file_{row['name']}"].grid(row=idx, column=2, sticky=tk.W, padx=5, pady=5)

        return end_row

    def update_label(self, label: str, label_text: str, text: str):
        self.labels_text[label_text].set(text)
        self.labels[label]["textvariable"] = self.labels_text[label_text]

    def update_progress(self) -> None:
        if self.labels_text["status_project"].get() != "Sin procesar":
            self.execution_seconds = self.execution_seconds + 1
            self.update_label("lbl_prog_time", "progress_time", time.strftime("%H:%M:%S", time.gmtime(self.execution_seconds)))
        else: 
            self.execution_seconds = 0
        self.root.after(1000, self.update_progress)

    def do_task(self, async_loop, action):
        """async loop manager

        Args:
            async_loop (event): actual event_loop
                The event loop is the core of every asyncio application. 
                Event loops run asynchronous tasks and callbacks, 
                perform network IO operations, and run subprocesses.
            action (function): async function to be executed
        """
        try:
            threading.Thread(target=lambda: async_loop.run_until_complete(action())).start()
        finally:
            self.buttons["btn_insert_file"]['state'] = tk.NORMAL
            print("event end")

    def insert_action(self, _type: str, name: str, cb: 'Function', event_loop=None, **kargs) -> None:
        """Inserts a function to the type selector.

        Args:
            _type (str): Valid type selector, button or input
            name (str): key of selector
            cb (Function): function that be executed with command selector 

        Raises:
            ValueError: name not found in list names of selector
        """
        def _sub_func(**options):
            if event_loop is None:
                return lambda: cb(self, **options)   
            else:
                async def func_temp(): 
                    await cb(self, **options)
                return func_temp

        sub =  _sub_func(**kargs)

        if _type == "button":
            if name not in self.buttons.keys(): raise ValueError(f"{name} not found in buttons")
            self.buttons[name]["command"] = sub if event_loop is None else lambda:self.do_task(event_loop, sub)
        elif _type == "input":
            if name not in self.inputs.keys(): raise ValueError(f"{name} not found in inputs")
            self.inputs[name]["command"] = sub if event_loop is None else lambda:self.do_task(event_loop, sub)

    def make_message(self, message: str, _type: str = "info", others_cb: 'list[function]' = []) -> 'Function':
        """Create a messagebox with a messagebox .

        Args:

            message (str): message to be displayed
            _type (str, optional): type of message. Defaults to "info".

        Returns:
            Function: cb function 
        """
        _title = self.root.title()
        if _type == "error":
            return lambda: [tk.messagebox.showerror(_title, message), *others_cb]
        elif _type == "warning":
            return lambda: [tk.messagebox.showwarning(_title, message), *others_cb]
        elif _type == "info":
            return lambda: [tk.messagebox.showinfo(_title, message), *others_cb]

    def insert_result(self, type: str, name: str) -> None:
        if type == "file":
            _path = self.search_for_file_path(types= self.extensions, required=True)
        elif type == "folder":
            _path = self.search_for_folder_path(required=True)

        self.results[f"{name}"].set(_path)
        self.inputs[f"path_{name}"].insert(0, _path)
        
    def validate_results(self) -> bool:
        """validate results insert in process

        Raises:
            FileNotFoundError: file not found in folder

        Returns:
            bool: all results validated
        """
        if len(self.results) == 0:
            tk.messagebox.showerror(self.root.title(), "No ha ingresado ningun archivo o carpeta")
            return False

        for key, result in self.results.items():
            files = []
            if path.isdir(result.get()):
                patterns = " ".join([patt[1] for patt in self.extensions])
                patterns = patterns.replace(" ", "|")

                for _file in listdir(result.get()):
                    if len(re.findall(patterns, _file)) > 0:
                        files.append(path.normpath(path.join(result.get(), _file)))
                
                if len(files) == 0:
                    raise FileNotFoundError(f"No se encontraron los archivos necesarios en: {result.get()}")

                self.results[key].set("|".join(files))

            elif path.isfile():
                pass
            else:
                tk.messagebox.showerror(self.root.title(), f"Se requiere el el archivo o carpeta para {key}, por favor intentelo de nuevo")
                return False

            return True
    
    def search_for_file_path (self, required: bool = False, types: 'tuple|str' = "*")-> 'str|None':
        """Search for a file path.
        Args:
            required (bool, optional): if file is required. Defaults to be False.
            types (tuple|str, optional): a sequence of (label, pattern) tuples, Defaults to be ‘*’ wildcard is allowed
        Returns:
            str|None: path of file
        """
        root = self.root

        file_found = False
        while not file_found:
            currdir = getcwd()
            tempdir = tk.filedialog.askopenfilename(parent=root, initialdir=currdir, title=self.root.title(), filetypes=types)
            if path.exists(tempdir):
                file_found = True
            elif required:
                tk.messagebox.showerror(self.root.title(), "Se requiere el archivo, por favor intentelo de nuevo")
            else:
                file_found = True
        
        return tempdir

    def search_for_folder_path (self, required: bool = False)-> 'str|None':
        """Search for a folder path.
        Args:
            required (bool, optional): if folder is required. Defaults to be False.
        Returns:
            str|None: path of folder
        """
        root = self.root

        folder_found = False
        while not folder_found:
            currdir = getcwd()
            tempdir = tk.filedialog.askdirectory(parent=root, initialdir=currdir, title=self.root.title())
            if path.exists(tempdir):
                folder_found = True
            elif required:
                tk.messagebox.showerror(self.root.title(), "La carpeta de archivos es necesaria, por favor intentelo de nuevo")
            else:
                folder_found = True
        
        return tempdir

    def run(self) -> None:
        self.root.mainloop()
    

    @staticmethod
    def change_text(variable: 'tk.StringVar', text: str):
        return lambda: variable.set(text)