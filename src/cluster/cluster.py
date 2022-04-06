#  -*- coding: utf-8 -*-
#    Created on 04/04/2022 13:28:12
#    @author: ErwingForero 
# 
import pandas as pd
from dataframes import dataframe_optimized as dto
from utils import index as utils
from .cluster_types import TYPE_CLUSTERS

class Cluster(dto.DataFrameOptimized):

    def __init__(self, table=None, **kargs) -> None:
        super().__init__(table, **kargs)

        self.base_final = None

    def process_base_partners(self, base_partners: dto.DataFrameOptimized, type: str) -> None:

        if not TYPE_CLUSTERS.exist(type):
            raise ValueError(f"Invalid type of cluster: {type}")

        if type == TYPE_CLUSTERS.DIRECTA.value:
            if self.base_final is None:
                self.base_final = base_partners
            else:
                pass
        elif type == TYPE_CLUSTERS.INDIRECTA.value:
            pass
        
    @staticmethod
    def preprocess_base(path: 'str|list', properties: dict) -> 'dto.DataFrameOptimized':
        if properties["type"] == "file":
            converters = properties["converters"] if "converters" in properties.keys() else None
            columns = properties["columns"] if "columns" in properties.keys() else None
            skiprows = properties["columns"] if "columns" in properties.keys() else None
            header_names = None

            if columns is not None:
                header_idx = [min(columns.values()), max(columns.values())]
                header_names = columns.keys()

            if skiprows is not None:
                skiprows = skiprows[:2] if isinstance(skiprows, (list, tuple)) else [int(skiprows), -1]

            return dto.DataFrameOptimized.get_table_excel(path, properties["sheet"], header_idx=header_idx, skiprows=skiprows, converters=converters, columns=header_names)

        elif properties["type"] == "folder":
            bases = None
            if not utils.is_iterable(path):
                raise ValueError("for type folder path must be a iterable")

            for file in path:
                if bases is None:
                    bases = Cluster.preprocess_base(file, properties)
                else:
                    bases.table = pd.concat((bases.table, Cluster.preprocess_base(file, properties).table), ignore_index=True) 
            return bases

    def merge_all():
        pass