#  -*- coding: utf-8 -*-
#    Created on 04/04/2022 13:28:12
#    @author: ErwingForero
#
import numpy as np
import pandas as pd
from dataframes import dataframe_optimized as dto
from utils import index as utils
from .cluster_types import TYPE_CLUSTERS


class Cluster(dto.DataFrameOptimized):

    def __init__(self, table=None, **kargs) -> None:
        super().__init__(table, **kargs)

        self.base_final = None

    def process_base_partners(self, base_partners: dto.DataFrameOptimized) -> None:

        columns = base_partners.table.columns.to_list()
        base = base_partners.table

        mask_client = base[columns[0]].str.contains(
            pat=r'\d+', regex=True)
        mask_loc = base[columns[1]].str.contains(
            pat=r'\d+', regex=True)
        mask_agent = base[columns[2]].str.contains(
            pat=r'\d+', regex=True)

        base["socios"] = False

        base.loc[((mask_client|mask_loc)|(mask_client&mask_agent)), "socios"] = True 

        self.base_final = base[[*columns[:2], "socios"]]

    def process_base_coords(self, base_coords: dto.DataFrameOptimized) -> None:

        columns_coords = base_coords.table.columns.to_list()
        columns_general = self.base_final.table.to_list()

        table_coords = base_coords.table
        table_general = self.base_final.table

        coords_indirecta = table_general.merge(
            right=table_coords[columns_coords[1:]],
            right_on=[columns_coords[3],columns_coords[4]], #agente y codigo ecom
            left_on= [columns_general[2], columns_general[0]], #agente y cod_cliente (ecom)
            how="left"
        )

        coords_directa = table_general.merge(
            right=table_coords[[columns_coords[0], *columns_coords[1:3]]], #cod_cliente, ...coords
            right_on=columns_coords[0], #cod_cliente
            left_on= columns_general[0], #agente y cod_cliente (ecom)
            how="left"
        )
        
        self.base_final.table = self.combine_columns(
            data=(coords_directa, coords_indirecta), 
            suffixes=("_x", "_y"),
            on=columns_general,
            how="left")
    
    def process_base_universe(self, bases_universe: 'list(dto.DataFrameOptimized)', types: 'list(str)') -> None:
        
        for base, type in zip(bases_universe, types):
            self.base_final
    @staticmethod
    def preprocess_base(path: 'str|list', properties: dict) -> 'dto.DataFrameOptimized':
        if properties["type"] == "file":
            converters = properties["converters"] if "converters" in properties.keys(
            ) else None
            columns = properties["columns"] if "columns" in properties.keys(
            ) else None
            skiprows = properties["skiprows"] if "skiprows" in properties.keys(
            ) else None
            header_names = None

            if columns is not None:
                header_names = columns.keys()
                # substract because the columns start with 1
                header_idx = [col["pos"] - 1 for col in columns]

            if skiprows is not None:
                skiprows = skiprows[:2] if isinstance(skiprows, (list, tuple)) else [
                    int(skiprows), -1]

            base = dto.DataFrameOptimized.get_table_excel(
                path, properties["sheet"], 
                header_idx=header_idx, 
                skiprows=skiprows, 
                converters=converters, 
                columns=header_names)

            return base

        elif properties["type"] == "folder":
            bases = None
            if not utils.is_iterable(path):
                raise ValueError("for type folder path must be a iterable")

            for file in path:
                if bases is None:
                    bases = Cluster.preprocess_base(file, properties)
                else:
                    bases.table = pd.concat((bases.table, Cluster.preprocess_base(
                        file, properties).table), ignore_index=True)
            return bases

    def merge_all():
        pass
