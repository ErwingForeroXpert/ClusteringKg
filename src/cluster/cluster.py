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
        
    def __extract_column(column: str):
        arr = column.split("-")[:2]
        return list(range(int(arr[0])-1, int(arr[0])))

    def __order_by(bases: dict[str, dto.DataFrameOptimized], order: 'list[str]'):

        order_base = {key: bases[key] for key in order.keys()}

        return order_base
        
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

        self.table = base[[*columns[:2], "socios"]]

    def process_base_coords(self, base_coords: dto.DataFrameOptimized) -> None:

        columns_coords = base_coords.table.columns.to_list()
        columns_general = self.table.to_list()

        table_coords = base_coords.table
        table_general = self.table

        coords_indirecta = table_general.merge(
            right=table_coords[columns_coords[1:]],
            right_on=[columns_coords[3],columns_coords[4]], #agente y codigo ecom
            left_on= [columns_general[2], columns_general[0]], #agente y cod_cliente (ecom)
            how="left"
        )

        coords_directa = table_general.merge(
            right=table_coords[[columns_coords[0], *columns_coords[1:3]]], #cod_cliente, ...coords
            right_on=columns_coords[0], #cod_cliente
            left_on= columns_general[0], #cod_cliente
            how="left"
        )
        
        self.table = self.combine_columns(
            data=(coords_directa, coords_indirecta), 
            suffixes=("_x", "_y"),
            on=columns_general,
            how="left")
    
    def process_bases_universe(self, bases_universe: 'list(dto.DataFrameOptimized)', types: 'list(str)') -> None:

        bases = []
        cols_found = []
        columns_general = self.table.columns.to_list()
        table_general = self.table

        for base, type in zip(bases_universe, types):
            table_universe = base.table
            columns_universe = base.table.columns

            cols_found = cols_found.append(columns_universe)

            if type == TYPE_CLUSTERS.DIRECTA.value:
                res_base = table_general.merge(
                    right=table_universe, #cod_cliente, ...coords
                    right_on=columns_universe[1], #cod_cliente
                    left_on= columns_general[0], #cod_cliente
                    how="left"
                )

                found = ~pd.isna(res_base[columns_universe]).any(axis=1)

                bases.append(
                    res_base[found]
                )
            elif type == TYPE_CLUSTERS.INDIRECTA.value:

                res_base = table_general.merge(
                    right=table_universe, #cod_cliente, ...coords
                    right_on=columns_universe[2], #cod_cliente
                    left_on= columns_general[0], #cod_cliente
                    how="left"
                )

                bases.append(
                    res_base
                )

        self.table = self.combine_columns(
            data=bases, 
            suffixes=("_x", "_y"),
            on=utils.get_same_list(cols_found), # get same columns in two bases
            how="left")

    def process_bases_query(self, bases_query: 'list(dto.DataFrameOptimized)', types: 'list(str)') -> None:
        bases = []
        cols_found = []
        columns_general = self.table.columns.to_list()
        table_general = self.table

        for base, type in zip(bases_query, types):
            table_query = base.table
            columns_query = base.table.columns

            cols_found = cols_found.append(columns_query)

            if type == TYPE_CLUSTERS.DIRECTA.value:
                res_base = table_general.merge(
                    right=table_query, #cod_cliente, ...coords
                    right_on=columns_query[1], #cod_cliente
                    left_on= columns_general[0], #cod_cliente
                    how="left"
                )

                found = ~pd.isna(res_base[columns_query]).any(axis=1)

                bases.append(
                    res_base[found]
                )
            elif type == TYPE_CLUSTERS.INDIRECTA.value:

                res_base = table_general.merge(
                    right=table_query, #cod_cliente, ...coords
                    right_on=columns_query[2], #cod_cliente
                    left_on= columns_general[0], #cod_cliente
                    how="left"
                )

                bases.append(
                    res_base
                )

        self.table = self.combine_columns(
            data=bases, 
            suffixes=("_x", "_y"),
            on=utils.get_same_list(cols_found), # get same columns in two bases
            how="left")

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
                # substract because the columns start with 1
                header_idx = []
                header_names = []
                for key, col in columns.items():
                    if str(col["pos"]).isnumeric():

                        header_idx.append(int(col["pos"])-1)
                        header_names.append(key)
                    elif not str(col["pos"]).isnumeric():

                        temp_headers = Cluster.__extract_column(str(col["pos"]))
                        header_idx.extend(temp_headers)
                        header_names.extend([f"{key}_{head}" for head in temp_headers])
                        
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

    def merge_all(self, bases: 'dict[str, dto.DataFrameOptimized]', order: 'list[str]'):
        pair_bases = []
        for key in order:
            if "socios" in key.lower():
                self.process_base_partners(bases[key])

            elif "coordenadas" in key.lower():
                self.process_base_coords(bases[key])
                
            elif "universo_directa" in key.lower():
                pair_bases.append(bases[key])

            elif "universo_indirecta" in key.lower():
                pair_bases.append(bases[key])
                self.process_bases_universe(pair_bases, types=(TYPE_CLUSTERS.DIRECTA.value, TYPE_CLUSTERS.INDIRECTA.value))
                pair_bases = []

            elif "consulta_directa" in key.lower():
                pair_bases.append(bases[key])

            elif "consulta_indirecta" in key.lower():
                pair_bases.append(bases[key])
                self.process_bases_universe(pair_bases, types=(TYPE_CLUSTERS.DIRECTA.value, TYPE_CLUSTERS.INDIRECTA.value))
                pair_bases = []