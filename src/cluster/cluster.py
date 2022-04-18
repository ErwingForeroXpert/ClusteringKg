#  -*- coding: utf-8 -*-
#    Created on 04/04/2022 13:28:12
#    @author: ErwingForero
#

from copy import copy
import re
import numpy as np
import pandas as pd
from dataframes import dataframe_optimized as dto, func
from utils import index as utils
from .cluster_types import TYPE_CLUSTERS


class Cluster(dto.DataFrameOptimized):

    def __init__(self, table=None, **kargs) -> None:
        super().__init__(table, **kargs)
        
    def __extract_column(column: str):
        arr = column.split("-")[:2]
        return list(range(int(arr[0])-1, int(arr[1])))

    def __order_by(bases: dict[str, dto.DataFrameOptimized], order: 'list[str]'):

        order_base = {key: bases[key] for key in order.keys()}

        return order_base
        
    def process_base_partners(self, base_partners: dto.DataFrameOptimized) -> None:

        columns = base_partners.table.columns.to_list()
        base = base_partners.table
        
        mask_client = base[columns[0]].astype(str).str.contains(
            pat=r'\d+', regex=True)
        mask_loc = base[columns[1]].astype(str).str.contains(
            pat=r'\d+', regex=True)
        mask_agent = base[columns[2]].astype(str).str.contains(
            pat=r'\d+', regex=True)

        base["socios"] = False

        base.loc[((mask_client|mask_loc)|(mask_client&mask_agent)), "socios"] = True 
        
        base[columns[0]] = base[columns[0]].astype(str).apply(func.mask_number) #cod_cliente
        base[columns[1]] = base[columns[1]].astype(str).apply(func.mask_number) #cod_loc
        base[columns[2]] = base[columns[2]].astype(str).apply(func.mask_number) #cod_agente

        self.table = base

    def process_base_coords(self, base_coords: dto.DataFrameOptimized) -> None:

        columns_coords = base_coords.table.columns.to_list()
        columns_general = self.table.columns.to_list()

        table_coords = base_coords.table
        table_general = self.table

        #convert into number
        table_coords[columns_coords[0]] = table_coords[columns_coords[0]].astype(str).apply(func.mask_number) #cod_cliente
        table_coords[columns_coords[3]] = table_coords[columns_coords[3]].astype(str).apply(func.mask_number) #cod_agente
        table_coords[columns_coords[4]] = table_coords[columns_coords[4]].astype(str).apply(func.mask_number) #cod_ecom

        coords_indirecta = table_general.merge(
            right=table_coords[columns_coords[1:]],
            right_on=[columns_coords[3],columns_coords[4]], #agente y codigo ecom
            left_on= [columns_general[2], columns_general[0]], #agente y cod_cliente (ecom)
            how="left"
        )

        #delete "cod_ecom" column
        coords_indirecta.drop(columns_coords[4], axis = 1, inplace = True) 

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

            cols_found.append(columns_universe)

            if type == TYPE_CLUSTERS.DIRECTA.value:
                #convert into number
                table_universe[columns_universe[0]] = table_universe[columns_universe[0]].astype(str).apply(func.mask_number) #cod_cliente

                res_base = table_general.merge(
                    right=table_universe, #cod_cliente, ...coords
                    right_on=columns_universe[0], #cod_cliente
                    left_on= columns_general[0], #cod_cliente
                    how="left"
                )

                found = ~pd.isna(res_base[columns_universe[1:]]).all(axis=1)

                res_base[columns_universe[4]] = res_base[columns_universe[4]].astype(str).apply(func.mask_number)

                bases.append(
                    res_base[found]
                )
            elif type == TYPE_CLUSTERS.INDIRECTA.value:
                #convert into number
                table_universe[columns_universe[2]] = table_universe[columns_universe[2]].astype(str).apply(func.mask_number) #cod_jefe

                res_base = table_general.merge(
                    right=table_universe, #cod_cliente, ...coords
                    right_on=columns_universe[2], #cod_jefe
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

    def process_bases_query(self, bases_query: 'list(list[dto.DataFrameOptimized])', types: 'list(str)') -> None:
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
    def preprocess_base(path: 'str|list', properties: dict) -> 'dto.DataFrameOptimized|list[dto.DataFrameOptimized]':
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
                for col in columns:
                    if str(col["pos"]).isnumeric():

                        header_idx.append(int(col["pos"])-1)
                        header_names.append(col["column"])
                    elif not str(col["pos"]).isnumeric():

                        temp_headers = Cluster.__extract_column(str(col["pos"]))
                        header_idx.extend(temp_headers)
                        header_names.extend([f"{col['column']}_{head}" for head in temp_headers])
            
            if converters is not None:
                converters = Cluster.process_converters(converters)

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
            bases = [None for _ in range(len(properties["columns"]))]

            if not utils.is_iterable(path):
                raise ValueError("for type folder path must be a iterable")

            properties["type"] = "file"
            actual_year = -1
            year = None

            for file in path:

                if year != (v:=re.search(r"(?<=_)(\d{4})(?=\.*)", file).group(0)): 
                    year = v
                    actual_year = min(len(properties["columns"])-1, actual_year+1)
                    
                temp_prop = copy(properties)
                temp_prop["columns"] = properties["columns"][actual_year]
                 
                if bases[actual_year] is None:
                    bases[actual_year] = Cluster.preprocess_base(file, temp_prop)
                else:
                    bases[actual_year].table = pd.concat((bases[actual_year].table, Cluster.preprocess_base(
                        file, temp_prop).table), ignore_index=True)
            return bases

    @staticmethod
    def process_converters(converters: dict) -> dict:
        _converters = {}
        for key, conv in converters.items():
            if conv.lower() == "number":
                _converters[key] = func.mask_number
            elif conv.lower() == "text":
                _converters[key] = func.mask_string
        return _converters

    async def merge_all(self, bases: 'dict[str, dto.DataFrameOptimized]', order: 'list[str]'):
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