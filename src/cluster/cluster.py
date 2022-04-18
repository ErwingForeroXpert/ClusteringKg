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
        
    def process_base_partners(self, base_partners: dto.DataFrameOptimized) -> None:

        columns = base_partners.table.columns.to_list()
        base = base_partners.table
        
        mask_isindirecta = base[columns[3]].astype(str).str.contains(
            pat=r'indi', regex=True, case=False)

        mask_client = base[columns[0]].astype(str).str.contains(
            pat=r'\d+', regex=True) 
        mask_loc = base[columns[1]].astype(str).str.contains(
            pat=r'\d+', regex=True)
        mask_agent = base[columns[2]].astype(str).str.contains(
            pat=r'\d+', regex=True)

        mask_directa = ((mask_client | mask_loc) & ~mask_isindirecta)
        mask_indirecta = ((mask_client & mask_agent) & mask_isindirecta)

        base["socios"] = False

        base.loc[(mask_directa | mask_indirecta), "socios"] = True 
        
        base[columns[0]] = base[columns[0]].astype(str).apply(func.mask_number) #cod_cliente
        base[columns[1]] = base[columns[1]].astype(str).apply(func.mask_number) #cod_loc
        base[columns[2]] = base[columns[2]].astype(str).apply(func.mask_number) #cod_agente

        self.table = base[[*columns[:3], "socios"]]

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
        columns_general = self.table.columns.to_list()
        table_general = self.table

        for base, type in zip(bases_universe, types):
            table_universe = base.table
            columns_universe = base.table.columns

            if type == TYPE_CLUSTERS.DIRECTA.value:
                #convert into number
                table_universe[columns_universe[0]] = table_universe[columns_universe[0]].astype(str).apply(func.mask_number) #cod_cliente

                res_base = table_general.merge(
                    right=table_universe, #cod_cliente, ...coords
                    right_on=columns_universe[0], #cod_cliente
                    left_on= columns_general[0], #cod_cliente
                    how="left"
                )

                #delete not found
                found = ~pd.isna(res_base[columns_universe[1:]]).all(axis=1)
                res_base[columns_universe[4]] = res_base[columns_universe[4]].astype(str).apply(func.mask_number)
                res_base = res_base[found]

                #get repeated "vendedores"
                mask_repeated = res_base.duplicated(subset=columns_general[0], keep='first')
                base_duplicated = res_base[mask_repeated].rename(columns={f"{columns_universe[2]}": f"{columns_universe[2]}_2", f"{columns_universe[3]}": f"{columns_universe[3]}_2"})
                columns_duplicated = base_duplicated.columns.to_list()

                #add the repeated "vendedores"
                new_base = res_base[~mask_repeated].merge(
                    right=base_duplicated[[columns_duplicated[0], f"{columns_universe[2]}_2", f"{columns_universe[3]}_2"]], 
                    right_on=columns_duplicated[0], #cod_cliente
                    left_on= columns_general[0], #cod_cliente
                    how="left"
                )

                bases.append(
                    new_base
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
            on=columns_general[:3], # merge by cod_cliente, cod_loc and cod_ecom
            how="left")

    def process_bases_query(self, bases_query: 'list(list[dto.DataFrameOptimized])', types: 'list(str)') -> None:
        bases = []
        columns_general = self.table.columns.to_list()
        table_general = self.table

        for base, type in zip(bases_query, types):

            if type == TYPE_CLUSTERS.DIRECTA.value:
                table_query = base.table
                columns_query = base.table.columns
                new_base = base.table.pivot(index=[columns_query[0], 'marca'], columns='mes', values=['venta_pesos', 'venta_kilos']).fillna(0)

                res_base = table_general.merge(
                    right=new_base, #cod_cliente, ...coords
                    right_on=columns_query[1], #cod_cliente
                    left_on= columns_general[0], #cod_cliente
                    how="left"
                )

                found = ~pd.isna(res_base[columns_query]).any(axis=1)

                bases.append(
                    res_base[found]
                )
            elif type == TYPE_CLUSTERS.INDIRECTA.value:
                base_1 = base[0].table
                base_2 = base[1].table
                columns_query_1 = base_1.columns.tolist()
                columns_query_2 = base_2.columns.tolist()

                max_sale_pesos = max([func.mask_number(sale) for sale in columns_query_1 if "ventas_pesos" in sale])

                #rename columns
                base_2.rename(columns={f"{sale}": f"{sale.replace(str(func.mask_number(sale)), max_sale_pesos + func.mask_number(sale))}" for sale in columns_query_2 if "ventas" in sale})
                
                res_base = base_1.merge(
                    right=base_2, #cod_cliente, ...coords
                    right_on=columns_query_2[:3], #cod_agente, cod_ecom, marca
                    left_on= columns_query_1[:3], 
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
                        temp_headers = [v-(min(temp_headers)-1) for v in temp_headers]

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
                self.process_bases_query(pair_bases, types=(TYPE_CLUSTERS.DIRECTA.value, TYPE_CLUSTERS.INDIRECTA.value))
    
    def get_average(row_values: pd.Series, lots: int, columns: list[str], umbral: int = 0.3) -> pd.Series:
        group_act = columns[len(columns)-lots:]
        group_ant = columns[len(columns)-(lots*2):len(columns)-lots]
        group_old = columns[:len(columns)-(lots*2)]
        
        size_old = np.array(group_old).size
        active_old = size_old if (v:=np.where(np.array(group_old) > 0)).size == 0 else v[0]
        old_quantity = (size_old - active_old)/size_old

        size_ant = np.array(group_ant).size
        active_ant = size_ant if (v:=np.where(np.array(group_ant) > 0)).size == 0 else v[0]
        ant_quantity = (size_ant - active_ant)/size_ant

        size_act = np.array(group_act).size
        active_act = size_act if (v:=np.where(np.array(group_act) > 0)).size == 0 else v[0]
        act_quantity = (size_act - active_act)/size_act

        prom_act, prom_ant = np.nan
        if ant_quantity > umbral:
            prom_act = np.sum(group_act)/lots
        
        if old_quantity > umbral:
            prom_ant = np.sum(group_ant)/lots
        
        return prom_ant, prom_act