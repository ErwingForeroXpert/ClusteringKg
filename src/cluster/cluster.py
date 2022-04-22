#  -*- coding: utf-8 -*-
#    Created on 04/04/2022 13:28:12
#    @author: ErwingForero
#

from copy import copy
from multiprocessing.pool import ThreadPool
import re
import numpy as np
import pandas as pd
from dataframes import dataframe_optimized as dto, func
from utils import feature_flags, index as utils
from .cluster_types import TYPE_CLUSTERS
from tqdm import tqdm

class Cluster(dto.DataFrameOptimized):

    def __init__(self, table=None, **kargs) -> None:
        super().__init__(table, **kargs)
        
    def __extract_column(column: str):
        arr = column.split("-")[:2]
        return list(range(int(arr[0])-1, int(arr[1])))
        
    def process_base_partners(self, base_partners: dto.DataFrameOptimized) -> None:
        """Process base of partners

        the next process does:
            Extract the columns and table of base_partners, then create three mask with the next rules:
                column[0,1,2] has a number inside
            then validate for directa:
                if "cliente" or "loc" has been found with numerical values
            then validate for indirecta:
                if "cliente" and "agente" has been found with numerical values
            then create a new column called "socios" with False or True based on the above rules
        Args:
            base_partners (dto.DataFrameOptimized): base of partners
        """
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
        
        #convert into number 
        if feature_flags.ENVIROMENT == "DEV":
            base[columns[0]] = np.vectorize(func.mask_number)(base[columns[0]].astype(str)) #cod_cliente
            base[columns[1]] = np.vectorize(func.mask_number)(base[columns[1]].astype(str)) #cod_loc
            base[columns[2]] = np.vectorize(func.mask_number)(base[columns[2]].astype(str)) #cod_agente

        self.table = base[[*columns[:3], "socios"]]

    def process_base_coords(self, base_coords: dto.DataFrameOptimized) -> None:
        """Process base of coords

        the next process does:
            Extract the columns and table of base_coords
            then same extract the columns and table of self.table.
            then convert in number the columns 0,3,4.
            then extract coords for base directa and does the same with indirecta
            then delete column 4
            and finally merge both bases and saved in self.table
        Args:
            base_coords (dto.DataFrameOptimized): base of coords
        """

        columns_coords = base_coords.table.columns.to_list()
        columns_general = self.table.columns.to_list()

        table_coords = base_coords.table
        table_general = self.table

        #convert into number 
        if feature_flags.ENVIROMENT == "DEV":
            table_coords[columns_coords[0]] = np.vectorize(func.mask_number)(table_coords[columns_coords[0]].astype(str)) #cod_cliente
            table_coords[columns_coords[3]] = np.vectorize(func.mask_number)(table_coords[columns_coords[3]].astype(str)) #cod_loc
            table_coords[columns_coords[4]] = np.vectorize(func.mask_number)(table_coords[columns_coords[4]].astype(str)) #cod_agente

        coords_indirecta = table_general.merge(
            right=table_coords[columns_coords[1:]],
            right_on=[columns_coords[3], columns_coords[4]], #agente y codigo ecom
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
        """Processes the bases_universe columns and returns a new list of bases .

        Args:
            bases_universe (list): [description]
            types (list): [description]
        """
        bases = []
        columns_general = self.table.columns.to_list()
        table_general = self.table

        for base, type in zip(bases_universe, types):
            table_universe = base.table
            columns_universe = base.table.columns

            if type == TYPE_CLUSTERS.DIRECTA.value:
                #convert into number
                if feature_flags.ENVIROMENT == "DEV":
                    table_universe[columns_universe[0]] = np.vectorize(func.mask_number)(table_universe[columns_universe[0]].astype(str)) #cod_cliente

                res_base = table_general.merge(
                    right=table_universe, #cod_cliente, ...coords
                    right_on=columns_universe[0], #cod_cliente
                    left_on= columns_general[0], #cod_cliente
                    how="left"
                )

                #delete not found
                found = ~pd.isna(res_base[columns_universe[1:]]).all(axis=1)

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
                if feature_flags.ENVIROMENT == "DEV":
                    table_universe[columns_universe[2]] = np.vectorize(func.mask_number)(table_universe[columns_universe[2]].astype(str)) #cod_jefe

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

    async def process_bases_query(self, bases_query: 'list(list[dto.DataFrameOptimized])', types: 'list(str)', lots: int = 6) -> None:
        ""
        bases = []
        columns_general = self.table.columns.to_list()
        table_general = self.table

        for base, type in zip(bases_query, types):

            if type == TYPE_CLUSTERS.DIRECTA.value:

                table_query = base[0].table
                columns = table_query.columns.tolist()

                #standardize year format
                mask_no_empty_months = ~pd.isna(table_query[columns[1]])
                table_query.loc[mask_no_empty_months, columns[1]] = \
                    np.vectorize(lambda x: f"{str(x).split('.')[0]}.{str(x).split('.')[1].zfill(2)}")(table_query.loc[mask_no_empty_months, columns[1]])

                new_base = table_query.drop_duplicates(columns[:3]).pivot(index=[columns[0], columns[2]], columns=columns[1], values=columns[3:]).fillna(0)

                #change columns for the same 
                columns_query = ["_".join(map(str, cols)) for cols in new_base.columns]
                new_base.columns = columns_query

                #unique dates
                dates = sorted(np.unique([v.group(0) for col in columns_query if \
                    (v:=re.search(r"\d{4}\.\d{1,2}", str(col))) is not None]))

                #change columns names for standar "type_num"
                new_base.rename(columns={f"{year}": f"{'_'.join(year.split('_')[:-1])}_{dates.index(v.group(0))+1}" \
                    for year in columns_query if (v:=re.search(r"\d{4}\.\d{1,2}", str(year))) is not None}, inplace=True)

                new_base.reset_index(inplace=True, level = new_base.index.names)

                cols_pesos = sorted([col for col in new_base.columns.tolist() if "venta_pesos" in col.lower()], key=lambda x: func.mask_number(x))
                cols_kilos = sorted([col for col in new_base.columns.tolist() if "venta_kilos" in col.lower()], key=lambda x: func.mask_number(x))

                #remove nan values
                new_base[[*cols_pesos, *cols_kilos]] = new_base[[*cols_pesos, *cols_kilos]].fillna(0)

                #group by "agente"
                base_agents = new_base.groupby(new_base.columns.tolist()[0], as_index=False).agg({
                    f"{column}": "sum" for column in [*cols_pesos, *cols_kilos]
                })
                
                #get active months
                months_cols = ["meses_ant_activos", "meses_act_activos"]
                base_agents[[*months_cols, "status"]] = np.apply_along_axis(lambda row: self.get_active_months(row, lots), 1, base_agents[cols_pesos].values)

                new_base[[*months_cols, "status"]] = new_base.merge(
                    right=base_agents[[base_agents.columns[0], *months_cols, "status"]], 
                    right_on=base_agents.columns[0], #cod_agente
                    left_on= new_base.columns[0], #cod_agente
                    how="left"
                )[[*months_cols, "status"]]

                #get prom of sales
                pool = ThreadPool(processes=2)
                results = [pool.apply_async(lambda: np.apply_along_axis(lambda row: self.get_average(row, lots, months_cols), 1, new_base[[*cols_pesos, *months_cols]].values), ()),
                            pool.apply_async(lambda: np.apply_along_axis(lambda row: self.get_average(row, lots, months_cols), 1, new_base[[*cols_kilos, *months_cols]].values), ())
                ]
                new_base[["prom_ant_pesos", "prom_act_pesos"]] = results[0].get()
                new_base[["prom_ant_kilos", "prom_act_kilos"]] = results[1].get()

                if feature_flags.ENVIROMENT == "DEV":
                    new_base[new_base.columns[0]] = np.vectorize(func.mask_number)(new_base[new_base.columns[0]].astype(str)) #cod_cliente

                #merge with principal table
                res_base = table_general.merge(
                    right=new_base, #cod_cliente, ...coords
                    right_on=new_base.columns[0], #cod_cliente
                    left_on= columns_general[0], #cod_cliente
                    how="left"
                )

                bases.append(
                    res_base
                )
                
            elif type == TYPE_CLUSTERS.INDIRECTA.value:

                base_1 = base[0].table
                base_2 = base[1].table
                columns_query_1 = base_1.columns.tolist()
                columns_query_2 = base_2.columns.tolist()

                max_sale_pesos = max([func.mask_number(sale) for sale in columns_query_1 if "venta_pesos" in sale])

                #rename columns
                base_2.rename(columns={f"{sale}": \
                    f"{sale.replace(str(func.mask_number(sale)), str(max_sale_pesos + func.mask_number(sale)))}" \
                        for sale in columns_query_2 if "venta" in sale}, inplace=True)
                
                #merge both bases year before and year after
                new_base = base_1.merge(
                    right=base_2, 
                    right_on=base_2.columns.tolist()[:3], #cod_agente, cod_ecom
                    left_on= columns_query_1[:3], 
                    how="left"
                )

                cols_pesos = sorted([col for col in new_base.columns.tolist() if "venta_pesos" in col.lower()], key=lambda x: func.mask_number(x))
                cols_kilos = sorted([col for col in new_base.columns.tolist() if "venta_kilos" in col.lower()], key=lambda x: func.mask_number(x))


                new_base[[*cols_pesos, *cols_kilos]] = new_base[[*cols_pesos, *cols_kilos]].fillna(0)

                #group by "cliente"
                base_clients = new_base.groupby([new_base.columns.tolist()[0]], as_index=False).agg({
                    f"{column}": "sum" for column in [*cols_pesos, *cols_kilos]
                })
                
                #get active months
                months_cols = ["meses_ant_activos", "meses_act_activos"]
                base_clients[[*months_cols, "status"]] = base_clients[cols_pesos].apply(lambda row: self.get_active_months(row, lots), axis=1).tolist()

                new_base[[*months_cols, "status"]] = new_base.merge(
                    right=base_clients[[base_clients.columns[0], *months_cols, "status"]], 
                    right_on=base_clients.columns[0], #cod_cliente
                    left_on= new_base.columns[0], #cod_cliente
                    how="left"
                )[[*months_cols, "status"]]

                #get prom of sales
                pool = ThreadPool(processes=2)
                results = [pool.apply_async(lambda: np.apply_along_axis(lambda row: self.get_average(row, lots, months_cols), 1, new_base[[*cols_pesos, *months_cols]].values), ()),
                            pool.apply_async(lambda: np.apply_along_axis(lambda row: self.get_average(row, lots, months_cols), 1, new_base[[*cols_kilos, *months_cols]].values), ())
                ]

                new_base[["prom_ant_pesos", "prom_act_pesos"]] = results[0].get()
                new_base[["prom_ant_kilos", "prom_act_kilos"]] = results[1].get()

                if feature_flags.ENVIROMENT == "DEV":
                    new_base[new_base.columns[0]] = np.vectorize(func.mask_number)(new_base[new_base.columns[0]].astype(str)) #cod_agente
                    new_base[new_base.columns[1]] = np.vectorize(func.mask_number)(new_base[new_base.columns[1]].astype(str)) #cod_ecom

                #merge with principal table
                res_base = table_general.merge(
                    right=new_base, 
                    right_on= new_base.columns.tolist()[:2], #cod_agente, cod_ecom
                    left_on= [columns_general[2], columns_general[0]], #cod_agente, cod_cliente (ecom)
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
        
        #get brands with more sales
        more_sales = self.table
        more_sales["total_pesos"] = more_sales[cols_pesos].sum(axis=1)
        brands_with_more_sales = more_sales.groupby(["marca"], as_index=False).agg({
                    "total_pesos": "sum"
                }).sort_values(by=["total_pesos"], ascending=False).iloc[:4]["marca"].tolist()
        
        #filter only for brands with more sales
        more_sales_pivot = more_sales[more_sales["marca"].isin(brands_with_more_sales)].drop_duplicates(["cod_cliente", "marca"]).pivot(index=["cod_cliente"], columns="marca", \
            values=["prom_ant_pesos", "prom_act_pesos", "prom_ant_kilos", "prom_act_kilos", "status"])
        columns_more_sales = ["_".join(map(str, cols)) for cols in more_sales_pivot.columns]
        more_sales_pivot.columns = columns_more_sales
        more_sales_pivot.reset_index(inplace=True)

        self.table = self.table.merge(
            right=more_sales_pivot, 
            on=more_sales_pivot.columns.tolist()[0], # merge by cod_cliente
            how="left"
        )

    def get_active_months(self, row_values: pd.Series, lots: int, umbral: int = 0.3) -> 'tuple(float, float, str)':
        """Get months active based on rules

        Args:
            row_values (pd.Series): values 
            lots (int): size of division values.

                Example: lots = 6, size_values = 18
                actual: 18-12
                anterior: 11-5
                viejo: 0-4

            umbral (int, optional): umbral of valid active lots. Defaults to 0.3.

        Returns:
            tuple(float, float, str): months active before, months active now, status
        """
        type_status = [
            "Nunca ha comprando", 
            "Empezo a comprar", 
            "Dejo de comprar", 
            "Sigue comprando"
        ]
        group_old = np.array(row_values[:len(row_values)-(lots*2)], dtype=np.float64)
        group_ant = np.array(row_values[len(row_values)-(lots*2):len(row_values)-lots], dtype=np.float64)
        group_act = np.array(row_values[len(row_values)-lots:], dtype=np.float64)
        
        size_old = group_old.size
        end_moth_old = size_old if (v:=np.where(group_old > 0)[0]).size == 0 else v[0]
        old_quantity = (size_old - end_moth_old)/size_old

        size_ant = group_ant.size
        end_moth_ant = size_ant if (v:=np.where(group_ant > 0)[0]).size == 0 else v[0]
        ant_quantity = (size_ant - end_moth_ant)/size_ant

        size_act = group_act.size
        end_moth_act = size_act if (v:=np.where(group_act > 0)[0]).size == 0 else v[0]
        act_quantity = (size_act - end_moth_act)/size_act

        end_moth_act, end_moth_ant = 0, 0
        status = type_status[0]

        if old_quantity > umbral:
            if ant_quantity > umbral and end_moth_ant <= lots//2:
                end_moth_ant = (lots-end_moth_ant)
                status = type_status[2]
                if act_quantity > umbral and end_moth_act <= lots//2:
                    status = type_status[3]
                    end_moth_ant = lots
                    end_moth_act = lots
        else:
            if ant_quantity > umbral and end_moth_ant <= lots//2:
                end_moth_ant = (lots-end_moth_ant)
                if act_quantity > umbral and end_moth_act <= lots//2:
                    end_moth_ant = lots
                    end_moth_act = lots
                    status = type_status[3]
                else:
                    status = type_status[2]
            elif act_quantity > umbral and end_moth_act <= lots//2:
                status = type_status[1]

        return end_moth_ant, end_moth_act, status

    def get_average(self, row_values: pd.Series, lots: int, cols_months: list[str]) -> 'tuple(float, float)':
        """get average of rows with values

            first split the row with lots of months, then validate if months active has a number and make the formula

        Returns:
            tuple(float, float): result of average
        """
        len_without_months = len(row_values)-len(cols_months)
        months = np.array(row_values[len_without_months:], dtype=np.float64) #ant, act
        group_act = np.array(row_values[len_without_months-lots:len_without_months], dtype=np.float64)
        group_ant = np.array(row_values[len_without_months-(lots*2):len_without_months-lots], dtype=np.float64)

        prom_ant, prom_act = 0, 0
        if months[0] != 0 and months[0] != np.nan:
            prom_ant = np.sum(group_ant)/months[0]
        
        if months[1] != 0 and months[1] != np.nan:
            prom_act = np.sum(group_act)/months[1]

        return prom_ant, prom_act

    async def merge_all(self, bases: 'dict[str, dto.DataFrameOptimized]', order: 'list[str]'):
        """Merge all bases

        Args:
            bases (dict[str, dto.DataFrameOptimized]): list of bases to merge
            order (list[str]): order to merge
        """
        pair_bases = []
        pbar = tqdm(total=4)
        for key in order:
            if "socios" in key.lower():
                pbar.write(f'procesando la base de socios...')
                self.process_base_partners(bases[key]) 
                pbar.update(1)

            elif "coordenadas" in key.lower():
                pbar.write(f'procesando la base de coordenadas...')
                self.process_base_coords(bases[key])
                pbar.update(1)

            elif "universo_directa" in key.lower():
                pair_bases.append(bases[key])

            elif "universo_indirecta" in key.lower():
                pbar.write(f'procesando la base de universos...')
                pair_bases.append(bases[key])
                self.process_bases_universe(pair_bases, types=(TYPE_CLUSTERS.DIRECTA.value, TYPE_CLUSTERS.INDIRECTA.value))
                pair_bases = []
                pbar.update(1)

            elif "consulta_directa" in key.lower():
                pair_bases.append(bases[key])

            elif "consulta_indirecta" in key.lower():
                pbar.write(f'procesando las bases de consulta...') 
                pair_bases.append(bases[key])
                await self.process_bases_query(pair_bases, types=(TYPE_CLUSTERS.DIRECTA.value, TYPE_CLUSTERS.INDIRECTA.value))
                pbar.update(1)

        pbar.close()

    @staticmethod
    def preprocess_base(path: 'str|list', properties: dict) -> 'dto.DataFrameOptimized|list[dto.DataFrameOptimized]':
        """Preprocess Base 

            for file:
                - It'll extract indices and names of columns, then process converters and skiprows finally get the dataframe
            for folder:
                - It'll extract num of bases based in num of list columns, then iterate through the files on the path and it'll extract actual year and based this
                year extract indice of column, then replace columns and extract the dataframe then be saved inside the list
        Raises:
            ValueError: if properties.type == "folder" isn't has a path iterable

        Returns:
            dto.DataFrameOptimized|list[dto.DataFrameOptimized]: one dataframe for properties.type == "file" or
                                                                    list of dataframe for properties.type == "folder"
        """
        if properties["type"] == "file":
            converters = properties["converters"] if "converters" in properties.keys(
            ) else None
            columns = properties["columns"] if "columns" in properties.keys(
            ) else None
            skiprows = properties["skiprows"] if "skiprows" in properties.keys(
            ) else None
            encoding = properties["encoding"] if "encoding" in properties.keys(
            ) else "utf-8"
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
                        temp_headers_init = [v-(min(temp_headers)-1) for v in temp_headers]

                        header_idx.extend(temp_headers)
                        header_names.extend([f"{col['column']}_{head}" for head in temp_headers_init])
            
            if converters is not None:
                converters = Cluster.process_converters(converters, header_names)

            if skiprows is not None:
                skiprows = skiprows[:2] if isinstance(skiprows, (list, tuple)) else [
                    int(skiprows), -1]

            base = dto.DataFrameOptimized.get_table_excel(
                    path, properties["sheet"], 
                    header_idx=header_idx, 
                    skiprows=skiprows,  
                    converters=converters,
                    encoding=encoding, 
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
    def process_converters(converters: dict, columns: list[str]) -> dict:
        """Change string converters to specific type

        Args:
            converters (dict): dict of converters

            Example:
                {
                    ...,
                    "column": "text"
                }
                result:
                {
                    ...,
                    "column": <func.mask_text>
                }

        Returns:
            dict: converters with function for convert
        """
        _converters = {}
        for key, conv in converters.items():
            columns_match = [col for col in columns if col in key]
            for col_match in columns_match:
                if conv.lower() == "number":
                    _converters[col_match] = func.mask_number
                elif conv.lower() == "text":
                    _converters[col_match] = func.mask_string
        return _converters