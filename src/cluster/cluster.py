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

        base["socios"] = "no"
        base.loc[(mask_directa | mask_indirecta), "socios"] = "si" 
        
        #convert into number 
        if feature_flags.ENVIROMENT == "DEV":
            base[columns[0]] = np.vectorize(func.mask_number)(base[columns[0]].astype(str)) #cod_cliente
            base[columns[1]] = np.vectorize(func.mask_number)(base[columns[1]].astype(str)) #cod_loc
            base[columns[2]] = np.vectorize(func.mask_number)(base[columns[2]].astype(str)) #cod_agente


        base_indi = self.table[self.table["atencion"] == "Indirecta"]
        base_dir = self.table[self.table["atencion"] == "Directa"]
        
        dir_merge = pd.concat([base_dir.dropna(subset=[columns[0]]).merge(base[[columns[0], "socios"]].drop_duplicates(), on=columns[0], how="left"),
                    base_dir.dropna(subset=[columns[0]]).merge(base[[columns[1], "socios"]].drop_duplicates(), left_on=columns[0], right_on=columns[1], how="left")],
                    axis=0, ignore_index=True).drop_duplicates(subset=columns[:2])

        #delete column "cod_loc"
        dir_merge.drop(columns[1], axis=1, inplace=True)

        #cod_cliente, cod_ac
        indi_merge = base_indi.dropna(subset=[columns[0], columns[2]]).merge(base[[columns[0], columns[2], "socios"]].drop_duplicates(), on=[columns[0], columns[2]], how="left")\
            .drop_duplicates(subset=[columns[0], columns[2]])

        new_base = pd.concat((dir_merge, indi_merge), axis=0, ignore_index=True)

        partners_not_found = pd.isna(new_base["socios"]) 
        new_base.loc[partners_not_found, "socios"] = "no"
        new_base.drop_duplicates(inplace=True)

        self.table = new_base

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
            # table_coords[columns_coords[3]] = np.vectorize(func.mask_number)(table_coords[columns_coords[3]].astype(str)) #cod_loc
            table_coords[columns_coords[3]] = np.vectorize(func.mask_number)(table_coords[columns_coords[3]].astype(str)) #cod_agente
            table_coords[columns_coords[4]] = np.vectorize(func.mask_number)(table_coords[columns_coords[4]].astype(str)) #cod_ecom

        cols_merge_indi = ["cod_agente", "cod_ecom"]
        coords_indirecta = table_general.merge(
            right=table_coords[columns_coords[1:]].drop_duplicates(subset=cols_merge_indi),
            right_on=cols_merge_indi, #cod_agente y codigo ecom
            left_on=["cod_agente", "cod_cliente"], #cod_agente y cod_cliente (ecom)
            how="left"
        )

        #delete "cod_ecom" column
        coords_indirecta.drop(columns_coords[4], axis = 1, inplace = True) 


        coords_directa = table_general.merge(
            right=table_coords[[columns_coords[0], *columns_coords[1:3]]].drop_duplicates(subset=columns_coords[0]), #cod_cliente, ...coords
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

        for base, type in zip(bases_universe, types):
            table_universe = base.table
            columns_universe = base.table.columns

            if type == TYPE_CLUSTERS.DIRECTA.value:
                #convert into number
                if feature_flags.ENVIROMENT == "DEV":
                    table_universe[columns_universe[0]] = np.vectorize(func.mask_number)(table_universe[columns_universe[0]].astype(str)) #cod_cliente

                #delete not found
                found = ~pd.isna(table_universe[columns_universe[1:]]).all(axis=1)

                res_base = table_universe[found]

                #get repeated "vendedores"
                mask_repeated = res_base.duplicated(subset=columns_universe[0], keep='first')
                base_duplicated = res_base[mask_repeated].rename(columns={f"{columns_universe[3]}": f"{columns_universe[3]}_2", f"{columns_universe[4]}": f"{columns_universe[4]}_2"})
                columns_duplicated = base_duplicated.columns.to_list()

                #add the repeated "vendedores"
                new_base = res_base[~mask_repeated].merge(
                    right=base_duplicated[[columns_duplicated[0], f"{columns_universe[3]}_2", f"{columns_universe[4]}_2"]], 
                    on= columns_universe[0], #cod_cliente
                    how="left"
                )
                new_base.drop_duplicates(inplace=True)

                #add type of "atencion"
                new_base["atencion"] = "Directa"

                bases.append(
                    new_base
                )
            elif type == TYPE_CLUSTERS.INDIRECTA.value:
                
                #convert into number
                if feature_flags.ENVIROMENT == "DEV":
                    table_universe[columns_universe[0]] = np.vectorize(func.mask_number)(table_universe[columns_universe[0]].astype(str)) #cod_agente

                #create columns "cod_jefe" and "jefe" with "cod_agente" and "agente"
                table_universe[["cod_jefe", "jefe"]] = table_universe[columns_universe[:2]]
                table_universe.drop_duplicates(inplace=True)

                #add type of "atencion"
                table_universe["atencion"] = "Indirecta"

                #create empty columns 
                table_universe[utils.get_diff_list((bases[0].columns.tolist(), table_universe.columns.tolist()), "left")] = np.nan

                bases.append(
                    table_universe
                )

        self.table = pd.concat(bases, ignore_index=True, axis=0)

    async def process_bases_query(self, bases_query: 'list(list[dto.DataFrameOptimized])', types: 'list(str)', lots: int = 6) -> None:
        ""
        bases = []
        general_bases = []
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

                #group values of sales
                if feature_flags.ENVIROMENT == "DEV":
                    table_query[table_query.columns[0]] = np.vectorize(func.mask_number)(table_query[table_query.columns[0]].astype(str)) #cod_cliente

                group_base = table_query.groupby(columns[:3], as_index=False).agg({columns[3]: "sum", columns[4]: "sum"})
                new_base = group_base.pivot_table(index=[columns[0], columns[2]], columns=columns[1], values=columns[3:], aggfunc="sum").fillna(0)

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

                #group by "cliente"
                base_clients = new_base.groupby(new_base.columns.tolist()[0], as_index=False).agg({
                    f"{column}": "sum" for column in [*cols_pesos, *cols_kilos]
                })

                #get active months
                months_cols = ["meses_ant_activos", "meses_act_activos"]
                base_clients[months_cols] = np.apply_along_axis(lambda row: self.get_active_months(row, lots), 1, base_clients[cols_pesos].values)

                new_base[months_cols] = new_base.merge(
                    right=base_clients[[base_clients.columns[0], *months_cols]], 
                    right_on=base_clients.columns[0], #cod_cliente
                    left_on= new_base.columns[0], #cod_cliente
                    how="left"
                )[months_cols]

                #get prom of sales
                pool = ThreadPool(processes=4)
                results = [pool.apply_async(lambda: np.apply_along_axis(lambda row: self.get_average(row, lots, months_cols), 1, new_base[[*cols_pesos, *months_cols]].values), ()),
                    pool.apply_async(lambda: np.apply_along_axis(lambda row: self.get_average(row, lots, months_cols), 1, new_base[[*cols_kilos, *months_cols]].values), ()),
                    pool.apply_async(lambda: np.apply_along_axis(lambda row: self.get_average(row, lots, months_cols), 1, base_clients[[*cols_pesos, *months_cols]].values), ()),
                    pool.apply_async(lambda: np.apply_along_axis(lambda row: self.get_average(row, lots, months_cols), 1, base_clients[[*cols_kilos, *months_cols]].values), ())
                ]

                new_base[["prom_ant_pesos", "prom_act_pesos", "status"]] = results[0].get()
                new_base[["prom_ant_kilos", "prom_act_kilos", "status"]] = results[1].get()
                #save the prom by client
                base_clients[["prom_ant_pesos", "prom_act_pesos", "status"]] = results[2].get()
                base_clients[["prom_ant_kilos", "prom_act_kilos"]] = results[3].get()[:,:2]
                general_bases.append(base_clients)

                pool.close()

                #drop empty registers
                new_base.dropna(thresh=new_base.shape[1]-2, inplace=True) # -2 omit "cod_cliente" and "marca"

                #merge with principal table
                only_direct = table_general["atencion"] == "Directa"
                res_base = table_general.loc[only_direct].merge(
                    right=new_base, #cod_cliente, ...coords
                    right_on=new_base.columns[0], #cod_cliente
                    left_on= columns_general[0], #cod_cliente
                    how="left"
                )

                #get only required columns
                only_cols_required = [col for col in res_base.columns if col not in cols_pesos and col not in cols_kilos]
                res_base = res_base[only_cols_required]

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
                
                if feature_flags.ENVIROMENT == "DEV":
                    base_1[base_1.columns[0]] = np.vectorize(func.mask_number)(base_1[base_1.columns[0]].astype(str)) #cod_agente
                    base_1[base_1.columns[1]] = np.vectorize(func.mask_number)(base_1[base_1.columns[1]].astype(str)) #cod_ecom
                    base_2[base_2.columns[0]] = np.vectorize(func.mask_number)(base_2[base_2.columns[0]].astype(str)) #cod_agente
                    base_2[base_2.columns[1]] = np.vectorize(func.mask_number)(base_2[base_2.columns[1]].astype(str)) #cod_ecom


                #merge both bases year before and year after
                new_base = base_1.merge(
                    right=base_2, 
                    right_on=base_2.columns.tolist()[:3], #cod_agente, cod_ecom, marca
                    left_on= columns_query_1[:3], 
                    how="left"
                )

                cols_pesos = sorted([col for col in new_base.columns.tolist() if "venta_pesos" in col.lower()], key=lambda x: func.mask_number(x))
                cols_kilos = sorted([col for col in new_base.columns.tolist() if "venta_kilos" in col.lower()], key=lambda x: func.mask_number(x))

                # delete
                # #group by "cod_agente", "cod_ecom" and "marca"
                # new_base = new_base.groupby(new_base.columns[:3].tolist(), as_index=False).agg({
                #         f"{column}": "sum" for column in [*cols_pesos, *cols_kilos]
                # })

                new_base[[*cols_pesos, *cols_kilos]] = new_base[[*cols_pesos, *cols_kilos]].fillna(0)

                #group by "agente"
                base_agents = new_base.groupby(new_base.columns[:2].tolist(), as_index=False).agg({
                    f"{column}": "sum" for column in [*cols_pesos, *cols_kilos]
                })
                

                #get active months
                months_cols = ["meses_ant_activos", "meses_act_activos"]
                base_agents[months_cols] = np.apply_along_axis(lambda row: self.get_active_months(row, lots), 1, base_agents[cols_pesos].values) 

                new_base[months_cols] = new_base.merge(
                    right=base_agents[[*base_agents.columns[:2], *months_cols]], 
                    right_on=base_agents.columns[:2].tolist(), #cod_agente, cod_ecom
                    left_on= new_base.columns[:2].tolist(), #cod_agente, cod_ecom
                    how="left"
                )[months_cols]

                #get prom of sales
                pool = ThreadPool(processes=4)
                results = [pool.apply_async(lambda: np.apply_along_axis(lambda row: self.get_average(row, lots, months_cols), 1, new_base[[*cols_pesos, *months_cols]].values), ()),
                    pool.apply_async(lambda: np.apply_along_axis(lambda row: self.get_average(row, lots, months_cols), 1, new_base[[*cols_kilos, *months_cols]].values), ()),
                    pool.apply_async(lambda: np.apply_along_axis(lambda row: self.get_average(row, lots, months_cols), 1, base_agents[[*cols_pesos, *months_cols]].values), ()),
                    pool.apply_async(lambda: np.apply_along_axis(lambda row: self.get_average(row, lots, months_cols), 1, base_agents[[*cols_kilos, *months_cols]].values), ())
                ]

                new_base[["prom_ant_pesos", "prom_act_pesos", "status"]] = results[0].get()
                new_base[["prom_ant_kilos", "prom_act_kilos", "status"]] = results[1].get()
                base_agents[["prom_ant_pesos", "prom_act_pesos", "status"]] = results[2].get()
                base_agents[["prom_ant_kilos", "prom_act_kilos"]] = results[3].get()[:,:2]
                general_bases.append(base_agents)

                pool.close()

                #drop empty registers
                new_base.dropna(thresh=new_base.shape[1]-3, inplace=True) # -3 omit "cod_agente", "cod_ecom" and "marca"

                #merge with principal table
                only_indir = table_general["atencion"] == "Indirecta"
                res_base = table_general.loc[only_indir].merge(
                    right=new_base, 
                    right_on= new_base.columns[:2].tolist(), #cod_agente, cod_ecom
                    left_on= ["cod_agente", "cod_cliente"], #cod_agente, cod_cliente (ecom)
                    how="left"
                )

                #get only required columns
                only_cols_required = [col for col in res_base.columns if col not in cols_pesos and col not in cols_kilos]
                res_base = res_base[only_cols_required]
                
                bases.append(
                    res_base
                )
        

        more_sales = pd.concat(bases, axis=0, ignore_index=True).drop_duplicates()
        
        #get brands with more sales
        more_sales["total_pesos"] = more_sales[['prom_act_pesos']].sum(axis=1)
        brands_with_more_sales = more_sales.groupby(["marca"], as_index=False).agg({
            "total_pesos": "sum"
        }).sort_values(by=["total_pesos"], ascending=False).iloc[:4]["marca"].tolist()
        

        #filter only for brands with more sales "directa"
        values_merge = ["prom_ant_pesos", "prom_act_pesos", "prom_ant_kilos", "prom_act_kilos", "status"]
        sales_dir = bases[0][bases[0]["marca"].isin(brands_with_more_sales)].drop_duplicates(["cod_cliente", "marca"]).pivot(index=["cod_cliente"], columns="marca", \
            values=values_merge)
        sales_dir.columns = [F"{cols[0]}_marca_{brands_with_more_sales.index(cols[1])+1}" for cols in sales_dir.columns]
        sales_dir.reset_index(inplace=True)

        #filter only for brands with more sales "indirecta"
        sales_indir = bases[1][bases[1]["marca"].isin(brands_with_more_sales)].drop_duplicates(["cod_cliente", "cod_agente", "marca"]).pivot(index=["cod_cliente", "cod_agente"], \
            columns="marca", values=values_merge)
        sales_indir.columns = [F"{cols[0]}_marca_{brands_with_more_sales.index(cols[1])+1}" for cols in sales_indir.columns]
        sales_indir.reset_index(inplace=True)

        #merge sales result
        clients_base, agents_base = general_bases
        get_cols_bases = ["meses_ant_activos", "meses_act_activos", "prom_ant_pesos", "prom_act_pesos", "prom_ant_kilos", "prom_act_kilos"]
        clients_base = clients_base[["cod_cliente", *get_cols_bases]].drop_duplicates(["cod_cliente"]).merge(
            right=sales_dir, 
            on=["cod_cliente"], 
            how="left"
        )
        
        agents_base.rename(columns={"cod_ecom":"cod_cliente"}, inplace=True)#change the column name only for merge
        agents_base = agents_base[["cod_cliente", "cod_agente", *get_cols_bases]].drop_duplicates(["cod_cliente", "cod_agente"]).merge(
            right=sales_indir, 
            on=["cod_cliente", "cod_agente"], 
            how="left"
        )

        #merge active months and sales of "clientes"
        group_clients = self.table.merge(
            right=clients_base, 
            on=["cod_cliente"], 
            how="left"
        )


        #merge active months and sales of "agentes"
        self.table = self.combine_columns(
            data=(group_clients, agents_base), 
            suffixes=("_x", "_y"),
            on=["cod_cliente","cod_agente"],
            how="left"    
        )

        #replace NaN by default values
        status_cols = [col for col in self.table.columns if "status" in col]
        prom_cols = [col for col in self.table.columns if "prom" in col]
        self.table[status_cols] = self.table[status_cols].fillna("Nunca ha comprando")
        self.table[prom_cols] = self.table[prom_cols].fillna(0)


        #delete
        # #merge result with principal bases
        # group_clients = self.table.merge(
        #     right=general_bases[0], 
        #     on=general_bases[0].columns.tolist()[0], # merge by cod_cliente
        #     how="left"
        # )

        # self.table = self.combine_columns(
        #     data=(group_clients, general_bases[1]), 
        #     suffixes=("_x", "_y"),
        #     on=general_bases[1].columns.tolist()[:2], # merge by cod_agente and cod_ecom
        #     how="left"    
        # ).merge(
        #     right=more_sales_pivot, 
        #     on=more_sales_pivot.columns.tolist()[0], # merge with brands
        #     how="left"
        # )

    def post_process_base(self, final_base: list) -> None:
        """Post process final base
        """
        columns = self.table.columns

        for idx, _type in enumerate(list(self.table.dtypes)):
            if _type == pd.StringDtype:
                self.table[columns[idx]] = np.vectorize(func.remove_accents)(self.table[columns[idx]].fillna("").astype(str))

        self.table = self.table[final_base]

    def get_active_months(self, row_values: pd.Series, lots: int, umbral: int = 0.3) -> 'tuple(float, float)':
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

        group_old = np.array(row_values[:len(row_values)-(lots*2)], dtype=np.float64)
        group_ant = np.array(row_values[len(row_values)-(lots*2):len(row_values)-lots], dtype=np.float64)
        group_act = np.array(row_values[len(row_values)-lots:], dtype=np.float64)
        

        #apply ReLu function
        group_old = group_old * (group_old > 0)
        group_ant = group_ant * (group_ant > 0)
        group_act = group_act * (group_act > 0)

        size_old = group_old.size
        end_moth_old = size_old if (v:=np.where(group_old > 0)[0]).size == 0 else v[0]
        old_quantity = (size_old - end_moth_old)/size_old

        size_ant = group_ant.size
        months_ant = size_ant if (v:=np.where(group_ant > 0)[0]).size == 0 else v[0]
        ant_quantity = (size_ant - months_ant)/size_ant

        size_act = group_act.size
        months_act = size_act if (v:=np.where(group_act > 0)[0]).size == 0 else v[0]
        act_quantity = (size_act - months_act)/size_act

        end_moth_act, end_moth_ant = 0, 0

        if old_quantity > umbral:
            if ant_quantity > umbral and months_ant <= lots//2:
                end_moth_ant = (lots-months_ant)
                if act_quantity > umbral and months_act <= lots//2:
                    end_moth_ant = lots
                    end_moth_act = lots
        else:
            if ant_quantity > umbral and months_ant <= lots//2:
                end_moth_ant = (lots-months_ant)
                if act_quantity > umbral and months_act <= lots//2:
                    end_moth_ant = lots
                    end_moth_act = lots

        return end_moth_ant, end_moth_act

    def get_average(self, row_values: pd.Series, lots: int, active_months: int) -> 'tuple(float, float, str)':
        """get average of rows with values

            first split the row with lots of months, then validate if months active has a number and make the formula

        Returns:
            tuple(float, float): result of average
        """

        type_status = [
            "Nunca ha comprando", 
            "Empezo a comprar", 
            "Dejo de comprar", 
            "Sigue comprando"
        ]

        length_without_months = len(row_values) - len(active_months)
        months_values = np.array(row_values[length_without_months:], dtype=np.float64)
        group_ant = np.array(row_values[length_without_months-(lots*2):length_without_months-lots], dtype=np.float64)
        group_act = np.array(row_values[length_without_months-lots:length_without_months], dtype=np.float64)

        #apply ReLu function
        group_ant = group_ant * (group_ant > 0)
        group_act = group_act * (group_act > 0)

        prom_ant, prom_act, status = 0, 0, type_status[0]

        if months_values[0] != 0 and months_values[0] != np.nan:
            prom_ant = np.sum(group_ant)/months_values[0]
            status = type_status[2]
        
        if months_values[1] != 0 and months_values[1] != np.nan:
            prom_act = np.sum(group_act)/months_values[1]
            if prom_ant != 0:
                status = type_status[3]
            else:
                status = type_status[1]

        return prom_ant, prom_act, status

    async def merge_all(self, bases: 'dict[str, dto.DataFrameOptimized]', properties: dict) -> None:
        """Merge all bases

        Args:
            bases (dict[str, dto.DataFrameOptimized]): list of bases to merge
            order (list[str]): order to merge
        """
        pair_bases = []
        pbar = tqdm(total=5)
        order = properties["order_base"]

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

        pbar.write(f'post procesando base...')     
        self.post_process_base(properties["final_base"])
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