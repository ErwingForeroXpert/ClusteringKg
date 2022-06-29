import os
import re
import pandas as pd
from cluster.cluster import Cluster, func, TYPE_CLUSTERS
from dataframes.dataframe_optimized import DataFrameOptimized
from utils import feature_flags as ft, constants as const, index as utils
from pandas import testing as pdtest
from test import const as test_const, test_utils
import unittest

class TestClusterClass(unittest.TestCase):
    
    @classmethod
    def setUp(cls) -> None:
        """Initialize the class .
        """
        cls.cache_raw = {}
        cls.cache_test = {}
        cls.cache_validation = {}
        cls.cache_cluster = Cluster()
        cls.cache_partner_bases = []

    def get_validation_files(self, path_validation: str) -> dict:

        result = {}
        for file in os.listdir(path_validation):
            if(v:=re.search(r"(?<=progress_).*(?=\.*)", file)) is not None:
                result[v.group(0)] = pd.read_pickle(os.path.join(path_validation, file))
        return result

    @test_utils.async_test
    async def get_bases(self, config: dict = None, files_raw: dict[str, str] = None, path_test: str = None, path_validation: str = None, base: int = 0, types: bool = ["raw", "test"]) :
        raw, test, validation = None, None, None

        if "raw" in types:
            if base not in self.cache_raw.keys():
                raw = await test_utils.get_bases(config["sources"], files_raw, cached_data=False)
                self.cache_raw[base] = raw
            else:
                raw = self.cache_raw[base]

        if "test" in types:
            if base not in self.cache_test.keys():
                test = await test_utils.get_bases(path_test, config["sources"], cached_data=True)
                self.cache_test[base] = test
            else:
                test = self.cache_test[base]
        
        if "validation" in types:
            if base not in self.cache_validation.keys():
                validation = self.get_validation_files(path_validation)
                self.cache_validation[base] = validation
            else:
                validation = self.cache_validation[base]

        return raw, test, validation
    
    def test_actual_bases(self) -> None:

        route_test_files = os.path.join(test_const.TEST_FILES, f"base_{ft.ACTUAL_BASES}")
        route_raw_files = os.path.join(const.ROOT_DIR, f"files/Bases")
        config = utils.get_config(os.path.join(const.ROOT_DIR, "/config.yml"))

        files_raw = test_utils.get_predeterminated_files(route_raw_files)
        files_test = test_utils.get_predeterminated_files(route_test_files)

        print("processing bases...")
        bases_raw, bases_test, _ = self.get_bases(config, files_raw, files_test, ft.ACTUAL_BASES)

        for key in bases_raw.keys():
            if utils.is_iterable(bases_raw[key]):
                for base_left, base_right in zip(bases_raw[key], bases_test[key]):
                    pdtest.assert_frame_equal(
                        base_left,
                        base_right,
                        check_dtype = False,
                        check_less_precise = 2,
                        check_index = False
                    )
            else:
                pdtest.assert_frame_equal(
                    bases_raw[key],
                    bases_test[key],
                    check_dtype = False,
                    check_less_precise = 2,
                    check_index = False
                )

    @test_utils.async_test
    async def test_multiple_bases(self) -> None:
        count = 0
        for _, dirs, _ in os.walk(test_const.TEST_FILES):
            if os.path.exists((root_dir:=os.path.join(dirs[0], f"base_{count}"))):
                config = os.path.join(root_dir, f"config.yml")
                files = test_utils.get_predeterminated_files(root_dir)

                print(f"processing bases...{count}")
                bases = await test_utils.get_bases(config["sources"], files, cached_data=True)
                pair_bases = []

                for key in config["order"]:
                    if "socios" in key.lower():
                        result = self.cache_cluster.process_base_partners(bases[key]) 
                        
                    elif "coordenadas" in key.lower():
                        self.process_base_coords(bases[key])

                    elif "universo_directa" in key.lower():
                        pair_bases.append(bases[key])

                    elif "universo_indirecta" in key.lower():
                        self.process_bases_universe(pair_bases, types=(TYPE_CLUSTERS.DIRECTA.value, TYPE_CLUSTERS.INDIRECTA.value))
                        pair_bases = []

                    elif "consulta_directa" in key.lower():
                        pair_bases.append(bases[key])

                    elif "consulta_indirecta" in key.lower():
                        pair_bases.append(bases[key])
                        await self.process_bases_query(pair_bases, types=(TYPE_CLUSTERS.DIRECTA.value, TYPE_CLUSTERS.INDIRECTA.value))
                count += 1
            else:
                break
                
    def test_all(self) -> None:
        
        self.test_process_base_universe()
        self.test_process_base_partners()
        self.test_process_base_coords()
        self.test_process_base_queries()

    @unittest.skip
    def test_process_base_universe(self) -> None:

        for _, dirs, _ in os.walk(test_const.TEST_FILES):

            base = int(re.search("\d+", dirs[0]).group(0)) 
            root_dir = os.path.join(test_const.TEST_FILES, dirs[0])
            config = utils.get_config(os.path.join(root_dir, f"config.yml"))
            cluster = Cluster()

            _, files_test, files_val = self.get_bases(
                config = config,
                path_test = root_dir, 
                path_validation = os.path.join(root_dir, "result"),
                base = base,
                types = ["raw", "validation"])

            cluster.process_bases_universe([files_test["base_universo_directa"], files_test["base_universo_indirecta"]],
                types = [TYPE_CLUSTERS.DIRECTA, TYPE_CLUSTERS.INDIRECTA])

            pdtest.assert_frame_equal(
                files_val["base_universo"],
                cluster.table,
                check_dtype = False,
                check_less_precise = 2,
                check_index = False
            )

    @unittest.skip
    def test_process_base_partners(self) -> None:
        for _, dirs, _ in os.walk(test_const.TEST_FILES):
            
            base = int(re.search("\d+", dirs[0]).group(0)) 
            root_dir = os.path.join(test_const.TEST_FILES, dirs[0])
            config = utils.get_config(os.path.join(root_dir, f"config.yml"))
            previous_cluster = Cluster(table = pd.read_pickle(os.path.join(root_dir, "progress_base_universo.pkl")))

            _, files_test, files_val = self.get_bases(
                config = config,
                path_test = root_dir, 
                path_validation = os.path.join(root_dir, "result"),
                base = base,
                types = ["test", "validation"])

            previous_cluster.process_base_partners(files_test["base_socios"])

            pdtest.assert_frame_equal(
                files_val["base_socios"],
                previous_cluster.table,
                check_dtype = False,
                check_less_precise = 2,
                check_index = False
            )

    @unittest.skip
    def test_process_base_coords(self) -> None:

        for _, dirs, _ in os.walk(test_const.TEST_FILES):

            base = int(re.search("\d+", dirs[0]).group(0)) 
            root_dir = os.path.join(test_const.TEST_FILES, dirs[0])
            config = utils.get_config(os.path.join(root_dir, f"config.yml"))
            previous_cluster = Cluster(table = pd.read_pickle(os.path.join(root_dir, "progress_base_socios.pkl")))

            _, files_test, files_val = self.get_bases(
                config = config,
                path_test = root_dir, 
                path_validation = os.path.join(root_dir, "result"),
                base = base,
                types = ["test", "validation"])

            previous_cluster.process_base_coords(files_test["base_coordenadas"])

            pdtest.assert_frame_equal(
                files_val["base_coordenadas"],
                previous_cluster.table,
                check_dtype = False,
                check_less_precise = 2,
                check_index = False
            )

    @unittest.skip
    def test_process_base_queries(self) -> None:

        for _, dirs, _ in os.walk(test_const.TEST_FILES):

            base = int(re.search("\d+", dirs[0]).group(0)) 
            root_dir = os.path.join(test_const.TEST_FILES, dirs[0])
            config = utils.get_config(os.path.join(root_dir, f"config.yml"))
            previous_cluster = Cluster(table = pd.read_pickle(os.path.join(root_dir, "progress_base_socios.pkl")))

            _, files_test, files_val = self.get_bases(
                config = config,
                path_test = root_dir, 
                path_validation = os.path.join(root_dir, "result"),
                base = base,
                types = ["test", "validation"])

            previous_cluster.process_base_coords(files_test["base_coordenadas"])

            pdtest.assert_frame_equal(
                files_val["base_coordenadas"],
                previous_cluster.table,
                check_dtype = False,
                check_less_precise = 2,
                check_index = False
            )

    def test_process_converters(self) -> None:
        convert = ["text", "number", "float"]
        conv_expected = [func.mask_string, func.mask_number, func.mask_float]
        for conv, expect in zip(convert, conv_expected):
            res = Cluster.process_converters(conv)
            self.assertIsInstance(res, conv_expected)

    @classmethod   
    def tearDown(cls) -> None:
        #clear data
        del cls.cache_raw
        del cls.cache_test