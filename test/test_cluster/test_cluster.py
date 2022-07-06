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
            if(v:=re.search(r"(?<=progress_).*(?=\..*)", file)) is not None:
                result[v.group(0)] = pd.read_pickle(os.path.join(path_validation, file))
        return result

    @test_utils.async_test
    async def get_bases(self, config: dict = None, files_raw: dict[str, str] = None, path_test: str = None, path_validation: str = None, base: int = 0, types: bool = ["raw", "test"]) :
        raw, test, validation = None, None, None

        if "raw" in types:
            if base not in self.cache_raw.keys():
                raw = await test_utils.get_bases(sources = config["sources"], files = files_raw, cached_data = False)
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
        """Test actual bases in project

            Expected:
            <feature flag> ACTUAL_BASES with the number of group bases
            <feature flag> ENVIROMENT with TEST inside

            Return: None if all test pas
        """

        route_test_files = os.path.join(test_const.TEST_FILES, f"bases_{ft.ACTUAL_BASES}")
        route_raw_files = os.path.join(const.ROOT_DIR, f"files/Bases")
        config = utils.get_config(os.path.join(const.ROOT_DIR, "config.yml"))

        files_raw = test_utils.get_predeterminated_files(route_raw_files)

        print("processing bases...")
        bases_raw, bases_test, _ = self.get_bases(config, files_raw = files_raw, path_test = route_test_files, base = ft.ACTUAL_BASES)

        for key in bases_raw.keys():
            if utils.is_iterable(bases_raw[key]):
                for base_left, base_right in zip(bases_raw[key], bases_test[key]):
                    pdtest.assert_frame_equal(
                        base_left.table,
                        base_right.table,
                        check_dtype = False,
                        check_less_precise = 2,
                        check_names = False
                    )
            else:
                pdtest.assert_frame_equal(
                    bases_raw[key].table,
                    bases_test[key].table,
                    check_dtype = False,
                    check_less_precise = 2,
                    check_names = False
                )

    @test_utils.async_test
    async def test_multiple_bases(self) -> None:
        """Test multiples bases inside the folder files

            Expected:
            inside route test\files almost has one folder base_n with files

            Return: None if all test pas
        """
        for dir in os.listdir(test_const.TEST_FILES):

            base = int(re.search("\d+", dir).group(0))
            root_dir = os.path.join(test_const.TEST_FILES, dir)
            config = utils.get_config(os.path.join(root_dir, f"config.yml"))
            cluster = Cluster()

            print(f"processing bases...{base}")

            _, files_test, files_val = self.get_bases(
                config = config,
                path_test = root_dir, 
                path_validation = os.path.join(root_dir, "result"),
                base = base,
                types = ["test", "validation"])

            pair_bases = []

            for key in config["order_base"]:
                if "socios" in key.lower():
                    cluster.process_base_partners(files_test[key]) 
                    pdtest.assert_frame_equal(
                        files_val["base_socios"],
                        cluster.table,
                        check_dtype = False,
                        check_less_precise = 2,
                        check_names = False
                    )
                elif "coordenadas" in key.lower():
                    cluster.process_base_coords(files_test[key]) 
                    pdtest.assert_frame_equal(
                        files_val["base_coordenadas"],
                        cluster.table,
                        check_dtype = False,
                        check_less_precise = 2,
                        check_names = False
                    )
                elif "universo_directa" in key.lower():
                    pair_bases.append(files_test[key])

                elif "universo_indirecta" in key.lower():
                    pair_bases.append(files_test[key])
                    cluster.process_bases_universe(pair_bases, types=(TYPE_CLUSTERS.DIRECTA.value, TYPE_CLUSTERS.INDIRECTA.value))
                    pdtest.assert_frame_equal(
                        files_val["base_universo"],
                        cluster.table,
                        check_dtype = False,
                        check_less_precise = 2,
                        check_names = False
                    )
                    pair_bases = []

                elif "consulta_directa" in key.lower():
                    pair_bases.append(files_test[key])

                elif "consulta_indirecta" in key.lower():
                    pair_bases.append(files_test[key])
                    await cluster.process_bases_query(pair_bases, types=(TYPE_CLUSTERS.DIRECTA.value, TYPE_CLUSTERS.INDIRECTA.value))
                    pdtest.assert_frame_equal(
                        files_val["base_consulta"],
                        cluster.table,
                        check_dtype = False,
                        check_less_precise = 2,
                        check_names = False
                    )

            cluster.post_process_base(config["final_base"])
            pdtest.assert_frame_equal(
                        files_val["base_final"],
                        cluster.table,
                        check_dtype = False,
                        check_less_precise = 2,
                        check_names = False
                    )

    def test_all(self) -> None:
        """Test all steps of bases

            Expected:
            <feature flag> ENVIROMENT with TEST inside

            Return: None if all test pas
        """
        self.test_process_base_universe()
        self.test_process_base_partners()
        self.test_process_base_coords()
        self.test_process_base_queries()

    def test_process_base_universe(self) -> None:
        """Test all groups of bases only in process: "proceso de bases de universo"

            Expected:
            <feature flag> ENVIROMENT with TEST inside

            Return: None if all test pas
        """
        for dir in os.listdir(test_const.TEST_FILES):

            base = int(re.search("\d+", dir).group(0)) 
            root_dir = os.path.join(test_const.TEST_FILES, dir)
            config = utils.get_config(os.path.join(root_dir, f"config.yml"))
            cluster = Cluster()

            _, files_test, files_val = self.get_bases(
                config = config,
                path_test = root_dir, 
                path_validation = os.path.join(root_dir, "result"),
                base = base,
                types = ["test", "validation"])

            cluster.process_bases_universe([files_test["base_universo_directa"], files_test["base_universo_indirecta"]],
                types = [TYPE_CLUSTERS.DIRECTA.value, TYPE_CLUSTERS.INDIRECTA.value])

            pdtest.assert_frame_equal(
                files_val["base_universo"],
                cluster.table,
                check_dtype = False,
                check_less_precise = 2,
                check_names = False
            )

    def test_process_base_partners(self) -> None:
        """Test all groups of bases only in process: "proceso de bases de universo"

            Expected:
            <feature flag> ENVIROMENT with TEST inside

            Return: None if all test pas
        """
        for dir in os.listdir(test_const.TEST_FILES):
            
            base = int(re.search("\d+", dir).group(0)) 
            root_dir = os.path.join(test_const.TEST_FILES, dir)
            config = utils.get_config(os.path.join(root_dir, f"config.yml"))
            previous_cluster = Cluster(table = pd.read_pickle(os.path.join(root_dir, "result/progress_base_universo.pkl")))

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
                check_names = False
            )

    def test_process_base_coords(self) -> None:
        """Test all groups of bases only in process: "proceso de bases de coordenadas"

            Expected:
            <feature flag> ENVIROMENT with TEST inside

            Return: None if all test pas
        """
        for dir in os.listdir(test_const.TEST_FILES):

            base = int(re.search("\d+", dir).group(0)) 
            root_dir = os.path.join(test_const.TEST_FILES, dir)
            config = utils.get_config(os.path.join(root_dir, f"config.yml"))
            previous_cluster = Cluster(table = pd.read_pickle(os.path.join(root_dir, "result/progress_base_socios.pkl")))

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
                check_names = False
            )

    @test_utils.async_test
    async def test_process_base_queries(self) -> None:
        """Test all groups of bases only in process: "proceso de bases de consulta"

            Expected:
            <feature flag> ENVIROMENT with TEST inside

            Return: None if all test pas
        """
        for dir in os.listdir(test_const.TEST_FILES):

            base = int(re.search("\d+", dir).group(0)) 
            root_dir = os.path.join(test_const.TEST_FILES, dir)
            config = utils.get_config(os.path.join(root_dir, f"config.yml"))
            previous_cluster = Cluster(table = pd.read_pickle(os.path.join(root_dir, "result/progress_base_coordenadas.pkl")))

            _, files_test, files_val = self.get_bases(
                config = config,
                path_test = root_dir, 
                path_validation = os.path.join(root_dir, "result"),
                base = base,
                types = ["test", "validation"])

            await previous_cluster.process_bases_query([files_test["base_consulta_directa"], files_test["base_consulta_indirecta"]],
                types = [TYPE_CLUSTERS.DIRECTA.value, TYPE_CLUSTERS.INDIRECTA.value])

            pdtest.assert_frame_equal(
                files_val["base_consulta"],
                previous_cluster.table,
                check_dtype = False,
                check_less_precise = 2,
                check_names = False
            )

    def test_process_converters(self) -> None:
        """Test function to process the converters

            Expected:
            <feature flag> ENVIROMENT with TEST inside

            Return: None if all test pas
        """
        convert = [{"key1":"text"}, {"key2":"number"}, {"key3":"float"}]
        conv_expected = [func.mask_string, func.mask_number, func.mask_float]
        for conv, expect in zip(convert, conv_expected):
            res = Cluster.process_converters(conv, None)
            self.assertEqual(res[list(conv.keys())[0]], expect)

    @classmethod   
    def tearDown(cls) -> None:
        #clear data
        del cls.cache_raw
        del cls.cache_test