import os
from utils import feature_flags as ft, constants as const, index as utils
from pandas import testing as pdtest
from test import const as test_const, test_utils
import unittest

class TestClusterClass(unittest.TestCase):
    
    @classmethod
    def setUp(cls) -> None:
        """Initialize the class .
        """
        cls.cache_raw = None
        cls.cache_test = None

    @test_utils.async_test
    async def get_bases(self, config: dict, files_raw: dict[str, str], files_test: dict[str, str]) :
        raw, test = None, None
        if self.cache_raw is None:
            raw = await test_utils.get_bases(config["sources"], files_raw, cached_data=False)
            self.cache_raw = raw
        else:
            raw = self.cache_raw

        if self.cache_test is None:
            test = await test_utils.get_bases(config["sources"], files_test, cached_data=True)
            self.cache_test = test
        else:
            test = self.cache_test
        
        return raw, test
    
    def test_actual_bases(self) -> None:

        route_test_files = os.path.join(test_const.TEST_FILES, f"base_{ft.ACTUAL_BASES}")
        route_raw_files = os.path.join(const.ROOT_DIR, f"files/Bases")
        config = utils.get_config(os.path.join(const.ROOT_DIR, "/config.yml"))

        files_raw = test_utils.get_predeterminated_files(route_raw_files)
        files_test = test_utils.get_predeterminated_files(route_test_files)

        print("processing bases...")
        bases_raw, bases_test = self.get_bases(config, files_raw, files_test)

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
            config = os.path.join(dirs[0], f"base_{count}/config.yml")
            files = test_utils.get_predeterminated_files(os.path.join(dirs[0], f"base_{count}"))

            print(f"processing bases...{count}")
            await test_utils.get_bases(config["sources"], files_test, cached_data=True)
            bases_raw, bases_test = self.get_bases(config, files_raw, files_test)

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

    def test_load_csv_files(self) -> None:
        pass

    def test(self) -> None:
        pass

    @classmethod   
    def tearDown(cls) -> None:
        #clear data
        del cls.cache_raw
        del cls.cache_test