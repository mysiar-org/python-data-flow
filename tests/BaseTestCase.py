import unittest
from zipfile import ZipFile

import pandas as pd


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        ZipFile(self.ZIP_FILE).extractall("./tests/data")

    ZIP_FILE = "./tests/data/annual-enterprise-survey-2023-financial-year-provisional.zip"
    CSV_FILE = "./tests/data/annual-enterprise-survey-2023-financial-year-provisional.csv"
    TEST_FEATHER_FILE = "/tmp/data-flow.feather"
    TEST_PARQUET_FILE = "/tmp/data-flow.parquet"
    TEST_CSV_FILE = "/tmp/data-flow.csv"
    TEST_JSON_FILE = "/tmp/data-flow.json"
    TEST_HDF_FILE = "/tmp/data-flow.h5"

    def assertPandasEqual(self, df1: pd.DataFrame, df2: pd.DataFrame):
        self.assertTrue(df1.equals(df2), "Pandas DataFrames are not equal !")
