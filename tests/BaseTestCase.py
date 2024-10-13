import unittest
from zipfile import ZipFile


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        zip = ZipFile(self.ZIP_FILE).extractall("./tests/data")

    ZIP_FILE = "./tests/data/annual-enterprise-survey-2023-financial-year-provisional.zip"
    CSV_FILE = "./tests/data/annual-enterprise-survey-2023-financial-year-provisional.csv"
    TEST_FEATHER_FILE = "/tmp/data-flow.feather"
    TEST_PARQUET_FILE = "/tmp/data-flow.parquet"
    TEST_CSV_FILE = "/tmp/data-flow.csv"
    TEST_JSON_FILE = "/tmp/data-flow.json"
    TEST_HDF_FILE = "/tmp/data-flow.h5"
