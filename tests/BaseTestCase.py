import unittest
from typing import Callable
from zipfile import ZipFile

import pandas as pd

from mysiar_data_flow import DataFlow
from mysiar_data_flow.lib import Operator


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

    def all(self, function: Callable):
        self._sequence(data=function())
        self._filter_Eq(data=function())
        self._filter_Gte(data=function())
        self._filter_Lte(data=function())
        self._filter_Gt(data=function())
        self._filter_Lt(data=function())
        self._filter_Ne(data=function())

    # @count_assertions
    def _sequence(self, data: DataFlow.DataFrame) -> None:
        self.assertPandasEqual(data.to_pandas(), DataFlow().DataFrame().from_csv(self.CSV_FILE).to_pandas())
        polars = data.to_polars()

        self.assertEqual(10, len(data.columns()))

        data.columns_delete(
            [
                "Industry_aggregation_NZSIOC",
                "Industry_code_NZSIOC",
                "Industry_name_NZSIOC",
                "Industry_code_ANZSIC06",
                "Variable_code",
                "Variable_name",
                "Variable_category",
            ]
        )

        self.assertEqual(3, len(data.columns()))
        self.assertListEqual(["Year", "Units", "Value"], data.columns())

        data.columns_rename(columns_mapping={"Year": "_year_", "Units": "_units_"})
        self.assertListEqual(["_year_", "_units_", "Value"], data.columns())

        data.columns_select(columns=["_year_"])
        self.assertListEqual(["_year_"], data.columns())

        self.assertPandasEqual(
            DataFlow().DataFrame().from_polars(polars).to_pandas(),
            DataFlow().DataFrame().from_csv(self.CSV_FILE).to_pandas(),
        )

    def _filter_Eq(self, data: DataFlow.DataFrame) -> None:
        data.filter_on_column(column="Year", operator=Operator.Eq, value=2018)
        self.assertListEqual([2018], list(data.to_pandas().Year.unique()))

    def _filter_Gte(self, data: DataFlow.DataFrame) -> None:
        data.filter_on_column(column="Year", operator=Operator.Gte, value=2018)
        result = data.to_pandas().Year.unique().tolist()
        result.sort()
        self.assertListEqual([2018, 2019, 2020, 2021, 2022, 2023], result)

    def _filter_Lte(self, data: DataFlow.DataFrame) -> None:
        data.filter_on_column(column="Year", operator=Operator.Lte, value=2018)
        result = data.to_pandas().Year.unique().tolist()
        result.sort()
        self.assertListEqual([2013, 2014, 2015, 2016, 2017, 2018], result)

    def _filter_Gt(self, data: DataFlow.DataFrame) -> None:
        data.filter_on_column(column="Year", operator=Operator.Gt, value=2018)
        result = data.to_pandas().Year.unique().tolist()
        result.sort()
        self.assertListEqual([2019, 2020, 2021, 2022, 2023], result)

    def _filter_Lt(self, data: DataFlow.DataFrame) -> None:
        data.filter_on_column(column="Year", operator=Operator.Lt, value=2018)
        result = data.to_pandas().Year.unique().tolist()
        result.sort()
        self.assertListEqual([2013, 2014, 2015, 2016, 2017], result)

    def _filter_Ne(self, data: DataFlow.DataFrame) -> None:
        data.filter_on_column(column="Year", operator=Operator.Ne, value=2018)
        result = data.to_pandas().Year.unique().tolist()
        result.sort()
        self.assertListEqual([2013, 2014, 2015, 2016, 2017, 2019, 2020, 2021, 2022, 2023], result)
