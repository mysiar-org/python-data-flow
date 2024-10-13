import os
import unittest
from typing import List

from data_flow import DataFlow

DATA_CSV = "/tmp/data_flow.csv"
DATA_FEATHER = "/tmp/data_flow.feather"
DATA_JSON = "/tmp/data_flow.json"
DATA_PARQUET = "/tmp/data_parquet.parquet"
DATA_HDF = "/tmp/data_flow.h5"


def delete_file(filename: str) -> None:
    if os.path.exists(filename):
        os.remove(filename)


def delete_files(files: List[str]) -> None:
    for file in files:
        delete_file(file)


@unittest.skip
class DataFlowTestCase(unittest.TestCase):
    def setUp(self):
        delete_files([DATA_CSV, DATA_FEATHER, DATA_JSON, DATA_PARQUET, DATA_HDF])

        (
            DataFlow()
            .DataFrame()
            .from_csv("./tests/data/annual-enterprise-survey-2023-financial-year-provisional.csv")
            .to_csv(DATA_CSV)
            .to_feather(DATA_FEATHER)
            .to_json(DATA_JSON)
            .to_parquet(DATA_PARQUET)
            .to_hdf(DATA_HDF)
        )

    def test_csv(self):
        data = DataFlow().DataFrame()
        data.from_csv(DATA_CSV).stats()
        data.del_columns(
            [
                "Industry_aggregation_NZSIOC",
                "Industry_code_NZSIOC",
                "Industry_name_NZSIOC",
                "Industry_code_ANZSIC06",
                "Variable_code",
                "Variable_name",
                "Variable_category",
            ]
        ).stats()

        self.assertEqual(3, len(data.columns()))
        self.assertListEqual(["Year", "Units", "Value"], data.columns())

    @unittest.skip
    def test_from_feather(self):
        data = DataFlow().DataFrame()
        data.from_feather(DATA_FEATHER).stats()

    @unittest.skip
    def test_from_json(self):
        data = DataFlow().DataFrame()
        data.from_json(DATA_JSON).stats()

    @unittest.skip
    def test_from_parquet(self):
        data = DataFlow().DataFrame()
        data.from_parquet(DATA_PARQUET).stats()

    @unittest.skip
    def test_from_hdf(self):
        data = DataFlow().DataFrame()
        data.from_hdf(DATA_HDF).stats()

    def test(self):
        DataFlow().DataFrame(in_memory=False).print()


if __name__ == "__main__":
    unittest.main()
