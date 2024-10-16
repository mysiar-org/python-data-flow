import unittest

from data_flow import DataFlow
from data_flow.lib import FileType
from data_flow.lib.tools import delete_file
from tests.BaseTestCase import BaseTestCase


class DataFlowCSVTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        delete_file(self.TEST_CSV_FILE)
        DataFlow().DataFrame().from_csv(self.CSV_FILE).to_csv(self.TEST_CSV_FILE)

    def test_memory(self):
        self.all(self.__memory)

    def test_parquet(self):
        self.all(self.__parquet)

    def test_feather(self):
        self.all(self.__feather)

    def __memory(self) -> DataFlow.DataFrame:
        return DataFlow().DataFrame().from_csv(self.TEST_CSV_FILE)

    def __parquet(self) -> DataFlow.DataFrame:
        return DataFlow().DataFrame(in_memory=False).from_csv(self.TEST_CSV_FILE)

    def __feather(self) -> DataFlow.DataFrame:
        return DataFlow().DataFrame(in_memory=False, file_type=FileType.feather).from_csv(self.TEST_CSV_FILE)


if __name__ == "__main__":
    unittest.main()
