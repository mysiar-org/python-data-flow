import unittest

from data_flow import DataFlow, FileType
from data_flow.lib.tools import delete_file
from tests.SequenceTestCase import SequenceTestCase


class DataFlowParquetTestCase(SequenceTestCase):
    def setUp(self):
        super().setUp()
        delete_file(self.TEST_PARQUET_FILE)
        DataFlow().DataFrame().from_csv(self.CSV_FILE).to_parquet(self.TEST_PARQUET_FILE)

    def test_memory(self):
        data = DataFlow().DataFrame().from_parquet(self.TEST_PARQUET_FILE)
        self._sequence(data=data)
        data.get_data_pandas().equals(DataFlow().DataFrame().from_csv(self.CSV_FILE).get_data_pandas())

    def test_parquet(self):
        data = DataFlow().DataFrame(in_memory=False).from_parquet(self.TEST_PARQUET_FILE)
        self._sequence(data=data)

    def test_feather(self):
        data = DataFlow().DataFrame(in_memory=False, file_type=FileType.feather).from_parquet(self.TEST_PARQUET_FILE)
        self._sequence(data=data)


if __name__ == "__main__":
    unittest.main()
