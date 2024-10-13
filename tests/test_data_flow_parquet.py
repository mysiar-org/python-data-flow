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
        df = DataFlow().DataFrame().from_parquet(self.TEST_PARQUET_FILE)

        self.assertPandasEqual(df.to_pandas(), DataFlow().DataFrame().from_csv(self.CSV_FILE).to_pandas())
        self._sequence(data=df)

    def test_parquet(self):
        df = DataFlow().DataFrame(in_memory=False).from_parquet(self.TEST_PARQUET_FILE)

        self.assertPandasEqual(df.to_pandas(), DataFlow().DataFrame().from_csv(self.CSV_FILE).to_pandas())
        self._sequence(data=df)

    def test_feather(self):
        df = DataFlow().DataFrame(in_memory=False, file_type=FileType.feather).from_parquet(self.TEST_PARQUET_FILE)

        self.assertPandasEqual(df.to_pandas(), DataFlow().DataFrame().from_csv(self.CSV_FILE).to_pandas())
        self._sequence(data=df)


if __name__ == "__main__":
    unittest.main()
