import unittest

from data_flow import DataFlow
from data_flow.lib import FileType
from data_flow.lib.tools import delete_file
from tests.SequenceTestCase import SequenceTestCase


class DataFlowJsonTestCase(SequenceTestCase):
    def setUp(self):
        super().setUp()
        delete_file(self.TEST_JSON_FILE)
        DataFlow().DataFrame().from_csv(self.CSV_FILE).to_json(self.TEST_JSON_FILE)

    def test_memory(self):
        df = DataFlow().DataFrame().from_json(self.TEST_JSON_FILE)

        self._sequence(data=df)

    def test_parquet(self):
        df = DataFlow().DataFrame(in_memory=False).from_json(self.TEST_JSON_FILE)

        self._sequence(data=df)

    def test_feather(self):
        df = DataFlow().DataFrame(in_memory=False, file_type=FileType.feather).from_json(self.TEST_JSON_FILE)

        self._sequence(data=df)


if __name__ == "__main__":
    unittest.main()
