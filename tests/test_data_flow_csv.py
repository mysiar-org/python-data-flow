import unittest

from data_flow import DataFlow, FileType
from tests.SequenceTestCase import SequenceTestCase


class DataFlowCSVTestCase(SequenceTestCase):
    def test_memory(self):
        data = (
            DataFlow().DataFrame().from_csv("./tests/data/annual-enterprise-survey-2023-financial-year-provisional.csv")
        )
        self._sequence(data=data)

        data.to_csv(self.TEST_CSV_FILE)
        data.get_data_pandas().equals(DataFlow().DataFrame().from_csv(self.TEST_CSV_FILE).get_data_pandas())

    def test_parquet(self):
        data = (
            DataFlow()
            .DataFrame(in_memory=False)
            .from_csv("./tests/data/annual-enterprise-survey-2023-financial-year-provisional.csv")
        )
        self._sequence(data=data)

    def test_feather(self):
        data = (
            DataFlow()
            .DataFrame(in_memory=False, file_type=FileType.feather)
            .from_csv("./tests/data/annual-enterprise-survey-2023-financial-year-provisional.csv")
        )
        self._sequence(data=data)


if __name__ == "__main__":
    unittest.main()
