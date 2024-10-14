import unittest

from data_flow import DataFlow
from data_flow.lib import FileType
from tests.SequenceTestCase import SequenceTestCase


class DataFlowCSVTestCase(SequenceTestCase):
    def test_memory(self):
        df = (
            DataFlow().DataFrame().from_csv("./tests/data/annual-enterprise-survey-2023-financial-year-provisional.csv")
        )
        df.to_csv(self.TEST_CSV_FILE)
        self.assertPandasEqual(df.to_pandas(), DataFlow().DataFrame().from_csv(self.CSV_FILE).to_pandas())
        self._sequence(data=df)

    def test_parquet(self):
        df = (
            DataFlow()
            .DataFrame(in_memory=False)
            .from_csv("./tests/data/annual-enterprise-survey-2023-financial-year-provisional.csv")
        )

        self.assertPandasEqual(df.to_pandas(), DataFlow().DataFrame().from_csv(self.CSV_FILE).to_pandas())
        self._sequence(data=df)

    def test_feather(self):
        df = (
            DataFlow()
            .DataFrame(in_memory=False, file_type=FileType.feather)
            .from_csv("./tests/data/annual-enterprise-survey-2023-financial-year-provisional.csv")
        )

        self.assertPandasEqual(df.to_pandas(), DataFlow().DataFrame().from_csv(self.CSV_FILE).to_pandas())
        self._sequence(data=df)


if __name__ == "__main__":
    unittest.main()
