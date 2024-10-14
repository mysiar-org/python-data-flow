from data_flow import DataFlow
from tests.BaseTestCase import BaseTestCase


class SequenceTestCase(BaseTestCase):
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
