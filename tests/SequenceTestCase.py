from data_flow import DataFlow
from tests.BaseTestCase import BaseTestCase


class SequenceTestCase(BaseTestCase):
    def _sequence(self, data: DataFlow.DataFrame) -> None:
        data.stats()
        data.head()
        self.assertEqual(10, len(data.columns()))

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
        )
        self.assertEqual(3, len(data.columns()))
        self.assertListEqual(["Year", "Units", "Value"], data.columns())

        data.rename_columns(columns_mapping={"Year": "_year_", "Units": "_units_"})
        self.assertListEqual(["_year_", "_units_", "Value"], data.columns())

        data.select_columns(columns=["_year_"])
        self.assertListEqual(["_year_"], data.columns())
