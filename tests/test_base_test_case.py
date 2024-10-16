import unittest

import pandas as pd

from tests.BaseTestCase import BaseTestCase


class BaseTestCaseTestCase(BaseTestCase):
    def test_assert_pandas_equal(self):
        df1 = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        df2 = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        df3 = pd.DataFrame({"Name": ["Tom", "nick", "krish", "jack"], "Age": [20, 21, 19, 18]})

        self.assertPandasEqual(df1, df2)

        with self.assertRaises(AssertionError) as context:
            self.assertPandasEqual(df1, df3)
        self.assertEqual(str(context.exception), "False is not true : Pandas DataFrames are not equal !")


if __name__ == "__main__":
    unittest.main()
