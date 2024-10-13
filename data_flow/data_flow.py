import os
import tempfile

import fireducks.pandas as fd
import pandas as pd
import polars as pl
from pyarrow import feather

from data_flow.lib import FileType
from data_flow.lib.data_columns import data_get_columns, data_delete_columns, data_rename_columns, data_select_columns
from data_flow.lib.data_from import (
    df_from_tmp_filename,
    from_csv_2_file,
    from_feather_2_file,
    from_parquet_2_file,
    from_json_2_file,
    from_hdf_2_file,
)
from data_flow.lib.data_to import (
    to_csv_from_file,
    to_feather_from_file,
    to_parquet_from_file,
    to_json_from_file,
    to_hdf_from_file,
)
from data_flow.lib.tools import generate_temporary_filename, delete_file


class DataFlow:
    class DataFrame:
        __in_memory: bool
        __file_type: FileType
        __data: fd.DataFrame = None
        __filename: str = None

        def __init__(self, in_memory: bool = True, file_type: FileType = FileType.parquet, tmp_file: str = None):
            self.__in_memory = in_memory
            self.__file_type = file_type
            if not in_memory and tmp_file is not None:
                self.__filename = tmp_file
            if not in_memory and tmp_file is None:
                self.__filename = os.path.join(tempfile.gettempdir(), generate_temporary_filename(ext=file_type.name))

        def __del__(self):
            if not self.__in_memory:
                delete_file(self.__filename)

        def to_fireducks(self) -> fd.DataFrame:
            if self.__in_memory:
                return self.__data
            else:
                return df_from_tmp_filename(tmp_filename=self.__filename, file_type=self.__file_type)

        def to_pandas(self) -> pd.DataFrame:
            if self.__in_memory:
                return self.__data.to_pandas()
            else:
                return df_from_tmp_filename(tmp_filename=self.__filename, file_type=self.__file_type).to_pandas()

        def to_polars(self) -> pl.DataFrame:
            if self.__in_memory:
                return pl.from_pandas(self.__data.to_pandas())
            else:
                return pl.from_pandas(
                    df_from_tmp_filename(tmp_filename=self.__filename, file_type=self.__file_type).to_pandas()
                )

        def from_csv(self, filename: str):
            if self.__in_memory:
                self.__data = fd.read_csv(filename)
            else:
                from_csv_2_file(filename=filename, tmp_filename=self.__filename, file_type=self.__file_type)
            return self

        def to_csv(self, filename: str, index=False):
            if self.__in_memory:
                self.__data.to_csv(filename, index=index)
            else:
                to_csv_from_file(filename=filename, tmp_filename=self.__filename, file_type=self.__file_type)
            return self

        def from_feather(self, filename: str):
            if self.__in_memory:
                self.__data = fd.from_pandas(feather.read_feather(filename))
            else:
                from_feather_2_file(filename=filename, tmp_filename=self.__filename, file_type=self.__file_type)
            return self

        def to_feather(self, filename: str):
            if self.__in_memory:
                self.__data.to_feather(filename)
            else:
                to_feather_from_file(filename=filename, tmp_filename=self.__filename, file_type=self.__file_type)
            return self

        def from_parquet(self, filename: str):
            if self.__in_memory:
                self.__data = fd.read_parquet(filename)
            else:
                from_parquet_2_file(filename=filename, tmp_filename=self.__filename, file_type=self.__file_type)
            return self

        def to_parquet(self, filename: str):
            if self.__in_memory:
                self.__data.to_parquet(filename)
            else:
                to_parquet_from_file(filename=filename, tmp_filename=self.__filename, file_type=self.__file_type)
            return self

        def from_json(self, filename: str):
            if self.__in_memory:
                self.__data = fd.read_json(filename)
            else:
                from_json_2_file(filename=filename, tmp_filename=self.__filename, file_type=self.__file_type)
            return self

        def to_json(self, filename: str):
            if self.__in_memory:
                self.__data.to_json(filename)
            else:
                to_json_from_file(filename=filename, tmp_filename=self.__filename, file_type=self.__file_type)
            return self

        def from_hdf(self, filename: str):
            if self.__in_memory:
                self.__data = fd.read_hdf(filename)
            else:
                from_hdf_2_file(filename=filename, tmp_filename=self.__filename, file_type=self.__file_type)
            return self

        def to_hdf(self, filename: str, key: str = "key"):
            if self.__in_memory:
                self.__data.to_hdf(path_or_buf=filename, key=key)
            else:
                to_hdf_from_file(filename=filename, tmp_filename=self.__filename, file_type=self.__file_type, key=key)
            return self

        def head(self):
            if self.__in_memory:
                print(self.__data.head())
            else:
                print(df_from_tmp_filename(tmp_filename=self.__filename, file_type=self.__file_type).head())
            return self

        def stats(self):
            if self.__in_memory:
                data = self.__data
            else:
                data = df_from_tmp_filename(tmp_filename=self.__filename, file_type=self.__file_type)

            print("***** Data stats *****")
            print(f"Columns names : {data.columns.to_list()}")
            print(f"Columns count : {len(data.columns)}")
            print(f"Rows count    :    {len(data)}")
            print("Data types    :")
            print(data.dtypes)
            print("**********************")
            return self

        def del_columns(self, columns: list):
            if self.__in_memory:
                self.__data.drop(columns=columns, inplace=True)
            else:
                data_delete_columns(tmp_filename=self.__filename, file_type=self.__file_type, columns=columns)

            return self

        def columns(self) -> list:
            if self.__in_memory:
                return self.__data.columns.to_list()
            else:
                return data_get_columns(tmp_filename=self.__filename, file_type=self.__file_type)

        def rename_columns(self, columns_mapping: dict):
            if self.__in_memory:
                self.__data.rename(columns=columns_mapping, inplace=True)
            else:
                data_rename_columns(
                    tmp_filename=self.__filename,
                    file_type=self.__file_type,
                    columns_mapping=columns_mapping,
                )
            return self

        def select_columns(self, columns: list):
            if self.__in_memory:
                self.__data = self.__data[columns]
            else:
                data_select_columns(tmp_filename=self.__filename, file_type=self.__file_type, columns=columns)
