import polars as pl

from data_flow.lib import FileType
from data_flow.lib.pandas import to_pandas_from_file, from_pandas_2_file


def from_polars_2_file(df: pl.DataFrame, tmp_filename: str, file_type: FileType) -> None:
    from_pandas_2_file(df=df.to_pandas(), tmp_filename=tmp_filename, file_type=file_type)


def to_polars_from_file(tmp_filename: str, file_type: FileType) -> pl.DataFrame:
    return pl.from_pandas(to_pandas_from_file(tmp_filename=tmp_filename, file_type=file_type))
