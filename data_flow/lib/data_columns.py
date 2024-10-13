import fireducks.pandas as fd

from data_flow.lib.FileType import FileType


def data_get_columns(tmp_filename: str, file_type: FileType) -> list:
    match file_type:
        case FileType.parquet:
            return fd.read_parquet(tmp_filename).columns.to_list()
        case FileType.feather:
            return fd.read_feather(tmp_filename).columns.to_list()
        case _:
            raise ValueError(f"File type not implemented: {file_type} !")


def data_delete_columns(tmp_filename: str, file_type: FileType, columns: list) -> None:
    match file_type:
        case FileType.parquet:
            data = fd.read_parquet(tmp_filename)
            data.drop(columns=columns, inplace=True)
            data.to_parquet(tmp_filename)
        case FileType.feather:
            data = fd.read_feather(tmp_filename)
            data.drop(columns=columns, inplace=True)
            data.to_feather(tmp_filename)
        case _:
            raise ValueError(f"File type not implemented: {file_type} !")


def data_rename_columns(tmp_filename: str, file_type: FileType, columns_mapping: dict) -> None:
    match file_type:
        case FileType.parquet:
            fd.read_parquet(tmp_filename).rename(columns=columns_mapping).to_parquet(tmp_filename)
        case FileType.feather:
            fd.read_feather(tmp_filename).rename(columns=columns_mapping).to_feather(tmp_filename)
        case _:
            raise ValueError(f"File type not implemented: {file_type} !")


def data_select_columns(tmp_filename: str, file_type: FileType, columns: list) -> None:
    match file_type:
        case FileType.parquet:
            data = fd.read_parquet(tmp_filename)[columns]
            data.to_parquet(tmp_filename)
        case FileType.feather:
            data = fd.read_feather(tmp_filename)[columns]
            data.to_feather(tmp_filename)

        case _:
            raise ValueError(f"File type not implemented: {file_type} !")


# def __slice(dataframe, start_row, end_row, start_col, end_col):
#     assert len(dataframe) > end_row and start_row >= 0
#     assert len(dataframe.columns) > end_col and start_col >= 0
#     list_of_indexes = list(dataframe.columns)[start_col:end_col]
#     return dataframe.iloc[start_row:end_row][list_of_indexes]
