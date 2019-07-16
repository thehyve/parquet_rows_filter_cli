import pandas as pd
import os

def filter_parquet_rows(input_parquet_file, output_parquet_file = None, flt = None):
    df = pd.read_parquet(input_parquet_file)
    if flt:
        for field, values in flt.items():
            df = df[df[field].isin(values)]
    if output_parquet_file is None:
        filtered_file = _get_default_filtered_file_path(input_parquet_file)
    else:
        filtered_file = output_parquet_file
    df.to_parquet(filtered_file)
    return filtered_file


def _get_default_filtered_file_path(input_file):
    inp_f_ext = os.path.splitext(os.path.basename(input_file))
    filtered_file_name = ''.join([inp_f_ext[0], '_filtered', inp_f_ext[1]])
    return os.path.join(os.getcwd(), filtered_file_name)
