import pandas as pd
import os

def filter_parquet_rows(input_parquet_file, filter = None):
    if filter is None:
        return input_parquet_file
    df = pd.read_parquet(input_parquet_file)
    for field, values in filter.items():
        df = df[df[field].isin(values)]
    inp_f_ext = os.path.splitext(os.path.basename(input_parquet_file))
    filtered_file_name = ''.join([inp_f_ext[0], '_filtered', inp_f_ext[1]])
    filtered_file = os.path.join(os.getcwd(), filtered_file_name)
    df.to_parquet(filtered_file)
    return filtered_file
