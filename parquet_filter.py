#!/usr/bin/env python3

import pandas as pd
import os
import sys

def filter_parquet_rows(input_parquet_file, output_parquet_file = None, flt = None):
    df = pd.read_parquet(input_parquet_file)
    if flt:
        for field, values in flt.items():
            df = df[df[field].isin(values)]
    if output_parquet_file is None:
        filtered_file = _get_default_filtered_file_path(input_parquet_file)
    else:
        filtered_file = output_parquet_file
    if len(df.index):
        os.makedirs(os.path.dirname(filtered_file), exist_ok=True)
        df.to_parquet(filtered_file)
        return filtered_file
    else:
        print('No rows of interest are found in ' + input_parquet_file, file=sys.stderr)
        return None


def _get_default_filtered_file_path(input_file):
    inp_f_ext = os.path.splitext(os.path.basename(input_file))
    filtered_file_name = ''.join([inp_f_ext[0], '_filtered', inp_f_ext[1]])
    return os.path.join(os.getcwd(), filtered_file_name)


def _collect_field_value_constraints(args):
    pos_args = []
    flt = dict()
    for arg in args:
        if arg.startswith('--'):
            field, value = arg[2:].split('=')
            if field not in flt:
                flt[field] = []
            flt[field].append(value)
        else:
            pos_args.append(arg)
    return pos_args, flt

if __name__ == '__main__':
    pos_args, filters = _collect_field_value_constraints(sys.argv[1:])
    if not pos_args:
        print("Script creates parquet files with rows that match contstaints.")
        print("Example: " + sys.argv[0] + " input.parquet output.parquet --field1=value1 --field1=value2 field2=value3")
        sys.exit(1)

    inpurt_parquet = pos_args[0]
    output_parquet = None
    if len(pos_args) > 1:
        output_parquet = pos_args[1]
    filter_parquet_rows(inpurt_parquet, output_parquet, filters)
