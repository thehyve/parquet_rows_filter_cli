#!/usr/bin/env python3

import unittest
import pandas as pd
import os.path
import tempfile
from parquet_filter import filter_parquet_rows, _collect_field_value_constraints

class ParquetFilterTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.input_parquet_file = os.path.join(tempfile.mkdtemp(), 'input.parquet')
        cls.input_records = [
            {'field1': 'a', 'field2': 1},
            {'field1': 'a', 'field2': 2},
            {'field1': 'b', 'field2': 1},
            {'field1': 'b', 'field2': 2},
        ]
        pd.DataFrame(cls.input_records).to_parquet(cls.input_parquet_file)

    def test_that_script_produces_file_equal_to_input(self):
        output_parquet_file = filter_parquet_rows(self.input_parquet_file)

        self.assertEqual(self.input_records, _get_records(output_parquet_file))

    def test_that_script_filters_on_a_field(self):
        output_parquet_file = filter_parquet_rows(self.input_parquet_file, None, { 'field1': ['a'] })
        records = _get_records(output_parquet_file)
        self.assertEqual(2, len(records))
        self.assertEqual({'a'}, { row['field1'] for row in records })
        self.assertEqual({1, 2}, { row['field2'] for row in records })

    def test_that_script_gets_output_file(self):
        output_parquet_file = os.path.join(tempfile.mkdtemp(), 'output.parquet')
        output_parquet_file2 = filter_parquet_rows(self.input_parquet_file, output_parquet_file)
        self.assertEqual(output_parquet_file, output_parquet_file2)
        self.assertEqual(self.input_records, _get_records(output_parquet_file2))

    def test_that_script_filters_on_multiple_values(self):
        output_parquet_file = filter_parquet_rows(self.input_parquet_file, None, { 'field1': ['a', 'b'] })

        self.assertEqual(self.input_records, _get_records(output_parquet_file))

    def test_that_script_filters_on_multiple_fields(self):
        output_parquet_file = filter_parquet_rows(self.input_parquet_file, None, { 'field1': ['a'], 'field2': [1] })

        records = _get_records(output_parquet_file)
        self.assertEqual(1, len(records))
        self.assertEqual({'a'}, { row['field1'] for row in records })
        self.assertEqual({1}, { row['field2'] for row in records })

    def test_script_cli_collects_filters(self):
        args, flt = _collect_field_value_constraints(['arg1', 'arg2', '--field1=a', '--field2=1', '--field2=2'])

        self.assertEqual(['arg1', 'arg2'], args)
        self.assertEqual({'field1': ['a'], 'field2': ['1', '2']} , flt)

    def test_that_script_does_not_produce_file_when_result_is_empty(self):
        output_parquet_file = filter_parquet_rows(self.input_parquet_file, None, { 'field1': ['c'] })
        self.assertTrue(output_parquet_file is None)


def _get_records(parquet_file):
    return pd.read_parquet(parquet_file).to_dict('records')


if __name__ == '__main__':
    unittest.main()
