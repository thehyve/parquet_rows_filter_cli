#!/usr/bin/env python3

import unittest
import pandas as pd
import os.path
import tempfile
from parquet_filter import filter_parquet_rows

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
        odf = pd.read_parquet(output_parquet_file)
        output_records = odf.to_dict('records')
        self.assertEqual(self.input_records, output_records)


if __name__ == '__main__':
    unittest.main()
