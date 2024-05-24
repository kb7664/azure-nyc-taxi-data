import unittest
import pandas as pd
from src.data_transformation import process_data, export_data
import os


class TestProcessData(unittest.TestCase):

    def setUp(self):
        data = {
            'tpepPickupDateTime': ['2009-01-05 00:00:00', '2009-01-06 00:00:00'],
            'totalAmount': [10.5, 15.0],
            'passengerCount': [1, 2],
            'paymentType': ['1', '2']
        }
        self.df = pd.DataFrame(data)
    
    def test_process_data(self):
        processed_df = process_data(self.df)
        self.assertIsInstance(processed_df, pd.DataFrame)
        self.assertFalse(processed_df.empty)
        self.assertTrue('totalAmount_mean' in processed_df.columns)
        self.assertTrue('passengerCount_mean' in processed_df.columns)
        self.assertTrue('totalAmount_median' in processed_df.columns)
        self.assertTrue('passengerCount_median' in processed_df.columns)

    def test_export_data(self):
        processed_df = process_data(self.df)
        export_data(processed_df, 'output/test_output.parquet', 'output/test_output.csv')
        self.assertTrue(os.path.exists('output/test_output.parquet'))
        self.assertTrue(os.path.exists('output/test_output.csv'))
        os.remove('output/test_output.parquet')
        os.remove('output/test_output.csv')


if __name__ == "__main__":
    unittest.main()
