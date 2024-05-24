import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from src.data_extraction import fetch_data


class TestFetchData(unittest.TestCase):

    @patch('src.data_extraction.NycTlcYellow')
    def test_fetch_data(self, MockNycTlcYellow):
        # Set up the mock to return a small DataFrame
        mock_df = pd.DataFrame({
            'tpepPickupDateTime': ['2009-01-05 00:00:00'],
            'paymentType': ['1'],
            'totalAmount': [10.5],
            'passengerCount': [1]
        })
        mock_instance = MockNycTlcYellow.return_value
        mock_instance.to_pandas_dataframe.return_value = mock_df
        start_date = '2009-01-01'
        end_date = '2009-03-01'
        df = fetch_data(start_date, end_date)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)
        self.assertTrue('tpepPickupDateTime' in df.columns)
        self.assertTrue('paymentType' in df.columns)
        self.assertEqual(len(df), 2)  # Assuming the function fetches two months' data


if __name__ == "__main__":
    unittest.main()
