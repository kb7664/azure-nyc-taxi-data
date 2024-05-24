import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from src.data_extraction import fetch_data


class TestFetchData(unittest.TestCase):

    @patch('src.data_extraction.NycTlcYellow')
    @patch('src.data_extraction.export_data')
    @patch('src.data_extraction.process_data')
    def test_fetch_data(self, MockProcessData, MockExportData, MockNycTlcYellow):
        # Set up the mock to return a small DataFrame
        mock_df = pd.DataFrame({
            'tpepPickupDateTime': ['2009-01-05 00:00:00'],
            'paymentType': ['1'],
            'totalAmount': [10.5],
            'passengerCount': [1]
        })
        mock_instance = MockNycTlcYellow.return_value
        mock_instance.to_pandas_dataframe.return_value = mock_df
        MockProcessData.return_value = mock_df
        start_date = '2009-01-01'
        end_date = '2009-03-01'
        output_parquet_path = 'output/test_output.parquet'
        output_csv_path = 'output/test_output.csv'
        fetch_data(start_date, end_date, output_parquet_path, output_csv_path)
        # Assertions to check the data frame contents
        self.assertEqual(mock_instance.to_pandas_dataframe.call_count, 2)  # Assuming two months of data fetched
        self.assertEqual(MockProcessData.call_count, 2)
        self.assertEqual(MockExportData.call_count, 2)


if __name__ == "__main__":
    unittest.main()
