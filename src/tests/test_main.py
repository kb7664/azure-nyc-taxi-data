import unittest
from unittest.mock import patch
from src.main import main


class TestMain(unittest.TestCase):

    @patch('src.main.fetch_data')
    @patch('src.main.process_data')
    @patch('src.main.export_data')
    def test_main(self, mock_export_data, mock_process_data, mock_fetch_data):
        mock_fetch_data.return_value = 'data_frame'
        mock_process_data.return_value = 'processed_data_frame'
        main()
        mock_fetch_data.assert_called_once()
        mock_process_data.assert_called_once_with('data_frame')
        mock_export_data.assert_called_once_with('processed_data_frame', 'output/output.parquet', 'output/output.csv')


if __name__ == "__main__":
    unittest.main()
