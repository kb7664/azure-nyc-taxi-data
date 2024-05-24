import unittest
from unittest.mock import patch
from src.main import main


class TestMain(unittest.TestCase):

    @patch('src.main.export_data')
    @patch('src.main.process_data')
    @patch('src.main.fetch_data')
    def test_main(self, mock_fetch_data, mock_process_data, mock_export_data):
        mock_fetch_data.side_effect = 'data_frame'
        mock_process_data.return_value = 'processed_data_frame'
        main()
        self.assertEqual(mock_fetch_data.call_count, 1)


if __name__ == "__main__":
    unittest.main()
