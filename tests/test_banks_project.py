import unittest
from unittest.mock import patch, mock_open
from datetime import datetime

from banks_project import log_progress, log_file

class TestLogProgress(unittest.TestCase):
    # intercepting file writing operations without creating an actual file:
    @patch('builtins.open', new_callable=mock_open)
    # Mock datetime.datetime to control the timestamp:
    @patch('datetime.datetime')
    def test_log_progress(self, mock_datetime, mock_open):
        # setting up mocked datetime:
        mock_now = datetime(2024, 10, 1, 9, 27)
        mock_datetime.now.return_value = mock_now

        # set expected log file path:
        log_file_test = 'log_data/01-10-24/code_log.txt'
        
        # writing expected results:
        expected_timestamp = '2024-Oct-01-09:27'
        message_test = 'ETL Process started'
        expected_output = expected_timestamp + ': ' + message_test + '\n'

        # calling the function:
        log_progress(message_test)
        
        # checking if the file was opened in append mode:
        mock_open.assert_called_once_with(log_file_test, 'a', encoding='utf-8')
        
        # checking if the correct data was written to the file:
        mock_open().write.assert_called_once_with(expected_output)

if __name__ == '__main__':
    unittest.main()
