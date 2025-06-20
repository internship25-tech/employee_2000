import unittest
from unittest.mock import patch, MagicMock
from io import BytesIO
import pandas as pd

from employee_scraper import EnhancedGoogleDriveEmployeeScraper

class TestEmployeeScraper(unittest.TestCase):

    def setUp(self):
        self.scraper = EnhancedGoogleDriveEmployeeScraper()

    @patch('employee_scraper.requests.Session.get')
    def test_file_download(self, mock_get):
        # Test Case 1: Verify CSV File Download
        mock_response = MagicMock()
        mock_response.iter_content = lambda chunk_size: [b"id,name\n1,Alice\n2,Bob"]
        mock_response.headers = {
            'content-type': 'text/csv',
            'content-disposition': 'attachment; filename="employees.csv"',
            'content-length': '100'
        }
        mock_response.raise_for_status = lambda: None
        mock_get.return_value = mock_response

        content, headers = self.scraper.download_file_with_retry("https://fake-url.com/test.csv")
        self.assertTrue(len(content) > 0)
        self.assertIn('content_type', headers)

    def test_parse_csv_extraction(self):
        # Test Case 2: Verify CSV File Extraction
        content = b"employee_id,first_name,last_name,email,job_title,phone_number,birth_date\n1,Alice,Smith,alice@example.com,Engineer,1234567890,1990-05-10"
        records = self.scraper.parse_csv_data(content)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['first_name'], 'Alice')

    def test_file_type_detection(self):
        # Test Case 3: Validate File Type and Format
        content = b"employee_id,first_name,last_name\n1,Alice,Smith"
        headers = {'content-type': 'text/csv'}
        file_type = self.scraper.detect_file_type(content, headers)
        self.assertEqual(file_type, 'csv')

    def test_data_structure_validation(self):
        # Test Case 4: Validate Data Structure
        raw_data = [{
            'employee_id': '1',
            'first_name': 'Alice',
            'last_name': 'Smith',
            'email': 'alice@example.com',
            'job_title': 'Engineer',
            'phone_number': '1234567890',
            'birth_date': '1990-05-10'
        }]
        mapped_data = self.scraper.map_employee_fields(raw_data)
        validation_result = self.scraper.validate_employee_data(mapped_data)
        self.assertEqual(len(validation_result['valid']), 1)

    def test_invalid_data_handling(self):
        # Test Case 5: Handle Missing or Invalid Data
        raw_data = [{
            'first_name': '',
            'last_name': '',
            'email': 'invalid-email',
            'job_title': 'Engineer',
            'phone_number': 'abc',
            'birth_date': '32/13/2020'  # Invalid date
        }]
        mapped_data = self.scraper.map_employee_fields(raw_data)
        validation_result = self.scraper.validate_employee_data(mapped_data)
        self.assertEqual(len(validation_result['invalid']), 1)
        self.assertIn('Invalid email format', validation_result['invalid'][0]['issues'])


if __name__ == '__main__':
    unittest.main()
