
# Employee Scraper Documentation

# Enhanced Google Drive Employee Scraper Documentation


## Overview

The **EnhancedGoogleDriveEmployeeScraper** is a robust and feature-rich Python-based utility designed to download, parse, validate, and export employee data from Google Drive links (primarily spreadsheets and CSVs). It is particularly well-suited for large datasets containing 1000+ records and supports advanced features like:

* Robust retry and download logic
* File type auto-detection (CSV, XLSX, XLS)
* Field normalization and flexible mapping (e.g., `user_id` to `employee_id`)
* Comprehensive date and phone number handling
* Validation with warnings and issue categorization
* CSV export of cleaned and validated employee records

---

## Class: `EnhancedGoogleDriveEmployeeScraper`

### Constructor

```python
__init__(self, max_retries=5, retry_delay=3, log_level="INFO")
```

* **max\_retries**: Maximum attempts for downloading a file
* **retry\_delay**: Delay between retries
* **log\_level**: Logging level (`DEBUG`, `INFO`, etc.)

---

## Methods

### `scrape_employee_data(url: str) -> Dict[str, Any]`

Main method that orchestrates:

1. Download from Google Drive
2. File type detection
3. File parsing (CSV/XLSX/XLS)
4. Field mapping
5. Validation
6. Result compilation with summaries

### `download_file_with_retry(url: str) -> Tuple[bytes, Dict[str, str]]`

Attempts to download a file with retries and progressive backoff.

### `detect_file_type(content: bytes, headers: Dict[str, str]) -> str`

Determines file type based on content signature, headers, and name.

### `parse_csv_data(content: bytes) -> List[Dict[str, Any]]`

Decodes and parses CSV data into a list of dictionaries.

### `parse_excel_data(content: bytes, file_type: str) -> List[Dict[str, Any]]`

Reads XLSX or XLS file into a list of dictionaries using pandas.

### `normalize_field_name(field_name: str) -> str`

Standardizes field names to lowercase underscore-separated format.

### `preserve_phone_number(phone: str) -> str`

Returns phone number as-is, stripping only leading/trailing whitespace.

### `parse_date(date_str: str) -> Optional[str]`

Tries multiple date formats and returns date in ISO format.

### `map_employee_fields(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]`

Maps raw dictionary keys to standardized employee fields.

### `validate_employee_data(employees: List[Dict[str, Any]]) -> Dict[str, Any]`

Validates each employee record. Categorizes into `valid` and `invalid`.

* Flags missing ID/email/name
* Preserves phone formats
* Warns on suspicious dates

### `export_to_csv(results: Dict[str, Any], filename: str) -> bool`

Exports all valid employee records to a specified CSV file.

### `_analyze_field_coverage(valid_employees: List[Dict[str, Any]]) -> Dict[str, Any]`

Summarizes coverage stats for each core employee field.

### `_analyze_phone_numbers(valid_employees: List[Dict[str, Any]]) -> Dict[str, Any]`

Collects stats on phone number formatting and validity.

---

## Usage Example

```python
scraper = EnhancedGoogleDriveEmployeeScraper()
url = "https://drive.google.com/uc?id=YOUR_FILE_ID&export=download"
results = scraper.scrape_employee_data(url)

if results['success']:
    scraper.export_to_csv(results, "output.csv")
```

---

## Output Structure (`results`)

```json
{
  "success": true,
  "timestamp": "2025-06-22T18:30:00",
  "processing_time_seconds": 12.45,
  "file_info": { ... },
  "data_summary": { ... },
  "field_coverage": { ... },
  "phone_number_stats": { ... },
  "employees": {
    "valid": [...],
    "invalid": [...]
  }
}
```

---

## Notes

* The class is compatible with Excel and CSV files encoded in common formats.
* It tolerates and logs imperfect records but prioritizes extracting usable data.
* Designed for scalable, automated ETL use in HR or data science workflows.

---

## Unit Tests: `TestEmployeeScraper`

### Setup

```python
def setUp(self):
    self.scraper = EnhancedGoogleDriveEmployeeScraper()
```

### Test Case 1: File Download with Mocked Response

```python
@patch('employee_scraper.requests.Session.get')
def test_file_download(self, mock_get):
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
```

### Test Case 2: CSV Parsing

```python
def test_parse_csv_extraction(self):
    content = b"employee_id,first_name,last_name,email,job_title,phone_number,birth_date\n1,Alice,Smith,alice@example.com,Engineer,1234567890,1990-05-10"
    records = self.scraper.parse_csv_data(content)
    self.assertEqual(len(records), 1)
    self.assertEqual(records[0]['first_name'], 'Alice')
```

### Test Case 3: File Type Detection

```python
def test_file_type_detection(self):
    content = b"employee_id,first_name,last_name\n1,Alice,Smith"
    headers = {'content-type': 'text/csv'}
    file_type = self.scraper.detect_file_type(content, headers)
    self.assertEqual(file_type, 'csv')
```

### Test Case 4: Valid Data Structure

```python
def test_data_structure_validation(self):
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
```

### Test Case 5: Handling Invalid Data

```python
def test_invalid_data_handling(self):
    raw_data = [{
        'first_name': '',
        'last_name': '',
        'email': 'invalid-email',
        'job_title': 'Engineer',
        'phone_number': 'abc',
        'birth_date': '32/13/2020'
    }]
    mapped_data = self.scraper.map_employee_fields(raw_data)
    validation_result = self.scraper.validate_employee_data(mapped_data)
    self.assertEqual(len(validation_result['invalid']), 1)
    self.assertIn('Invalid email format', validation_result['invalid'][0]['issues'])
```

---

These tests validate critical parts of the scraper including download handling, CSV parsing, file type detection, and data validation. They ensure reliability, correctness, and maintainability across future updates.
