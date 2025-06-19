import requests
import pandas as pd
import csv
import io
import time
import logging
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
import openpyxl
from openpyxl import load_workbook

class GoogleDriveEmployeeScraper:
    """
    A comprehensive scraper for downloading and processing employee data from Google Drive files.
    Supports CSV and Excel formats with robust error handling and data validation.
    """
    
    def __init__(self, max_retries: int = 3, retry_delay: int = 2, log_level: str = "INFO"):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Setup logging
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)
        
        # Field mappings for different naming conventions
        self.field_mappings = {
            'employee_id': ['employee_id', 'emp_id', 'id', 'employee id', 'empid', 'employee_number'],
            'first_name': ['first_name', 'firstname', 'first name', 'fname', 'given_name', 'first'],
            'last_name': ['last_name', 'lastname', 'last name', 'lname', 'surname', 'family_name', 'last'],
            'email': ['email', 'email_address', 'e_mail', 'mail', 'email address'],
            'job_title': ['job_title', 'title', 'position', 'role', 'job title', 'designation', 'job_position'],
            'phone_number': ['phone', 'phone_number', 'mobile', 'contact', 'telephone', 'phone number'],
            'hire_date': ['hire_date', 'start_date', 'join_date', 'employment_date', 'hired_date', 'date_hired']
        }

    def download_file_with_retry(self, url: str) -> Tuple[bytes, Dict[str, str]]:
        """Download file from URL with retry logic."""
        last_error = None
        
        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.info(f"Attempt {attempt}/{self.max_retries}: Downloading file from {url}")
                
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                if len(response.content) == 0:
                    raise ValueError("Downloaded file is empty")
                
                headers_info = {
                    'content_type': response.headers.get('content-type', ''),
                    'content_disposition': response.headers.get('content-disposition', ''),
                    'content_length': response.headers.get('content-length', '0')
                }
                
                self.logger.info(f"Successfully downloaded file ({len(response.content)} bytes)")
                self.logger.debug(f"Headers: {headers_info}")
                
                return response.content, headers_info
                
            except Exception as e:
                last_error = e
                self.logger.error(f"Download attempt {attempt} failed: {str(e)}")
                
                if attempt < self.max_retries:
                    self.logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
        
        raise Exception(f"Failed to download file after {self.max_retries} attempts. Last error: {str(last_error)}")

    def detect_file_type(self, content: bytes, headers: Dict[str, str]) -> str:
        """Detect file type based on content and headers."""
        
        # Check file signature (magic numbers)
        if len(content) >= 4:
            signature = content[:4]
            
            # Excel XLSX signature (ZIP format)
            if signature[:2] == b'PK':
                return 'xlsx'
            
            # Old Excel XLS signature
            if signature == b'\xd0\xcf\x11\xe0':
                return 'xls'
        
        # Check content type header
        content_type = headers.get('content_type', '').lower()
        if 'spreadsheet' in content_type or 'excel' in content_type:
            return 'xlsx' if 'openxml' in content_type else 'xls'
        elif 'csv' in content_type or 'text/plain' in content_type:
            return 'csv'
        
        # Check content disposition filename
        content_disposition = headers.get('content_disposition', '')
        if content_disposition:
            filename_match = re.search(r'filename[^;=\n]*=(([\'"]).*?\2|[^;\n]*)', content_disposition)
            if filename_match:
                filename = filename_match.group(1).strip('\'"')
                ext = Path(filename).suffix.lower()
                if ext in ['.csv', '.xlsx', '.xls']:
                    return ext[1:]  # Remove the dot
        
        # Try to detect CSV by content analysis
        try:
            text_sample = content[:1000].decode('utf-8', errors='ignore')
            if ',' in text_sample and '\n' in text_sample:
                # Count commas and newlines to determine if it's likely CSV
                lines = text_sample.split('\n')[:5]  # Check first 5 lines
                comma_counts = [line.count(',') for line in lines if line.strip()]
                if comma_counts and len(set(comma_counts)) <= 2:  # Consistent comma count
                    return 'csv'
        except:
            pass
        
        return 'unknown'

    def parse_csv_data(self, content: bytes) -> List[Dict[str, Any]]:
        """Parse CSV content into list of dictionaries."""
        try:
            # Try different encodings
            for encoding in ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']:
                try:
                    text_content = content.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise ValueError("Unable to decode CSV file with any supported encoding")
            
            # Use pandas for robust CSV parsing
            df = pd.read_csv(io.StringIO(text_content))
            
            # Remove completely empty rows
            df = df.dropna(how='all')
            
            self.logger.info(f"Parsed {len(df)} rows from CSV file")
            
            return df.to_dict('records')
            
        except Exception as e:
            raise Exception(f"Failed to parse CSV data: {str(e)}")

    def parse_excel_data(self, content: bytes, file_type: str) -> List[Dict[str, Any]]:
        """Parse Excel content into list of dictionaries."""
        try:
            # Use pandas to read Excel file
            df = pd.read_excel(io.BytesIO(content), engine='openpyxl' if file_type == 'xlsx' else 'xlrd')
            
            # Remove completely empty rows
            df = df.dropna(how='all')
            
            self.logger.info(f"Parsed {len(df)} rows from Excel file")
            
            return df.to_dict('records')
            
        except Exception as e:
            raise Exception(f"Failed to parse Excel file: {str(e)}")

    def normalize_field_name(self, field_name: str) -> str:
        """Normalize field name for mapping."""
        return field_name.lower().strip().replace(' ', '_').replace('-', '_')

    def map_employee_fields(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Map raw data fields to standardized employee fields."""
        normalized_data = []
        
        for row_idx, row in enumerate(raw_data):
            employee = {}
            
            # Create normalized field lookup
            normalized_row = {}
            for key, value in row.items():
                if pd.notna(value):  # Skip NaN values
                    normalized_key = self.normalize_field_name(str(key))
                    normalized_row[normalized_key] = str(value).strip()
            
            # Map fields using field mappings
            for standard_field, possible_keys in self.field_mappings.items():
                for possible_key in possible_keys:
                    normalized_key = self.normalize_field_name(possible_key)
                    if normalized_key in normalized_row:
                        employee[standard_field] = normalized_row[normalized_key]
                        break
            
            # Only include rows with some identifying information
            if any(employee.get(field) for field in ['employee_id', 'first_name', 'last_name', 'email']):
                # Add row number for tracking
                employee['_row_number'] = row_idx + 1
                normalized_data.append(employee)
        
        self.logger.info(f"Mapped {len(normalized_data)} employee records from {len(raw_data)} total rows")
        return normalized_data

    def validate_employee_data(self, employees: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate employee data and return validation results."""
        validation_results = {
            'valid': [],
            'invalid': [],
            'warnings': []
        }
        
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        phone_pattern = re.compile(r'^[\d\s\-+()]+$')
        
        for employee in employees:
            issues = []
            warnings = []
            
            # Check for required identification
            if not any(employee.get(field) for field in ['employee_id', 'first_name', 'last_name']):
                issues.append("Missing employee identification (ID, first name, or last name)")
            
            # Email validation
            email = employee.get('email')
            if email:
                if not email_pattern.match(email):
                    issues.append("Invalid email format")
            else:
                warnings.append("Missing email address")
            
            # Phone validation
            phone = employee.get('phone_number')
            if phone:
                if not phone_pattern.match(phone):
                    issues.append("Invalid phone number format")
            
            # Date validation
            hire_date = employee.get('hire_date')
            if hire_date:
                try:
                    pd.to_datetime(hire_date)
                except:
                    issues.append("Invalid hire date format")
            
            # Check for missing important fields
            if not employee.get('job_title'):
                warnings.append("Missing job title")
            
            # Categorize the record
            if not issues:
                if warnings:
                    employee['_warnings'] = warnings
                validation_results['valid'].append(employee)
            else:
                validation_results['invalid'].append({
                    'row': employee.get('_row_number', 'unknown'),
                    'employee': employee,
                    'issues': issues,
                    'warnings': warnings
                })
        
        self.logger.info(f"Validation completed: {len(validation_results['valid'])} valid, "
                        f"{len(validation_results['invalid'])} invalid records")
        
        return validation_results

    def scrape_employee_data(self, url: str) -> Dict[str, Any]:
        """Main method to scrape employee data from Google Drive URL."""
        try:
            self.logger.info("Starting employee data scraping process")
            start_time = time.time()
            
            # Step 1: Download file
            content, headers = self.download_file_with_retry(url)
            
            # Step 2: Detect file type
            file_type = self.detect_file_type(content, headers)
            self.logger.info(f"Detected file type: {file_type}")
            
            if file_type == 'unknown':
                raise ValueError("Unable to determine file type. Supported formats: CSV, Excel (.xlsx, .xls)")
            
            # Step 3: Parse file based on type
            if file_type == 'csv':
                raw_data = self.parse_csv_data(content)
            elif file_type in ['xlsx', 'xls']:
                raw_data = self.parse_excel_data(content, file_type)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            
            if not raw_data:
                raise ValueError("No data found in the file")
            
            # Step 4: Map fields to standard format
            mapped_data = self.map_employee_fields(raw_data)
            
            # Step 5: Validate data
            validation_results = self.validate_employee_data(mapped_data)
            
            # Step 6: Compile results
            processing_time = time.time() - start_time
            
            results = {
                'success': True,
                'timestamp': datetime.now().isoformat(),
                'processing_time_seconds': round(processing_time, 2),
                'file_info': {
                    'type': file_type,
                    'size_bytes': len(content),
                    'headers': headers
                },
                'data_summary': {
                    'total_rows_processed': len(raw_data),
                    'employees_mapped': len(mapped_data),
                    'valid_employees': len(validation_results['valid']),
                    'invalid_employees': len(validation_results['invalid']),
                    'success_rate': round((len(validation_results['valid']) / len(mapped_data)) * 100, 2) if mapped_data else 0
                },
                'employees': {
                    'valid': validation_results['valid'],
                    'invalid': validation_results['invalid']
                }
            }
            
            self.logger.info(f"Scraping completed successfully in {processing_time:.2f} seconds")
            return results
            
        except Exception as e:
            self.logger.error(f"Scraping failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

    def export_to_csv(self, results: Dict[str, Any], filename: str = "employees.csv") -> bool:
        """Export valid employee data to CSV file."""
        try:
            if not results.get('success') or not results.get('employees', {}).get('valid'):
                self.logger.warning("No valid employee data to export")
                return False
            
            valid_employees = results['employees']['valid']
            
            # Remove internal fields
            clean_employees = []
            for emp in valid_employees:
                clean_emp = {k: v for k, v in emp.items() if not k.startswith('_')}
                clean_employees.append(clean_emp)
            
            df = pd.DataFrame(clean_employees)
            df.to_csv(filename, index=False)
            
            self.logger.info(f"Exported {len(clean_employees)} employees to {filename}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to export to CSV: {str(e)}")
            return False

def main():
    """Main function to demonstrate the scraper."""
    print(" Google Drive Employee Data Scraper (Python)")
    print("=" * 60)
    
    # Initialize scraper
    scraper = GoogleDriveEmployeeScraper(
        max_retries=3,
        retry_delay=2,
        log_level="INFO"
    )
    
    # Google Drive URL
    url = "https://drive.google.com/uc?id=1AWPf-pJodJKeHsARQK_RHiNsE8fjPCVK&export=download"
    
    # Scrape data
    results = scraper.scrape_employee_data(url)
    
    # Display results
    print("\n SCRAPING RESULTS")
    print("=" * 60)
    
    if results['success']:
        print(" Status: SUCCESS")
        print(f" File Type: {results['file_info']['type'].upper()}")
        print(f" File Size: {results['file_info']['size_bytes']:,} bytes")
        print(f"  Processing Time: {results['processing_time_seconds']} seconds")
        print(f" Total Rows: {results['data_summary']['total_rows_processed']}")
        print(f" Employees Mapped: {results['data_summary']['employees_mapped']}")
        print(f" Valid Records: {results['data_summary']['valid_employees']}")
        print(f" Invalid Records: {results['data_summary']['invalid_employees']}")
        print(f" Success Rate: {results['data_summary']['success_rate']}%")
        
        # Show sample valid employees
        valid_employees = results['employees']['valid']
        if valid_employees:
            print(f"\n👥 SAMPLE VALID EMPLOYEES (showing first 3):")
            print("-" * 40)
            for i, emp in enumerate(valid_employees[:3], 1):
                print(f"{i}. {emp.get('first_name', 'N/A')} {emp.get('last_name', 'N/A')}")
                print(f"   ID: {emp.get('employee_id', 'N/A')}")
                print(f"   Email: {emp.get('email', 'N/A')}")
                print(f"   Title: {emp.get('job_title', 'N/A')}")
                print(f"   Phone: {emp.get('phone_number', 'N/A')}")
                print(f"   Hire Date: {emp.get('hire_date', 'N/A')}")
                if emp.get('_warnings'):
                    print(f"     Warnings: {', '.join(emp['_warnings'])}")
                print()
        
        # Show validation issues
        invalid_employees = results['employees']['invalid']
        if invalid_employees:
            print("  VALIDATION ISSUES (showing first 3):")
            print("-" * 40)
            for invalid in invalid_employees[:3]:
                print(f"Row {invalid['row']}: {', '.join(invalid['issues'])}")
                if invalid['warnings']:
                    print(f"  Warnings: {', '.join(invalid['warnings'])}")
            print()
        
        # Export to CSV
        if scraper.export_to_csv(results, "scraped_employees.csv"):
            print(" Data exported to 'scraped_employees.csv'")
    
    else:
        print(" Status: FAILED")
        print(f" Error: {results['error']}")
        print(f" Timestamp: {results['timestamp']}")

if __name__ == "__main__":
    main()
