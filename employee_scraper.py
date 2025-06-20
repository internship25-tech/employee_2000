import requests
import pandas as pd
import csv
import io
import time
import logging
import re
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
import openpyxl
from openpyxl import load_workbook
import phonenumbers
from phonenumbers import NumberParseException

class EnhancedGoogleDriveEmployeeScraper:
    """
    Enhanced scraper for downloading and processing ALL employee data from Google Drive files.
    Maps user_id to employee_id and preserves invalid phone numbers as-is.
    Designed to extract complete datasets (1000+ records).
    """
    
    def __init__(self, max_retries: int = 5, retry_delay: int = 3, log_level: str = "INFO"):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Setup logging
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)
        
        # Enhanced field mappings - Added user_id variations to employee_id mapping
        self.field_mappings = {
            'employee_id': [
                'employee_id', 'emp_id', 'id', 'employee id', 'empid', 'employee_number',
                'employee_no', 'emp_no', 'staff_id', 'staff_number', 'worker_id', 'badge_number',
                'user_id', 'userid', 'user id', 'user_number', 'user_no', 'uid', 'user'
            ],
            'first_name': [
                'first_name', 'firstname', 'first name', 'fname', 'given_name', 'first',
                'forename', 'christian_name', 'name_first'
            ],
            'last_name': [
                'last_name', 'lastname', 'last name', 'lname', 'surname', 'family_name', 'last',
                'name_last', 'family', 'name_family'
            ],
            'email': [
                'email', 'email_address', 'e_mail', 'mail', 'email address', 'e-mail',
                'work_email', 'business_email', 'company_email'
            ],
            'job_title': [
                'job_title', 'title', 'position', 'role', 'job title', 'designation', 'job_position',
                'job', 'occupation', 'post', 'rank', 'job_role', 'position_title'
            ],
            'phone_number': [
                'phone', 'phone_number', 'mobile', 'contact', 'telephone', 'phone number',
                'cell', 'cell_phone', 'mobile_phone', 'work_phone', 'business_phone',
                'tel', 'contact_number', 'phone_no', 'mobile_no', 'cell_no'
            ],
            'hire_date': [
                'hire_date', 'start_date', 'join_date', 'employment_date', 'hired_date', 'date_hired',
                'joining_date', 'start_employment', 'employment_start', 'date_joined', 'onboard_date'
            ],
            'date_of_birth': [
                'date_of_birth', 'dob', 'birth_date', 'birthdate', 'date of birth', 'birthday',
                'birth_day', 'born_date', 'date_born', 'birth', 'dob_date'
            ]
        }

    def download_file_with_retry(self, url: str) -> Tuple[bytes, Dict[str, str]]:
        """Download file from URL with enhanced retry logic for large files."""
        last_error = None
        
        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.info(f"Attempt {attempt}/{self.max_retries}: Downloading file from {url}")
                
                # Enhanced headers for better compatibility
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive',
                }
                
                # Increased timeout for large files
                response = self.session.get(url, headers=headers, timeout=60, stream=True)
                response.raise_for_status()
                
                # Download in chunks to handle large files
                content = b''
                chunk_size = 8192
                total_size = 0
                
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        content += chunk
                        total_size += len(chunk)
                        
                        # Log progress for large files
                        if total_size % (1024 * 1024) == 0:  # Every MB
                            self.logger.debug(f"Downloaded {total_size // (1024*1024)} MB...")
                
                if len(content) == 0:
                    raise ValueError("Downloaded file is empty")
                
                headers_info = {
                    'content_type': response.headers.get('content-type', ''),
                    'content_disposition': response.headers.get('content-disposition', ''),
                    'content_length': response.headers.get('content-length', '0')
                }
                
                self.logger.info(f"Successfully downloaded file ({len(content):,} bytes)")
                self.logger.debug(f"Headers: {headers_info}")
                
                return content, headers_info
                
            except Exception as e:
                last_error = e
                self.logger.error(f"Download attempt {attempt} failed: {str(e)}")
                
                if attempt < self.max_retries:
                    wait_time = self.retry_delay * attempt  # Exponential backoff
                    self.logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
        
        raise Exception(f"Failed to download file after {self.max_retries} attempts. Last error: {str(last_error)}")

    def detect_file_type(self, content: bytes, headers: Dict[str, str]) -> str:
        """Enhanced file type detection."""
        
        # Check file signature (magic numbers)
        if len(content) >= 4:
            signature = content[:4]
            
            # Excel XLSX signature (ZIP format)
            if signature[:2] == b'PK':
                # Check for Excel-specific files in ZIP
                if b'xl/' in content[:2000] or b'worksheets/' in content[:2000]:
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
                    return ext[1:]
        
        # Enhanced CSV detection
        try:
            text_sample = content[:5000].decode('utf-8', errors='ignore')  # Larger sample
            lines = text_sample.split('\n')[:10]  # Check more lines
            
            if len(lines) >= 2:
                # Check for consistent comma patterns
                comma_counts = [line.count(',') for line in lines if line.strip()]
                if comma_counts and len(set(comma_counts)) <= 3:  # Allow some variation
                    # Additional checks for CSV-like content
                    if any(',' in line and len(line.split(',')) > 2 for line in lines[:5]):
                        return 'csv'
        except:
            pass
        
        return 'unknown'

    def parse_csv_data(self, content: bytes) -> List[Dict[str, Any]]:
        """Enhanced CSV parsing to handle all data without truncation."""
        try:
            # Try different encodings
            text_content = None
            for encoding in ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']:
                try:
                    text_content = content.decode(encoding)
                    self.logger.debug(f"Successfully decoded with {encoding}")
                    break
                except UnicodeDecodeError:
                    continue
            
            if text_content is None:
                raise ValueError("Unable to decode CSV file with any supported encoding")
            
            # Use pandas with enhanced options for large datasets
            df = pd.read_csv(
                io.StringIO(text_content),
                encoding=None,  # Let pandas auto-detect
                low_memory=False,  # Read entire file into memory for consistency
                na_values=['', 'NULL', 'null', 'N/A', 'n/a', 'NA', 'na', '#N/A'],
                keep_default_na=True,
                skip_blank_lines=True
            )
            
            # Remove completely empty rows
            df = df.dropna(how='all')
            
            # Log detailed information about the dataset
            self.logger.info(f"Parsed {len(df)} rows and {len(df.columns)} columns from CSV file")
            self.logger.info(f"Columns found: {list(df.columns)}")
            
            # Convert to records without any truncation
            records = df.to_dict('records')
            
            self.logger.info(f"Successfully converted to {len(records)} employee records")
            return records
            
        except Exception as e:
            raise Exception(f"Failed to parse CSV data: {str(e)}")

    def parse_excel_data(self, content: bytes, file_type: str) -> List[Dict[str, Any]]:
        """Enhanced Excel parsing to handle all data without truncation."""
        try:
            # Use pandas with enhanced options for large datasets
            engine = 'openpyxl' if file_type == 'xlsx' else 'xlrd'
            
            df = pd.read_excel(
                io.BytesIO(content),
                engine=engine,
                na_values=['', 'NULL', 'null', 'N/A', 'n/a', 'NA', 'na', '#N/A'],
                keep_default_na=True
            )
            
            # Remove completely empty rows
            df = df.dropna(how='all')
            
            # Log detailed information
            self.logger.info(f"Parsed {len(df)} rows and {len(df.columns)} columns from Excel file")
            self.logger.info(f"Columns found: {list(df.columns)}")
            
            records = df.to_dict('records')
            
            self.logger.info(f"Successfully converted to {len(records)} employee records")
            return records
            
        except Exception as e:
            raise Exception(f"Failed to parse Excel file: {str(e)}")

    def normalize_field_name(self, field_name: str) -> str:
        """Enhanced field name normalization."""
        if pd.isna(field_name):
            return ""
        
        normalized = str(field_name).lower().strip()
        # Remove special characters and replace with underscore
        normalized = re.sub(r'[^\w\s]', '_', normalized)
        # Replace spaces and multiple underscores with single underscore
        normalized = re.sub(r'[\s_]+', '_', normalized)
        # Remove leading/trailing underscores
        normalized = normalized.strip('_')
        
        return normalized

    def preserve_phone_number(self, phone: str) -> str:
        """Preserve phone numbers as-is, only clean up whitespace."""
        if pd.isna(phone) or not phone:
            return ""
        
        # Only strip whitespace, preserve all other formatting
        phone_str = str(phone).strip()
        
        # Log if we're preserving what might be considered "invalid"
        if phone_str and not re.search(r'\d{7,}', phone_str):
            self.logger.debug(f"Preserving potentially invalid phone number: {phone_str}")
        
        return phone_str

    def parse_date(self, date_str: str) -> Optional[str]:
        """Enhanced date parsing for various formats."""
        if pd.isna(date_str) or not date_str:
            return None
        
        date_str = str(date_str).strip()
        
        # Common date formats to try
        date_formats = [
            '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%m-%d-%Y', '%d-%m-%Y',
            '%Y/%m/%d', '%d.%m.%Y', '%m.%d.%Y', '%Y.%m.%d',
            '%B %d, %Y', '%d %B %Y', '%b %d, %Y', '%d %b %Y',
            '%Y-%m-%d %H:%M:%S', '%m/%d/%Y %H:%M:%S'
        ]
        
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt).date()
                return parsed_date.isoformat()
            except ValueError:
                continue
        
        # Try pandas date parsing as fallback
        try:
            parsed_date = pd.to_datetime(date_str).date()
            return parsed_date.isoformat()
        except:
            pass
        
        return date_str  # Return original if can't parse

    def map_employee_fields(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enhanced field mapping including user_id to employee_id mapping."""
        normalized_data = []
        
        self.logger.info(f"Starting field mapping for {len(raw_data)} records")
        self.logger.info("Mapping user_id variations to employee_id field")
        
        for row_idx, row in enumerate(raw_data):
            employee = {}
            
            # Create normalized field lookup
            normalized_row = {}
            for key, value in row.items():
                if pd.notna(value) and str(value).strip():  # Skip empty values
                    normalized_key = self.normalize_field_name(str(key))
                    normalized_row[normalized_key] = str(value).strip()
            
            # Map fields using enhanced field mappings
            for standard_field, possible_keys in self.field_mappings.items():
                for possible_key in possible_keys:
                    normalized_key = self.normalize_field_name(possible_key)
                    if normalized_key in normalized_row:
                        value = normalized_row[normalized_key]
                        
                        # Apply field-specific processing
                        if standard_field == 'phone_number':
                            # Preserve phone numbers as-is
                            value = self.preserve_phone_number(value)
                        elif standard_field in ['hire_date', 'date_of_birth']:
                            value = self.parse_date(value)
                        elif standard_field == 'email':
                            value = value.lower().strip()
                        elif standard_field == 'employee_id':
                            # Log when we map user_id to employee_id
                            if 'user' in normalized_key:
                                self.logger.debug(f"Mapped {possible_key} -> employee_id: {value}")
                        
                        employee[standard_field] = value
                        break
            
            # Include rows with any identifying information
            if any(employee.get(field) for field in ['employee_id', 'first_name', 'last_name', 'email']):
                employee['_row_number'] = row_idx + 1
                employee['_original_data'] = {k: v for k, v in row.items() if pd.notna(v)}
                normalized_data.append(employee)
        
        self.logger.info(f"Successfully mapped {len(normalized_data)} employee records from {len(raw_data)} total rows")
        return normalized_data

    def validate_employee_data(self, employees: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Enhanced validation that preserves invalid phone numbers."""
        validation_results = {
            'valid': [],
            'invalid': [],
            'warnings': []
        }
        
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        
        for employee in employees:
            issues = []
            warnings = []
            
            # Enhanced identification validation
            has_id = bool(employee.get('employee_id'))
            has_name = bool(employee.get('first_name')) or bool(employee.get('last_name'))
            has_email = bool(employee.get('email'))
            
            if not (has_id or has_name or has_email):
                issues.append("Missing employee identification (ID, name, or email)")
            
            # Employee ID validation
            if not has_id:
                warnings.append("Missing employee ID")
            
            # Email validation
            email = employee.get('email')
            if email:
                if not email_pattern.match(email):
                    issues.append("Invalid email format")
            else:
                warnings.append("Missing email address")
            
            # Phone validation - PRESERVE ALL PHONE NUMBERS, just warn about format
            phone = employee.get('phone_number')
            if phone:
                # Check if phone looks unusual but DON'T reject it
                if not re.search(r'\d{7,}', phone):  # Less than 7 digits
                    warnings.append(f"Phone number may be invalid format: {phone}")
                # Always keep the phone number regardless of format
            else:
                warnings.append("Missing phone number")
            
            # Date validations
            hire_date = employee.get('hire_date')
            if hire_date:
                try:
                    if hire_date and hire_date != 'None':
                        parsed_date = datetime.fromisoformat(hire_date) if '-' in hire_date else pd.to_datetime(hire_date)
                        # Check if hire date is reasonable (not in future, not too old)
                        if parsed_date.year < 1950 or parsed_date.year > datetime.now().year:
                            warnings.append("Hire date seems unrealistic")
                except:
                    issues.append("Invalid hire date format")
            else:
                warnings.append("Missing hire date")
            
            # Date of birth validation
            dob = employee.get('date_of_birth')
            if dob:
                try:
                    if dob and dob != 'None':
                        parsed_dob = datetime.fromisoformat(dob) if '-' in dob else pd.to_datetime(dob)
                        # Check if DOB is reasonable
                        age = datetime.now().year - parsed_dob.year
                        if age < 16 or age > 100:
                            warnings.append("Date of birth seems unrealistic")
                except:
                    issues.append("Invalid date of birth format")
            else:
                warnings.append("Missing date of birth")
            
            # Job title validation
            if not employee.get('job_title'):
                warnings.append("Missing job title")
            
            # Categorize the record - be more lenient, only reject for critical issues
            critical_issues = [issue for issue in issues if "Missing employee identification" in issue or "Invalid email format" in issue or "Invalid date" in issue]
            
            if not critical_issues:
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
        self.logger.info("Phone numbers preserved as-is regardless of format")
        
        return validation_results

    def scrape_employee_data(self, url: str) -> Dict[str, Any]:
        """Enhanced main method to scrape ALL employee data (1000+ records)."""
        try:
            self.logger.info("Starting enhanced employee data scraping process")
            self.logger.info("Target: Extract ALL available records (1000+)")
            self.logger.info("Special handling: user_id -> employee_id, preserve invalid phone numbers")
            start_time = time.time()
            
            # Step 1: Download file with enhanced retry logic
            content, headers = self.download_file_with_retry(url)
            
            # Step 2: Detect file type
            file_type = self.detect_file_type(content, headers)
            self.logger.info(f"Detected file type: {file_type}")
            
            if file_type == 'unknown':
                raise ValueError("Unable to determine file type. Supported formats: CSV, Excel (.xlsx, .xls)")
            
            # Step 3: Parse file based on type (no data limits)
            if file_type == 'csv':
                raw_data = self.parse_csv_data(content)
            elif file_type in ['xlsx', 'xls']:
                raw_data = self.parse_excel_data(content, file_type)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            
            if not raw_data:
                raise ValueError("No data found in the file")
            
            self.logger.info(f"Raw data extraction complete: {len(raw_data)} total records")
            
            # Step 4: Map fields to standard format (all records)
            mapped_data = self.map_employee_fields(raw_data)
            
            # Step 5: Validate data (all records)
            validation_results = self.validate_employee_data(mapped_data)
            
            # Step 6: Compile comprehensive results
            processing_time = time.time() - start_time
            
            results = {
                'success': True,
                'timestamp': datetime.now().isoformat(),
                'processing_time_seconds': round(processing_time, 2),
                'file_info': {
                    'type': file_type,
                    'size_bytes': len(content),
                    'size_mb': round(len(content) / (1024*1024), 2),
                    'headers': headers
                },
                'data_summary': {
                    'total_rows_in_file': len(raw_data),
                    'employees_mapped': len(mapped_data),
                    'valid_employees': len(validation_results['valid']),
                    'invalid_employees': len(validation_results['invalid']),
                    'success_rate': round((len(validation_results['valid']) / len(mapped_data)) * 100, 2) if mapped_data else 0,
                    'target_achieved': len(validation_results['valid']) >= 1000
                },
                'field_coverage': self._analyze_field_coverage(validation_results['valid']),
                'phone_number_stats': self._analyze_phone_numbers(validation_results['valid']),
                'employees': {
                    'valid': validation_results['valid'],
                    'invalid': validation_results['invalid']
                }
            }
            
            self.logger.info(f"Scraping completed successfully in {processing_time:.2f} seconds")
            self.logger.info(f"Extracted {len(validation_results['valid'])} valid employee records")
            self.logger.info(f"Target of 1000+ records: {' ACHIEVED' if results['data_summary']['target_achieved'] else ' NOT ACHIEVED'}")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Scraping failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

    def _analyze_field_coverage(self, valid_employees: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze field coverage across all valid employees."""
        if not valid_employees:
            return {}
        
        total_records = len(valid_employees)
        field_stats = {}
        
        for field in ['employee_id', 'first_name', 'last_name', 'email', 'job_title', 'phone_number', 'hire_date', 'date_of_birth']:
            count = sum(1 for emp in valid_employees if emp.get(field))
            field_stats[field] = {
                'count': count,
                'percentage': round((count / total_records) * 100, 2)
            }
        
        return field_stats

    def _analyze_phone_numbers(self, valid_employees: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze phone number formats and preservation."""
        phone_stats = {
            'total_with_phone': 0,
            'potentially_invalid': 0,
            'formats_preserved': [],
            'sample_formats': []
        }
        
        for emp in valid_employees:
            phone = emp.get('phone_number')
            if phone:
                phone_stats['total_with_phone'] += 1
                
                # Check if potentially invalid (less than 7 digits)
                if not re.search(r'\d{7,}', phone):
                    phone_stats['potentially_invalid'] += 1
                
                # Collect sample formats (first 10 unique)
                if len(phone_stats['sample_formats']) < 10 and phone not in phone_stats['sample_formats']:
                    phone_stats['sample_formats'].append(phone)
        
        return phone_stats

    def export_to_csv(self, results: Dict[str, Any], filename: str = "all_employees_complete.csv") -> bool:
        """Export ALL valid employee data to CSV file."""
        try:
            if not results.get('success') or not results.get('employees', {}).get('valid'):
                self.logger.warning("No valid employee data to export")
                return False
            
            valid_employees = results['employees']['valid']
            
            # Remove internal fields but keep all employee data
            clean_employees = []
            for emp in valid_employees:
                clean_emp = {k: v for k, v in emp.items() if not k.startswith('_')}
                clean_employees.append(clean_emp)
            
            df = pd.DataFrame(clean_employees)
            df.to_csv(filename, index=False)
            
            self.logger.info(f"Exported ALL {len(clean_employees)} employees to {filename}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to export to CSV: {str(e)}")
            return False

def main():
    """Enhanced main function demonstrating user_id mapping and phone preservation."""
    print(" Enhanced Google Drive Employee Data Scraper (Python)")
    print(" Target: Extract ALL 1000+ Employee Records")
    print(" Special: user_id -> employee_id mapping")
    print(" Special: Preserve invalid phone numbers as-is")
    print("=" * 70)
    
    # Initialize enhanced scraper
    scraper = EnhancedGoogleDriveEmployeeScraper(
        max_retries=5,
        retry_delay=3,
        log_level="INFO"
    )
    
    # Google Drive URL
    url = "https://drive.google.com/uc?id=1AWPf-pJodJKeHsARQK_RHiNsE8fjPCVK&export=download"
    
    # Scrape ALL data
    results = scraper.scrape_employee_data(url)
    
    # Display comprehensive results
    print("\n COMPLETE SCRAPING RESULTS")
    print("=" * 70)
    
    if results['success']:
        print(" Status: SUCCESS")
        print(f" File Type: {results['file_info']['type'].upper()}")
        print(f" File Size: {results['file_info']['size_mb']} MB ({results['file_info']['size_bytes']:,} bytes)")
        print(f"  Processing Time: {results['processing_time_seconds']} seconds")
        print(f" Total Rows in File: {results['data_summary']['total_rows_in_file']:,}")
        print(f" Employees Mapped: {results['data_summary']['employees_mapped']:,}")
        print(f" Valid Records: {results['data_summary']['valid_employees']:,}")
        print(f" Invalid Records: {results['data_summary']['invalid_employees']:,}")
        print(f" Success Rate: {results['data_summary']['success_rate']}%")
        print(f" 1000+ Target: {' ACHIEVED' if results['data_summary']['target_achieved'] else ' NOT ACHIEVED'}")
        
        # Field coverage analysis
        print(f"\n FIELD COVERAGE ANALYSIS:")
        print("-" * 50)
        coverage = results['field_coverage']
        for field, stats in coverage.items():
            print(f"{field.replace('_', ' ').title()}: {stats['count']:,} records ({stats['percentage']}%)")
        
        # Phone number analysis
        phone_stats = results['phone_number_stats']
        print(f"\n PHONE NUMBER PRESERVATION ANALYSIS:")
        print("-" * 50)
        print(f"Total with phone numbers: {phone_stats['total_with_phone']:,}")
        print(f"Potentially invalid formats preserved: {phone_stats['potentially_invalid']:,}")
        print(f"Sample phone formats preserved:")
        for i, phone_format in enumerate(phone_stats['sample_formats'][:5], 1):
            print(f"  {i}. {phone_format}")
        
        # Show sample valid employees with all fields
        valid_employees = results['employees']['valid']
        if valid_employees:
            print(f"\n👥 SAMPLE COMPLETE EMPLOYEE RECORDS (showing first 5):")
            print("-" * 60)
            for i, emp in enumerate(valid_employees[:5], 1):
                print(f"{i}. Employee ID: {emp.get('employee_id', 'N/A')}")
                print(f"   Name: {emp.get('first_name', 'N/A')} {emp.get('last_name', 'N/A')}")
                print(f"   Email: {emp.get('email', 'N/A')}")
                print(f"   Job Title: {emp.get('job_title', 'N/A')}")
                print(f"   Phone (preserved): {emp.get('phone_number', 'N/A')}")
                print(f"   Hire Date: {emp.get('hire_date', 'N/A')}")
                print(f"   Date of Birth: {emp.get('date_of_birth', 'N/A')}")
                if emp.get('_warnings'):
                    print(f"     Warnings: {', '.join(emp['_warnings'])}")
                print()
        
        # Show validation issues
        invalid_employees = results['employees']['invalid']
        if invalid_employees:
            print("  VALIDATION ISSUES (showing first 5):")
            print("-" * 50)
            for invalid in invalid_employees[:5]:
                print(f"Row {invalid['row']}: {', '.join(invalid['issues'])}")
                if invalid['warnings']:
                    print(f"  Warnings: {', '.join(invalid['warnings'])}")
            print()
        
        # Export ALL data to CSV
        if scraper.export_to_csv(results, "complete_employee_dataset_with_preserved_phones.csv"):
            print(" ALL employee data exported to 'complete_employee_dataset_with_preserved_phones.csv'")
            print(f" Total records exported: {len(valid_employees):,}")
            print(" All phone numbers preserved in original format")
    
    else:
        print(" Status: FAILED")
        print(f" Error: {results['error']}")
        print(f" Timestamp: {results['timestamp']}")

if __name__ == "__main__":
    main()
