"""
Functions for extracting tabular data from BC LDB Quarterly Reports (LMR)
Python replacement for ldb_extract_functions_v2.R
"""

import os
import re
import pandas as pd
import requests
from pathlib import Path
from urllib.parse import unquote
import PyPDF2
import pdfplumber
from typing import Dict, List, Tuple, Optional
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LMRExtractor:
    """Main class for extracting data from LMR PDFs"""
    
    def __init__(self, input_folder: str = "input/"):
        self.input_folder = Path(input_folder)
        self.input_folder.mkdir(parents=True, exist_ok=True)
        self.output_folder = Path("output/")
        self.output_folder.mkdir(parents=True, exist_ok=True)
        self.data_folder = Path("../data/")
        self.data_folder.mkdir(exist_ok=True)
        
    def clean_filename_from_url(self, url: str) -> str:
        """Convert URL to standardized filename format"""
        url_clean = unquote(url).replace("%20", "_")
        url_parts = url_clean.split("/")
        filename_parts = url_parts[-1].split("_")
        
        if len(filename_parts) >= 8:
            # Standard format with FY reference
            fy_qtr = f"FY{filename_parts[4]}{filename_parts[5]}"
            year = filename_parts[7].replace(".pdf", "")
            month_map = {
                'March': '03', 'June': '06', 
                'September': '09', 'December': '12'
            }
            month = month_map.get(filename_parts[6], '00')
            
        elif len(filename_parts) == 6:
            # Format without FY reference
            year = filename_parts[5].replace(".pdf", "")
            cal_year = int(year.replace("20", ""))
            
            # Determine fiscal year
            if filename_parts[4] == 'March':
                fy = cal_year
            else:
                fy = cal_year + 1
                
            fy_qtr = f"FY{fy}{filename_parts[3]}"
            month_map = {
                'March': '03', 'June': '06', 
                'September': '09', 'December': '12'
            }
            month = month_map.get(filename_parts[4], '00')
            
        else:
            # Fallback for unexpected formats
            logger.warning(f"Unexpected filename format: {url_parts[-1]}")
            return url_parts[-1]
            
        return f"LMR_{year}_{month}_{fy_qtr}.pdf"
    
    def download_pdf(self, url: str, force_download: bool = False) -> Tuple[str, str]:
        """Download PDF if not exists, return filename and path"""
        filename = self.clean_filename_from_url(url)
        filepath = self.input_folder / filename
        
        # Update/maintain file list
        self._update_file_list(filename, url)
        
        if not filepath.exists() or force_download:
            logger.info(f"Downloading {filename}...")
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info(f"Downloaded: {filename}")
        else:
            logger.info(f"Using existing file: {filename}")
            
        return filename, str(filepath)
    
    def _update_file_list(self, filename: str, url: str):
        """Maintain list of files and URLs"""
        list_file = self.input_folder / "01_lmr_list.csv"
        
        new_entry = pd.DataFrame({
            'lmr_name': [filename], 
            'lmr_url': [url]
        })
        
        if list_file.exists():
            existing_list = pd.read_csv(list_file)
            if filename not in existing_list['lmr_name'].values:
                updated_list = pd.concat([existing_list, new_entry], ignore_index=True)
                updated_list.sort_values('lmr_name').to_csv(list_file, index=False)
        else:
            new_entry.to_csv(list_file, index=False)
    
    def extract_text_from_pdf(self, filepath: str) -> List[str]:
        """Extract text from PDF pages using pdfplumber"""
        pages_text = []
        
        with pdfplumber.open(filepath) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
                else:
                    pages_text.append("")
                    
        return pages_text
    
    def is_table_page(self, page_text: str) -> bool:
        """Determine if page contains a data table"""
        # Skip pages with chart indicators or glossary
        if re.search(r"Item Subcategory", page_text):
            return False
        if re.search(r"Glossary", page_text, re.IGNORECASE):
            return False
        
        # Look for table indicators (column headers, data patterns)
        table_indicators = [
            r"Current Quarter",
            r"Same Quarter",
            r"Year to Date",
            r"\$\d+",  # Dollar amounts
            r"\d+\.\d+",  # Decimal numbers (likely litres)
        ]
        
        return any(re.search(pattern, page_text) for pattern in table_indicators)
    
    def extract_page_metadata(self, page_text: str, page_num: int, 
                            prev_table_name: str = "", prev_cat_type: str = "") -> Dict:
        """Extract metadata from page (table name, category type, metric type)"""
        lines = page_text.strip().split('\n')
        
        # Category type patterns
        cat_type_patterns = {
            'beer': r'beer',
            'wine': r'wine', 
            'spirits': r'spirits',
            'refreshment': r'refreshment|cooler'
        }
        
        # Metric type patterns  
        metric_patterns = {
            'netsales': r'net sales|\$',
            'litres': r'litres|volume'
        }
        
        cat_type = prev_cat_type
        metric = 'netsales'  # default
        table_name = prev_table_name
        is_continuation = False
        
        # Find category type and metric in first few lines
        header_text = ' '.join(lines[:5]).lower()
        
        for cat, pattern in cat_type_patterns.items():
            if re.search(pattern, header_text, re.IGNORECASE):
                cat_type = cat
                break
                
        for met, pattern in metric_patterns.items():
            if re.search(pattern, header_text, re.IGNORECASE):
                metric = met
                break
        
        # Generate table name
        if cat_type and metric:
            new_table_name = f"{cat_type}_{metric}"
            if new_table_name == prev_table_name:
                is_continuation = True
            table_name = new_table_name
            
        return {
            'table_name': table_name,
            'cat_type': cat_type,
            'metric': metric,
            'page_num': page_num,
            'is_continuation': is_continuation
        }
    
    def clean_page_content(self, page_text: str) -> List[str]:
        """Clean page content by removing noise and empty lines"""
        lines = page_text.strip().split('\n')
        
        # Remove empty lines and common noise
        cleaned_lines = []
        for line in lines:
            line = line.strip()
            if line and not re.match(r'^\d+$', line):  # Skip page numbers
                cleaned_lines.append(line)
                
        return cleaned_lines
    
    def extract_table_data(self, lines: List[str], metadata: Dict, 
                          categories_df: pd.DataFrame) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
        """Extract tabular data from page lines"""
        # Find data rows (containing numbers)
        data_rows = []
        header_row = None
        
        for i, line in enumerate(lines):
            # Look for header row with quarters/periods
            if re.search(r'Current Quarter|Same Quarter|Year to Date', line):
                header_row = line
                continue
                
            # Look for data rows (containing dollar amounts or decimal numbers)
            if re.search(r'\$[\d,]+|\d+\.\d+', line):
                data_rows.append(line)
        
        if not data_rows or not header_row:
            logger.warning(f"No data found on page {metadata['page_num']}")
            return None
            
        # Parse header to get column names
        columns = self._parse_header_row(header_row)
        
        # Parse data rows
        parsed_data = []
        for row in data_rows:
            parsed_row = self._parse_data_row(row, columns, categories_df, metadata)
            if parsed_row:
                parsed_data.append(parsed_row)
        
        if not parsed_data:
            return None
            
        # Create wide format DataFrame
        df_wide = pd.DataFrame(parsed_data)
        
        # Create long format DataFrame
        df_long = self._pivot_to_long(df_wide, metadata)
        
        return df_wide, df_long
    
    def _parse_header_row(self, header_row: str) -> List[str]:
        """Parse header row to extract column names"""
        # Common column patterns in LMR reports
        columns = ['category', 'subcategory']  # Always present
        
        # Look for quarter/period columns
        if 'Current Quarter' in header_row:
            columns.append('current_quarter')
        if 'Same Quarter' in header_row:
            columns.append('same_quarter_prev_year')
        if 'Year to Date' in header_row:
            columns.append('ytd_current')
            columns.append('ytd_prev_year')
            
        return columns
    
    def _parse_data_row(self, row: str, columns: List[str], 
                       categories_df: pd.DataFrame, metadata: Dict) -> Optional[Dict]:
        """Parse individual data row"""
        # Split row by multiple spaces or tabs
        parts = re.split(r'\s{2,}|\t+', row.strip())
        
        if len(parts) < 2:
            return None
            
        # First part is usually category/subcategory
        category_part = parts[0]
        
        # Try to match against known categories
        category, subcategory = self._match_category(category_part, categories_df, metadata['cat_type'])
        
        # Extract numeric values
        numeric_values = []
        for part in parts[1:]:
            cleaned_value = self._clean_numeric_value(part)
            if cleaned_value is not None:
                numeric_values.append(cleaned_value)
        
        # Map values to columns
        row_data = {
            'cat_type': metadata['cat_type'],
            'category': category,
            'subcategory': subcategory
        }
        
        # Map numeric values to column names
        numeric_columns = [col for col in columns if col not in ['category', 'subcategory']]
        for i, col in enumerate(numeric_columns):
            if i < len(numeric_values):
                row_data[col] = numeric_values[i]
            else:
                row_data[col] = None
                
        return row_data
    
    def _match_category(self, category_text: str, categories_df: pd.DataFrame, 
                       cat_type: str) -> Tuple[str, str]:
        """Match category text against known categories"""
        # Filter categories for current category type
        cat_subset = categories_df[categories_df['cat_type'] == cat_type] if not categories_df.empty else pd.DataFrame()
        
        # Simple matching logic - can be enhanced
        category_text_clean = category_text.strip().lower()
        
        if not cat_subset.empty:
            for _, row in cat_subset.iterrows():
                if category_text_clean in row['subcategory'].lower():
                    return row['category'], row['subcategory']
                    
        # Default fallback
        return category_text, category_text
    
    def _clean_numeric_value(self, value_str: str) -> Optional[float]:
        """Clean and convert numeric string to float"""
        if not value_str:
            return None
            
        # Remove $ signs, commas, and other formatting
        cleaned = re.sub(r'[,$]', '', value_str.strip())
        
        # Handle parentheses for negative values
        if '(' in cleaned and ')' in cleaned:
            cleaned = '-' + cleaned.replace('(', '').replace(')', '')
            
        try:
            return float(cleaned)
        except ValueError:
            return None
    
    def _pivot_to_long(self, df_wide: pd.DataFrame, metadata: Dict) -> pd.DataFrame:
        """Convert wide format to long format for database upload"""
        # Identify value columns (non-category columns)
        id_cols = ['cat_type', 'category', 'subcategory']
        value_cols = [col for col in df_wide.columns if col not in id_cols]
        
        if not value_cols:
            return pd.DataFrame()
            
        # Melt to long format
        df_long = df_wide.melt(
            id_vars=id_cols,
            value_vars=value_cols,
            var_name='period',
            value_name=metadata['metric']
        )
        
        return df_long
    
    def load_categories_reference(self) -> pd.DataFrame:
        """Load categories reference from most recent output file"""
        try:
            # Find most recent output file
            output_files = list(self.output_folder.glob("LMR_*.csv"))
            if not output_files:
                logger.warning("No existing category reference files found")
                return pd.DataFrame(columns=['cat_type', 'category', 'subcategory'])
                
            # Get most recent file
            latest_file = max(output_files, key=os.path.getmtime)
            df = pd.read_csv(latest_file)
            
            # Extract unique category combinations
            categories = df[['cat_type', 'category', 'subcategory']].drop_duplicates()
            
            # Save reference file
            categories.to_csv(self.output_folder / 'cat_subcat_all.csv', index=False)
            
            return categories
            
        except Exception as e:
            logger.error(f"Error loading categories reference: {e}")
            return pd.DataFrame(columns=['cat_type', 'category', 'subcategory'])
    
    def validate_data(self, df_netsales: pd.DataFrame, df_litres: pd.DataFrame) -> Dict:
        """Validate extracted data for completeness"""
        validation_results = {
            'missing_netsales': [],
            'missing_litres': [],
            'total_netsales_rows': len(df_netsales),
            'total_litres_rows': len(df_litres)
        }
        
        # Check for missing matches between netsales and litres
        join_cols = ['cat_type', 'category', 'subcategory', 'period']
        
        if not df_netsales.empty and not df_litres.empty:
            # Find rows in litres without matching netsales
            missing_netsales = df_litres.merge(
                df_netsales, on=join_cols, how='left', indicator=True
            ).query('_merge == "left_only"')[join_cols]
            
            # Find rows in netsales without matching litres  
            missing_litres = df_netsales.merge(
                df_litres, on=join_cols, how='left', indicator=True
            ).query('_merge == "left_only"')[join_cols]
            
            validation_results['missing_netsales'] = missing_netsales.to_dict('records')
            validation_results['missing_litres'] = missing_litres.to_dict('records')
            
        return validation_results
    
    def save_data(self, df_wide: pd.DataFrame, df_long: pd.DataFrame, 
                  filename_base: str, table_name: str):
        """Save data in both wide and long formats"""
        wide_path = self.data_folder / f"{filename_base}-{table_name}.csv"
        long_path = self.data_folder / f"{filename_base}-{table_name}_long.csv"
        
        df_wide.to_csv(wide_path, index=False)
        df_long.to_csv(long_path, index=False)
        
        logger.info(f"Saved: {wide_path.name}")
        logger.info(f"Saved: {long_path.name}")


def quick_data_check(df: pd.DataFrame):
    """Quick data validation check"""
    if df.empty:
        print("DataFrame is empty")
        return
        
    print(f"Total rows: {len(df)}")
    print(f"Unique categories: {df['cat_type'].nunique()}")
    
    # Check for missing values
    missing_data = df[df['netsales'].isna() | df['litres'].isna()]
    if not missing_data.empty:
        print(f"Rows with missing data: {len(missing_data)}")
        print(missing_data[['cat_type', 'category', 'subcategory']])
    else:
        print("No missing data found")
    
    # Summary by category
    summary = df.groupby('cat_type').agg({
        'netsales': ['count', 'sum'],
        'litres': ['count', 'sum']
    }).round(2)
    print("\nSummary by category:")
    print(summary)