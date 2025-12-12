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
        
        # Load category reference data
        self.category_reference = self.load_category_reference()
        
        # Track category mismatches for reporting
        self.category_mismatches = set()
        
    def load_category_reference(self) -> pd.DataFrame:
        """Load category reference data from input/cat_subcat_all.csv"""
        ref_path = self.input_folder / "cat_subcat_all.csv"
        
        if not ref_path.exists():
            logger.warning(f"Category reference file not found: {ref_path}")
            return pd.DataFrame(columns=['cat_type', 'category', 'subcategory'])
            
        try:
            reference_df = pd.read_csv(ref_path)
            logger.info(f"Loaded {len(reference_df)} category references from {ref_path}")
            return reference_df
        except Exception as e:
            logger.error(f"Error loading category reference: {e}")
            return pd.DataFrame(columns=['cat_type', 'category', 'subcategory'])
        
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
    
    # NOT used...download pdf manually and save in input folder
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
    
    # NOT used
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
        
        # Look for actual LMR table patterns
        table_indicators = [
            r"Net Sales \$ Fiscal",     # Net sales tables
            r"Litres Fiscal",          # Volume tables  
            r"Fiscal \d{4}/\d{2}",     # Fiscal year patterns
            r"\$[\d,]+",               # Dollar amounts
            r"\d{1,3}(?:,\d{3})+",     # Large numbers with commas (like 30,186,179)
            r"Sales \(Net \$\)",       # Sales header pattern
            r"Sales \(Litres\)",       # Volume header pattern
        ]
        
        return any(re.search(pattern, page_text) for pattern in table_indicators)
    
    def extract_page_metadata(self, page_text: str, page_num: int, 
                            prev_table_name: str = "", prev_cat_type: str = "") -> Dict:
        """Extract metadata from page (table name, category type, metric type)"""
        lines = page_text.strip().split('\n')
        
        # Category type patterns - look at first few lines for titles
        cat_type_patterns = {
            'Beer': r'beer',
            'Wine': r'wine', 
            'Spirits': r'spirits',
            'Refreshment Beverages': r'refreshment|cooler'
        }
        
        # Metric type patterns - more specific for LMR format
        metric_patterns = {
            'netsales': r'net sales|\(net \$\)|\$ fiscal',
            'litres': r'litres|\(litres\)'
        }
        
        cat_type = prev_cat_type
        metric = 'netsales'  # default
        table_name = prev_table_name
        is_continuation = False
        
        # Analyze first few lines for metadata
        header_text = ' '.join(lines[:5]).lower()
        
        # More precise category detection - look for specific patterns
        for cat, pattern in cat_type_patterns.items():
            if re.search(pattern, header_text, re.IGNORECASE):
                cat_type = cat
                break
        
        # If no category found, try to infer from context
        if not cat_type and lines:
            first_line = lines[0].lower()
            if 'beer' in first_line:
                cat_type = 'Beer'
            elif 'wine' in first_line:
                cat_type = 'Wine'
            elif 'spirits' in first_line:
                cat_type = 'Spirits'
            elif 'refreshment' in first_line or 'cooler' in first_line:
                cat_type = 'Refreshment Beverages'
                
        # Detect metric type
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
        # Find header row with fiscal year pattern
        header_row = None
        data_start_idx = None
        
        for i, line in enumerate(lines):
            # Look for header row with fiscal year pattern
            if re.search(r'Fiscal \d{4}/\d{2}', line):
                header_row = line
                data_start_idx = i + 1
                break
        
        if not header_row:
            logger.warning(f"No header row found on page {metadata['page_num']}")
            return None
            
        # Extract data rows from after header
        data_rows = []
        if data_start_idx and data_start_idx < len(lines):
            for line in lines[data_start_idx:]:
                # Look for lines with numbers, dollar amounts, decimal values, or negatives
                if re.search(r'(\([^)]*\$[0-9,]+\)|\$[0-9,]+|-[0-9,]+|(?<!\$)[0-9,]+|[0-9]+\.[0-9]+)', line):
                    # Skip summary rows, headers, and footer content
                    if not re.search(r'Summary|Total', line, re.IGNORECASE):
                        # Skip footer lines with "BC Liquor Distribution Branch"
                        if not re.search(r'BC Liquor Distribution Branch.*Liquor Market Review', line, re.IGNORECASE):
                            # Skip inventory adjustment footer notes
                            if not re.search(r'As a result of.*inventory adjustments.*products', line, re.IGNORECASE):
                                # Skip page number only lines
                                if not re.match(r'^\s*\d{1,2}\s*$', line):
                                    data_rows.append(line)
        
        if not data_rows:
            logger.warning(f"No data rows found on page {metadata['page_num']}")
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
            logger.warning(f"No parseable data rows on page {metadata['page_num']}")
            return None
            
        # Create wide format DataFrame
        df_wide = pd.DataFrame(parsed_data)
        
        # Create long format DataFrame
        df_long = self._pivot_to_long(df_wide, metadata)
        
        return df_wide, df_long
    
    def _parse_header_row(self, header_row: str) -> List[str]:
        """Parse header row to extract column names"""
        columns = ['category', 'subcategory']  # Always present
        
        # Look for fiscal year patterns like "Fiscal 2024/25 Q1"
        fiscal_quarters = re.findall(r'Fiscal (\d{4})/(\d{2}) Q(\d)', header_row)
        for year1, year2, quarter in fiscal_quarters:
            # Convert "Fiscal 2024/25 Q1" to "FY2025Q1" 
            # Use the second year (2025 from 2024/25) with full 4 digits
            full_year2 = f"20{year2}" if len(year2) == 2 else year2
            column_name = f"FY{full_year2}Q{quarter}"
            columns.append(column_name)
            
        # If no fiscal quarters found, try to find other period indicators
        if len(columns) == 2:  # Only category and subcategory
            # Look for year patterns like "2024/25"
            year_patterns = re.findall(r'(\d{4})/(\d{2})', header_row)
            for i, (year1, year2) in enumerate(year_patterns):
                full_year2 = f"20{year2}" if len(year2) == 2 else year2
                columns.append(f"FY{full_year2}Q{i+1}")
                
        return columns
    
    def _parse_data_row(self, row: str, columns: List[str], 
                       categories_df: pd.DataFrame, metadata: Dict) -> Optional[Dict]:
        """Parse individual data row"""
        # Extract all dollar amounts and numbers first (including small numbers and negatives)
        numeric_values = []
        # Extract all numeric values in document order (left to right)
        # Combined pattern that preserves order and handles all formats
        numeric_matches = re.findall(r'\([^)]*\$[0-9,]+\)|\$[0-9,]+|-[0-9,]+|[0-9,]+', row)
        
        for match in numeric_matches:
            cleaned_value = self._clean_numeric_value(match)
            if cleaned_value is not None:
                numeric_values.append(cleaned_value)
        
        if not numeric_values:
            return None
            
        # Extract category/subcategory part by removing all numeric values
        category_part = row
        for match in numeric_matches:
            category_part = category_part.replace(match, '', 1)
        category_part = category_part.strip()
        
        # Clean up extra spaces
        category_part = re.sub(r'\s+', ' ', category_part)
        
        # Try to match against known categories or parse manually
        category, subcategory = self._match_category(category_part, categories_df, metadata['cat_type'])
        
        # If no match found, try to split category part
        if category == category_part and subcategory == category_part:
            # Try to split by common patterns
            if ' - ' in category_part:
                parts = category_part.split(' - ', 1)
                category = parts[0].strip()
                subcategory = parts[1].strip() if len(parts) > 1 else category
            else:
                category = category_part
                subcategory = category_part
        
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
        """Match category text against reference categories with mismatch tracking"""
        category_text_clean = category_text.strip()
        
        # Use the loaded reference data instead of categories_df parameter
        if not self.category_reference.empty:
            # Filter reference for current category type
            cat_subset = self.category_reference[self.category_reference['cat_type'] == cat_type]
            
            if not cat_subset.empty:
                # Try exact match first (exact equality)
                for _, row in cat_subset.iterrows():
                    if category_text_clean.lower() == row['subcategory'].lower():
                        return row['category'], row['subcategory']
                
                # Try exact substring match (category text is exactly in subcategory)
                for _, row in cat_subset.iterrows():
                    if category_text_clean.lower() in row['subcategory'].lower():
                        return row['category'], row['subcategory']
                
                # Try reverse match (subcategory substring in text)
                for _, row in cat_subset.iterrows():
                    subcategory_lower = row['subcategory'].lower()
                    if subcategory_lower in category_text_clean.lower():
                        return row['category'], row['subcategory']
                
                # Try keyword matching (at least 2 matching words > 3 chars)
                for _, row in cat_subset.iterrows():
                    subcategory_words = set(word for word in row['subcategory'].lower().split() if len(word) > 3)
                    text_words = set(word for word in category_text_clean.lower().split() if len(word) > 3)
                    matching_words = subcategory_words.intersection(text_words)
                    if len(matching_words) >= 2:
                        return row['category'], row['subcategory']
        
        # Track mismatch for reporting
        mismatch_key = f"{cat_type}: {category_text_clean}"
        self.category_mismatches.add(mismatch_key)
        logger.warning(f"Category mismatch: {mismatch_key}")
        
        # Default fallback
        return category_text_clean, category_text_clean
    
    def report_category_mismatches(self):
        """Report all category mismatches found during extraction"""
        if self.category_mismatches:
            logger.warning(f"Found {len(self.category_mismatches)} category mismatches:")
            print("\n=== CATEGORY MISMATCHES ===")
            print("The following categories/subcategories were not found in reference file:")
            for mismatch in sorted(self.category_mismatches):
                print(f"  - {mismatch}")
            print("\nConsider adding these to input/cat_subcat_all.csv if they are valid categories.")
            print("=" * 50)
        else:
            logger.info("No category mismatches found - all categories matched reference file")
    
    def _clean_numeric_value(self, value_str: str) -> Optional[float]:
        """Clean and convert numeric string to float"""
        if not value_str:
            return None
            
        # Handle bracket format negatives first: ($123,456) or (123,456)
        if value_str.startswith('(') and value_str.endswith(')'):
            # Remove brackets and $ signs, add negative sign
            cleaned = '-' + re.sub(r'[(),$]', '', value_str.strip())
        else:
            # Remove $ signs, commas, and other formatting
            cleaned = re.sub(r'[,$]', '', value_str.strip())
            
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
            categories.to_csv(self.output_folder / 'cat_subcat_latest.csv', index=False)
            
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
    
    # Find most recent quarter for quality check comparison
    if 'fy_qtr' in df.columns:
        # Get most recent quarter (assumes fiscal quarters are sortable)
        most_recent_qtr = df['fy_qtr'].max()
        recent_data = df[df['fy_qtr'] == most_recent_qtr]
        
        print(f"\n=== MOST RECENT QUARTER SUMMARY ({most_recent_qtr}) ===")
        print("For easy comparison with PDF source:")
        
        recent_summary = recent_data.groupby('cat_type').agg({
            'netsales': ['count', 'sum'],
            'litres': ['count', 'sum']
        }).round(0)
        
        # Format for easier reading
        for cat_type in recent_summary.index:
            netsales_count = recent_summary.loc[cat_type, ('netsales', 'count')]
            netsales_sum = recent_summary.loc[cat_type, ('netsales', 'sum')]
            litres_count = recent_summary.loc[cat_type, ('litres', 'count')]  
            litres_sum = recent_summary.loc[cat_type, ('litres', 'sum')]
            
            print(f"\n{cat_type.upper()}:")
            if not pd.isna(netsales_sum):
                print(f"  Net Sales: ${netsales_sum:,.0f} ({netsales_count:.0f} items)")
            if not pd.isna(litres_sum):
                print(f"  Litres:    {litres_sum:,.0f} ({litres_count:.0f} items)")
    
    # Summary by category (all quarters)
    summary = df.groupby('cat_type').agg({
        'netsales': ['count', 'sum'],
        'litres': ['count', 'sum']
    }).round(2)
    print("\n=== OVERALL SUMMARY (All Quarters) ===")
    print(summary)