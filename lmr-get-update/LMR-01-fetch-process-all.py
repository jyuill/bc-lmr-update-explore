#!/usr/bin/env python3
"""
LMR Data Extraction - Python Version
Replaces LMR-01-fetch-process-all_v4.R

Extract tabular data from BC LDB Quarterly Market Review PDFs

To run:
- get latest URL for PDF from https://www.bcldb.com/publications/liquor-market-review
- python LMR-01-fetch-process-all.py
"""

import pandas as pd
from pathlib import Path
import sys
import logging
from lmr_extract_functions import LMRExtractor, quick_data_check

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main extraction process"""
    
    # 1. MANUAL INPUT: SPECIFY PDF URL
    # Find link at: https://www.bcldb.com/publications/liquor-market-review
    pdf_url = "https://www.bcldb.com/files/Liquor_Market_Review_F25_26_Q1_June_2025.pdf"
    
    logger.info("Starting LMR data extraction process")
    logger.info(f"Processing URL: {pdf_url}")
    
    try:
        # Initialize extractor
        extractor = LMRExtractor()
        
        # 2. DOWNLOAD PDF
        filename, filepath = extractor.download_pdf(pdf_url)
        filename_base = filename.replace('.pdf', '')
        logger.info(f"Working with file: {filename}")
        
        # 3. LOAD CATEGORIES REFERENCE
        categories_df = extractor.load_categories_reference()
        logger.info(f"Loaded {len(categories_df)} category references")
        
        # 4. EXTRACT TEXT FROM PDF
        pages_text = extractor.extract_text_from_pdf(filepath)
        logger.info(f"Extracted text from {len(pages_text)} pages")
        
        # 5. PROCESS EACH PAGE
        tables_all_netsales = pd.DataFrame()
        tables_all_litres = pd.DataFrame()
        prev_table_name = ""
        prev_cat_type = ""
        
        # Storage for combined tables by category
        combined_tables = {}
        
        logger.info("Starting page processing loop")
        
        # Skip first 3 pages (cover, TOC, intro)
        for page_num in range(3, len(pages_text)):
            page_text = pages_text[page_num]
            
            # Check if page has a table
            if not extractor.is_table_page(page_text):
                logger.info(f"Page {page_num + 1}: skipping (no table)")
                continue
                
            logger.info(f"Page {page_num + 1}: processing table")
            
            # Clean page content
            cleaned_lines = extractor.clean_page_content(page_text)
            
            # Extract metadata
            metadata = extractor.extract_page_metadata(
                page_text, page_num + 1, prev_table_name, prev_cat_type
            )
            
            prev_table_name = metadata['table_name']
            prev_cat_type = metadata['cat_type']
            
            # Extract table data
            table_data = extractor.extract_table_data(cleaned_lines, metadata, categories_df)
            
            if table_data is None:
                logger.warning(f"Page {page_num + 1}: no data found")
                continue
                
            df_wide, df_long = table_data
            
            if df_wide.empty:
                continue
                
            # Handle multi-page tables
            table_key = metadata['table_name']
            
            if metadata['is_continuation'] and table_key in combined_tables:
                # Append to existing table
                combined_tables[table_key]['wide'] = pd.concat([
                    combined_tables[table_key]['wide'], df_wide
                ], ignore_index=True)
                combined_tables[table_key]['long'] = pd.concat([
                    combined_tables[table_key]['long'], df_long
                ], ignore_index=True)
            else:
                # Start new table
                combined_tables[table_key] = {
                    'wide': df_wide.copy(),
                    'long': df_long.copy(),
                    'metadata': metadata
                }
            
            # Save individual page/table data
            extractor.save_data(
                combined_tables[table_key]['wide'], 
                combined_tables[table_key]['long'],
                filename_base, 
                table_key
            )
            
            # Add to overall collections by metric type
            if metadata['metric'] == 'netsales':
                tables_all_netsales = pd.concat([tables_all_netsales, df_long], ignore_index=True)
            else:
                tables_all_litres = pd.concat([tables_all_litres, df_long], ignore_index=True)
        
        # 6. DATA VALIDATION
        logger.info("Starting data validation")
        validation_results = extractor.validate_data(tables_all_netsales, tables_all_litres)
        
        logger.info(f"Net sales rows: {validation_results['total_netsales_rows']}")
        logger.info(f"Litres rows: {validation_results['total_litres_rows']}")
        
        if validation_results['missing_netsales']:
            logger.warning(f"Rows in litres with no netsales match: {len(validation_results['missing_netsales'])}")
            
        if validation_results['missing_litres']:
            logger.warning(f"Rows in netsales with no litres match: {len(validation_results['missing_litres'])}")
        
        # 7. JOIN NETSALES AND LITRES
        if not tables_all_netsales.empty and not tables_all_litres.empty:
            logger.info("Joining netsales and litres data")
            
            # Join on category and period columns
            join_cols = ['cat_type', 'category', 'subcategory', 'period']
            
            tables_all = pd.merge(
                tables_all_litres.rename(columns={tables_all_litres.columns[-1]: 'litres'}),
                tables_all_netsales.rename(columns={tables_all_netsales.columns[-1]: 'netsales'}),
                on=join_cols,
                how='outer'
            )
            
            # Rename period column
            tables_all = tables_all.rename(columns={'period': 'fy_qtr'})
            
        elif not tables_all_netsales.empty:
            tables_all = tables_all_netsales.rename(columns={'period': 'fy_qtr'})
            tables_all['litres'] = None
            
        elif not tables_all_litres.empty:
            tables_all = tables_all_litres.rename(columns={'period': 'fy_qtr'})
            tables_all['netsales'] = None
            
        else:
            logger.error("No data extracted from PDF")
            return 1
        
        # 8. FINAL DATA CHECK
        logger.info("Final data validation")
        missing_values = tables_all[tables_all['netsales'].isna() | tables_all['litres'].isna()]
        
        if not missing_values.empty:
            logger.warning(f"Found {len(missing_values)} rows with missing values:")
            print(missing_values[['cat_type', 'category', 'subcategory', 'fy_qtr']].head())
        else:
            logger.info("No missing values found")
        
        # Quick data summary
        quick_data_check(tables_all)
        
        # 9. SAVE FINAL DATABASE UPLOAD FILE
        output_filename = f"{filename_base}_db_upload.csv"
        output_path = extractor.output_folder / output_filename
        
        tables_all.to_csv(output_path, index=False)
        logger.info(f"Saved final database upload file: {output_filename}")
        
        logger.info("LMR extraction process completed successfully")
        
        # Print summary
        print(f"\n=== EXTRACTION SUMMARY ===")
        print(f"File processed: {filename}")
        print(f"Total rows extracted: {len(tables_all)}")
        print(f"Categories found: {tables_all['cat_type'].nunique()}")
        print(f"Output saved to: {output_path}")
        print(f"Ready for database upload via LMR-03_db_upload.R")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error in extraction process: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)