# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a BC Liquor Market Review (LMR) data processing project that updates an AWS database with quarterly LMR data from BC LDB. The project supports shiny dashboards available via the Figure 4 website and data products in the `bc-lmr-data-products` repo.

## Data Processing Pipeline

The project follows a sequential data processing workflow:

1. **Data Extraction** (`lmr-get-update/LMR-01-fetch-process-all.py`): Python-based PDF extraction and processing (replaces R version)
2. **Data Validation** (`lmr-get-update/LMR-02-data-check.R`): Validates extracted data quality  
3. **Database Upload** (`lmr-get-update/LMR-03_db_upload.R`): Uploads processed data to database
4. **Database Management** (`lmr-get-update/LMR-04_db-mgmt-postgres.R`): PostgreSQL database operations

### Legacy Files
- `lmr-get-update/LMR-01-fetch-process-all_v4.R`: Original R-based extraction (replaced by Python version)
- `lmr-get-update/LMR-05-PDF-parser.py`: Empty file (functionality integrated into main Python script)

## Database Configuration

The project is transitioning from MySQL to PostgreSQL:

- **Current**: PostgreSQL RDS instance on AWS
- **Legacy**: MySQL RDS instance (still referenced in some files)
- **Functions**: 
  - `functions/lmr_db_functions_postgres.R` - PostgreSQL database functions
  - `functions/lmr_db_functions.R` - Legacy MySQL functions
  - `functions/ldb_extract_functions_v2.R` - Data extraction utilities

## Environment Setup

Database credentials are managed through `.env` files:

**PostgreSQL (current)**:
```
AWS_ENDPT_PG=...rds.amazonaws.com
AWS_PWD_PG=...
AWS_PORT_PG=5432
AWS_USER_PG=...
AWS_DB_NAME_PG=...
```

**MySQL (legacy)**:
```
AWS_ENDPT=...rds.amazonaws.com
AWS_PWD=...
AWS_PORT=3306
AWS_USER=...
```

## Key Dependencies

**Python packages** (managed via uv):
- `pandas` >= 2.0.0 - Data processing and manipulation
- `requests` >= 2.28.0 - HTTP requests for PDF downloads  
- `PyPDF2` >= 3.0.0 - PDF text extraction
- `pdfplumber` >= 0.9.0 - Advanced PDF table extraction

**R packages** (for database operations):
- `tidyverse`, `lubridate`, `scales`, `glue` - Data processing
- `RPostgres` - PostgreSQL connectivity (current)
- `RMariaDB` - MySQL connectivity (legacy)
- `dotenv` - Environment variable management

**Legacy R packages** (no longer needed):
- `pdftools`, `tesseract` - PDF processing and OCR (replaced by Python)

## Development Notes

- PDF format is now readable without OCR (Sep 2024 changes were temporary)
- New quarterly LMR data is typically available within 2 months of quarter end
- Check [LMR website](https://www.fig4.com/products/bc-lmr-dashboard.html) for new releases
- Python extraction system replaces R-based system for better maintainability
- Quarto project setup exists but may be for documentation/analysis

## Python Extraction System

**Main script**: `lmr-get-update/LMR-01-fetch-process-all.py`
**Functions**: `lmr-get-update/lmr_extract_functions.py`
**Dependencies**: Managed with `uv` (see `lmr-get-update/pyproject.toml`)

**Setup and Usage**:
1. Navigate to `lmr-get-update/` directory
2. Install dependencies: `uv sync`
3. Edit PDF URL in main script
4. Run: `uv run python LMR-01-fetch-process-all.py`
5. Output saved to `output/{filename}_db_upload.csv`
6. Continue with R-based validation and database upload steps


**Key Features**:
- Automatic PDF download and filename standardization
- Table extraction using pdfplumber (no OCR required)
- Data validation and quality checks
- Compatible output format for existing R database upload workflow

## Current Development Status

Based on `Notes.md`, active work includes:
- ✅ Completed: Python PDF extraction system
- Setting up PostgreSQL RDS credentials and connections  
- Updating database functions for PostgreSQL
- Ensuring compatibility across all data category tables