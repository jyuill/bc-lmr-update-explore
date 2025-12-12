# bc-lmr-update-explore

## Purpose

1. Update BC LDB LMR database maintained on AWS with new data published quarterly by BC LDB.
2. General exploration of data.
   
Supports products in the [**bc-lmr-data-products**](https://github.com/jyuill/bc-lmr-data-products) repo, primarily **shiny dashboards** available via [Figure 4 website](https://www.fig4.com).

Repo renamed Sep 6, 2025 from 'lmr-explore'.

## Update Process for LMR Data

When new LMR is published by BC LDB (typically within a couple of months of calendar quarter ending - need to check [LMR website](https://www.bcldb.com/publications/liquor-market-review)), refer to process info below.

### Main files

* located in **/lmr-get-update** folder in this repo
* starting point: **LMR-01-fetch-process-all.py**
* data checks: **LMR-02-data-check-py.R**
* db upload: **LMR-03-db_upload_postgres.R**
* **/functions/** folder has functions used by these files
  - **lmr_extract_functions.py** for data extraction from PDF
  - **lmr_db_functions_postgres.R** for database upload

### Process

1. Download new Liquor Market Review from [BC LDB website](https://www.bcldb.com/publications/liquor-market-review) and place in /lmr-get-update/input folder in this repo.
    - can download directly into folder
    - no need to rename
2. Run **LMR-01-fetch-process-all.py** to extract data from PDF.
    - additional notes on operation in file
    - results saved to /lmr-get-update/output folder
3. Check results and cross-reference with pdf
    - **LMR-02-data-check-py.R** contains R code to assist with checking data, based on results from Python extract process.
    - summarize and manually cross-check with PDF.
    - if any discrepancies, use **DEEP DIVE** section in the file to identify and fix
    - if need to check on extraction process code, this is in **lmr_extract_functions.py**.
4. Upload new data to Postgres database on AWS
    - use **LMR-03-db_upload_postgres.R** to upload data to Postgres database on AWS.
    - by default, will overwrite most recent 5 quarters (including new quarter just imported) in database - this is to ensure alignment with most recent published data, since previous quarters are often adjusted.
5. Check / confirm correct data in database
    - Upload process includes function to summarize data in database after upload, for review.

