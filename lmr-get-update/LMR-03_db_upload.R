## Upload LMR data gathered from pdf report to MySQL database

library(tidyverse)
library(lubridate)
library(scales)
library(glue)
library(readr) ## for easy conversion of $ characters to numeric
library(RPostgres) ## for PostgreSQL

## load functions for upload ----
source(here('functions/lmr_db_functions.R'))

## SET parameters for function ----
db_tbl <- "public.lmr_data"

## GET file to upload ----
# In original R process (LMR-01-fetch-process-all_v4.R): 
# - tables_all or tbl_upload data frame 
# Python process doesn't create a data frame in the session
# - instead, data is saved to output folder
# - use lmr_data_latest.csv (as below)
# if table available produced frm LMR-fetch-process-all_vX.R in current/recent session
if(exists('tables_all')){
  tbl_upload <- tables_all 
} else if(exists('tbl_upload')) {
  tbl_upload <- tbl_upload
} else {
  # Python process: import from saved file for most recent process
  tbl_upload <- read_csv(here('lmr-get-update', 'output/lmr_data_latest.csv'))
  # OR choose file
  #f_path <- file.choose()
  #tbl_upload <- read_csv(f_path)
}
# confirm data characteristics
unique(tbl_upload$fy_qtr)
unique(tbl_upload$cat_type)

# [OPTIONAL] FILTER TO ONLY MOST RECENT QUARTER
# - if concerned about data quality in historical data
# - cancelled as of Sep 2025 - assuming new python process more reliable
# original reasons to only upload most recent quarter:
# - more efficient upload
# - easier error checking / fixing -> especially with new system with ocr starting Sep 2024
# - risks missing historical data if updated in report -> not sure if this ever happens
#   - can provide disclaimer if concerned
#tbl_upload <- tbl_upload %>% filter(fy_qtr == max(fy_qtr))

## Quarters ----
## check/add data to LDB_quarters tbl -> automatically add if not present
qtrs_check <- dbx_fetch_update_qtrs(tbl_upload)

## RUN function to UPLOAD DATA ----
dbx_upload(db_tbl, tbl_upload)

## CHECK: spot-check data by category for most recent quarters
dbx_check_data(min(tbl_upload$fy_qtr), max(tbl_upload$fy_qtr))
