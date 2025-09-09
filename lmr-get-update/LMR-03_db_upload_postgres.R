## Upload LMR data gathered from pdf report to MySQL database

library(tidyverse)
library(lubridate)
library(scales)
library(glue)
library(readr) ## for easy conversion of $ characters to numeric
library(RPostgres) ## for PostgreSQL

## load functions for upload ----
source('functions/lmr_db_functions_postgres.R')

## SET parameters for function ----
db_tbl <- "public.lmr_data"

## GET file to upload ----
# if table available produced frm LMR-fetch-process-all_vX.R in current/recent session
if(exists('tables_all')){
  tbl_upload <- tables_all 
} else if(exists('tbl_upload')) {
  tbl_upload <- tbl_upload
} else {
  # ALTERNATELY, import from saved -> CHANGE to match latest
  #tbl_upload <- read_csv('output/lmr_data_latest.csv')
  f_path <- file.choose()
  tbl_upload <- read_csv(f_path)
}

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
qtrs_check <- fn_db_qtrs(tbl_upload)

## RUN function to UPLOAD DATA ----
fn_db_upload(db_tbl, tbl_upload)

## CHECK: spot-check data by category
fn_db_check()
