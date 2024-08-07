## Upload LMR data gathered from pdf report to MySQL database

library(tidyverse)
library(lubridate)
library(scales)
library(glue)
library(readr) ## for easy conversion of $ characters to numeric
library(RMariaDB) ## for MySQL

## load functions for MySQL upload
source('functions/lmr_db_functions.R')

## SET parameters for function
mysql_tbl <- "bcbg.tblLDB_lmr"

## GET file to upload
# if table available produced frm LMR-fetch-process-all_vX.R in current/recent session
if(exists('tables_all_fyqtr')){
  tbl_upload <- tables_all_fyqtr 
} else if(exists('tbl_upload')) {
  tbl_upload <- tbl_upload
} else {
  # ALTERNATELY, import from saved -> CHANGE to match latest
  #tbl_upload <- read_csv('output/LMR_2023_09_FY24Q2_db_upload.csv')
  f_path <- file.choose()
  tbl_upload <- read_csv(f_path)
}

## check/add data to LDB_quarters tbl -> automatically add if not present
fn_db_qtrs(tbl_upload)

## RUN function to UPLOAD DATA
fn_db_upload(mysql_tbl, tbl_upload)

## CHECK: spot-check data by category
fn_db_check()
