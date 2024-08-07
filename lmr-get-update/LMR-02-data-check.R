## Test data - compare downloaded to online report

library(tidyverse)
library(lubridate)
library(scales)
library(glue)
library(here)
library(formattable)

## CHECK DATA: TABLE COMPILED FROM PDF ####
# ALSO AT END OF fetch-process
## assumes data available in final table for each report
fn_data_check(tables_all_fyqtr)

## CHECK DATA: MYSQL ####
# ALSO AT END OF DB UPLOAD
## load functions for MySQL queries
source('functions/lmr_db_functions.R')
fn_db_check()
