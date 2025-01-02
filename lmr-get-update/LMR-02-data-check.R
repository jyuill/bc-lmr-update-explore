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
source('functions/ldb_extract_functions_v2.R')
fn_data_check(tables_all_fyqtr)

## CHECK DATA: MYSQL ####
# ALSO AT END OF DB UPLOAD
## load functions for MySQL queries
source('functions/lmr_db_functions.R')
fn_db_check()

## DEEP DIVE analysis if needed for specific issues detected
# FOCUS on MOST RECENT QUARTER
# Sep 2024 - ocr prone to random errors, sporadic and inconsistent
# - decided to only upload most recent quarter to save on error checking/fixing
# enter filter values for troubleshooting
fy_period_select <- 'FY2025Q2'
col_select <- c(1,2,4,6)
cat_type_select <- 'Wine'
cat_select <- c('Argentina Wine', 'Fortified Wine','France Wine','Georgia Wine',
                'Germany Wine','South Africa Wine','Spain Wine')

# check CATEGORY totols
# selected qtr
check_cat <- tables_all %>% 
  filter(fy_qtr == fy_period_select & cat_type == cat_type_select) %>% 
  group_by(category) %>% 
  summarise(litres = sum(litres),
            netsales = sum(netsales))
# all qtrs
check_cat <- tables_all %>% 
  filter(cat_type == cat_type_select) %>% 
  group_by(category, fy_qtr) %>% 
  summarise(litres = sum(litres),
            netsales = sum(netsales)) %>%
  pivot_wider(names_from = fy_qtr, values_from = c(litres, netsales))

# check SUBCATEGORY totals for selected CATEGORY
check_subcat <- tables_all %>% 
  filter(fy_qtr == fy_period_select & cat_type == cat_type_select & category == cat_select) %>% 
  group_by(category, subcategory) %>% 
  summarise(litres = sum(litres),
            netsales = sum(netsales))
# all qtrs - all cat/subcat
check_subcat <- tables_all %>% 
  filter(cat_type %in% cat_type_select & category %in% cat_select) %>% 
  group_by(category, subcategory, fy_qtr) %>% 
  summarise(litres = sum(litres),
            netsales = sum(netsales)) %>%
  pivot_wider(names_from = fy_qtr, values_from = c(litres, netsales)) %>%
  select(col_select)