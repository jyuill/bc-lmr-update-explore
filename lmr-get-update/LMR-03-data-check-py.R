## Test data - compare downloaded to online report
# check data after being processed by Python script

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
# generic file saved from LMR-01-fetch-process-all.py
tables_all_fyqtr <- read_csv(here('lmr-get-update', 'output', 'lmr_data_latest.csv'))
fn_data_check(tables_all_fyqtr)

## CHECK DATA: DATABASE ####
# ALSO AT END OF DB UPLOAD
## load functions for MySQL queries
source('functions/lmr_db_functions.R')
fn_db_check()

## DEEP DIVE & FIX ####
## DEEP DIVE analysis if needed for specific issues detected
# FOCUS on MOST RECENT QUARTER
# as of Sep 2024: OCR process with R prone to random errors, sporadic and inconsistent
# - decided to only upload most recent quarter to save on error checking/fixing
# enter filter values for troubleshooting
fy_period_select <- 'FY2025Q3'
col_select <- c(1,6)
cat_type_select <- 'Wine'

# check CAT totals ----
# all qtrs

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

# check SUBCAT totals for selected CATEGORY ----
fy_period_select <- fy_period_select
cat_type_select <- cat_type_select
cat_select <- c('Greece Wine')
# check
check_subcat <- tables_all %>% 
  filter(fy_qtr == fy_period_select & 
           cat_type == cat_type_select & 
           category %in% cat_select) %>% 
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

# FIXES ----
# add code for fixing specific report issues here -> most recent at top
# > DEC 2024 ----
# Dec 2024 report: Beer litres FY2025Q2 came in as FY2/25Q2
# - replace malformed period value
#tables_all_litres <- tables_all_litres %>% mutate(
#  period = ifelse(cat_type == 'Beer' & str_detect(period, "/"), 'FY2025Q2', period)
#)
#>> Fixes - litres ####
# - all fixes are at subcategory and period level, so only need to set those variables
# - AFTER FIX: need to re-run the join in LMR-01-fetch to get the correct values
subcat_select <- 'Other Fortified Domestic Wine'
replacement_val <- 53022
fy_period_select <- fy_period_select
# replacement based on parameters above
tables_all_litres <- tables_all_litres %>% mutate(
  litres = ifelse(subcategory == subcat_select & 
                    period == fy_period_select, replacement_val, litres)
)

# > misc other fixes
# replace NA ----
val_replace <- 0
ct <- "Wine"
cat <- 'South Africa Wine'
tables_all_litres <- tables_all_litres %>% mutate(
  litres = ifelse(cat_type == ct & category == cat &
                    is.na(litres), val_replace, litres)
)
# OR sometimes shows up as text
tables_all_litres <- tables_all_litres %>% mutate(
  litres = ifelse(cat_type == ct & category == cat &
                    litres == 'NA', val_replace, litres)
)
# check
rnums <- which(is.na(tables_all_litres$litres)) # check for NA values)
tables_all_litres[rnums,] # check rows with NA values
# REPLACE values ----
cat_type_sel <- 'Wine'
subcat_sel <- 'Greece Rose Wine'
per_sel <- 'FY2025Q3'
#val_replace <- 7935822 # don't need this extra detail
val_new <- 532
tables_all_litres <- tables_all_litres %>% 
  mutate(litres = ifelse(cat_type == cat_type_sel &
                           subcategory == subcat_sel &
                           period == per_sel, val_new, litres))
# check
tables_all_litres %>% filter(subcategory == subcat_sel & period == per_sel)
# > SEP 2024 ----
# Sep 2024 report: error in various litres -> '7' being read in as '1'
# 74,573 read in by OCR as 14,753
# - replace value, using temp table
# set values for filter and replacement
#cat_type_sel <- 'Wine'
#cat_sel <- 'Spain Wine'
#subcat_sel <- 'Spain Sparkling Wine'
#per_sel <- 'FY2025Q1'
# use for checking multiple quarters
#per_sel_multi <- c('FY2024Q2','FY2024Q3','FY2024Q4','FY2025Q1','FY2025Q2')
# confirm location by filter
tables_all_litres %>% 
  filter(cat_type == cat_type_sel & category == cat_sel & 
           subcategory == subcat_sel & 
           period %in% per_sel_multi)
# replace value
#val_replace <- 58894
#fix <- tables_all_litres %>% mutate(
# litres = ifelse(cat_type == cat_type_sel & category == cat_sel &
#             subcategory == subcat_sel & period == per_sel, val_replace, litres)
#)
# confirm replacement
fix %>% 
  filter(cat_type == cat_type_sel & category == cat_sel & subcategory == subcat_sel & 
           period == per_sel)
#capy table back to original - then run join above again
#tables_all_litres <- fix