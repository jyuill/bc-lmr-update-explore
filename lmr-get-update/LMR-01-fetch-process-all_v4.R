## GET lIQUOR SALES DATA FROM LDB - released quarterly in pdf format
## Requires scraping data tables from within PDF docs
## references:
## pdftools vignette: https://cran.r-project.org/web/packages/pdftools/pdftools.pdf
## pdftools website: https://docs.ropensci.org/pdftools/ 
##  manual: https://docs.ropensci.org/pdftools/reference/pdftools.html

library(pdftools)
library(tidyverse)
library(lubridate)
library(scales)
library(glue)
library(here)
library(tesseract)
library(readr) ## for easy conversion of $ characters to numeric
## clear environment to avoid confusion
rm(list=ls())

## 1. MANUAL INPUT: LINK TO PDF ####
## SPECIFY LINK AND DESIRED FILE NAME: needed for each issue
## find link at: https://www.bcldb.com/publications/liquor-market-review 
## older reports: https://www.bcldb.com/publications/archives?y%5Bvalue%5D%5Byear%5D=&r=4&b= 
furl <- "https://www.bcldb.com/files/Liquor_Market_Review_F24_25_Q2_September_2024_0.pdf"
## > rest of process is automated to end > run via 'Source'

## PROCESS DESCR. ####
## LDB QMR has pages with single table per page
## - by category, separate table for net sales $ and litres
## - standard format/layout.
## Process below works by:
## 0. Identify link and download PDF.
## 1. Get report meta data to identify unique report
## 2. Go through each page, determine if table or other content (based on table patterns used).
## 3. If table pg: 
##    >> parse out meta data: category type, metric ($ or vol).
##    >> parse out col headings.
##    >> assemble data from table in std structure
##    >> If table runs across multiple pgs, combine all
## 4. Save individual table data, in both wide and long formats
## 5. Combine with previous tables
## 6. When all pages in report completed, join net sales and litres into 
##    single table for upload to database.
## 7. Upload to database -> delete any existing rows for period and overwrite with latest data.

## PROCESS START ####
## Load functions ####
source("functions/ldb_extract_functions_v2.R")

## 2. IMPORT PDF ####
## Function to:
## - Import pdf from input folder if previously downloaded
## - otherwise, download, transform (rotate/ocr), save, based on URL above
#    - indicate if rotation of pages needed (starting Sep 2024, pages are landscape format)
#    - if rotation, png will be created for each pg as by-product; will be deleted at end
lmr <- fn_lmr(furl, rotate=TRUE)
# used to get name for saving files, in form: LMR_20XX_0X_FYXXQX; passed back with fn_lmr fn
lmr_name <- unlist(lmr[2]) 
lmr_name_clean <- str_remove(lmr_name,"\\.pdf")
lmr <- unlist(lmr[1]) # report content is passed back with fn_lmr function
cat(paste0(lmr_name,' import/process complete \n'))
## - see 'pdftools-explore.R' for different ways to access page info
## > get report meta info ####
#title_pg <- unlist(strsplit(lmr[1], "\n"))
#title_pg_dt <- str_replace(trimws(title_pg[8])," ","_")

## 2a. Categories ####
### categories ----
# get list of categories and subcategories for matching with row content
# use existing data file to get category type, categories and subcategories
# - needed to determine how to separate rows into cols, since no other consistent markings
# - pre-Sep-2024 could use double spaces (commented out code remains below)
# get list of existing files to filter for most recent
data_files <- list.files(here('lmr-get-update','output'))
data_files_info <- file.info(here('lmr-get-update','output',data_files))
data_files_info$file <- row.names(data_files_info)
data_file <- data_files_info %>% select(file, mtime) %>% 
  filter(str_detect(file,"LMR")) %>% filter(mtime == max(mtime))
# import most recent data file to get cat / subcat
cat_subcat <- read_csv(data_file$file)
# summarize by unique cat_type/category/subcategory 
cat_subcat_all <- cat_subcat %>% group_by(cat_type, category, subcategory) %>%
  summarize(number=n()) %>% ungroup() %>% select(-number)
# save category reference if needed for reference
write_csv(cat_subcat_all, here('lmr-get-update','output','cat_subcat_all.csv'))

## 3. GET EACH tbl (page) data ####
## Look at each page to determine which ones have tables
## can skip the first 3 pgs - always cover, toc, intro
## > netsales df and litres df ####
tables_all_netsales <- data.frame()
tables_all_litres <- data.frame()
tbl_name_prev <- "" ## set start prev pg name, used for tables that continue over pgs 
tbl_cat_type <- ""
## > START LOOP thru ea pg to get data ####
cat('Starting page loop \n')
for(p in 4:length(lmr)){
  ## > identify/skip non-tbl pgs
  ## test for 'Item Subcategory' or 'Glossary' -> identifies chart pages and Glossary pg for exclusion
  if(str_detect(lmr[p],"Item Subcategory")){
    cat(p, "chart pg \n")  
    } else if(str_detect(lmr[p], regex("Glossary", ignore_case = TRUE))){
      cat(p, "glossary page \n")
    } else { ## do the main thing
      cat(p, "tbl pg: processing \n")
      ## > get pg content from PDF ####
      tbl_pg <- lmr[p]
      tbl_pg_rows_init <- unlist(strsplit(tbl_pg, "\n"))
      ## clean pg -> remove summary, blank or other 'noise' rows
      tbl_pg_rows <- fn_pg_clean(tbl_pg_rows_init)
      
      ## > get meta data ####
      ## table name, category type for processing / saving
      tbl_meta <- fn_pg_meta(tbl_pg_rows, p, tbl_name_prev, tbl_cat_type)
      tbl_name_clean <- tbl_meta[[1]]
      # set current name to prev to handle multi-pg tables
      tbl_name_prev <- tbl_name_clean
      tbl_cat_type <- tbl_meta[[3]]
      tbl_cont <- tbl_meta[[5]] # to identify if continuation of prev page table
      
      # > category setup ----
      # filter list of categories for current cat_type, pass to function for tbl processing
      cat_subcat <- cat_subcat_all %>% filter(cat_type==tbl_meta[[3]])
      
      ## > PROCESS tbl on each page ####
      ## process content - pass in page content in rows, along with meta data
      # breaks out text rows to cols for table
      
      ### process rows ea pg ----
      # get back table of data for each pg: original + long version
      page_data_tbls <- fn_tbl_content(tbl_pg_rows, tbl_meta, cat_subcat)
      
      # check if page comes back with data
      if(is.null(page_data_tbls[[1]])){
        cat(p, "no data found on this page \n")
      } else {
        ## > SAVE: wide, long ####
        ## save results for page - identifying report by name and table -> wide and long versions
        ## confirm that table returned has data -> any more than 1 NA in first row, skip it
        if(rowSums(is.na(page_data_tbls[[1]][1,]))<2){
          ## as shown in report
          tbl_wide <- page_data_tbls[[1]]
          ## pivot long for database
          tbl_long <- page_data_tbls[[2]]
          ## if continued frm prev page, append
          if(tbl_cont){
            tbl_wide_cat <- bind_rows(tbl_wide_cat, tbl_wide)
            tbl_long_cat <- bind_rows(tbl_long_cat, tbl_long)
          } else {
            ## if not continued, set/reset to new tables
            tbl_wide_cat <- tbl_wide
            tbl_long_cat <- tbl_long
          }
          
          ## SAVE for reference
          write_csv(tbl_wide_cat, paste0('data/',lmr_name_clean,"-",tbl_name_clean,".csv"))
          cat(paste0('Saved: ',lmr_name_clean,"-",tbl_name_clean,"\n"))
          write_csv(tbl_long_cat, paste0('data/',lmr_name_clean,"-",tbl_name_clean,"_long.csv"))
          cat(paste0('Saved: ',lmr_name_clean,"-",tbl_name_clean,"_long\n"))
          
          ## > ADD to existing report data ####
          ## add to existing data from prev tables in report - depending on metric
          if(tbl_meta[[4]]=='netsales'){
            tables_all_netsales <- bind_rows(tables_all_netsales, tbl_long)
          } else {
            tables_all_litres <- bind_rows(tables_all_litres, tbl_long)
          }
        } ## end save & append section
      }
    } ## end of the main page loop action
} ## > END page loop: STOP here for basic testing of pdf-scraping ----

## 4. JOIN: netsales + litres ####
## - join tables with all pages/tables in each of netsales and litres as metrics
## - join on all fields except metrics
tables_all <- full_join(tables_all_litres, tables_all_netsales, by=c("cat_type", "category", "subcategory", "period"))

## simplify period format and rename to fy_qtr to match MySQL database
#
## from 'Fiscal 2021/22 Q4' to 'FY2022Q4'
tables_all_fyqtr <- tables_all %>% mutate(
  fy_qtr = str_replace(period, "Fiscal ", "FY"),
  ## remove two digits for prev yr + '/' ("21/" in example above)
  fy_qtr = str_replace_all(str_remove(fy_qtr, str_sub(fy_qtr, start=5, end=7))," ","")
) %>% select(-period)

## > SAVE joined tbl ####
## table for upload - complete, clean RAW data - without extra date dimensions  
#  - date dimensions are in separate table, joined when querying
tbl_save <- here('lmr-get-update','output',paste0(lmr_name_clean,"_db_upload.csv"))
write_csv(tables_all_fyqtr, here('output',paste0(lmr_name_clean,"_db_upload.csv")))

## > data check ####
# quick check by category
fn_data_check(tables_all_fyqtr)

## 5. NEXT: MySQL (other file) ####
## currently in LMR_db_upload.R
## - run separately for quality assurance
