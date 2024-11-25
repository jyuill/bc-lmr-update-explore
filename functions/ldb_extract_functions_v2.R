## functions for pulling tabular data out of LDB Quarterly Reports

library(pdftools)
library(tidyverse)
library(lubridate)
library(scales)
library(glue)
library(readr) ## for easy conversion of $ characters to numeric
library(here)
library(tesseract)

## > Clean file name, DL ####
## get data - download if not already
# provide pdf file url and indicate if rotation needed (reports in landscape starting with Sep 2024)
fn_lmr <- function(furl, rotate){
  # clean up name -----
  ## convert URL to filename in standard format so don't have to specify
  furl_clean <- str_replace_all(furl,"%20","_") # replace %20 (if any) with "_" for clean URL
  fname_url <- str_split(furl_clean,"/")
  fname_url2 <- str_split(fname_url[[1]][5],"_")[[1]]
  if(length(fname_url2)>=8){ # typically 8 components; sometimes 9 if prepended with '_2' or '_new'
    fname_url_qtr <- paste0("FY",fname_url2[5],fname_url2[6])
    fname_url_yr <- str_split(fname_url2[8], "\\.")[[1]][1]
    fname_url_mth <- case_when(
      fname_url2[7] == 'March' ~ '03',
      fname_url2[7] == 'June' ~ '06',
      fname_url2[7] == 'September' ~ '09',
      fname_url2[7] == 'December' ~ '12',
    )
  } else if (length(fname_url2)==6){ ## cases where URL doesn't include FY reference
    fname_url_yr <- fname_url2[6] %>% str_replace("\\.pdf","")
    fname_url_cyr <- as.numeric(str_replace(fname_url_yr,"20",""))
    # if March, FY same as CY -> otherwise add 1
    if(fname_url2[5]=='March'){
      fname_fy <- fname_url_cyr
    } else {
      fname_fy <- fname_url_cyr+1
    }
    fname_url_qtr <- paste0("FY",fname_fy,fname_url2[4])
    fname_url_mth <- case_when(
      fname_url2[5] == 'March' ~ '03',
      fname_url2[5] == 'June' ~ '06',
      fname_url2[5] == 'September' ~ '09',
      fname_url2[5] == 'December' ~ '12',
    )
  } 
  ## combine name components for clarity
  fname <- paste0("LMR_",fname_url_yr,"_",fname_url_mth,"_",fname_url_qtr,".pdf")
  
  # working with existing files ----
  # input file location/path
  input_folder <- "lmr-get-update/input/"
  input_file <- "01_lmr_list.csv"
  input_file_path <- paste0(input_folder,input_file)
  ## on-going list of reports and urls
  # list item for new file
  lmr_list <- data.frame(lmr_name=fname, lmr_url=furl_clean)
  ## check if list file exists, if so read in and add new item
  if(file.exists(input_file_path)){
    lmr_list_exist <- read_csv(input_file_path)
    lmr_list_exist <- lmr_list_exist %>% arrange(lmr_name)
  } else {
    lmr_list_exist <- data.frame()
  }
  # add new file to list and save (if not already)
  if(!(fname %in% lmr_list_exist$lmr_name)) {
    lmr_all <- bind_rows(lmr_list_exist, lmr_list)
    write_csv(lmr_all, input_file_path)
  }
  # path for latest file
  pdf_file <- paste0(input_folder,fname)
  # pdf dwnld/import ------
  ## if file doesn't exist download it, then import into session
  if(!file.exists(pdf_file)){
    download.file(furl, pdf_file, mode='wb') # mode='wb' on windows for text files
    #info <- pdf_info(pdf_file) # get metadata: # of pgs, title, etc
  }
    # convert pdf to readable format in R and import
    if(rotate==TRUE){
      #lmr <- pdf_text(pdf_file) # original process - before LDB reformatted
      # setup for separate file for rotated version, if needed (testing?)
      #output_pdf_file <- paste0(input_folder,"rotated_",fname)
      #system(paste("qpdf --rotate=+90", pdf_file, output_pdf_file))
      # rotate and replace original pdf file
      system(paste("qpdf --rotate=+90 --replace-input", pdf_file))
    }
  # convert rotated pdf from images to text (needed starting Sep 2024)
  # complicated to save result as pdf, so just running every time
  lmr_ocr <- tesseract::ocr(pdf_file)
  # process generates png files in root directory, but not needed
  # get List all .png files in the directory
  png_files <- list.files(path = here(), pattern = "\\.png$", full.names = TRUE)
  # Remove the .png files - not needed
  file.remove(png_files)
  
  # leaving option open for other proc if pdf format changes after Sep 2024
  lmr_use <- lmr_ocr
  return(list(lmr_use, fname))
}



## clean pg -> remove summary, blank rows
fn_pg_clean <- function(tbl_pg_rows) {
  ## > RM SUMMARY ROWS (or blank) ####
  cat("clean tbl: remove summary, blank rows \n")
  srows <- NULL
  for(r in 1:length(tbl_pg_rows)){
    if(str_detect(tbl_pg_rows[r],"Summary")==TRUE | tbl_pg_rows[r]==""){
      srow <- r
      srows <- c(srows,srow)
    }
  }
  ## want to remove Summary OR blank rows
  tbl_pg_rows_ns <- tbl_pg_rows[c(-srows)]
  return(tbl_pg_rows_ns)
}

## get meta data from pg
fn_pg_meta <- function(tbl_pg_rows, pg_num, tbl_name_prev, tbl_cat_type){
  ## > get META data ####
  tbl_name <- trimws(tbl_pg_rows[1])
  
  tbl_name_clean <- str_replace_all(tbl_name,"\\(","")
  tbl_name_clean <- str_replace_all(tbl_name_clean,"\\)","")
  tbl_name_clean <- str_replace_all(tbl_name_clean,"\\$","")
  tbl_name_clean <- trimws(tbl_name_clean, "right")
  tbl_name_clean <- str_replace_all(tbl_name_clean," ","_")
  cat("pg:",pg_num,'tbl:', tbl_name_clean,"\n")
  ## check for prev same table name -> if so, keep using prev, since indicates continuation
  if(exists('tbl_name_prev')){
    if(tbl_name_clean==tbl_name_prev){
      tbl_cat_type <- tbl_cat_type
      hrow <- 2
    } else { ## 
      tbl_cat_type <- tbl_pg_rows[2]
      hrow <- 3
    }
  } else {
    tbl_cat_type <- tbl_pg_rows[2]
    hrow <- 3
  }
  ## get table metric
  # this no longer works with OCR versions (Sep 2024)
  #tbl_metric <- unlist(str_split(tbl_pg_rows[hrow], "\\s{2,}"))[2]
  # get first three items to test 
  tbl_metric <- paste0(unlist(str_split(tbl_pg_rows[hrow], "\\s{1,}"))[1:3], 
                       collapse=" ")
  if(tbl_metric=='Net Sales $'){
    tbl_metric <- tbl_metric # leave as is
  } else { # otherwise, take only the first work (litres)
    tbl_metric <- unlist(str_split(tbl_pg_rows[hrow], "\\s{1,}"))[1]
  }
  tbl_metric <- case_when(
    tbl_metric=='Net Sales $' ~ 'netsales',
    tbl_metric=='Litres' ~ 'litres'
  )
  cat('metric:',tbl_metric,"\n")
  
  return(list(tbl_name_clean, hrow, tbl_cat_type, tbl_metric))
}

## extract tbl content from pg
fn_tbl_content <- function(tbl_pg_rows, tbl_meta){
  cat("start content process function \n")
  ## unpack meta data
  tbl_name <- tbl_meta[[1]]
  hrow <- tbl_meta[[2]] ## identifies heading row, depends on pg layout; -1 for blank row
  tbl_cat_type <- tbl_meta[[3]]
  tbl_metric <- tbl_meta[[4]]
  
  ## > TABLE + HEADING ####
  cat("get headings, set up table \n")
  #tbl_heading <- unlist(str_split(tbl_pg_rows[hrow], "\\s{2,}"))[c(-1,-2)] # drop first two: empty, metric name
  # ocr version: split on 1 or more spaces
  # need to drop everything before the first 'Fiscal'
  # then split by occurences of 'Fiscal' and do some cleaning
  tbl_heading <- unlist(str_split(tbl_pg_rows[hrow], "\\s{1,}"))
  cat("raw heading:",tbl_heading,"\n")
  
  # determine headings - always same structure but need to detect the right Quarters
  # get starting point for each occurence of 'Fiscal'
  f_pos <- which(str_detect(tbl_heading, "Fiscal"))
  # determine length
  fs_len <- length(f_pos)
  # build heading for each occurence of 'Fiscal'
  h_all <- NULL
  for(h in 1:fs_len) {
    h_next <- paste(tbl_heading[f_pos[h]:(f_pos[h]+2)], collapse = " ")
    # remove any '.'
    h_next <- str_replace_all(h_next, "Q1.*","Q1") 
    h_next <- str_replace_all(h_next, "Q2.*","Q2") 
    h_next <- str_replace_all(h_next, "Q3.*","Q3") 
    h_next <- str_replace_all(h_next, "Q4.*.","Q4") 
    #str_replace_all(h1, "\\/","x")
    h_all <- c(h_all, h_next)
    #cat(h_all)
  }
  cat("headings: ",h_all,"\n")
  h_all <- str_replace(h_all, "Fiscal ", "FY")
  ## remove two digits for prev yr + '/' ("21/" in example above)
  tbl_heading <- str_replace_all(str_remove(h_all, str_sub(h_all, start=5, end=7))," ","")
  
  # add standard cols to headings
  first_cols <- c("cat_type","category","subcategory")
  tbl_heading <- c(first_cols, tbl_heading)
  cat("final heading:",tbl_heading,"\n")
  
  ## initiate empty table with proper headings
  tbl <- data.frame(matrix(ncol=length(tbl_heading)))
  colnames(tbl) <- tbl_heading
  
  ## > PROCESS DATA ####
  ## get data from each row and add to table
  cat("process data \n")
  start <- hrow+1
  ## loop through each row to collect data
  for(r in start:length(tbl_pg_rows)){
    ## pre-defined tbl will accumulate rows through loop
    cat("table row: ", r, "; ")
    # original version: split on 2 or more spaces
    #row_content <- unlist(str_split(trimws(tbl_pg_rows[r]), "\\s{2,}"))
    # ocr version: split on 1 or more spaces
    row_content <- unlist(str_split(trimws(tbl_pg_rows[r]), "\\s{1,}"))
    col_nums <- length(row_content)
    cat("col nums:", col_nums, "\n")
    if(col_nums>5){ ## only rows with data
      tr <- r-hrow ## set table row number by subtract pdf start num
      tbl[tr,"cat_type"] <- tbl_cat_type
      if(col_nums==7){ ## rows with category shown
        tbl$category[tr] <- row_content[1]
        tbl$subcategory[tr] <- row_content[2]
        tbl[tr,c(4:(col_nums+1))] <- row_content[c(3:col_nums)]
      } else {
        ## get category from prev row
        tbl$category[tr] <- tbl$category[tr-1]
        tbl$subcategory[tr] <- row_content[1]
        tbl[tr,c(4:(col_nums+2))] <- row_content[c(2:col_nums)]
      }
    } ## end row conditional
    print(tbl)
  } ## end single page loop
  
  ## TIDY FORMAT ####
  # check for NA within each tbl row? 
  # not sure what this does so setting it to max NAs per row less than 2
  if(max(rowSums(is.na(tbl)))<2){
    tbl_long <- fn_tidy_structure(tbl, tbl_metric)
  } else {
    tbl_long <- NULL
  }
  
  ## save table name for comparison with next table
  ## - in some cases, tables run over multiple pages
  tbl_name_prev <- tbl_name
  
  ## return tbl: wide and long
  return(list(tbl, tbl_long))
} ## end single page process

## extend to long format
fn_tidy_structure <- function(tbl, tbl_metric){
  cat("tidy process for tbl_long \n")
  ## > convert to long ####
  ## convert table with metrics in multiple rows to tidy format
  tbl_long <- tbl %>% pivot_longer(cols=c(4:ncol(tbl)), names_to='period', values_to=tbl_metric)
  tbl_long <- tbl_long %>% mutate(
    !!tbl_metric := parse_number(!!sym(tbl_metric))
  )
  tbl_long
}

## DATA CHECK ####
# compare data processed to LMR report before uploading
fn_data_check <- function(data_check) {
  fyqtrs <- unique(data_check$fy_qtr)
  print(fyqtrs)
  data_smry_cat <- data_check %>% group_by(cat_type, fy_qtr) %>% 
    summarize(netsales=sum(netsales),
              litres=sum(litres)
    ) 
  # chart for each category, each qtr
  data_chart <- data_smry_cat %>% ggplot(aes(x=fy_qtr, y=netsales))+geom_col()+
    facet_grid(.~cat_type)+
    scale_y_continuous(labels=comma_format())+
    theme(axis.text.x = element_text(angle=45, hjust=1),
          axis.ticks.x = element_blank())+
          labs(x="")
  print(data_chart)
  # summary data by category for most recent quarter
  data_smry_qtr <- data_smry_cat %>% filter(fy_qtr==max(fyqtrs))
  # format fields for readability
  data_smry_qtr$litres <- format(data_smry_qtr$litres, big.mark=",", scientific=FALSE, trim=TRUE, justify=c("right"))
  data_smry_qtr$netsales <- format(data_smry_qtr$netsales, big.mark=",", scientific=FALSE, trim=TRUE, format='i', justify=c("right"))
  print(data_smry_qtr)
}
