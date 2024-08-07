## functions for pulling tabular data out of LDB Quarterly Reports

library(pdftools)
library(tidyverse)
library(lubridate)
library(scales)
library(glue)
library(readr) ## for easy conversion of $ characters to numeric
library(here)

## > Clean file name, DL ####
## get data - download if not already
fn_lmr <- function(furl){
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
  # input file location/path
  input_folder <- "lmr-get-update/input/"
  input_file <- "01_lmr_list.csv"
  input_file_path <- paste0(input_folder,input_file)
  ## on-going list of reports and urls
  lmr_list <- data.frame(lmr_name=fname, lmr_url=furl_clean)
  if(file.exists(input_file_path)){
    lmr_list_exist <- read_csv(input_file_path)
    lmr_list_exist <- lmr_list_exist %>% arrange(lmr_name)
  } else {
    lmr_list_exist <- data.frame()
  }
  if(!(fname %in% lmr_list_exist$lmr_name)) {
    lmr_all <- bind_rows(lmr_list_exist, lmr_list)
    write_csv(lmr_all, input_file_path)
  }
  pdf_file <- paste0(input_folder,fname)
  ## if file doesn't exist download it, otherwise import
  if(!file.exists(pdf_file)){
    download.file(furl, pdf_file, mode='wb') # mode='wb' on windows for text files
    lmr <- pdf_text(pdf_file)
  } else {
    lmr <- pdf_text(pdf_file)
  }
  return(list(lmr, fname))
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
  tbl_metric <- unlist(str_split(tbl_pg_rows[hrow], "\\s{2,}"))[2]
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
  tbl_heading <- unlist(str_split(tbl_pg_rows[hrow], "\\s{2,}"))[c(-1,-2)] # drop first two: empty, metric name
  first_cols <- c("cat_type","category","subcategory")
  tbl_heading <- c(first_cols, tbl_heading)
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
    row_content <- unlist(str_split(trimws(tbl_pg_rows[r]), "\\s{2,}"))
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