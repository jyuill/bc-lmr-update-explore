## functions for pulling tabular data out of LDB Quarterly Reports

library(pdftools)
library(tidyverse)
library(lubridate)
library(scales)
library(glue)
library(readr) ## for easy conversion of $ characters to numeric
library(here)
library(tesseract)
library(gt)

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
      } # end file name clean-up
  ## combine name components for clarity
  fname <- paste0("LMR_",fname_url_yr,"_",fname_url_mth,"_",fname_url_qtr,".pdf")
  
  # working with existing files ----
  # maintain list of files and URLs for on-going reference
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
  # pdf dwnld ------
  ## if file doesn't exist download it
  if(!file.exists(pdf_file)){
    download.file(furl, pdf_file, mode='wb') # mode='wb' on windows for text files
    #info <- pdf_info(pdf_file) # get metadata: # of pgs, title, etc
    # convert pdf to readable format in R and import
    if(rotate==TRUE){
      cat("start rotate pdf pages \n")
      #lmr <- pdf_text(pdf_file) # original process - before LDB reformatted
      # setup for separate file for rotated version, if needed (testing?)
      #output_pdf_file <- paste0(input_folder,"rotated_",fname)
      #system(paste("qpdf --rotate=+90", pdf_file, output_pdf_file))
      # rotate and replace original pdf file
      system(paste("qpdf --rotate=+90 --replace-input", pdf_file))
    }
  }
    
  # OCR conversion ----
  cat("start OCR conversion \n")
  # set filename for ocr object
  ocr_file <- paste0(input_folder,str_replace(fname,"\\.pdf",".rds"))
  # check if exists: if so read in, otherwise run ocr & save
  if(file.exists(ocr_file)){
    lmr_ocr <- readRDS(here(ocr_file))
    cat(paste0("using existing ocr object from: ",ocr_file,"\n"))
  } else {
    # convert rotated pdf from images to text (needed starting Sep 2024)
    lmr_ocr <- tesseract::ocr(pdf_file)
    # save ocr object based on fname, for reuse later if needed
    cat(paste0("saving ocr object as: ",ocr_file ,"\n"))
    saveRDS(lmr_ocr, ocr_file)
    # process generates png files in root directory, but not needed
    # clean up .png files produced as side-effect
    # get List all .png files in the directory
    png_files <- list.files(path = here(), pattern = "\\.png$", full.names = TRUE)
    # Remove the .png files - not needed
    file.remove(png_files)
  }
  
  # leaving option open for other proc if pdf format changes after Sep 2024
  lmr_use <- lmr_ocr
  return(list(lmr_use, fname))
  cat('PDF import/process complete \n')
}

## clean pg -> remove summary, blank rows, etc
fn_pg_clean <- function(tbl_pg_rows) {
  ## > RM SUMMARY ROWS (or blank) ####
  cat(paste0("page cleaning: ",length(tbl_pg_rows), " rows total \n"))
  # check for and remove unwanted rows
  srows <- NULL
  for(r in 1:length(tbl_pg_rows)){
    if(str_detect(tbl_pg_rows[r],"Summary")==TRUE | 
       tbl_pg_rows[r]=="" |
       str_detect(tbl_pg_rows[r],"Liquor Market Review") |
       str_detect(tbl_pg_rows[r], "TOP")){
      srow <- r
      srows <- c(srows,srow) # accumulate rows to remove
    }
  }
  ## want to remove Summary OR blank rows
  if(length(srows)==0){
    cat("no summary or blank rows to remove \n")
  } else {
    cat("removing rows: ", srows,"\n")
  }
  tbl_pg_rows_ns <- tbl_pg_rows[c(-srows)]
  cat(paste0(length(tbl_pg_rows_ns), " rows after cleaning \n"))
  print(tbl_pg_rows_ns)
  return(tbl_pg_rows_ns)
}

## get meta data from pg
fn_pg_meta <- function(tbl_pg_rows, pg_num, tbl_name_prev, tbl_cat_type){
  ## > get META data ####
  cat("get META data \n")
  tbl_name <- trimws(tbl_pg_rows[1])
  
  tbl_name_clean <- str_replace_all(tbl_name,"\\(","")
  tbl_name_clean <- str_replace_all(tbl_name_clean,"\\)","")
  tbl_name_clean <- str_replace_all(tbl_name_clean,"\\$","")
  tbl_name_clean <- trimws(tbl_name_clean, "right")
  tbl_name_clean <- str_replace_all(tbl_name_clean," ","_")
  cat("pg:",pg_num,', tbl:', tbl_name_clean,", tbl prev:",tbl_name_prev,"\n")
  ## check for prev same table name -> if so, 
  #. - keep using prev, since indicates continuation
  if(exists('tbl_name_prev')){
    if(tbl_name_clean==tbl_name_prev){ ## continuation of table
      tbl_cat_type <- tbl_cat_type
      hrow <- 2 # row 2 when page is continuation of table on prev pg
      tbl_cont <- TRUE
    } else { ## 
      tbl_cat_type <- tbl_pg_rows[2]
      hrow <- 3
      tbl_cont <- FALSE
    }
  } else {
    tbl_cat_type <- tbl_pg_rows[2]
    hrow <- 3
    tbl_cont <- FALSE
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
  cat('hrow:',hrow,'; tbl_cat_type:',tbl_cat_type,'; tbl_metric:',tbl_metric,'; tbl_cont?',tbl_cont,"\n")
  
  return(list(tbl_name_clean, hrow, tbl_cat_type, tbl_metric, tbl_cont))
} # end meta data

## extract tbl content pg by pg in loop
fn_tbl_content <- function(tbl_pg_rows, tbl_meta, categories){
  ## >> start content process function ----
  cat("fn_tbl_content: start content process function \n")
  ## unpack meta data
  tbl_name <- tbl_meta[[1]]
  hrow <- tbl_meta[[2]] ## identifies heading row, depends on pg layout; -1 for blank row
  tbl_cat_type <- tbl_meta[[3]]
  tbl_metric <- tbl_meta[[4]]
  
  # >> show meta data ----
  cat(paste0('tbl_name: ',tbl_name,'; ', 'tbl_cat_type: ',tbl_cat_type,'; ', 
             'tbl_metric: ',tbl_metric,'; hrow: ',hrow,'\n'))

  ## > TABLE + HEADING ####
  ## >> get headings, set up table ----
  cat("get headings, set up table \n")
  #tbl_heading <- unlist(str_split(tbl_pg_rows[hrow], "\\s{2,}"))[c(-1,-2)] # drop first two: empty, metric name
  # ocr version: split on 1 or more spaces
  # need to drop everything before the first 'Fiscal'
  # then split by occurences of 'Fiscal' and do some cleaning
  tbl_heading <- unlist(str_split(tbl_pg_rows[hrow], "\\s{1,}"))
  #cat("raw heading:",tbl_heading,"\n")
  
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
  #cat("headings: ",h_all,"\n")
  h_all <- str_replace(h_all, "Fiscal ", "FY")
  ## remove two digits for prev yr + '/' ("21/" in example above)
  tbl_heading <- str_replace_all(str_remove(h_all, str_sub(h_all, start=5, end=7))," ","")
  
  # add standard cols to headings
  first_cols <- c("cat_type","category","subcategory")
  tbl_heading <- c(first_cols, tbl_heading)
  ## >> final heading ----
  cat("final heading:",tbl_heading,"\n")
  
  ## initiate empty table with proper headings
  tbl_pg_data <- data.frame(matrix(ncol=length(tbl_heading)))
  colnames(tbl_pg_data) <- tbl_heading
  
  # > PROC ROW DATA ####
  ## get data from each row and add to table
  ## >> process data ----
  cat("process data \n")
  start <- hrow+1
  ## confirm that start is less than total rows -> data is available
  if(start<length(tbl_pg_rows)){
    # process data rows
    ## loop through each row to collect data - if more rows beyond start
    for(r in start:length(tbl_pg_rows)){
      ## pre-defined tbl will accumulate rows through loop
      cat("\n tbl row: ", r, "of ", length(tbl_pg_rows),"\n")
      # original pre Sep 2024 version: split on 2 or more spaces
      #row_content <- unlist(str_split(trimws(tbl_pg_rows[r]), "\\s{2,}"))
      # col_nums <- length(row_content)
      # cat("col nums:", col_nums, "\n")
      # if(col_nums>5){ ## only rows with data
      #   tr <- r-hrow ## set table row number by subtract pdf start num
      #   tbl[tr,"cat_type"] <- tbl_cat_type
      #   if(col_nums==7){ ## rows with category shown
      #     tbl$category[tr] <- row_content[1]
      #     tbl$subcategory[tr] <- row_content[2]
      #     tbl[tr,c(4:(col_nums+1))] <- row_content[c(3:col_nums)]
      #   } else {
      #     ## get category from prev row
      #     tbl$category[tr] <- tbl$category[tr-1]
      #     tbl$subcategory[tr] <- row_content[1]
      #     tbl[tr,c(4:(col_nums+2))] <- row_content[c(2:col_nums)]
      #   }
      # end original pre Sep 2024 version
      # new process as of Sep 2024 based on OCR of tables
      row_content <- tbl_pg_rows[r] 
      #cat("row content:", row_content, "\n")
      # >> known issues in row ----
      # fix known issues that have been identified 
      # - such as OCR not recognizing characters correctly
      #   - will result in NA cat/subcat values in table
      cat(" if errors, fix known issues in row \n")
      row_content <- case_when(
        str_detect(row_content, "lrish") ~ str_replace(row_content, "lrish", "Irish"),
        str_detect(row_content, "ltal") ~ str_replace(row_content, "ltal", "Ital"),
        str_detect(row_content, "|") ~ str_replace(row_content, "\\| ", ""),
        TRUE ~ row_content
      )
      # >> content split ====
      # split version for detecting number values - split on spaces
      # if $ present, split row content on $ to get number values
      if(str_detect(row_content, "\\$")){
        row_content_split <- unlist(str_split(trimws(row_content), "\\$"))
      } else {
        row_content_split <- unlist(str_split(trimws(row_content), "\\s{1,}"))
      }
      
      cat("content split:", row_content_split, "\n")
      # get number values starting from end, working back (accounts for variable length)
      # - clean up text convert to number format
      #   - remove any $ signs, commas, spaces, other extraneous characters
      # - confirm row has content before moving ahead
      if(length(row_content_split)>4) {
        ttl_length <- length(row_content_split)
        num_values <- row_content_split[(ttl_length-4):ttl_length]
        num_values <- str_remove_all(num_values,"\\$")
        num_values <- str_remove_all(num_values, ",")
        num_values <- str_remove_all(num_values, " ")
        # farther edge cases caused by incorrect ocr
        # - brackets
        num_values <- str_remove_all(num_values, "\\)")
        num_values <- str_remove_all(num_values, "\\(")
        # - slash sometimes shows up instead of 7
        num_values <- str_replace_all(num_values, "\\/", "7")
        num_values <- as.numeric(num_values)
        trow <- r-hrow # get row number based on r minus hrow value to start at 1
        tbl_pg_data[trow, "cat_type"] <- tbl_cat_type
        
        # >> cat/subcat matching ----
        # use category tbl provided and loop through to find match for subcategory
        # - on subcategory match, set category and subcategory columns
        # - works for rows with no category available
        # - possible issue: if categories change in new data, will match with outdated category
        for(sc in 1:nrow(categories) ) {
          # check for match on cat+subcat to ensure match between rows with both category and subcategory, with min false positives
          # if not, check if match on cat+subcat when cat prepended to row content
          cat_test <- paste(categories$category[sc],categories$subcategory[sc])
          row_content_cat <- paste(categories$category[sc], row_content)
          cat('cat_test:',cat_test,'\n row_content_cat:',row_content_cat,'\n')
          if(str_detect(row_content, cat_test)) {
            tbl_pg_data[trow,"category"] <- categories$category[sc]
            tbl_pg_data[trow,"subcategory"] <- categories$subcategory[sc]
            cat('match cat/subcat:',categories$category[sc],categories$subcategory[sc],'\n')
          } else if(str_detect(row_content_cat, cat_test)) {
            tbl_pg_data[trow,"category"] <- categories$category[sc]
            tbl_pg_data[trow,"subcategory"] <- categories$subcategory[sc]
            cat('match subcat:',categories$category[sc],categories$subcategory[sc],'\n')
          } 
          # else (str_detect(row_content, categories$subcategory[sc])) {
          #   tbl_pg_data[trow,"category"] <- NA
          #   tbl_pg_data[trow,"subcategory"] <- row_content_split[1]
          # }
          # append number values in each col, based on values obtained above
          tbl_pg_data[trow,4:8] <- num_values
        } # end category / subcategory
      } else { 
        tbl_pg_data <- NULL
        tbl_pg_data_long <- NULL
      } # end content test
      print(tbl_pg_data[trow,])
    } ## end row conditional
    } else {
    cat("no data rows to process \n")
    tbl_pg_data <- NULL
    tbl_pg_data_long <- NULL
  } # end start row check condition
  
    print(tbl_pg_data)
    
    # check for null before proceeding
    cat(paste0("proceed with final process, return data? ",!is.null(tbl_pg_data),"\n"))
    if(!is.null(tbl_pg_data)){
      
      ## > SAVE current page ----
      # >> save tbl for troubleshoot ----
      # as temp version troubleshooting (overwrite each time)
      write_csv(tbl_pg_data, "lmr-get-update/output/temp.csv")  
    
      ##> TIDY FORMAT ####
      ## >> over to fn_tidy_structure process for tbl_long ----
      # check for NA within each tbl row, as these may cause trouble; 
      # setting to max NAs per row less than 2
      # NEEDS TO BE CHANGED: should only consider NAs when checking categories
      # - ok if some NAs in data come through
      if(max(rowSums(!is.na(tbl_pg_data[1:3])))){
        tbl_pg_data_long <- fn_tidy_structure(tbl_pg_data, tbl_metric)
        } else {
          cat("TABLE HAS CATEGORY NAs - investigate! \n")
          tbl_pg_data_long <- NULL
        }
  
      ## save table name for comparison with next table
      ## - in some cases, tables run over multiple pages
      #  - doesn't do anything because not passed back (handled elsewhere)
      tbl_name_prev <- tbl_name
      
      ## return tbl: wide and long
      return(list(tbl_pg_data, tbl_pg_data_long))
    } # end of null check for tbl
} ## end single page process

## extend to long format - called from within fn_tbl_content above
fn_tidy_structure <- function(tbl, tbl_metric){
  # troubleshooting
  #tbl <- read_csv("lmr-get-update/output/temp.csv")
  #tbl_metric <- tbl_meta[[4]]
  ## >> tidy process for tbl_long ----
  cat("tidy process for tbl_long \n")
  cat(tbl_metric,"\n")
  ## > convert to long ####
  ## convert table with metrics in multiple rows to tidy format
  tbl_long <- tbl %>% pivot_longer(cols=c(4:ncol(tbl)), names_to='period', values_to=tbl_metric)
  # used in original process based on LMR format before Sep 2024
  #tbl_long <- tbl_long %>% mutate(
  #  !!tbl_metric := parse_number(!!sym(tbl_metric))
  #)
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
  # net sales
  data_chart <- data_smry_cat %>% ggplot(aes(x=fy_qtr, y=netsales))+geom_col()+
    facet_grid(.~cat_type)+
    scale_y_continuous(labels=label_comma(scale = 1e-6,
                                          suffix = 'M'))+
    theme(axis.text.x = element_text(angle=45, hjust=1),
          axis.ticks.x = element_blank())+
          labs(x="")
  print(data_chart)
  # litres
  data_chart <- data_smry_cat %>% ggplot(aes(x=fy_qtr, y=litres))+geom_col()+
    facet_grid(.~cat_type)+
    scale_y_continuous(labels=label_comma(scale = 1e-6,
                                          suffix = 'M'))+
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
  
  # summary data by category for all quarters
  # qtr summary
  data_smry_qtrs <- data_smry_cat %>% group_by(cat_type, fy_qtr) %>% 
    summarize(netsales=sum(netsales),
              litres=sum(litres)
    )
  # clumsy attempt at some formatting
  data_smry_qtrs$litres <- format(data_smry_qtrs$litres, big.mark=",", scientific=FALSE, trim=TRUE, justify=c("right"))
  # format netsales as dollars with $ sign and right justified
  data_smry_qtrs$netsales <- format(data_smry_qtrs$netsales, big.mark=",", scientific=FALSE, trim=TRUE, format='i', justify=c("right"))
  data_smry_qtrs$netsales <- paste0("$",data_smry_qtrs$netsales)
  
  # net sales
  data_smry_qtrs_ns <- data_smry_qtrs %>% select(-litres) %>% 
    pivot_wider(names_from=fy_qtr, values_from=netsales)
  print(data_smry_qtrs_ns)
  #data_smry_qtrs_ns |> gt()
  # litres
  data_smry_qtrs_ltr <- data_smry_qtrs %>% select(-netsales) %>% 
    pivot_wider(names_from=fy_qtr, values_from=litres)
  print(data_smry_qtrs_ltr)
}
