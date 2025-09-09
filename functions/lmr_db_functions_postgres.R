## Upload LMR data gathered from pdf report to MySQL database

library(tidyverse)
library(lubridate)
library(scales)
library(glue)
library(readr) ## for easy conversion of $ characters to numeric
library(RPostgres) ## for PostgreSQL
library(dotenv)

## Run from LMR_db_upload.R for convenience/functional

## credentials #### 
# using dotenv pkg and .env file
# create a .env file in the root directory of the project
# and add the following lines (no quotes!)
# (complete values can be found in gdrive file '01 Database Mgm...')
# AWS_ENDPT=...rds.amazonaws.com
# AWS_PWD = A...KOCX
# AWS_PORT = 3..6
# AWS_USER= ...

## save .env contents to system environment
#dotenv::load_dot_env()
# Load environment variables
readRenviron('.env')

a.endpt <- Sys.getenv('AWS_ENDPT_PG')
a.pwd <- Sys.getenv("AWS_PWD_PG")
a.user <- Sys.getenv("AWS_USER_PG")
a.port <- as.numeric(Sys.getenv("AWS_PORT_PG"))
database_name <- Sys.getenv("AWS_DB_NAME_PG")

## TEST parameters for function
#db_tbl <- "bcbg.tblLDB_lmr"
#tbl_upload <- tables_all_fyqtr ## table produced frm LMR-fetch-process-all_vX.R

## TEST RUN below function ----
#fn_db_upload(db_tbl, tbl_upload)

## CHECK QTRS ----
## will add latest quarter if not already present
fn_db_qtrs <- function(tbl_upload) {
  # amazon RDS PostgreSQL
  con <- dbConnect(RPostgres::Postgres(),
                     dbname=database_name,
                     host=a.endpt,
                     user=a.user,
                     password=a.pwd,
                     port=a.port)
  dbListTables(con) # check connection by getting list of tables
  # get list of quarters covered
  qtrs <- dbGetQuery(con, "SELECT * FROM lmr_quarters;")
  
  if(max(qtrs$fy_qtr) < max(tbl_upload$fy_qtr)) {
    ## set up values for new qtr info
    fy_qtr_new <- max(tbl_upload$fy_qtr)
    fyr_new <- as.numeric(str_sub(fy_qtr_new, start=3, end=6))
    qtr_new <- str_sub(fy_qtr_new, start=7, end=8)
    end_qtr_new <- case_when(
      qtr_new == 'Q1' ~ '06-30',
      qtr_new == 'Q2' ~ '09-30',
      qtr_new == 'Q3' ~ '12-31',
      qtr_new == 'Q4' ~ '03-31'
    )
    if(qtr_new=='Q4') {
      cyr_new <- fyr_new
    } else {
      cyr_new <- fyr_new-1
    }
    end_qtr_dt_new <- paste0(cyr_new,"-",end_qtr_new)
    if(qtr_new=='Q1' | qtr_new=='Q2') {
      season_new <- 'summer'
    } else {
      season_new <- 'winter'
    }
    # calendar quarter
    cqtr_new <- case_when(
      qtr_new == 'Q1' ~ 'Q2',
      qtr_new == 'Q2' ~ 'Q3',
      qtr_new == 'Q3' ~ 'Q4',
      qtr_new == 'Q4' ~ 'Q1'
    )
    ## insert info for new qtr
    dbExecute(con, glue("INSERT INTO lmr_quarters (
                fy_qtr,
                fyr,
                qtr,
                end_qtr,
                end_qtr_dt,
                cyr,
                cqtr,
                season,
                created_at
              ) 
              VALUES('{fy_qtr_new}',
              {fyr_new},
              '{qtr_new}',
              '{end_qtr_new}',
              '{end_qtr_dt_new}',
              '{cyr_new}',
              '{cqtr_new}',
              '{season_new}',
              '{Sys.time()}'
              );"))
  }
  # get updated list of quarters covered
  qtrs_current <- dbGetQuery(con, "SELECT * FROM lmr_quarters;")
  ## always disconnect when done
  dbDisconnect(con)
  # pass back latest version of quarters tbl
  return(qtrs_current)
}

## INSERT LMR DATA TO DB ####
## set up as function for flexbility
fn_db_upload <- function(db_tbl, tbl_upload) {
    # connection needed for upload
    # amazon postgresql
    con <- dbConnect(RPostgres::Postgres(),
                     dbname=database_name,
                     host=a.endpt,
                     user=a.user,
                     password=a.pwd,
                     port=a.port)
    # test if nec
    #dbGetQuery(con, "SELECT * FROM lmr_data;")
    
    for(r in 1:nrow(tbl_upload)) {
      ## delete any existing match
      dbExecute(con, glue("DELETE FROM {db_tbl}
                           WHERE fy_qtr='{tbl_upload$fy_qtr[r]}' AND
                           cat_type='{tbl_upload$cat_type[r]}' AND
                           category='{tbl_upload$category[r]}' AND
                           subcategory='{tbl_upload$subcategory[r]}';"))
      ## insert row from new data
      dbExecute(con, glue("INSERT INTO {db_tbl} (
                fy_qtr,
                cat_type,
                category,
                subcategory,
                litres,
                netsales,
                created_at
              ) 
              VALUES('{tbl_upload$fy_qtr[r]}',
              '{tbl_upload$cat_type[r]}',
              '{tbl_upload$category[r]}',
              '{tbl_upload$subcategory[r]}',
              {tbl_upload$litres[r]},
              {tbl_upload$netsales[r]},
              '{Sys.time()}'
              );"))
      cat("Inserted: ", r, tbl_upload$fy_qtr[r], 
          tbl_upload$cat_type[r],
          tbl_upload$category[r],
          tbl_upload$subcategory[r],"\n")
    }
    ## always disconnect when done
    dbDisconnect(con)
}

## DATA CHECK ####
fn_db_check <- function() {
  # amazon postgresql
  con <- dbConnect(RPostgres::Postgres(),
                   dbname=database_name,
                   host=a.endpt,
                   user=a.user,
                   password=a.pwd,
                   port=a.port)
  # query
  data_db <- dbGetQuery(con, "SELECT * FROM lmr_data;")
  ## always disconnect when done
  dbDisconnect(con)
  
  # check number of records by category for each qtr
  # - to check for duplicates or missing
  # - first filter for most recent 5 qtrs
  recent_qtrs <- tail(unique(data_db$fy_qtr),6)
  qtr_cat_count <- data_db %>% group_by(fy_qtr, cat_type) %>% tally() %>%
    filter(fy_qtr %in% recent_qtrs) %>% pivot_wider(names_from = fy_qtr, values_from = n)
  print(qtr_cat_count)

  # summary data by category, each qtr
  data_smry_qtr_db <- data_db %>% group_by(cat_type, fy_qtr) %>% 
    summarize(
      netsales=sum(netsales),
      litres=sum(litres)
    ) 
  fyqtrs <- unique(data_smry_qtr_db$fy_qtr)
  # summary data by category for most recent quarter
  data_smry_qtr_db <- data_smry_qtr_db %>% filter(fy_qtr==max(fyqtrs))
  # chart for each category, each qtr
  data_chart <- data_smry_qtr_db %>% ggplot(aes(x=cat_type, y=netsales))+
    geom_col(position = position_dodge())+
    scale_y_continuous(labels=comma_format())+
    coord_flip()+
    theme(axis.ticks.y = element_blank())+
    labs(x="", title=max(fyqtrs))+theme_bw()
  print(data_chart)
  
  # format fields for readability
  data_smry_qtr_db$netsales <- as.numeric(data_smry_qtr_db$netsales)
  data_smry_qtr_db$litres <- as.numeric(data_smry_qtr_db$litres)
  data_smry_qtr_db$litres <- format(data_smry_qtr_db$litres, big.mark=",", scientific=FALSE, trim = TRUE, justify=c("right"))
  data_smry_qtr_db$netsales <- format(data_smry_qtr_db$netsales, big.mark=",", scientific=FALSE, trim = TRUE, format='i', justify=c("right"))
  # convert netsales to comma format with prefix $
  data_smry_qtr_db$netsales <- paste0("$",data_smry_qtr_db$netsales)
  print(data_smry_qtr_db)
}