## VARIOUS FUNCTIONS FOR WORKING WITH LMR DATABASE 
# PostgreSQL version
# - Fetch data
# - Upload new LMR data gathered from pdf report to MySQL database
# - Check data in database
# - and more
# - using 'dbx_' prefix to avoid confusion with other db functions

library(tidyverse)
library(lubridate)
library(scales)
library(glue)
library(readr) ## for easy conversion of $ characters to numeric
library(RPostgres) ## for PostgreSQL
library(dotenv)

## Source from LMR_db_upload.R (or elsewhere) for convenience/functional

## credentials #### 
# using dotenv pkg and .env file
# create a .env file in the root directory of the project
# and add the following lines (no quotes!)
# (complete values can be found in gdrive file '01 Database Mgm...')
# AWS_ENDPT=...rds.amazonaws.com
# AWS_PWD = A...KOCX
# AWS_PORT = 3..6
# AWS_USER= ...

## save .env contents to system environment - only needs to be run once
#dotenv::load_dot_env()
# Load environment variables
readRenviron('.env')

# set vars for easy use
# - NO LONGER NEEDED - since using dbx_get_con below
#a.endpt <- Sys.getenv('AWS_ENDPT_PG')
#a.user <- Sys.getenv("AWS_USER_PG")
#a.pwd <- Sys.getenv("AWS_PWD_PG")
#a.port <- as.numeric(Sys.getenv("AWS_PORT_PG"))
#database_name <- Sys.getenv("AWS_DB_NAME_PG")

## Set connection string for easy use
# Amazon RDS PostgreSQL
# Define this once - use anywhere
dbx_get_con <- function() {
  dbConnect(
    RPostgres::Postgres(),
    dbname   = Sys.getenv("AWS_DB_NAME_PG"),
    host     = Sys.getenv('AWS_ENDPT_PG'),
    user     = Sys.getenv("AWS_USER_PG"),
    password = Sys.getenv("AWS_PWD_PG"),
    port     = as.numeric(Sys.getenv("AWS_PORT_PG"))
  )
}

## LIST TABLES IN DB ----
dbx_list_tables <- function() {
  con <- dbx_get_con() ## use connection function
  tables <- dbListTables(con) # check connection by getting list of tables
  ## always disconnect when done
  dbDisconnect(con)
  return(tables)
}

## FETCH DATA ----
# get all the data - uses new dbx_get_con function for ease of use
dbx_fetch_basic <- function(db_tbl="public.lmr_data") {
  con <- dbx_get_con()
  # query - get all data
  data_db <- dbGetQuery(con, glue("SELECT * FROM {db_tbl};"))
  ## always disconnect when done
  dbDisconnect(con)
  # pass back data
  return(data_db)
}
# get data with joined quarters info
dbx_fetch_join_qtrs <- function() {
  con <- dbx_get_con()
  lmr_data_db <- dbGetQuery(con, "SELECT 
                           lmr.*
                          , qtr.fyr
                          , qtr.qtr
                          , qtr.end_qtr
                          , qtr.end_qtr_dt
                          , qtr.cyr
                          , qtr.season
                          , qtr.cqtr 
                          FROM public.lmr_data lmr 
                          LEFT JOIN public.lmr_quarters qtr 
                          ON lmr.fy_qtr = qtr.fy_qtr;")
  ## always disconnect when done
  dbDisconnect(con)
  # pass back data
  return(lmr_data_db)
}
# get data with joined quarters & short names
dbx_fetch_qtr_shortname <- function() {
  con <- dbx_get_con()
  lmr_data_db <- dbGetQuery(con, "SELECT 
                           lmr.*
                          , qtr.fyr
                          , qtr.qtr
                          , qtr.end_qtr
                          , qtr.end_qtr_dt
                          , qtr.cyr
                          , qtr.season
                          , qtr.cqtr
                          , l_cat_type.cat_type_short 
                          , l_cat.category_short
                          , l_subcat.subcategory_short
                          FROM public.lmr_data lmr 
                          LEFT JOIN public.lmr_quarters qtr 
                            ON lmr.fy_qtr = qtr.fy_qtr
                          LEFT JOIN public.lmr_shortname_cat_type l_cat_type
                            ON lmr.cat_type = l_cat_type.cat_type
                          LEFT JOIN public.lmr_shortname_category l_cat 
                            ON lmr.category = l_cat.category
                          LEFT JOIN public.lmr_shortname_subcategory l_subcat 
                            ON lmr.subcategory = l_subcat.subcategory;")
  ## always disconnect when done
  dbDisconnect(con)
  # pass back data
  return(lmr_data_db)
}


## UPLOAD DATA ----

## CHECK QTRS ----
## will add latest quarter if not already present
dbx_fetch_update_qtrs <- function(tbl_upload) {
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
dbx_upload <- function(db_tbl, tbl_upload) {
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
dbx_check_data <- function(min_qtr, max_qtr) {
  # amazon postgresql
  con <- dbx_get_con()
  # query - filtering for most recent quarters
  data_db <- dbGetQuery(con, "SELECT * FROM lmr_data
                          WHERE fy_qtr>= {min_qtr} AND fy_qtr<= {max_qtr};")
  ## always disconnect when done
  dbDisconnect(con)
  
  # filter data for most recent 5 qtrs
  #data_db <- data_db %>% filter(fy_qtr>= min_qtr)
  # check number of records by category for each qtr
  # - to check for duplicates or missing
  # - first filter for most recent 5 qtrs
  #recent_qtrs <- tail(unique(data_db$fy_qtr),5)
  qtr_cat_count <- data_db %>% group_by(fy_qtr, cat_type) %>% tally() %>%
    #filter(fy_qtr %in% recent_qtrs) %>% 
    pivot_wider(names_from = fy_qtr, values_from = n)
  print(qtr_cat_count)

  # summary data by category, each qtr
  data_smry_qtr_db <- data_db %>% group_by(cat_type, fy_qtr) %>% 
    summarize(
      netsales=sum(netsales),
      litres=sum(litres)
    ) 
  #fyqtrs <- unique(data_smry_qtr_db$fy_qtr)
  # summary data by category for most recent quarter and earliest in current report
  data_smry_qtr_db <- data_smry_qtr_db %>% filter(fy_qtr %in% c(min_qtr, max_qtr))
  # chart for each category, most recent qtr
  data_chart <- data_smry_qtr_db %>% filter(fy_qtr==max_qtr) %>% 
    ggplot(aes(x=cat_type, y=netsales))+
    geom_col(position = position_dodge())+
    scale_y_continuous(labels=comma_format())+
    coord_flip()+
    theme(axis.ticks.y = element_blank())+
    labs(x="", title=max(recent_qtrs))+theme_bw()
  print(data_chart)
  
  # format fields for readability
  data_smry_qtr_db$netsales <- as.numeric(data_smry_qtr_db$netsales)
  data_smry_qtr_db$litres <- as.numeric(data_smry_qtr_db$litres)
  data_smry_qtr_db$litres <- format(data_smry_qtr_db$litres, big.mark=",", scientific=FALSE, trim = TRUE, justify=c("right"))
  data_smry_qtr_db$netsales <- format(data_smry_qtr_db$netsales, big.mark=",", scientific=FALSE, trim = TRUE, format='i', justify=c("right"))
  # convert netsales to comma format with prefix $
  data_smry_qtr_db$netsales <- paste0("$",data_smry_qtr_db$netsales)
  print("Summary of DB data by category for most recent quarter and same qtr prev yr:")
  print(data_smry_qtr_db)
}

## SHORT NAME TABLES ####
# function to upload based on: short name data frame and target db table
# - assumes only 2 columns in data frame and target table
# db_tbls: lmr_shortname_cat_type, lmr_shortname_category, lmr_shortname_subcategory
dbx_short_name_insert <- function(tbl_upload, db_tbl) {
  con <- get_con() ## use connection function
  cols <- paste0(colnames(tbl_upload), collapse = ",")
  for(r in 1:nrow(tbl_upload)) {   
    val_01 <- tbl_upload[r,1]
    val_02 <- tbl_upload[r,2]
  dbExecute(con, glue("INSERT INTO {db_tbl} (
                  {cols}
                ) 
                VALUES('{val_01}',
                '{val_02}'
                );"))
  }
  dbDisconnect(con)
}


