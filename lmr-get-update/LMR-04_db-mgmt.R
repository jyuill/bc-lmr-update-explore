# Queries for database mgmt as needed

library(tidyverse)
library(lubridate)
library(RPostgres)
library(glue)

# use functions from lmr_db_functions.R, incl authentication
source('functions/lmr_db_functions.R')

# check tables
dbx_list_tables()

# check data in main table
lmr_main <- dbx_fetch_basic()

# lmr data with qtrs info
lmr_main_qtrs <- dbx_fetch_join_qtrs()

# lmr data with qtrs AND short names


# check tblLDB_quarters
dbListFields(dbx_get_con(), 'lmr_quarters')
qtr_db <- dbGetQuery(dbx_get_con(), "SELECT * FROM public.lmr_quarters;")

# determine calendar quarter from fiscal quarter field (qtr)
# add cqtr field (already available)
qtr <- qtr_db %>% 
  mutate(cqtr = case_when(
    qtr == 'Q1' ~ 'Q2',
    qtr == 'Q2' ~ 'Q3',
    qtr == 'Q3' ~ 'Q4',
    qtr == 'Q4' ~ 'Q1'
  ))

# UPDATE cqtr in lmr_quarters
# - not needed, already done, keeping for reference
qtr <- qtr %>% 
  select(qtr, cqtr) %>% 
  mutate(qtr = paste0("'",qtr,"'"),
         cqtr = paste0("'",cqtr,"'")) %>% 
  mutate(qry = paste0("UPDATE lmr_quarters SET cqtr = ",cqtr," WHERE qtr = ",qtr,";"))
# connect to database
con_aws <- dbConnect( 
  RPostgres::Postgres(),
  host = a.endpt, 
  dbname = a.db_name,
  port = a.port,
  user = a.user,
  password = a.pwd)
# update cqtr in tblLDB_quarter
for(i in 1:nrow(qtr)){
  dbExecute(con_aws, qtr$qry[i])
}
