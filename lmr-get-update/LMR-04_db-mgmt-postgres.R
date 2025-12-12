# Queries for database mgmt as needed

library(tidyverse)
library(lubridate)
library(RPostgres)
library(glue)

# add new column for calendar quarter: cqtr
source('credo_db.R')

# connect to database
# make the connection
con_aws <- dbConnect( 
  RPostgres::Postgres(),
  host = a.endpt, 
  dbname = a.db_name,
  port = a.port,
  user = a.user,
  password = a.pwd)

# check tables
dbListTables(con_aws)

# check data in main table
lmr_pgt <- dbGetQuery(con_aws, 
  "SELECT * FROM public.lmr_data;")

dbDisconnect(con_aws)

# check tblLDB_quarters
# use connection from above
dbListFields(con_aws, 'public.lmr_quarters')
qtr_db <- dbGetQuery(con_aws, "SELECT * FROM public.lmr_quarters;")
# close connection
dbDisconnect(con_aws)

# determine calendar quarter from fiscal quarter field (qtr)
qtr <- qtr %>% 
  mutate(cqtr = case_when(
    qtr == 'Q1' ~ 'Q2',
    qtr == 'Q2' ~ 'Q3',
    qtr == 'Q3' ~ 'Q4',
    qtr == 'Q4' ~ 'Q1'
  ))

# write query to update cqtr in lmr_quarters
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
