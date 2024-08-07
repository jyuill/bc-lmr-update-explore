# Queries for database mgmt as needed

library(tidyverse)
library(lubridate)
library(RMariaDB)
library(glue)

# add new column for calendar quarter: cqtr
source('credo_db.R')

# connect to database
con <- dbConnect(RMariaDB::MariaDB(), 
                 user = a.user, 
                 password = a.pwd, 
                 dbname = database_name, 
                 host = a.endpt, 
                 port = a.port)
# check tblLDB_quarters
dbListFields(con, 'tblLDB_quarter')
qtr_db <- dbGetQuery(con, "SELECT * FROM bcbg.tblLDB_quarter;")
# add new column cqtr to tblLDB_quarter
qry <- "ALTER TABLE bcbg.tblLDB_quarter ADD COLUMN cqtr VARCHAR(6);"
dbExecute(con, qry)
# close connection
dbDisconnect(con)

# determine calendar quarter from fiscal quarter field (qtr)
qtr <- qtr %>% 
  mutate(cqtr = case_when(
    qtr == 'Q1' ~ 'Q2',
    qtr == 'Q2' ~ 'Q3',
    qtr == 'Q3' ~ 'Q4',
    qtr == 'Q4' ~ 'Q1'
  ))
# all below provided by copilot -> WORKED!!!
# write query to update cqtr in tblLDB_quarter
qtr <- qtr %>% 
  select(qtr, cqtr) %>% 
  mutate(qtr = paste0("'",qtr,"'"),
         cqtr = paste0("'",cqtr,"'")) %>% 
  mutate(qry = paste0("UPDATE bcbg.tblLDB_quarter SET cqtr = ",cqtr," WHERE qtr = ",qtr,";"))
# connect to database
con <- dbConnect(RMariaDB::MariaDB(), 
                 user = a.user, 
                 password = a.pwd, 
                 dbname = database_name, 
                 host = a.endpt, 
                 port = a.port)
# update cqtr in tblLDB_quarter
for(i in 1:nrow(qtr)){
  dbExecute(con, qtr$qry[i])
}
