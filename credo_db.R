
## credentials #### 
# using dotenv pkg and .env file
# create a .env file in the root directory of the project
# and add the following lines (no quotes!)
# full values can be found in a gdrive file '01 Database Mgmt...'
# AWS_ENDPT = db-...rds.amazonaws.com
# AWS_PWD = A...KOCX
# AWS_PORT = 3..6
# AWS_USER= admin

## save .env contents to system environment
dotenv::load_dot_env()
# Load environment variables
readRenviron('.env')

a.endpt <- Sys.getenv('AWS_ENDPT')
a.pwd <- Sys.getenv("AWS_PWD")
a.user <- Sys.getenv("AWS_USER")
a.port <- as.numeric(Sys.getenv("AWS_PORT"))

database_name <- "bcbg"