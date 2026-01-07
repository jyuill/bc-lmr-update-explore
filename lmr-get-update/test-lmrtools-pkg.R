# TESTING the lmrtools package I developed for managing queries 
# to the LMR database
# install package - may be needed for updates
devtools::install_github("jyuill/lmrtools")

# load package
library(lmrtools)
# use function to fetch data from LMR database
lmrtools_01 <- lmrtools::list_tables()
lmrtools_02 <- lmrtools::fetch_db_basic('lmr_data')
lmrtools_03 <- lmrtools::fetch_lmr_complete_filter(replace = TRUE,
                                                  min_end_qtr_dt = '2024-12-31',
                                                  max_end_qtr_dt = '2025-09-30',
                                                  cat_type = 'Wine')

