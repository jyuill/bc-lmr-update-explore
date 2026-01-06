# Code for creating short name tables
# - used to provide short name tables for cat_type, category, and subcategory
# - tables can then be combined with main data table when querying
# - enables consistent system without ad-hoc short name creation
#   - much more scalable / flexible for future analysis and reporting
# - needed because original names can be awkwardly long for reports and analysis

# RUNNING THIS CODE GENERATES 3 TABLES WITH SHORT NAMES:
# - cat_type_short: cat_type and short versions
# - category_short: category and short versions
# - subcategory_short: subcategory and short versions
# - once uploaded to Postgresql database, no need to re-run 
#    unless changes in source reports (or editing needed)
# - UPLOAD functions called below - from lmr_db_functions_postgres.R 

library(tidyverse)
source('functions/lmr_db_functions_postgres.R') # get db functions

# LOAD data from database ----
# - for reference
# - can use function for other tables as well 
lmr_data <- fn_db_fetch('lmr_data')

# Get unique cat_type, category, subcategory values from lmr_data
# - for reference
cat_unique <- lmr_data %>% distinct(cat_type, category, subcategory)

# CREATE short name tables ----
# cat_type short names ----
cat_type_short <- lmr_data %>% group_by(cat_type) %>% summarize(
  cat_type_short=first(cat_type)) %>% ungroup() %>%
  mutate(cat_type_short = str_replace(cat_type, "Refreshment Beverages", "Refresh Bev"))

# category short names ----
category_short <- lmr_data %>% group_by(category) %>% summarize(
  category_short=first(category)) %>% ungroup() %>% 
  mutate(
    category_short = str_replace(category, " Wine", ""),
    category_short = str_replace(category_short, "Other Country", "Other Ctry"),
    category_short = str_replace(category_short, "Other Spirits", "Other"),
    category_short = str_replace(category_short, "Other Style", "Other"), # "Other Style Wine" (Wine already removed)
    category_short = str_replace(category_short, "South Africa", "S. Africa"),
    category_short = case_when(
      category == "Domestic - BC Beer" ~ "BC",
      category == "Domestic - Other Province Beer" ~ "Other Prov",
      category == "Import Beer" ~ "Import",
      str_detect(category, "Brandy") ~ "Brandy",
      TRUE ~ category_short
    ))

# subcategory short names ----
subcategory_short <- lmr_data %>% group_by(subcategory) %>% summarize(
  subcategory_short=first(subcategory)) %>% ungroup() %>%
  mutate(
    subcategory_short = str_replace(subcategory, " Wine", ""), # remove Wine since redundant
    subcategory_short = str_replace(subcategory_short, " Spirits", ""), # spirits is redundant
    subcategory_short = str_replace(subcategory_short, " And | and ", " & "),
    subcategory_short = str_replace(subcategory_short, "Miscellaneous", "Misc"),
    subcategory_short = str_replace(subcategory_short, "Flavoured ", "Flav "),
    subcategory_short = str_replace(subcategory_short, "Fortified", "Fort"),
    # remove country names since redundant - esp. in Wine category type
    subcategory_short = str_replace(subcategory_short, 
      "Argentina |Australia |Austria |Bulgaria |Chile ", ""),
    subcategory_short = str_replace(subcategory_short,
      "Canada - BC |Canada - Other |France |Georgia |Germany |Greece ", ""),
    subcategory_short = str_replace(subcategory_short,
      "Hungary |Israel |Italy |New Zealand |Portugal |South Africa |Spain ", ""),
    # clean up Beer cat type
    subcategory_short = case_when(
      subcategory == "Domestic - BC Commercial Beer" ~ "BC Major",
      subcategory == "Domestic - BC Regional Beer" ~ "BC Regional",
      subcategory == "Domestic - BC Micro Brew Beer" ~ "BC Micro",
      subcategory == "Domestic - Other Province Commercial Beer" ~ "Other Prov Major",
      subcategory == "Domestic - Other Province Regional Beer" ~ "Other Prov Reg.",
      subcategory == "Domestic - Other Province Micro Brew Beer" ~ "Other Prov Micro",
      subcategory == "Asia And South Pacific Beer" ~ "Asia",
      subcategory == "Europe Beer" ~ "Europe",
      subcategory == "Mexico And Caribbean Beer" ~ "Mex/Carib",
      #subcategory == "USA Beer" ~ "USA",
      TRUE ~ subcategory_short
    ),
    # clean-up 'Other Country': treatment depends on whether Wine variety or other drinks
    # non-wine: replace with 'Other Ctry' within category
    subcategory_short = str_replace(subcategory_short, "Other Country (Asian Spirits|Beer|Sake|Whisky)","Other Ctry"),
    # wine: category is already 'Other Country' so just leave wine variety
    subcategory_short = str_replace(subcategory_short, "Other Country ",""),
    subcategory_short = str_replace(subcategory_short, "Other Style ",""),
    subcategory_short = ifelse(str_detect(subcategory_short, "USA (Beer|Sake)"), "USA", str_replace(subcategory_short,"USA ","")),
    subcategory_short = ifelse(str_detect(subcategory_short, "China (Fruit|Red|White)"), str_replace(subcategory_short,"China ",""), subcategory_short)
  )

# UPLOAD TO POSTGRESQL DATABASE ----
# cat_type short names ----
fn_short_names(cat_type_short, "public.lmr_shortname_cat_type")
# category short names ----
fn_short_names(category_short, "public.lmr_shortname_category")
# subcategory short names ----
fn_short_names(subcategory_short, "public.lmr_shortname_subcategory")
