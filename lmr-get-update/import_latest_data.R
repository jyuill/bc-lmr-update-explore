## R Code to Import Latest LMR Extraction Results
## Run this after Python extraction to get data into R session for review

library(tidyverse)
library(here)

# Import the latest extracted data
lmr_latest <- read_csv(here("lmr-get-update", "output", "lmr_data_latest.csv"))

# Basic data overview
cat("=== LATEST LMR DATA SUMMARY ===\n")
cat("Total rows:", nrow(lmr_latest), "\n")
cat("Columns:", ncol(lmr_latest), "\n")
cat("Categories:", lmr_latest$cat_type |> n_distinct(), "\n")
cat("Quarters:", lmr_latest$fy_qtr |> n_distinct(), "\n")

# Show most recent quarter summary
most_recent_qtr <- max(lmr_latest$fy_qtr)
cat("\nMost recent quarter:", most_recent_qtr, "\n")

recent_summary <- lmr_latest |> 
  filter(fy_qtr == most_recent_qtr) |> 
  group_by(cat_type) |> 
  summarise(
    items_netsales = sum(!is.na(netsales)),
    items_litres = sum(!is.na(litres)),
    total_netsales = sum(netsales, na.rm = TRUE),
    total_litres = sum(litres, na.rm = TRUE),
    .groups = "drop"
  ) |> 
  mutate(
    netsales_formatted = scales::dollar(total_netsales, scale = 1e-6, suffix = "M"),
    litres_formatted = scales::number(total_litres, scale = 1e-6, suffix = "M")
  )

print(recent_summary)

# Quick data quality checks
cat("\n=== DATA QUALITY CHECKS ===\n")

# Check for missing values
missing_summary <- lmr_latest |> 
  summarise(
    missing_netsales = sum(is.na(netsales)),
    missing_litres = sum(is.na(litres)),
    missing_both = sum(is.na(netsales) & is.na(litres))
  )
print(missing_summary)

# Show structure
cat("\nData structure:\n")
str(lmr_latest)

cat("\n=== DATA NOW AVAILABLE AS 'lmr_latest' ===\n")
cat("Use lmr_latest for further analysis in R\n")
cat("Examples:\n")
cat("  View(lmr_latest)  # Open data viewer\n")
cat("  summary(lmr_latest)  # Summary statistics\n")
cat("  lmr_latest |> filter(cat_type == 'beer')  # Filter by category\n")
cat("  lmr_latest |> filter(fy_qtr == '", most_recent_qtr, "')  # Most recent quarter\n", sep = "")
cat("  unique(lmr_latest$fy_qtr)  # Show all quarters (now in FY2025Q1 format)\n")