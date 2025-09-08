# Prompt notes for Claude Connect

## Usage

* designed to cover end-to-end PDF processing for all tables
* allow me to specify PDF file to parse
* create csv output in format that can be imported to database
    - or at least easily transformed by existing R code for database import

## Prompt 1

You are an expert Python programmer and data engineer. You have experience with PDF parsing, data cleaning, and working with tabular data. You are familiar with libraries such as PyMuPDF, pdfplumber, pandas, and tabula-py. You also have experience with regular expressions and text processing. You are skilled at writing modular, reusable code and functions. You are detail-oriented and careful about edge cases and data quality. 

You are to help me write a Python script that processes a PDF file containing multiple tables of data. The PDF has the following characteristics:
- The PDF contains multiple pages that are a mix of text, tables, and charts. 
- All I care about are the tables.
- It covers four main category types: Beer, Refreshment Beverages, Spirits, and Wine.
- Each category type has two tables: one for net sales in dollars and one for sales in litres.
- The tables may span multiple pages, particularly for Spirts and Wine.
- Each table in the PDF starts with a header line that contains partial column names.
- the first column has no heading but it is the category for the category type.
- the category is only shown once at the start of the category section and then all subsequent rows in that section have a blank first column, so that should be filled down.
- the second column has the name of the metric - either "Net Sales $" or "Litres" but this column is the subcategory within the category.
- the remaining columns have data for the most recent five quarters with headings in the form 'Fiscal 2024/25 Q1' and so on. The column headings will need to be cleaned up so that spaces and special characters are removed, the first year number is dropped and only the second year after the slash is kept (as YYYY) and the quarter number also kept as Qx.
- summary rows are subtotals or totals that should be excluded.
- sales numbers are in comma format or currency format with dollar signs and commas and should be cleaned to be numeric only.


