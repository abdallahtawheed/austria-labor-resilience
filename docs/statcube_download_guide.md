# STATcube Download Guide

The raw data files are not committed to this repository (too large, 
reproducible from source). Follow these steps to download them.

## Source
https://statcube.at/statistik.at/ext/statcube/jsf/dataCatalogueExplorer.xhtml

## Table 1: Sector Employment by Region
- Database: Austrian Micro Census — Labour Force Survey Yearly Data
- Rows: Year, Province (NUTS 2)
- Columns: Labour Status (Employed only), ÖNACE sector 2008 (Manufacturing -Section C- + Healthcare -Section Q-)
- Format: CSV database format
- Years: 2013–2025
- Save as: data/raw/table_sectorbyregionbyyear.csv

## Table 2: Total Employment by Region  
- Same database
- Rows: Year, Province (NUTS 2)
- Columns: Labour Status (Employed only)
- Format: CSV database format
- Years: 2013–2025
- Save as: data/raw/table_totalbyregionbyyear.csv

## Table 3: Population by Age Group by Region
- Database: Austrian Micro Census — Labour Force Survey Yearly Data
- Rows: Year, Province (NUTS 2)
- Columns: Age groups (Under 15, 15-24, 25-34, 35-44, 45-54, 55-64, 65+)
- Format: CSV database format
- Years: 2013–2025
- Save as: data/raw/table_totalagebyregionbyyear.csv

## Parsing Notes
All three files have a 9-line metadata header before the actual data.
The ingestion script handles this automatically with skiprows=9.
Encoding is latin1 due to Windows-1252 characters in sector names.