# ETL Pipeline for Extracting and Transforming Banking Data

This project implements an ETL (Extract, Transform, Load) pipeline to extract data on the largest banks in the world, transform it by converting market capitalizations into multiple currencies, and load the processed data into both a CSV file and an SQLite database. It also includes logging to track the progress of the ETL process.

## Directory Structure
The following directory structure is created automatically when the script is run:

- ├─ prepared_data/
- │   └── <date>/   - Stores the output CSV file and SQLite database.
- ├── log_data/
- │   └── <date>/   - Stores logs tracking the ETL pipeline's progress.
- ├── rates_data/
- │   └── exchange_rate.csv - Stores exchange rates for currency conversion.

- prepared_data/: Contains the output files, including the CSV file and SQLite database (Banks.db).
- log_data/: Contains log files that track the progress of the pipeline, including any errors or successes.
- rates_data/: Contains the CSV file exchange_rate.csv that holds currency exchange rates for EUR, GBP, and INR.

## Code Overview

The script performs the following operations:

1. Extract: Scrapes banking data (market capitalization) from a Wikipedia page.
2. Transform: Converts the market capitalization from USD to EUR, GBP, and INR using exchange rates from a CSV file.
3. Load: Saves the transformed data to:
- A CSV file (Largest_banks_data.csv).
- An SQLite database (Banks.db).

## Extract Function

The extract(url) function scrapes the banking data from the provided URL and stores the extracted data into a Pandas DataFrame.

## Transform Function

The transform(dataframe, csv_path) function takes the extracted DataFrame and transforms it by adding columns for market capitalization in EUR, GBP, and INR. These conversions are based on exchange rates stored in the CSV file.

## Load Functions

- load_to_csv(output_df): Saves the transformed data to a CSV file.
- load_to_db(output_df, sql_connection, TABLE_NAME): Loads the transformed data into an SQLite database under the Largest_banks table.
- run_query(query_statement, sql_connection): Runs queries on the database to validate the loaded data.

## How to Run the Code
### Prerequisites

Ensure the following Python libraries are installed:

- pip install requests beautifulsoup4 pandas sqlite3

## Running the Code

1. Clone the repository to your local machine.
2. Place the exchange rates file in the rates_data/ directory as exchange_rate.csv. This file should contain at least the following structure:

Currency,Rate
EUR,0.85
GBP,0.76
INR,74.38

**To run the python script:** python banks_project.py

This will execute the following steps:

1. Data will be scraped from the given URL.
2. The data will be transformed using the exchange rates.
3. The transformed data will be saved to both a CSV file and a SQLite database.
4. Log files will be generated to track the progress.

## Log Files

Log files are saved under the log_data/ directory with the current date as the folder name. These logs provide detailed information about the pipeline's progress, errors, and key events.

## Querying the Database

After loading the data into the SQLite database, some test queries are executed and the results are printed to the console. You can customize the queries within the script by modifying the queries list.
