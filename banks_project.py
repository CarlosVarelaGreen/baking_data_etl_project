"""
ETL Pipeline for the extraction of banking data from the given
URL, further transformation to add more currencies, and loading
into a sqlite db as well as a csv file.
"""
# importing necessary libraries:
import os
from datetime import datetime
import sqlite3
import requests
from bs4 import BeautifulSoup
import pandas as pd

# Declaring global variables and paths:
target_directory = os.path.join('prepared_data', datetime.now().strftime('%d-%m-%y'))
target_file = target_directory + '/Largest_banks_data.csv'
log_directory = os.path.join('log_data', datetime.now().strftime('%d-%m-%y'))
log_file = log_directory + '/code_log.txt'
db_name = target_directory + '/Banks.db'
TABLE_NAME = 'Largest_banks'
DATA_URL = ("https://web.archive.org/web/20230908091635/"
            "https://en.wikipedia.org/wiki/List_of_largest_banks")

# Creating data directories:
if not os.path.exists(target_directory):
    os.makedirs(target_directory)

if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# defining a logging function to track progress and code activity:
def log_progress(message):
    """
    This function creates a log file to track de progress of the
    ETL pipeline for control and debugging purposes.
    """
    timestamp_format = '%Y-%h-%d-%H:%M'#:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a', encoding='utf-8') as file:
        file.write(timestamp + ': ' + message + '\n')

log_progress('Preliminaries complete. Initiating ETL process')
RATES_CSV_PATH = 'rates_data/exchange_rate.csv'

# Write a function to extract the tabular information from the given URL:
def extract(url):
    """
    Web scraping function to extract raw data from source using the given url.
    The function loops over the table and returns a dataframe ready for 
    transformation.
    """
    # requesting data from url:
    try:
        log_progress('Requesting data from source')
        html_content = requests.get(url, timeout=5).text
        raw_data = BeautifulSoup(html_content, 'html.parser')

        table_data = raw_data.find('table', {'class':'wikitable'})
        # report missing table:
        if not table_data:
            log_progress('Failed to find table with given class')
            return 'Failed to find table with given class'

        # Extracting the rows from the table:
        log_progress('Extracting data from source table')
        market_cap_data = table_data.find('tbody').find_all('tr')

        # Define target df to store records:
        market_cap_df = pd.DataFrame(columns = ['Name', 'MC_USD_Billion'])

        # Storing records:
        for row in market_cap_data:
            attributes = row.find_all('td')
            if len(attributes)>=0:
                # Attempt to extract records:
                try:
                    record = {
                        'Name': attributes[1].get_text(strip=True),
                        'MC_USD_Billion': attributes[2].get_text(strip=True)
                    }
                    record_df = pd.DataFrame([record])
                    market_cap_df = pd.concat([market_cap_df, record_df], ignore_index=True)
                # report any issues:
                except Exception as row_error:
                    log_progress(f"Ignoring row: {row_error}")
            else:
                log_progress(f"Skipping row with insufficient data: {attributes}")

    except requests.exceptions.Timeout:
        log_progress('Request timed out')
        return 'Request timed out'
    
    # Report url request errors if any:
    except Exception as error:
        log_progress(f'Failed to scrape data: {error} error catched')
        return f'Failed to scrape data: {error} error catched'

    return market_cap_df

# Adding columns for Market cap in GBP, EUR, and INR, rounded to 2 decimals:
def transform(dataframe, csv_path):
    """
    Transforms the given DataFrame by converting market capitalization values in USD 
    to EUR, GBP, and INR using exchange rates from a CSV file.
    """
    log_progress('Transforming extracted data')
    # converting values to float type:
    dataframe['MC_USD_Billion'] = dataframe['MC_USD_Billion'].astype(float)
    # opening rates file as dict for quick access:
    rates_data = pd.read_csv(csv_path).set_index('Currency')['Rate'].to_dict()
    # Vectorizing rate transformation opperations for better performance:
    dataframe['MC_EUR_Billion'] = (dataframe['MC_USD_Billion'] * rates_data['EUR']).round(2)
    dataframe['MC_GBP_Billion'] = (dataframe['MC_USD_Billion'] * rates_data['GBP']).round(2)
    dataframe['MC_INR_Billion'] = (dataframe['MC_USD_Billion'] * rates_data['INR']).round(2)

    log_progress('Successfully transfored extracted data')

    return dataframe

# Load the transformed data frame to an output CSV file:
def load_to_csv(output_df):
    """
    Save the given DataFrame to a CSV file and log the progress.

    Args:
        output_df (DataFrame): The transformed data to be saved to a CSV file.
    """
    output_df.to_csv(target_file, index = False)
    log_progress('Data saved to CSV file')

# Load the transformed data frame to an SQL database server as a table:
def load_to_db(output_df, sql_connection, TABLE_NAME):
    """
    Saves the output of the transform function to a sqlite database under
    the table name specified in the global variables.
    """
    output_df.to_sql(TABLE_NAME,
                     sql_connection,
                     if_exists = 'replace',
                     index=False)
    log_progress(f'Loaded transformed data to database at {db_name} directory')

# Write a function to run queries on the database table:
def run_query(query_statement, sql_connection):
    query_df = pd.read_sql_query(query_statement, sql_connection)
    print('Query: ', query_statement)
    print()
    print(query_df)

# Extracting data:
extracted_data = extract(DATA_URL)
log_progress('Data extraction complete. Initiating Transformation process')

# Transforming data:
transformed_data = transform(extracted_data, RATES_CSV_PATH)
log_progress('Data transformation complete. Initiating Loading process')

# Loading data as csv:
load_to_csv(transformed_data)

# Create the connection to SQLite3:
connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')

# Call the function and pass the connection, table name, and data frame:
load_to_db(transformed_data, connection, TABLE_NAME)
log_progress('Data loaded to Database as a table, Executing queries')

# Defining list of testing queries:
queries = [""" SELECT * FROM Largest_banks """,
           """ SELECT AVG(MC_GBP_Billion) FROM Largest_banks """,
           """ SELECT Name FROM Largest_banks LIMIT 5 """]

# Call the function and run queries:
for query in queries:
    run_query(query, connection)

log_progress('Process Complete')

# Close the connection after all operations are done:
connection.close()
log_progress('Server Connection closed')