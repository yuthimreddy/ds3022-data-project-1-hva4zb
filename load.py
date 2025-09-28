import duckdb
import os
import logging
import time


logging.basicConfig(
   level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
   filename='load.log'
)
logger = logging.getLogger(__name__)




# Creating 3 functions to handle each step:


# Step 1: First creating our tables and setting up our DB
# only selecting the 4 columns of interest (pickup_datetime, dropoff_datetime, passenger_count, trip_distance)
def setup_database_and_tables(con):
   logger.info("Setting up database and tables for yellow_trips, green_trips, vehicle_emissions") # log statement


   #Creating table for yellow taxi trips from 2015-2024:
   con.execute("""
       CREATE OR REPLACE TABLE yellow_trips (
           pickup_datetime TIMESTAMP,
           dropoff_datetime TIMESTAMP,
           passenger_count INTEGER,
           trip_distance DOUBLE
       );
   """) # using replace to make script idempotent so anytime script is run, it starts with new empty table


   #Creating table for green taxi trips from 2015-2024:
   con.execute("""
       CREATE OR REPLACE TABLE green_trips (
           pickup_datetime TIMESTAMP,
           dropoff_datetime TIMESTAMP,
           passenger_count INTEGER,
           trip_distance DOUBLE
       );
   """)


   # Loading in vehicle emissions data from CSV file:
   con.execute("""
       CREATE OR REPLACE TABLE vehicle_emissions AS SELECT * FROM read_csv_auto('data/vehicle_emissions.csv');
   """)
   #log statement:
   logger.info("Tables created successfully")


# Step 2: Loading in all the trip data from the last 10 years for both yellow and green taxi rides
# All the data will be programmatically pulled through a for-loop


def load_trip_data(con):
   taxi_types = ['yellow', 'green']
   years = range(2015, 2025)  # From 2015 to 2024


   for taxi in taxi_types:
       logger.info(f"Starting data load for {taxi} taxis") # log statement for which taxi type is being processed


       # Adjust column names based on taxi type (yellow uses 'tpep', green uses 'lpep')
       pickup_col = 'tpep_pickup_datetime' if taxi == 'yellow' else 'lpep_pickup_datetime'
       dropoff_col = 'tpep_dropoff_datetime' if taxi == 'yellow' else 'lpep_dropoff_datetime'


       for year in years:
           for month in range(1, 13):
               # URL for Parquet file:
               url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi}_tripdata_{year}-{month:02d}.parquet"


               # using try and except statements: error handling
               # in case any of the files are inaccessible or giving a timeout error
               try:
                   query = f"""
                       INSERT INTO {taxi}_trips (pickup_datetime, dropoff_datetime, passenger_count, trip_distance)
                       SELECT
                           "{pickup_col}",
                           "{dropoff_col}",
                           "passenger_count",
                           "trip_distance"
                       FROM read_parquet('{url}')
                   """


                   con.execute(query)
                   logger.info(f"Successfully loaded data from {url}")
                  


               except Exception as e:
                   logger.warning(f"Could not load data from {url}. Error: {e}")


               time.sleep(30)  # brief pause to avoid overwhelming the server
  
   logger.info(f"Finished data loading for {taxi} taxis")




# Step 3: Summarizing/Aggregating the data loaded into the tables:


def summarize_data(con):
   """Perform basic summarization of the trip data with ROW COUNTS"""
   logger.info("Starting data summarization")
   for table_name in ['yellow_trips', 'green_trips', 'vehicle_emissions']:
       # Getting row count:
       count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


       # outputting 'count' to log file (per rubric)
       summary = f"Table {table_name} has {count} rows."
       logger.info(summary) # log statement for summary
       print(summary)


# Now connecting to DB to run all the functions so we:
   # 1. Set up DB and tables
   # 2. Load in trip data
   # 3. Summarize the data and export count to the log file


def load_parquet_files():


   con = None


   try:
       # Connect to local DuckDB instance
       con = duckdb.connect(database='emissions.duckdb', read_only=False)
       logger.info("Connected to DuckDB instance")


       # Setting up Tables:
       setup_database_and_tables(con)


       # Looping through and pulling all the trip data:
       load_trip_data(con)


       # Summarizing/Descriptive Stats:
       summarize_data(con)


   # in case functions do not run properly, log the error:
   except Exception as e:
       print(f"An error occurred: {e}")
       logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
   load_parquet_files()
