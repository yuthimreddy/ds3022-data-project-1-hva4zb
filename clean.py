import duckdb
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='clean.log')

# Defining cleaning function:
def clean_parquet():
    
    con = None

    try: 
        # connecting to local DuckDB
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logging.info("Connected to DuckDB successfully")

        # CLEANING both yellow and green tables:
        tables = ['yellow_trips', 'green_trips']

        for table in tables:
            logging.info(f"Starting cleaning for {table} table")

            # 1) DUPLICATE REMOVAL - Selecting all the distinct records and 
            # replacing the original table:
            con.execute(f"""
                CREATE OR REPLACE TABLE {table}
                AS SELECT DISTINCT * FROM {table};
            """)

            # LOG MESSAGE: Confirmation that duplicates have been removed for each table:
            logging.info(f"Duplicates removed for {table} table")

            # 2) REMOVING 0 - removing trips with 0 passengers
            con.execute(f"""
                DELETE FROM {table} WHERE passenger_count = 0;                 
            """)

            logging.info(f"Removed trips with 0 passengers from {table} table")

            # 3) REMOVING TRIPS WITH 0 DISTANCE (and any negative distances):
            con.execute(f"""
                DELETE FROM {table} WHERE trip_distance <= 0;
            """)

            logging.info(f"Removed trips with 0 or negative distance from {table} table")

            # 4) REMOVING TRIPS WITH TRIP_DISTANCE over 100 MILES:
            con.execute(f"""
                DELETE FROM {table} WHERE trip_distance > 100;
            """)

            logging.info(f"Removed trips with distance over 100 miles from {table} table")

            # 5) REMOVING TRIPS LASTING MORE THAN 1 DAY (86400s):
            con.execute(f"""
                DELETE FROM {table} WHERE (julian(dropoff_datetime) - julian(pickup_datetime)) * 86400 > 86400;
            """)

            logging.info(f"Removed trips lasting more than 1 day from {table} table")  

            # VERIFICATION TESTS - Ensuring cleaning steps were executed: 
            logging.info(" -- Verification Tests -- ")

            zero_passenger = con.execute(f"SELECT COUNT(*) FROM {table} WHERE passenger_count = 0;").fetchone()[0]
            print(f"For {table}, trips with 0 passengers: {zero_passenger}")
            logging.info(f"For {table}, trips with 0 passengers: {zero_passenger}")

            zero_distance = con.execute(f"SELECT COUNT(*) FROM {table} WHERE trip_distance <= 0;").fetchone()[0]
            print(f"For {table}, trips with 0 or negative distance: {zero_distance}")
            logging.info(f"For {table}, trips with 0 or negative distance: {zero_distance}")

            long_distance = con.execute(f"SELECT COUNT (*) FROM {table} WHERE trip_distance > 100;").fetchone()[0]
            print(f"For {table}, trips with distance over 100 miles: {long_distance}")
            logging.info(f"For {table}, trips with distance over 100 miles: {long_distance}")

            long_duration = con.execute(f"SELECT COUNT(*) FROM {table} WHERE (julian(dropoff_datetime) - julian(pickup_datetime)) * 86400 > 86400;").fetchone()[0]
            print(f"For {table}, trips lasting more than 1 day: {long_duration}")
            logging.info(f"For {table}, trips lasting more than 1 day: {long_duration}")


            logging.info(f" VERIFICATION TESTS COMPLETED for {table} table -- Cleaning COMPLETED --")

    except Exception as e:
        print(f"An error occurred: {e}")
        logging.error(f"An error occurred: {e}")
    finally:
        if con:
            con.close()
            logging.info("DuckDB connection closed")

if __name__ == "__main__":
    clean_parquet()
