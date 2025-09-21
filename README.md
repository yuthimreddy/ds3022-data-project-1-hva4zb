# DS3022 - Data Project 1 (Fall 2025)

## Assignment

This project will draw upon basic data engineering and data science skills. Using the freely available
NYC Trip Record data you will calculate CO2 output for rides within 2024 and perform some basic statistical
analysis based on transformations you add to these data.

Use the structure of this repository to submit your work. Complete the Python scripts as indicated below,
and add DBT files within that subfolder.

Begin by forking this repository into your own account within GitHub. You will be unable to push
changes back to this source:

[**FORK**](https://github.com/uvasds-systems/ds3022-data-project-1/fork)

## Data

The data for this assignment are avilable from the NYC Taxi Comission Trip Record Data page:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

You will be working with ALL data for 2024 for both YELLOW and GREEN taxis. Each month is available
as a Parquet file.

There is also a small `data/vehicle_emissions.csv` file that can serve as a reference when calculating
CO2 output based on distance (in miles) and `co2_grams_per_mile`.

Assemble all trip data (Yellow and Green) into a single DuckDB table in a local DuckDB database.

Complete the `load.py` script to create a local, persistent DuckDB database that loads three tables:

1. A full table of YELLOW taxi trips for all of 2024.
2. A full table of GREEN taxi trips for all of 2024.
3. A lookup table of `vehicle_emissions` based on the included CSV file above.

## Clean

Trips should be cleaned and checked for the following conditions (whether or not they exist):

1. Remove any duplicate trips.
2. Remove any trips with `0` passengers.
3. Remove any trips shorter than 0.1 mile in length.
4. Remove any trips longer than 100 miles in length.
5. Remove any trips lasting more than 1 day in length (86400 seconds).

Complete the `clean.py` script to perform these steps and to check/verify that these conditions no longer exist in 
either the YELLOW or GREEN trip tables in your DuckDB database.

## Transform

After cleaning you should have 2 cleaned trip tables representing YELLOW and GREEN trips for all of 2024. Perform the following
transformations to the data:

1. Calculate total CO2 output per trip based on the `co2_grams_per_mile` value in the `vehicle_emissions.csv` file,
divide by 1000 (to calculate Kg), and insert that value as a new column named `trip_co2_kgs`. This calculation should
be based upon a lookup of the vehicle_emissions table and not hard-coded as a numberic figure.
2. Extract the HOUR of the day of the `pickup_time` and insert it as a new column `hour_of_day`.
3. Extract the DAY OF WEEK from the pickup time and insert it as a new column `day_of_week`.
4. Extract the WEEK NUMBER from the pickup time and insert it as a new column `week_of_year`.
5. Extract the MONTH from the pickup time and insert it as a new column `month_of_year`.

Complete the `transform.py` script to perform these steps using python-based DuckDB commands.

For an additional 6 points, perform these steps using models in DBT. Save these files to `dbt/models/`.

## Analyze

Complete the `analysis.py` script to report the following calculations using DuckDB/SQL:

1. What was the single largest carbon producing trip of the year for YELLOW and GREEN trips? (One result for each type)
2. Across the entire year, what on average is the most carbon heavy hour of the day for YELLOW and for GREEN trips? (1-24)
3. Across the entire year, what on average is the most carbon heavy day of the week for YELLOW and for GREEN trips? (Sun-Sat)
4. Across the entire year, what on average is the most carbon heavy week of the year for YELLOW and for GREEN trips? (1-52)
5. Across the entire year, what on average is the most carbon heavy month of the year for YELLOW and for GREEN trips? (Jan-Dec)
6. Using a plotting library of your choice (matplotlib, seaborn, etc.) generate a time-series or histogram plot with MONTH
on the X-axis and CO2 total output on the Y-axis. Render two lines/bars, one each for YELLOW and GREEN taxi trips.

Your script should output each calculation WITH a label explaining the value. The plot should be output as a PNG/JPG/GIF image 
committed within your project.

## Rubric

You will be graded according to the rubric distributed with this assignment. Partial credit is given, and you may
choose to complete only some of the requirements.
