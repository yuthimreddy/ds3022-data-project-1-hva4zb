import duckdb
import logging
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log')

logger = logging.getLogger(__name__)

def analysis():

    con = None 
   
    try:
        # Connect to DuckDB and only providing read access since we are just querying results
        con = duckdb.connect(database='emissions.duckdb', read_only=True)
        logger.info("Starting analysis of taxi trips and vehicle emissions data")

        # 1) Finding largest CO2 Trip:
        for taxi_type in ['yellow', 'green']:
            logger.info(f"Analyzing {taxi_type} taxi data")

            result = con.execute(f"""
                SELECT pickup_datetime, trip_co2_kgs FROM transform_trips WHERE taxi_type = '{taxi_type}'
                ORDER BY trip_co2_kgs 
                DESC LIMIT 1;
                """).fetchall()
            print(f"The largest CO2 trip: {result}")
            logger.info(f"Largest CO2 trip for {taxi_type} taxi: {result}")

            # 2) Average most carbon heavy and carbon light hours of the day (for both yellow and green):
            heavy_hour = con.execute(f"""
                SELECT trip_hour FROM transform_trips WHERE taxi_type = '{taxi_type}'
                GROUP BY 1 ORDER BY AVG(trip_co2_kgs) DESC LIMIT 1;""").fetchone()[0]
            print(f"Carbon heavy hour for {taxi_type} taxis: {heavy_hour}")
            logger.info(f"Carbon heavy hour for {taxi_type} taxis: {heavy_hour}")

            least_heavy_hour = con.execute(f"""
                SELECT trip_hour FROM transform_trips WHERE taxi_type = '{taxi_type}' 
                GROUP BY 1 ORDER BY AVG(trip_co2_kgs) ASC LIMIT 1;""").fetchone()[0]
            print(f"Carbon light hour for {taxi_type} taxis: {least_heavy_hour}")
            logger.info(f"Carbon light hour for {taxi_type} taxis: {least_heavy_hour}")

            #3) Most and least carbon-heavy day of the week:
            heavy_day = con.execute(f"""
                SELECT trip_day_of_week FROM transform_trips WHERE taxi_type = '{taxi_type}'
                GROUP BY 1 ORDER BY AVG(trip_co2_kgs) DESC LIMIT 1;""").fetchone()[0]
            print(f"Carbon heavy day for {taxi_type} taxis: {heavy_day}")
            logger.info(f"Carbon heavy day for {taxi_type} taxis: {heavy_day}")

            least_heavy_day = con.execute(f"""
                SELECT trip_day_of_week FROM transform_trips WHERE taxi_type = '{taxi_type}'
                GROUP BY 1 ORDER BY AVG(trip_co2_kgs) ASC LIMIT 1;""").fetchone()[0]
            print(f"Carbon light day for {taxi_type} taxis: {least_heavy_day}")
            logger.info(f"Carbon light day for {taxi_type} taxis: {least_heavy_day}")

            #4) Most and least carbon-heavy week of the year: 

            heavy_week = con.execute(f"""
                SELECT trip_week_number FROM transform_trips WHERE taxi_type = '{taxi_type}'
                GROUP BY 1 ORDER BY AVG(trip_co2_kgs) DESC LIMIT 1;""").fetchone()[0]
            print(f"Carbon heavy week for {taxi_type} taxis: {heavy_week}")
            logger.info(f"Carbon heavy week for {taxi_type} taxis: {heavy_week}")

            least_week = con.execute(f"""
                SELECT trip_week_number FROM transform_trips WHERE taxi_type = '{taxi_type}'
                GROUP BY 1 ORDER BY AVG(trip_co2_kgs) ASC LIMIT 1;""").fetchone()[0]
            print(f"Carbon light week for {taxi_type} taxis: {least_week}")
            logger.info(f"Carbon light week for {taxi_type} taxis: {least_week}")   

            #5) most/least carbon-heavy month of the year:

            heavy_month = con.execute(f"""
                SELECT trip_month FROM transform_trips WHERE taxi_type = '{taxi_type}'
                GROUP BY 1 ORDER BY AVG(trip_co2_kgs) DESC LIMIT 1;""").fetchone()[0]
            print(f"Carbon heavy month for {taxi_type} taxis: {heavy_month}")
            logger.info(f"Carbon heavy month for {taxi_type} taxis: {heavy_month}")

            least_month = con.execute(f"""
                SELECT trip_month FROM transform_trips WHERE taxi_type = '{taxi_type}'
                GROUP BY 1 ORDER BY AVG(trip_co2_kgs) ASC LIMIT 1;""").fetchone()[0]
            print(f"Carbon light month for {taxi_type} taxis: {least_month}")
            logger.info(f"Carbon light month for {taxi_type} taxis: {least_month}")


    #6) -- PLOTTING HISTOGRAMS OF MONTHLY CO2 EMISSIONS FOR BOTH YELLOW AND GREEN TAXIS FROM 2015-2024

    # a) Querying data for the plot: 
        plot_df = con.execute(f"""
            SELECT
                EXTRACT(year FROM pickup_datetime) AS year,
                trip_month,
                taxi_type,
                SUM(trip_co2_kgs) AS total_co2
                    FROM transform_trips
                        WHERE EXTRACT(year FROM pickup_datetime) >= 2015
                              GROUP BY ALL ORDER BY ALL;
            """).fetchdf()
            
        # b) Plotting dates (trip_month on x and co2 emissions on y-axis)
        plot_df['date'] = pd.to_datetime(plot_df['year'].astype(str) + '-' + plot_df['trip_month'].astype(str))
        
        plt.style.use('seaborn-v0_8-whitegrid')
        fig, ax = plt.subplots(figsize=(15, 8))
        # making 2 subplots:
        fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(15, 12), sharex=True)
        fig.suptitle('Total Monthly CO2 Emissions by Taxi Type (2015-2024)', fontsize=16, y=0.95)
        

        # plotting taxis with their respective colors:
        colors = {'yellow':'#FFBF00', 'green':'#00693E'}


        # making loop so we plot for both yellow and green taxis:
        for i, taxi in enumerate(['yellow', 'green']):
            ax = axes[i]
            subset = plot_df[plot_df['taxi_type'] == taxi]
            ax.plot(subset['date'], subset['total_co2'], label=f'{taxi.title()} Taxis', marker='o', color=colors[taxi])
        
            ax.set_title(f'{taxi.title()} Taxis', fontsize=14)
            ax.set_xlabel('Year')
            ax.set_ylabel('Total CO2 Emissions (Kilograms)')
            ax.legend()
            ax.grid(True, which='both', linestyle='--', linewidth=0.5)

        axes[1].set_xlabel('Year', fontsize=12)
        axes[1].xaxis.set_major_locator(mdates.YearLocator())
        axes[1].xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        plt.xlim([pd.to_datetime('2015-01-01'), pd.to_datetime('2024-12-31')])

    # Saving plot as png file in a new directory called output:
        os.makedirs('output', exist_ok=True)
        plot_filename = 'output/monthly_co2_emissions_2015_2024.png'
        plt.savefig(plot_filename)
        
        # Confirmation Message:
        print(f"\nPlot successfully saved to {plot_filename}")
        logger.info(f"Plot successfully saved to {plot_filename}")

        
    except Exception as e:
        print(f"Error during analysis: {e}")
        logger.error(f"Error during analysis: {e}") 
    finally:
        if con:
            con.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    analysis()     
        