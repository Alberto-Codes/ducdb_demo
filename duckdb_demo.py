import duckdb
import os
import urllib.request

# URLs for the Parquet files
yellow_taxi_url = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
)
green_taxi_url = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet"
)

# Filenames for the downloaded Parquet files
yellow_taxi_file = "yellow_tripdata_2024-01.parquet"
green_taxi_file = "green_tripdata_2024-01.parquet"


def download_file(url, filename):
    """Download a file from a given URL if it does not exist.

    Args:
        url (str): The URL to download the file from.
        filename (str): The name of the file to save the downloaded content.

    Returns:
        bool: True if the file was downloaded or already exists, False if there
        was an error during download.
    """
    if not os.path.exists(filename):
        try:
            urllib.request.urlretrieve(url, filename)
            print(f"Downloaded {filename}")
        except Exception as e:
            print(f"Failed to download {filename}: {e}")
            return False
    return True


# Download the Parquet files
if not download_file(yellow_taxi_url, yellow_taxi_file):
    print("Failed to download yellow taxi data. Exiting.")
elif not download_file(green_taxi_url, green_taxi_file):
    print("Failed to download green taxi data. Exiting.")
else:
    # Connect to DuckDB and create tables from the Parquet files
    con = duckdb.connect(database=":memory:")

    # Create yellow_taxi table
    con.execute(
        f"CREATE TABLE yellow_taxi AS SELECT * FROM read_parquet('{yellow_taxi_file}')"
    )

    # Create green_taxi table
    con.execute(
        f"CREATE TABLE green_taxi AS SELECT * FROM read_parquet('{green_taxi_file}')"
    )

    # List columns in both tables
    yellow_columns = con.execute("DESCRIBE yellow_taxi").fetchall()
    green_columns = con.execute("DESCRIBE green_taxi").fetchall()

    print("Columns in yellow_taxi:")
    for col in yellow_columns:
        print(col)

    print("\nColumns in green_taxi:")
    for col in green_columns:
        print(col)

    # Assuming 'tpep_pickup_datetime' and 'lpep_pickup_datetime' as common columns for the example
    yellow_common_column = "tpep_pickup_datetime"
    green_common_column = "lpep_pickup_datetime"

    if yellow_common_column in [col[0] for col in yellow_columns] and green_common_column in [col[0] for col in green_columns]:
        # Perform a join between yellow_taxi and green_taxi on common columns
        query = f"""
        SELECT 
            yellow_taxi.{yellow_common_column} AS yellow_pickup,
            green_taxi.{green_common_column} AS green_pickup,
            yellow_taxi.tpep_dropoff_datetime AS yellow_dropoff,
            green_taxi.lpep_dropoff_datetime AS green_dropoff,
            yellow_taxi.passenger_count AS yellow_passengers,
            green_taxi.passenger_count AS green_passengers
        FROM 
            yellow_taxi 
        JOIN 
            green_taxi 
        ON 
            yellow_taxi.{yellow_common_column} = green_taxi.{green_common_column}
        LIMIT 10
        """
        result = con.execute(query).fetchall()
        print(f"Join result:\n{result}")
    else:
        print("The common columns do not exist in both tables.")
