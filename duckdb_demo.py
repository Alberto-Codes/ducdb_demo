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

# Directory for the downloaded Parquet files
data_dir = "parquet_data"
db_file = "taxi_data.duckdb"


def download_file(url, directory):
    """Download a file from a given URL into a specified directory.

    Args:
        url (str): The URL to download the file from.
        directory (str): The directory to save the downloaded file.

    Returns:
        bool: True if the file was downloaded or already exists, False if there
        was an error during download.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    filename = os.path.join(directory, url.split("/")[-1])
    if not os.path.exists(filename):
        try:
            urllib.request.urlretrieve(url, filename)
            print(f"Downloaded {filename}")
        except Exception as e:
            print(f"Failed to download {filename}: {e}")
            return False
    return True


# Download the Parquet files
if not download_file(yellow_taxi_url, data_dir):
    print("Failed to download yellow taxi data. Exiting.")
elif not download_file(green_taxi_url, data_dir):
    print("Failed to download green taxi data. Exiting.")
else:
    # Remove existing database file if it exists
    if os.path.exists(db_file):
        os.remove(db_file)
        print(f"Deleted existing database file: {db_file}")

    # Connect to DuckDB and create a database on disk
    con = duckdb.connect(database=db_file)

    # Perform a join between yellow_taxi and green_taxi on common columns
    query = f"""
    SELECT 
        yellow.tpep_pickup_datetime AS yellow_pickup,
        green.lpep_pickup_datetime AS green_pickup,
        yellow.tpep_dropoff_datetime AS yellow_dropoff,
        green.lpep_dropoff_datetime AS green_dropoff,
        yellow.passenger_count AS yellow_passengers,
        green.passenger_count AS green_passengers
    FROM 
        read_parquet('{os.path.join(data_dir, 'yellow_tripdata_2024-01.parquet')}') AS yellow
    JOIN 
        read_parquet('{os.path.join(data_dir, 'green_tripdata_2024-01.parquet')}') AS green
    ON 
        yellow.tpep_pickup_datetime = green.lpep_pickup_datetime
    LIMIT 10
    """
    result = con.execute(query).fetchall()
    print(f"Join result:\n{result}")

    # Close the connection
    con.close()
    print(f"Database saved to {db_file}")
