import os
import urllib.request
from pathlib import Path
from typing import Dict, Tuple

import duckdb

# Constants
TAXI_URLS: Dict[str, str] = {
    "yellow": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
    "green": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet",
}
DATA_DIR: Path = Path("parquet_data")
DB_FILE: Path = Path("taxi_data.duckdb")


def download_file(url: str, directory: Path) -> bool:
    """Download a file from a given URL into a specified directory.

    Args:
        url (str): The URL to download the file from.
        directory (Path): The directory to save the downloaded file.

    Returns:
        bool: True if the file was downloaded or already exists, False if there
        was an error during download.
    """
    directory.mkdir(parents=True, exist_ok=True)
    filename = directory / url.split("/")[-1]
    if not filename.exists():
        try:
            urllib.request.urlretrieve(url, filename)
            print(f"Downloaded {filename}")
        except Exception as e:
            print(f"Failed to download {filename}: {e}")
            return False
    return True


def download_taxi_data() -> Tuple[bool, bool]:
    """Download yellow and green taxi data.

    Returns:
        Tuple[bool, bool]: A tuple containing two boolean values indicating the
        success of the yellow and green taxi data downloads, respectively.
    """
    return (
        download_file(TAXI_URLS["yellow"], DATA_DIR),
        download_file(TAXI_URLS["green"], DATA_DIR),
    )


def create_database():
    """Create and populate the DuckDB database.

    This function removes any existing database file, creates a new DuckDB
    database, performs a join between yellow and green taxi data, and saves
    the results in the database file.
    """
    if DB_FILE.exists():
        DB_FILE.unlink()
        print(f"Deleted existing database file: {DB_FILE}")

    with duckdb.connect(str(DB_FILE)) as con:
        query = f"""
        SELECT 
            yellow.tpep_pickup_datetime AS yellow_pickup,
            green.lpep_pickup_datetime AS green_pickup,
            yellow.tpep_dropoff_datetime AS yellow_dropoff,
            green.lpep_dropoff_datetime AS green_dropoff,
            yellow.passenger_count AS yellow_passengers,
            green.passenger_count AS green_passengers
        FROM 
            read_parquet('{DATA_DIR / "yellow_tripdata_2024-01.parquet"}') AS yellow
        JOIN 
            read_parquet('{DATA_DIR / "green_tripdata_2024-01.parquet"}') AS green
        ON 
            yellow.tpep_pickup_datetime = green.lpep_pickup_datetime
        LIMIT 10
        """
        result = con.execute(query).fetchall()
        print(f"Join result:\n{result}")

    print(f"Database saved to {DB_FILE}")


def main():
    """Main function to orchestrate the download and database creation."""
    yellow_success, green_success = download_taxi_data()
    if not yellow_success:
        print("Failed to download yellow taxi data. Exiting.")
    elif not green_success:
        print("Failed to download green taxi data. Exiting.")
    else:
        create_database()


if __name__ == "__main__":
    main()
