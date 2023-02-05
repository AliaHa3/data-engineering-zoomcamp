from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(log_prints=True,retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    df = pd.read_parquet(Path(f"../data/{gcs_path}"))
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df



@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-cred")

    df.to_gbq(
        destination_table="trips_data_all.yellow_taxi_trips",
        project_id="dezoomcamp-375819",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(months,year,color):
    """Main ETL flow to load data into Big Query"""

    for month in months:
        df = extract_from_gcs(color, year, month)
        write_bq(df)


if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2019
    etl_gcs_to_bq(months,year,color)
