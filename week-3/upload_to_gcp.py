import os
import dlt
import requests
import pandas as pd
from io import BytesIO
from dlt.destinations import filesystem

# --------------------------------------------------
# CONFIG
# --------------------------------------------------

# Only needed if using filesystem (GCS)
os.environ["BUCKET_URL"] = "gs://de-zoomcamp-2026-aa"

# If using BigQuery via service account:
# export GOOGLE_APPLICATION_CREDENTIALS="path/to/your-key.json"


# --------------------------------------------------
# SOURCE DEFINITION (multiple parquet files → multiple tables)
# --------------------------------------------------

@dlt.source(name="rides_source")
def download_parquet_files():
    prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata"

    for month in range(1, 7):
        file_name = f"yellow_tripdata_2024-0{month}"
        url = f"{prefix}_2024-0{month}.parquet"

        print(f"Downloading {url}")
        response = requests.get(url)
        response.raise_for_status()

        df = pd.read_parquet(BytesIO(response.content))

        yield dlt.resource(df, name=file_name)


# --------------------------------------------------
# PIPELINE 1 — FILESYSTEM (GCS)
# --------------------------------------------------

def run_filesystem_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="rides_pipeline_fs",
        destination=filesystem(layout="{schema_name}/{table_name}.{ext}"),
        dataset_name="rides_dataset",
    )

    load_info = pipeline.run(
        download_parquet_files(),
        loader_file_format="parquet"
    )

    print(load_info)


# --------------------------------------------------
# PIPELINE 2 — DATABASE (DuckDB for local testing)
# --------------------------------------------------

@dlt.resource(name="rides", write_disposition="replace")
def download_parquet_table():
    prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata"

    for month in range(1, 7):
        url = f"{prefix}_2024-0{month}.parquet"

        print(f"Downloading {url}")
        response = requests.get(url)
        response.raise_for_status()

        df = pd.read_parquet(BytesIO(response.content))

        yield df


def run_bigquery_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="rides_pipeline_db",
        destination="bigquery",  # change to "bigquery" for production
        dataset_name="rides_dataset",
    )

    info = pipeline.run(download_parquet_table())
    print(info)

    return pipeline


# --------------------------------------------------
# QUERY DUCKDB
# --------------------------------------------------

#def query_bigquery(pipeline):
  #  import duckdb

  #  conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")
  #  conn.sql(f"SET search_path = '{pipeline.dataset_name}'")
#
   # print("\nTables in dataset:")
   # print(conn.sql("SHOW TABLES").df())

   # print("\nRow count:")
  #  print(conn.sql("SELECT count(1) FROM rides").df())


# --------------------------------------------------
# MAIN
# --------------------------------------------------

if __name__ == "__main__":

    # Choose ONE of these:

    # 1️⃣ Load to GCS filesystem
    run_filesystem_pipeline()

    # 2️⃣ Load to DuckDB (local testing)
   # pipeline = run_bigquery_pipeline()
    #query_bigquery(pipeline)