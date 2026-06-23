from src.config import OFF_CSV_PATH, OFF_CSV_URL, BRONZE_PRODUCTS
from delta.tables import DeltaTable
from pathlib import Path
from pyspark.sql import functions as f

import sys
import urllib.request


def download_dump(url: str = OFF_CSV_URL, dir_path: Path = OFF_CSV_PATH) -> Path:
    """
    Download le dump csv OFF if not exists.
    """
    if not dir_path.exists():
        # Create the data/raw dir
        dir_path.parent.mkdir(parents=True, exist_ok=True)
        # Download the csv file only when it doesn't exist
        urllib.request.urlretrieve(url, dir_path)


def read_csv(spark):
    return spark.read.csv(
        str(OFF_CSV_PATH),
        sep="\t",
        header=True,
        multiLine=True,
        quote='"',
        escape='"',
        encoding="utf-8"
    )


def write_bronze(spark, df) -> int:
    # Get timestamp for the ingestion
    df_stamped = df.withColumn("ingest_at_t", f.current_timestamp())

    # For incremental loads, compare existing lastest last_midified_t as "watermark" with the new run,
    # append only rows where last_modified_t > watermark

    # Check if it exists a Delta Table, False write everything, True, do incremental
    if DeltaTable.isDeltaTable(spark, str(BRONZE_PRODUCTS)):
        watermark = (
            spark.read.format("delta").load(str(BRONZE_PRODUCTS))
            .agg(f.max(f.col("last_modified_t").cast("long")))
            .collect()[0][0]
        )
        # Filter the new ingested data according to the watermark
        df_new = df_stamped.filter(
            f.col("last_modified_t").cast("long") > watermark)
        df_new.write.format("delta").mode("append").save(str(BRONZE_PRODUCTS))
    else:
        df_stamped.write.format("delta").mode(
            "overwrite").save(str(BRONZE_PRODUCTS))
