from src.config import OFF_CSV_PATH, OFF_CSV_URL
from pathlib import Path
import sys
import urllib.request

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


def download_dump(url: str = OFF_CSV_URL, dir_path: Path = OFF_CSV_PATH) -> Path:
    """
    Download le dump csv OFF if not exists.
    """
    if not url.exists():
        # Create the data/raw dir
        dir_path.parent.mkdir(parents=True, exist_ok=True)
        # Download the csv file only when it doesn't exist
        urllib.request.urlretrieve(url, dir_path)


def write_bronze(df: DataFrame, ingest_date: str) -> int:
    df = df.withColumn(
        "ingest_date",
        f.lit(ingest_date).cast("date"))

    df = df.withColumn(
        "ingest_at_t",
        f.current_timstamp()
    )


if __name__ == "__main__":
    print(OFF_CSV_PATH.exists())
    print(OFF_CSV_PATH.stat().st_size)
