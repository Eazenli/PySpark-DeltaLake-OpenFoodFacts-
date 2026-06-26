from src.config import OFF_CSV_PATH, OFF_CSV_URL, BRONZE_PRODUCTS, PARQUET_PATH
from delta.tables import DeltaTable
from pathlib import Path
from pyspark.sql import functions as f

import urllib.request

COLS_TO_READ = [
    "code", "last_modified_t", "product_name", "brands",
    "categories_en", "labels_en", "countries_en", "nutriscore_grade",
    "ingredients_tags", "food_groups_en", "energy-kcal_100g",
    "fat_100g", "saturated-fat_100g", "carbohydrates_100g",
    "sugars_100g", "proteins_100g", "salt_100g"
]


def parquet_needs_rebuild() -> bool:
    """
    Check if CSV and parquet files exist or not:
    parquet doesn't exist -> yes, need to create it 
    CSV doesn't exist -> no (nothing to compare against)
    if both exist:
        CSV modified AFTER parquet -> need to rebuild casue it's outdated
        CSV modified BEFORE parquet -> parquet is up to date 
    """
    parquet_exists = PARQUET_PATH.exists()
    csv_exists = OFF_CSV_PATH.exists()

    if not parquet_exists:
        return True
    if not csv_exists:
        return False

    csv_last_modified = OFF_CSV_PATH.stat().st_mtime
    parquet_last_modified = PARQUET_PATH.stat().st_mtime

    return csv_last_modified > parquet_last_modified  # CSV is newer -> rebuild


def download_dump(url: str = OFF_CSV_URL, dir_path: Path = OFF_CSV_PATH, force=False) -> None:
    """
    When force=True and CSV exists, 
    delete the old CSV path and re-download the new one, compare with parquet's timestamp to rebuild parquet file |
    Download le dump csv OFF if not exists.
    """
    if force and dir_path.exists():
        dir_path.unlink()

    if not dir_path.exists():
        # Create a .tmp file to avoid download a corrupt .csv.gz file in data/raw
        dir_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = dir_path.with_suffix(".tmp")
        try:
            urllib.request.urlretrieve(url, tmp_path)
            tmp_path.rename(dir_path)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise


def read_csv(spark):
    if not parquet_needs_rebuild():
        return spark.read.parquet(str(PARQUET_PATH))

    df_raw = spark.read.csv(
        str(OFF_CSV_PATH),
        sep="\t",
        header=True,
        multiLine=True,
        quote='"',
        escape='"',
        encoding="utf-8"
    )
    df_raw.select(COLS_TO_READ).write.mode("overwrite").parquet(
        str(PARQUET_PATH), compression="snappy")
    return spark.read.parquet(str(PARQUET_PATH))


def write_bronze(spark, df, bronze_path=BRONZE_PRODUCTS) -> None:
    # Get timestamp for the ingestion
    df_stamped = df.withColumn("ingest_at_t", f.current_timestamp())

    # For incremental loads, compare existing lastest last_midified_t as "watermark" with the new run,
    # append only rows where last_modified_t > watermark

    # Check if it exists a Delta Table, False write everything, True, do incremental
    if DeltaTable.isDeltaTable(spark, str(bronze_path)):
        watermark = (
            spark.read.format("delta").load(str(bronze_path))
            .agg(f.max(f.col("last_modified_t").cast("long")))
            .collect()[0][0]
        )
        if watermark is None:
            df_new = df_stamped
        else:
            # Filter the new ingested data according to the watermark
            df_new = df_stamped.filter(
                f.col("last_modified_t").cast("long") > watermark)
        df_new.write.format("delta").mode("append").save(str(bronze_path))
    else:
        df_stamped.write.format("delta").mode(
            "overwrite").save(str(bronze_path))
