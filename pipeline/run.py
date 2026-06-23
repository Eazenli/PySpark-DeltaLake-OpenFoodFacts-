from src.config import get_spark, create_data_dir, BRONZE_PRODUCTS
from pipeline.ingestion import download_dump, read_csv, write_bronze
from pipeline.silver import SilverTransformer, write_silver


def run():
    create_data_dir()
    spark = get_spark()

    # Bronze Layer
    download_dump()
    df_raw = read_csv(spark)
    write_bronze(spark, df_raw)

    # Silver Layer
    df_bronze = spark.read.format("delta").load(str(BRONZE_PRODUCTS))
    df_silver = SilverTransformer(spark).transform_in_order(df_bronze)
    write_silver(spark, df_silver)


if __name__ == "__main__":
    run()
