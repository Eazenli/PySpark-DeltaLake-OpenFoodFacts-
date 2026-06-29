import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType

PROJECT_ROOT = Path(__file__).resolve().parents[1]
# Principal documents
DATA_DIR = PROJECT_ROOT / "data"
DATA_RAW = DATA_DIR / "raw"
DATA_SAMPLES = DATA_DIR / "samples"
# Convert into parquet format after download
PARQUET_PATH = DATA_RAW / "products.parquet"
LAKEHOUSE = DATA_DIR / "lakehouse"

# Lakehouse Layer
LH_BRONZE = LAKEHOUSE / "bronze"
LH_SILVER = LAKEHOUSE / "silver"
LH_GOLD = LAKEHOUSE / "gold"

# BRONZE_PRODUCTS = LH_BRONZE / "off_products"
# SILVER_PRODUCTS = LH_SILVER / "off_products"
# GOLD_PRODUCTS = LH_GOLD / "products"

# S3
S3_BUCKET = "s3a://openfoodfacts-pyspark-deltalake"

BRONZE_PRODUCTS = f"{S3_BUCKET}/lakehouse/bronze/off_products"
SILVER_PRODUCTS = f"{S3_BUCKET}/lakehouse/silver/off_products"
GOLD_PRODUCTS = f"{S3_BUCKET}/lakehouse/gold/off_products"

# Source Open Food Facts
OFF_CSV_URL = "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"
OFF_CSV_PATH = DATA_RAW / "products.csv.gz"

# Schema Validation in Silver Layer
SILVER_SCHEMA = StructType([
    StructField("code", StringType(), nullable=False),
    StructField("last_modified_t", LongType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
    StructField("brands", StringType(), nullable=True),
    StructField("labels_en", StringType(), nullable=True),
    StructField("countries_en", StringType(), nullable=True),
    StructField("food_groups_en", StringType(), nullable=True),
    StructField("categories_en", StringType(), nullable=True),
    StructField("nutriscore_grade", StringType(), nullable=True),
    StructField("ingredients_tags", ArrayType(StringType()), nullable=True),
    StructField("energy-kcal_100g", DoubleType(), nullable=True),
    StructField("fat_100g", DoubleType(), nullable=True),
    StructField("saturated-fat_100g", DoubleType(), nullable=True),
    StructField("carbohydrates_100g", DoubleType(), nullable=True),
    StructField("sugars_100g", DoubleType(), nullable=True),
    StructField("proteins_100g", DoubleType(), nullable=True),
    StructField("salt_100g", DoubleType(), nullable=True)
])


def create_data_dir():
    for path in [
        DATA_RAW,
        DATA_SAMPLES
        # LH_BRONZE,
        # LH_SILVER,
        # LH_GOLD,
    ]:
        path.mkdir(parents=True, exist_ok=True)


def get_spark(app_name: str = "LakeHouse_OFF") -> SparkSession:
    session = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.python.worker.faulthandler.enabled", "true")
        # remove delta stat for memory optimization tradeoff
        .config("spark.databricks.delta.stats.collect", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # download java libraries for delta lake and S3 via hadoop-aws
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.3.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.endpoint.region", os.environ["AWS_DEFAULT_REGION"])
        .getOrCreate()
    )
    return session
