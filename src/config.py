from pathlib import Path
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[1]
# Dossiers principaux
DATA_DIR = PROJECT_ROOT / "data"
DATA_RAW = DATA_DIR / "raw"
DATA_SAMPLES = DATA_DIR / "samples"
LAKEHOUSE = DATA_DIR / "lakehouse"

# Couches lakehouse
LH_BRONZE = LAKEHOUSE / "bronze"
LH_SILVER = LAKEHOUSE / "silver"
LH_GOLD = LAKEHOUSE / "gold"

# Tables Bronze
BRONZE_PRODUCTS = LH_BRONZE / "off_products"

# Tables Silver
SILVER_PRODUCTS = LH_SILVER / "off_products"

# Tables Gold
GOLD_PRODUCTS = LH_GOLD / "products"

# Source Open Food Facts
OFF_CSV_URL = "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"
OFF_CSV_PATH = DATA_RAW / "products.csv.gz"


def create_data_dir():
    for path in [
        DATA_RAW,
        DATA_SAMPLES,
        LH_BRONZE,
        LH_SILVER,
        LH_GOLD,
    ]:
        path.mkdir(parents=True, exist_ok=True)


def get_spark():
    spark = (
        SparkSession.builder
        .appName("LakeHouse_OFF")
        .master("local[1]")
    )
