# PySpark-DeltaLake-OpenFoodFacts

This is a data engineering learning project built on the **Open Food Facts** (https://world.openfoodfacts.org/) public dataset.
A Bronze / Silver / Gold lakehouse architecture is implemented using PySpark and Delta Lake, containerized with Docker to avoid OS Windows and PySpark comptability issues. 

## Dataset

Source: Open Food Facts CSV dump (https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz) (~1.2 GB
compressed, ~4.5M products).

Note: The pipeline selects 17 columns at ingestion time and caches them as Snappy-compressed Parquet before writing to Delta, to keep memory usage within the limits of a local machine.

## Stack

| Tool | Version | Purpose |
|---|---|---|
| PySpark | 3.5.x | Distributed data processing |
| Delta Lake | 3.3.0 | ACID transactions, lakehouse storage |
| Docker | — | Portable PySpark runtime |
| Poetry | — | Dependency management |
| GitHub Actions | — | CI — runs tests on every push |
| AWS S3 | — | Cloud Sotrage |
| AWS Glue | — | Data Catalogue |
| AWS Athena | — | Run SQL queries with S3 and create table in gold layer with Athena CTAS|


## Architecture
Local:
data/raw/
products.csv.gz        ← source dump from Open Food Facts, only for EDA
products.parquet       ← columnar cache (17 selected columns, snappy)

AWS S3:
data/lakehouse/
bronze/off_products/   ← raw ingestion with timestamp, incremental append
silver/off_products/   ← cleaned, cast, deduplicated, outliers removed
gold/                  ← aggregated analytical layer 

**Bronze** — ingests the parquet cache into a Delta table, stamped with `ingest_at_t`. Supports incremental loads via
`last_modified_t` watermark extract from the last ingestion.

**Silver** — cleans and transforms bronze data: casts serveral columns, filters outliers in numeric nutritional columns with quantile,
deduplicates by most recent `last_modified_t`, normalises missing values, parses ingredient tags.

**Gold** — aggragated dataset with some analytical goals:
![image](images\image_5.png)
1. Brands with best average nutritional profile
![image](images\img_brand_country_nutri_profile_sql.png)
![image](images\img_brand_country_nutri_profile.png)
2. Nutriscore by countries with nomber of products
![image](images\img_nutriscore_country_sql.png)
![image](images\img_nutriscore_country.png)
3. Most common ingredients by nutriscore grade
![image](images\img_nutriscore_ingredient_sql.png)
![image](images\img_nutriscore_ingredient.png)


## Project structure

```
LAKEHOUSE-OPENFOODFACTS
├── pipeline/
│   ├── ingestion.py   # download, parquet cache, write bronze
│   ├── silver.py      # Class SilverTransformer + write_silver
│   └── run.py         # pipeline entrypoint
├── src/
│   └── config.py      # SparkSession, paths, schema
├── test/
│   ├── conftest.py    # shared SparkSession fixture
│   ├── test_ingestion.py
│   └── test_silver.py
├── notebooks/
│   ├── Exploration.ipynb
│   └── Inspection.ipynb
|── .github/
│   └── workflows
|       └── ci.yml
├── Dockerfile
├── docker-compose.yml
└── pyproject.toml
```

## Quickstart

```bash
docker compose run --rm test

Run the full pipeline (download → bronze → silver):
docker compose run --rm pipeline 2>&1 | tee pipeline.log # problems can be analysed with pipeline.log

Inspect data and delta_log in Jupyter:
docker compose run --rm --service-ports inspection-notebook
Then open http://127.0.0.1:8888 in the browser. 
```
## Possible improvement
Orchestration ? -> Moving to a ECS and trigger the pipeline weekly by EventBridge Scheduler ?

