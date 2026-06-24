from delta.tables import DeltaTable
from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType
from src.config import SILVER_PRODUCTS, SILVER_SCHEMA


class SilverTransformer:
    COLS_TO_KEEP = [
        "code",
        "last_modified_t",
        "product_name",
        "brands",
        "categories_en",
        "labels_en",
        "countries_en",
        "nutriscore_grade",
        "ingredients_tags",
        "food_groups_en",
        "energy-kcal_100g",
        "fat_100g",
        "saturated-fat_100g",
        "carbohydrates_100g",
        "sugars_100g",
        "proteins_100g",
        "salt_100g"
    ]
    NUTRI_COLS = [
        "energy-kcal_100g",
        "fat_100g",
        "saturated-fat_100g",
        "carbohydrates_100g",
        "sugars_100g",
        "proteins_100g",
        "salt_100g"
    ]
    VALID_GRADES = ["a", "b", "c", "d", "e"]

    def __init__(self, spark):
        self.spark = spark

    def trim_columns_and_drop_product_code_null(self, df):

        return df.select([c for c in self.COLS_TO_KEEP if c in df.columns]).filter(
            f.col("product_name").isNotNull()
            & f.col("code").isNotNull()
            & (f.col("product_name") != "")
            & (f.col("code") != ""))

    def cast_columns(self, df):
        for col in self.NUTRI_COLS:
            df = df.withColumn(col, f.col(col).cast(DoubleType()))
        df = df.withColumn("last_modified_t", f.col(
            "last_modified_t").cast("long"))
        return df

    def filter_outiler_nutrition(self, df):

        return df.filter(
            (f.col("energy-kcal_100g").isNull() | f.col("energy-kcal_100g").between(0, 900)) &
            (f.col("fat_100g").isNull() | f.col("fat_100g").between(0, 100)) &
            (f.col("saturated-fat_100g").isNull() | f.col("saturated-fat_100g").between(0, 100)) &
            (f.col("carbohydrates_100g").isNull() | f.col("carbohydrates_100g").between(0, 100)) &
            (f.col("sugars_100g").isNull() | f.col("sugars_100g").between(0, 100)) &
            (f.col("proteins_100g").isNull() | f.col("proteins_100g").between(0, 100)) &
            (f.col("salt_100g").isNull() | f.col("salt_100g").between(0, 100))
        )

    def drop_full_null_rows(self, df):
        col_to_check = [c for c in df.columns if c not in [
            "product_name", "code"]]
        nb_col = len(col_to_check)

        df_full_rows_null = df.withColumn(
            "null_count",
            sum(f.col(c).isNull().cast("int") for c in col_to_check)
        ).withColumn(
            "null_perc",
            f.round(f.col("null_count") / nb_col, 2)
        )
        return df_full_rows_null.filter(f.col("null_perc") < 1).drop("null_count", "null_perc")

    def deduplicate_product_codes(self, df):
        df_latest = df.groupBy("code").agg(
            f.max("last_modified_t").alias("last_modified_t")
        )
        return df.join(df_latest, on=["code", "last_modified_t"], how="inner")

    def replace_missing_values(self, df):
        string_cols = [c for c, t in df.dtypes if t == "string"]

        return df.select([
            f.when(
                f.trim(f.lower(f.col(c))).isin("unknown", "n/a", "na", ""),
                None
            ).otherwise(f.col(c)).alias(c)
            if c in string_cols else f.col(c)
            for c in df.columns
        ])

    def clean_nutriscore(self, df):
        return df.withColumn(
            "nutriscore_grade",
            f.when(
                f.lower(f.col("nutriscore_grade")).isin(self.VALID_GRADES),
                f.lower(f.col("nutriscore_grade"))
            ).otherwise(None)
        )

    def parse_ingredients(self, df):
        return df.withColumn(
            "ingredients_tags",
            f.transform(
                f.split(f.col("ingredients_tags"), ","),
                lambda x: f.regexp_replace(f.trim(x), "^[a-z]{2}:", "")
            )
        )

    def transform_in_order(self, df):
        df = self.replace_missing_values(df)
        df = self.cast_columns(df)
        df = self.trim_columns_and_drop_product_code_null(df)
        df = self.drop_full_null_rows(df)
        df = self.filter_outiler_nutrition(df)
        df = self.clean_nutriscore(df)
        df = self.parse_ingredients(df)
        df = self.deduplicate_product_codes(df)
        return df


def write_silver(spark, df):
    if DeltaTable.isDeltaTable(spark, str(SILVER_PRODUCTS)):
        # Use MERGE operation to handle upsert (update + insert).
        silver_delta_table = DeltaTable.forPath(spark, str(SILVER_PRODUCTS))
        (
            silver_delta_table.alias("existing")
            .merge(df.alias("new"), "existing.code = new.code")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").schema(SILVER_SCHEMA).mode(
            "overwrite").save(str(SILVER_PRODUCTS))
