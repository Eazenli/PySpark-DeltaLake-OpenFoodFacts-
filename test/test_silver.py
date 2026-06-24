from pipeline.silver import SilverTransformer
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


def test_replace_missing_values(spark):
    data = [("AAA", "unknown"), ("BBB", "n/a"), ("CCC", ""), ("DDD", "Nestlé")]
    df = spark.createDataFrame(data, ["code", "brands"])

    result = SilverTransformer(spark).replace_missing_values(df)
    rows = {r["code"]: r["brands"] for r in result.collect()}

    assert rows["AAA"] is None
    assert rows["BBB"] is None
    assert rows["CCC"] is None
    assert rows["DDD"] == "Nestlé"


def test_trim_drop_null_product_code(spark):
    data = [("AAA", "Apple juice"), (None, "Milk"), ("CCC", None)]
    df = spark.createDataFrame(data, ["code", "product_name"])

    result = SilverTransformer(
        spark).trim_columns_and_drop_product_code_null(df)
    codes = [r["code"] for r in result.collect()]
    rows = {r["code"]: r["product_name"] for r in result.collect()}

    assert "AAA" in codes
    assert None not in codes
    assert "CCC" not in codes
    assert result.count() == 1


def test_drop_full_null_rows_remove_intermediate_cols(spark):
    schema = StructType([
        StructField("code", StringType()),
        StructField("product_name", StringType()),
        StructField("brands", StringType()),
    ])
    data = [("AAA", "Coffee", "Nestlé"), ("BBB", "Milk", None)]
    df = spark.createDataFrame(data, schema)

    result = SilverTransformer(spark).drop_full_null_rows(df)

    assert "null_count" not in result.columns
    assert "null_perc" not in result.columns
    # BBB's null_perc = 1.0 → dropped
    codes = [r["code"] for r in result.collect()]
    assert "AAA" in codes
    assert "BBB" not in codes


def test_filter_outlier_nutrition(spark):
    # (Cast_columns runs before this step in the real pipeline to cast nutritional columns into numeric)
    schema = StructType([
        StructField("code", StringType()),
        StructField("energy-kcal_100g", DoubleType()),
        StructField("fat_100g", DoubleType()),
        StructField("saturated-fat_100g", DoubleType()),
        StructField("carbohydrates_100g", DoubleType()),
        StructField("sugars_100g", DoubleType()),
        StructField("proteins_100g", DoubleType()),
        StructField("salt_100g", DoubleType()),
    ])
    data = [
        ("AAA", 12.0, 5.0, 2.0, 20.0, 10.0, 5.0, 1.0),
        # remove product with at least one outlier (prod_quantile = 0.99)
        ("BBB", 999999.0, 5.0,  2.0, 20.0, 10.0, 5.0, 1.0),
        # Keep product with no data in nutritional cols
        ("CCC", None, None, None, None, None, None, None),
    ]
    df = spark.createDataFrame(data, schema)

    result = SilverTransformer(spark).filter_outiler_nutrition(df)
    codes = [r["code"] for r in result.collect()]

    assert "AAA" in codes
    assert "BBB" not in codes
    assert "CCC" in codes


def test_clean_nutriscore(spark):
    data = [("AAA", "a"), ("BBB", "590"), ("CCC", "Italy"), ("DDD", None)]
    df = spark.createDataFrame(data, ["code", "nutriscore_grade"])

    result = SilverTransformer(spark).clean_nutriscore(df)
    grades = {r["code"]: r["nutriscore_grade"] for r in result.collect()}

    assert grades["AAA"] == "a"
    assert grades["BBB"] is None
    assert grades["CCC"] is None
    assert grades["DDD"] is None


def test_parse_ingredients(spark):
    data = [("AAA", "en:water,en:milk"), ("BBB", None)]
    df = spark.createDataFrame(data, ["code", "ingredients_tags"])

    result = SilverTransformer(spark).parse_ingredients(df)
    rows = {r["code"]: r["ingredients_tags"] for r in result.collect()}

    assert rows["AAA"] == ["water", "milk"]
    assert rows["BBB"] is None


def test_deduplicate_keeps_latest(spark):
    data = [("AAA", 1000, "Apple v1"),
            ("AAA", 2000, "Apple v2"), ("BBB", 1500, "Milk")]
    df = spark.createDataFrame(
        data, ["code", "last_modified_t", "product_name"])

    result = SilverTransformer(spark).deduplicate_product_codes(df)
    rows = {r["code"]: r["product_name"] for r in result.collect()}

    assert rows["AAA"] == "Apple v2"
    assert rows["BBB"] == "Milk"
    assert "count_dup" not in result.columns
