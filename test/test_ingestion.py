from pipeline.ingestion import write_bronze


def test_write_bronze_first_run(spark, tmp_path):
    test_path = tmp_path / "bronze"

    df = spark.createDataFrame(
        [
            ("AAA", "Milk", 1000),
            ("BBB", "Banana", 2000),
        ],
        ["code", "product_name", "last_modified_t"]
    )
    write_bronze(spark, df, test_path)
    result = spark.read.format("delta").load(str(test_path))

    assert result.count() == 2
    assert "ingest_at_t" in result.columns


def test_write_bronze_incremental(spark, tmp_path):
    test_path = tmp_path / "bronze_2"

    df1 = spark.createDataFrame(
        [
            ("AAA", "Milk", 1000),
            ("BBB", "Banana", 2000),
        ],
        ["code", "product_name", "last_modified_t"]
    )
    write_bronze(spark, df1, test_path)
    df2 = spark.createDataFrame(
        [
            ("AAA", "Milk", 1000),
            ("BBB", "Biscuit", 3000),
            ("CCC", "Coffee", 4000)
        ],
        ["code", "product_name", "last_modified_t"]
    )
    write_bronze(spark, df2, test_path)
    result = spark.read.format("delta").load(str(test_path))
    assert result.count() == 4
