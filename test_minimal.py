from pyspark.sql import SparkSession
import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


spark = SparkSession.builder \
    .master("local[1]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

df = spark.createDataFrame(
    [("AAA", "Nestlé"), ("BBB", "test")], ["code", "brand"])
print(df.collect())
spark.stop()
