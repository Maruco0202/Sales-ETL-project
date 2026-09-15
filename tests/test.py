from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Test")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.csv(
    "data/source/data.csv",
    header=True,
    inferSchema=True
)

print("Record Count:", df.count())

df.show(5)

spark.stop()