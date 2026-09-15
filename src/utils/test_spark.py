from spark_session import SparkSessionManager

spark = SparkSessionManager.get_spark_session()

print("Spark Started Successfully")

print("Spark Version:", spark.version)

spark.stop()