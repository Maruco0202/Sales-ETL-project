from pyspark.sql import SparkSession


class SparkSessionManager:

    @staticmethod
    def get_spark_session():

        spark = (
            SparkSession.builder
            .appName("Sales_ETL_Pipeline")
            .master("local[*]")
            .getOrCreate()
        )

        return spark