from src.utils.spark_session import SparkSessionManager


class TestSparkSession:

    def test_spark_session_creation(self):

        spark = (
            SparkSessionManager
            .get_spark_session()
        )

        assert spark is not None

        spark.stop()

    def test_spark_version(self):

        spark = (
            SparkSessionManager
            .get_spark_session()
        )

        assert spark.version is not None

        spark.stop()