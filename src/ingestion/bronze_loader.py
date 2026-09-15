from pyspark.sql.functions import (
    current_timestamp,
    current_date,
    lit
)

from src.utils.spark_session import SparkSessionManager
from src.utils.config_reader import ConfigReader
from src.utils.logger import get_logger
from src.utils.exception import PipelineException


class BronzeLoader:

    def __init__(self):

        self.spark = SparkSessionManager.get_spark_session()

        self.config = ConfigReader.load_config()

        self.logger = get_logger()

    def read_source(self):

        try:

            self.logger.info("Reading source file")

            df = (
                self.spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(self.config["source_path"])
            )

            self.logger.info(
                f"Successfully loaded {df.count()} records"
            )

            return df

        except Exception as e:

            self.logger.error(
                f"Error while reading source file: {str(e)}"
            )

            raise PipelineException(str(e))

    def add_metadata(self, df):

        df = (
            df
            .withColumn(
                "ingestion_timestamp",
                current_timestamp()
            )
            .withColumn(
                "load_date",
                current_date()
            )
            .withColumn(
                "source_file_name",
                lit("data.csv")
            )
        )

        return df

    def write_bronze(self, df):

        try:

            self.logger.info(
                "Writing data to Bronze Layer"
            )

            (
                df.write
                .mode("overwrite")
                .parquet(
                    self.config["bronze_path"]
                )
            )

            self.logger.info(
                "Bronze Layer written successfully"
            )

        except Exception as e:

            self.logger.error(
                f"Error writing Bronze Layer: {str(e)}"
            )

            raise PipelineException(str(e))   

if __name__ == "__main__":

    bronze_loader = BronzeLoader()

    df = bronze_loader.read_source()

    df = bronze_loader.add_metadata(df)

    bronze_loader.write_bronze(df)

    print("Bronze Load Completed Successfully")

    