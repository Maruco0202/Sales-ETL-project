import sys

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
    """
    Responsible for reading source data,
    adding metadata and writing Bronze Layer.
    """

    def __init__(self):

        self.spark = (
            SparkSessionManager
            .get_spark_session()
        )

        self.config = (
            ConfigReader
            .load_config()
        )

        self.logger = (
            get_logger()
        )

    def read_source(self):
        """
        Reads source CSV file
        and returns DataFrame.
        """

        try:

            self.logger.info(
                "Reading source file"
            )

            df = (
                self.spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(
                    self.config[
                        "source_path"
                    ]
                )
            )

            record_count = (
                df.count()
            )

            self.logger.info(
                f"Successfully loaded "
                f"{record_count} records"
            )

            return df

        except Exception as e:

            self.logger.error(
                f"Error while reading source file: {e}"
            )

            raise PipelineException(
                e,
                sys,
                "Bronze Layer"
            )

    def add_metadata(self, df):
        """
        Adds metadata columns required
        for auditability and traceability.
        """

        try:

            self.logger.info(
                "Adding metadata columns"
            )

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
                    lit(
                        self.config[
                            "source_file_name"
                        ]
                    )
                )
            )

            self.logger.info(
                "Metadata columns added successfully"
            )

            return df

        except Exception as e:

            self.logger.error(
                f"Error while adding metadata: {e}"
            )

            raise PipelineException(
                e,
                sys,
                "Bronze Layer"
            )

    def write_bronze(self, df):
        """
        Writes DataFrame to Bronze Layer.
        """

        try:

            record_count = (
                df.count()
            )

            self.logger.info(
                f"Writing {record_count} records "
                f"to Bronze Layer"
            )

            (
                df.write
                .mode("overwrite")
                .parquet(
                    self.config[
                        "bronze_path"
                    ]
                )
            )

            self.logger.info(
                f"Bronze Layer written successfully "
                f"with {record_count} records"
            )

        except Exception as e:

            self.logger.error(
                f"Error writing Bronze Layer: {e}"
            )

            raise PipelineException(
                e,
                sys,
                "Bronze Layer"
            )


if __name__ == "__main__":

    bronze_loader = BronzeLoader()

    df = (
        bronze_loader
        .read_source()
    )

    df = (
        bronze_loader
        .add_metadata(df)
    )

    bronze_loader.write_bronze(
        df
    )

    print(
        "Bronze Load Completed Successfully"
    )