from pyspark.sql.functions import (
    col,
    to_date,
    current_date,
    when
)

from src.utils.spark_session import SparkSessionManager
from src.utils.config_reader import ConfigReader
from src.utils.logger import get_logger
from src.utils.exception import PipelineException
import sys

class SilverCleaning:

    def __init__(self):

        self.spark = SparkSessionManager.get_spark_session()

        self.config = ConfigReader.load_config()

        self.logger = get_logger()

    def read_bronze(self):

        try:

            self.logger.info(
                "Reading Bronze Data"
            )

            df = (
                self.spark.read
                .parquet(
                    self.config["bronze_path"]
                )
            )

            return df

        except Exception as e:

            raise PipelineException(
                e,
                sys,
                "Silver Cleaning"
            )

    #1. Reject records with NULL order_id    
    def reject_null_order_ids(self, df):

        rejected_df = (
            df.filter(
                col("order_id").isNull()
            )
        )

        rejected_count = (
            rejected_df.count()
        )

        self.write_rejected_data(
            rejected_df,
            "null_order_ids"
        )

        cleaned_df = (
            df.filter(
                col("order_id").isNotNull()
            )
        )

        print(
            f"1. Rejected {rejected_count} records with NULL order_id"
        )

        return cleaned_df

    #2. Flag records with NULL product_id
    def handle_null_product_ids(self, df):

        null_count = (
            df.filter(
                col("product_id").isNull()
            ).count()
        )

        review_df = (
            df.withColumn(
                "product_review_flag",
                when(
                    col("product_id").isNull(),
                    "MISSING_PRODUCT_ID"
                ).otherwise(
                    "VALID"
                )
            )
        )

        print(
            f"2. Flagged {null_count} records with missing product_id for review"
        )

        return review_df

    #3. Remove records with invalid countries
    def handle_invalid_countries(self, df):

        valid_countries = (
            self.config[
                "valid_countries"
            ]
        )

        invalid_count = (
            df.filter(
                ~col("country")
                .isin(valid_countries)
            ).count()
        )

        cleaned_df = (
            df.withColumn(
                "country",
                when(
                    ~col("country")
                    .isin(valid_countries),
                    "UNKNOWN_COUNTRY"
                ).otherwise(
                    col("country")
                )
            )
        )

        print(
            f"3. Replaced {invalid_count} invalid country values with UNKNOWN_COUNTRY"
        )

        return cleaned_df

    #4. Remove records with invalid ship modes
    def handle_invalid_ship_modes(self, df):

        valid_ship_modes = (
            self.config[
                "valid_ship_modes"
            ]
        )

        invalid_count = (
            df.filter(
                ~col("ship_mode")
                .isin(valid_ship_modes)
            ).count()
        )

        cleaned_df = (
            df.withColumn(
                "ship_mode",
                when(
                    ~col("ship_mode")
                    .isin(valid_ship_modes),
                    "UNKNOWN_SHIP_MODE"
                ).otherwise(
                    col("ship_mode")
                )
            )
        )

        print(
            f"4. Replaced {invalid_count} invalid ship mode values with UNKNOWN_SHIP_MODE"
        )

        return cleaned_df

    #5. Flag negative sales for review

    def handle_negative_sales(self, df):

        negative_count = (
            df.filter(
                col("sales_amount") < 0
            ).count()
        )

        cleaned_df = (
            df.withColumn(
                "sales_review_flag",
                when(
                    col("sales_amount") < 0,
                    "REVIEW_REQUIRED"
                ).otherwise(
                    "VALID"
                )
            )
        )

        print(
            f"5. Flagged {negative_count} negative sales records for business review"
        )

        return cleaned_df
    
    #6. duplicate row_ids
    def deduplicate_row_ids(self, df):

        before_count = df.count()

        cleaned_df = (
            df.dropDuplicates(["row_id"])
        )

        after_count = cleaned_df.count()

        print(
            f"6. Removed {before_count - after_count} duplicate row_id records"
        )

        return cleaned_df

    #7. records with NULL customer_name

    def handle_null_customer_names(self, df):

        null_count = (
            df.filter(
                col("customer_name").isNull()
            ).count()
        )

        cleaned_df = (
            df.withColumn(
                "customer_name",
                when(
                    col("customer_name").isNull(),
                    "UNKNOWN_CUSTOMER"
                ).otherwise(
                    col("customer_name")
                )
            )
        )

        print(
            f"7. Replaced {null_count} NULL customer_name values with UNKNOWN_CUSTOMER"
        )

        return cleaned_df
    
    #8. Remove records with future order dates

    def reject_future_order_dates(self, df):

        rejected_df = (
            df.filter(
                to_date(
                    col("order_date"),
                    "dd-MM-yyyy"
                ) > current_date()
            )
        )

        rejected_count = (
            rejected_df.count()
        )

        self.write_rejected_data(
            rejected_df,
            "future_order_dates"
        )

        valid_df = (
            df.filter(
                to_date(
                    col("order_date"),
                    "dd-MM-yyyy"
                ) <= current_date()
            )
        )

        print(
            f"8. Rejected {rejected_count} records with Future Order Dates"
        )

        return valid_df
    


    def write_rejected_data(
        self,
        rejected_df,
        folder_name
    ):
        """
        Writes rejected records to
        rejected layer for auditing.
        """

        if rejected_df.count() > 0:

            (
                rejected_df.write
                .mode("overwrite")
                .option("header", True)
                .csv(
                    f"{self.config['rejected_path']}/{folder_name}"
                )
            )

            self.logger.info(
                f"Rejected records written to {folder_name}"
            )
            
    #Export the cleaned DataFrame to Silver Layer
    def write_silver(self, df):

        (
            df.write
            .mode("overwrite")
            .parquet(
                self.config["silver_path"]
            )
        )

        print(
            "\nSilver Layer Written Successfully"
        )

        self.logger.info(
            "Silver Layer Written Successfully"
        )
           

if __name__ == "__main__":

    silver = SilverCleaning()

    df = silver.read_bronze()

    print(
        f"Bronze Record Count : {df.count()}"
    )

    print("\nStarting Silver Cleaning Process:-")

    df = silver.reject_null_order_ids(df)

    df = silver.handle_null_product_ids(df)

    df = silver.handle_invalid_countries(df)

    df = silver.handle_invalid_ship_modes(df)

    df = silver.handle_negative_sales(df)

    df = silver.deduplicate_row_ids(df)

    df = silver.handle_null_customer_names(df)

    df = silver.reject_future_order_dates(df)

    silver.write_silver(df)

    print(
        f"\nSilver Record Count : {df.count()}" ) 

  
                          