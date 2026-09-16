from pyspark.sql.functions import (
    col,
    to_date,
    current_date
)

from src.utils.spark_session import SparkSessionManager
from src.utils.config_reader import ConfigReader
from src.utils.logger import get_logger
from src.utils.exception import PipelineException


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

            raise PipelineException(str(e))

    #1. Remove records with NULL order_id    
    def remove_null_order_ids(self, df):

        before_count = df.count()

        cleaned_df = (
            df.filter(
                col("order_id").isNotNull()
            )
        )

        after_count = cleaned_df.count()

        print(
            f"1. Removed {before_count - after_count} records with NULL order_id"
        )

        return cleaned_df

    #2. Remove records with NULL product_id
    def remove_null_product_ids(self, df):

        before_count = df.count()

        cleaned_df = (
            df.filter(
                col("product_id").isNotNull()
            )
        )

        after_count = cleaned_df.count()

        print(
            f"2. Removed {before_count - after_count} records with NULL product_id"
        )

        return cleaned_df

    #3. Remove records with invalid countries
    def remove_invalid_countries(self, df):

        valid_countries = [
            "India",
            "USA",
            "UK",
            "Canada",
            "Germany"
        ]

        before_count = df.count()

        cleaned_df = (
            df.filter(
                col("country").isin(valid_countries)
            )
        )

        after_count = cleaned_df.count()

        print(
            f"3. Removed {before_count - after_count} records with Invalid Country"
        )

        return cleaned_df

    #4. Remove records with invalid ship modes
    def remove_invalid_ship_modes(self, df):

        valid_ship_modes = [
            "First Class",
            "Second Class",
            "Standard Class",
            "Same Day"
        ]

        before_count = df.count()

        cleaned_df = (
            df.filter(
                col("ship_mode").isin(valid_ship_modes)
            )
        )

        after_count = cleaned_df.count()

        print(
            f"4. Removed {before_count - after_count} records with Invalid Ship Mode"
        )

        return cleaned_df

    #5. Remove records with negative sales
    def remove_negative_sales(self, df):

        before_count = df.count()

        cleaned_df = (
            df.filter(
                col("sales_amount") >= 0
            )
        )

        after_count = cleaned_df.count()

        print(
            f"5. Removed {before_count - after_count} records with Negative Sales"
        )

        return cleaned_df

    #6. Remove duplicate row_ids
    def remove_duplicate_row_ids(self, df):

        before_count = df.count()

        cleaned_df = (
            df.dropDuplicates(["row_id"])
        )

        after_count = cleaned_df.count()

        print(
            f"6. Removed {before_count - after_count} duplicate row_id records"
        )

        return cleaned_df

    #7. Remove records with NULL customer_name
    def remove_null_customer_names(self, df):

        before_count = df.count()

        cleaned_df = (
            df.filter(
                col("customer_name").isNotNull()
            )
        )

        after_count = cleaned_df.count()

        print(
            f"7. Removed {before_count - after_count} records with NULL customer_name"
        )

        return cleaned_df
    
    #8. Remove records with future order dates

    def remove_future_order_dates(self, df):

        before_count = df.count()

        cleaned_df = (
            df.filter(
                to_date(
                    col("order_date"),
                    "dd-MM-yyyy"
                ) <= current_date()
            )
        )

        after_count = cleaned_df.count()

        print(
            f"8. Removed {before_count - after_count} records with Future Order Dates"
        )

        return cleaned_df

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

    df = silver.remove_null_order_ids(df)
    df = silver.remove_null_product_ids(df)
    df = silver.remove_invalid_countries(df)
    df = silver.remove_invalid_ship_modes(df)
    df = silver.remove_negative_sales(df)
    df = silver.remove_duplicate_row_ids(df)
    df = silver.remove_null_customer_names(df)
    df = silver.remove_future_order_dates(df)
    silver.write_silver(df)
    print(
        f"\nSilver Record Count : {df.count()}" ) 

  
                          