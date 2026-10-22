from pyspark.sql.functions import (
    col,
    current_date,
    to_date,
    trim,
    when,
    split,
    count,
    date_sub,
    lit
)

import sys

from src.utils.spark_session import SparkSessionManager
from src.utils.config_reader import ConfigReader
from src.utils.logger import get_logger
from src.utils.exception import PipelineException


class Transformations:
    """
    Centralized transformation layer.

    Responsibilities:
    - Data Validation
    - Data Cleaning
    - Silver Layer Processing
    - Sales Gold Generation
    - Customer Gold Generation
    - Data Quality Reporting
    - Rejected Records Management
    """

    def __init__(self):

        self.spark = (
            SparkSessionManager.get_spark_session()
        )

        self.config = (
            ConfigReader.load_config()
        )

        self.logger = (
            get_logger()
        )

        self.quality_results = []

    def _handle_exception(
        self,
        exception,
        layer
    ):
        self.logger.error(
            f"{layer} Failed : {str(exception)}"
        )

        raise PipelineException(
            exception,
            sys,
            layer
        )

    def read_bronze(self):
        """
        Read data from Bronze layer.
        """    
        try:
    
                self.logger.info(
                    "Reading Bronze Layer"
                )
    
                df = (
                    self.spark.read
                    .parquet(
                        self.config["bronze_path"]
                    )
                )
    
                return df
    
        except Exception as e:
    
                self._handle_exception(
                    e,
                    "Validation Layer"
                )

    # =====================================================
    #                SILVER LAYER VALIDATION
    # =====================================================    

    #Errors
    
    #1. Checking for NULL values
    def check_nulls(self, df):

        print("\n1. Checking NULL Values:-")

        columns_to_check = [
            "order_id",
            "customer_id",
            "customer_name",
            "product_id",
            "product_name"
        ]

        for column_name in columns_to_check:

            null_count = (
                df.filter(
                    col(column_name).isNull()
                ).count()
            )

            print(
                f"{column_name} -> NULL Count : {null_count}"
            )

            self.logger.info(
                f"{column_name} -> NULL Count : {null_count}"
            )

            self.quality_results.append(
                [
                    f"Null_{column_name}",
                    null_count,
                    
                ]
            )


    #2. Checking for Duplicate Row IDs
    def check_duplicate_row_ids(self, df):

        print("\n2. Checking Duplicate Row IDs:-")

        duplicate_count = (
            df.groupBy("row_id")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        print(
            f"Duplicate Row IDs Found : {duplicate_count}"
        )

        self.logger.info(
            f"Duplicate Row IDs Found : {duplicate_count}"
        )

        self.quality_results.append(
            [
                "Duplicate_Row_ID",
                duplicate_count,
                
            ]
        )

    #3. Checking for Invalid Countries
    def check_distinct_countries(self, df):

        print("\nDistinct Countries:")

        df.select("country").distinct().show(100, False)

    def check_invalid_country(self, df):

        print("3. Checking Invalid Countries:-")

        valid_countries = (
            self.config[
                "valid_countries"
            ]
        )

        invalid_count = (
            df.filter(
                ~col("country").isin(valid_countries)
            ).count()
        )

        print(
            f"Invalid Countries Found : {invalid_count}"
        )

        self.logger.info(
            f"Invalid Countries Found : {invalid_count}"
        )

        self.quality_results.append(
            [
                "Invalid_Country",
                invalid_count,
               
            ]
        )

    #4. Checking for Invalid Ship Modes
    def check_distinct_ship_modes(self, df):

        print("\nDistinct Ship Modes:")

        df.select("ship_mode").distinct().show(100, False)

    def check_invalid_ship_modes(self, df):

        print("4. Checking Invalid Ship Modes:-")

        valid_ship_modes = (
            self.config[
                "valid_ship_modes"
            ]
        )

        invalid_count = (
            df.filter(
                ~col("ship_mode").isin(valid_ship_modes)
            ).count()
        )

        print(
            f"Invalid Ship Modes Found : {invalid_count}"
        )

        self.logger.info(
            f"Invalid Ship Modes Found : {invalid_count}"
        )

        self.quality_results.append(
            [
                "Invalid_Ship_Mode",
                invalid_count,
               
            ]
        )

    #5. Date Quality Checks
    def check_future_order_dates(self, df):
        print("\n5. Date Quality Checks:")    
        print("a. Checking Future Order Dates:-")

        future_count = (
            df.filter(
                to_date(col("order_date"), "dd-MM-yyyy") > current_date()
            ).count()
        )

        print(
            f"Future Order Dates Found : {future_count}"
        )

        self.logger.info(
            f"Future Order Dates Found : {future_count}"
        )

        self.quality_results.append(
            [
                "Future_Order_Date",
                future_count,
                
            ]
        )

    def check_invalid_ship_dates(self, df):

        print("b. Checking Ship Date Before Order Date:-")

        invalid_count = (
            df.filter(
                to_date(col("ship_date"), "dd-MM-yyyy")
                <
                to_date(col("order_date"), "dd-MM-yyyy")
            ).count()
        )

        print(
            f"Ship Date Before Order Date Found : {invalid_count}"
        )

        self.logger.info(
            f"Ship Date Before Order Date Found : {invalid_count}"
        )

        self.quality_results.append(
            [
                "Ship_Date_Before_Order_Date",
                invalid_count,
                
            ]
        )

    def check_invalid_order_date_format(self, df):
        print("c. Checking Invalid Order Date Format:-")

        invalid_Format_count = (
            df.filter(
                col("order_date").isNotNull()
            ).filter(
                to_date(
                    col("order_date"),
                    "dd-MM-yyyy"
                ).isNull()
            ).count()
        )

        print(
                f"Invalid Order Date Format Found : {invalid_Format_count}"
            )
        
        self.logger.info(
                f"Invalid Order Date Format Found : {invalid_Format_count}"
            )

        self.quality_results.append(
                [
                    "Invalid_Order_Date_Format",
                    invalid_Format_count,
                    
                ]

           ) 

    def check_invalid_ship_date_format(self, df):

        print("d. Checking Invalid Ship Date Format:-")

        invalid_count = (
            df.filter(
                col("ship_date").isNotNull()
            ).filter(
                to_date(
                    col("ship_date"),
                    "dd-MM-yyyy"
                ).isNull()
            ).count()
        )

        print(
            f"Invalid Ship Date Format Found : {invalid_count}"
        )

        self.logger.info(
            f"Invalid Ship Date Format Found : {invalid_count}"
        )

        self.quality_results.append(
            [
                "Invalid_Ship_Date_Format",
                invalid_count,
              
            ]
        )

    #6. Checking for Negative Sales Amounts
    def check_negative_sales(self, df):

        print("\n6. Checking Negative Sales Amounts:-")

        negative_count = (
            df.filter(
             col("sales_amount") < 0
            ).count()
        )

        print(
            f"Negative Sales Records Found : {negative_count}"
        )

        self.logger.info(
            f"Negative Sales Records Found : {negative_count}"
        )    

        self.quality_results.append(
            [
                "Negative_Sales",
                negative_count,
               
            ]
        )

    #7. Checking for Invalid Quantity
    def check_invalid_quantity(self, df):

        print("\n7. Checking Invalid Quantity:-")

        invalid_quantity_count = (
            df.filter(
                col("quantity") <= 0
            ).count()
        )

        print(
            f"Invalid Quantity Records Found : {invalid_quantity_count}"
        )

        self.logger.info(
            f"Invalid Quantity Records Found : {invalid_quantity_count}"
        )

        self.quality_results.append(
            [
                "Invalid_Quantity",
                invalid_quantity_count,
                
            ]
        )

    #8. Blank Customer Names

    def check_blank_customer_names(self, df):

        print("\n8. Checking Blank Customer Names:-")

        blank_count = (
            df.filter(
                trim(col("customer_name")) == ""
            ).count()
        )

        print(
            f"Blank Customer Names Found : {blank_count}"
        )

        self.logger.info(
            f"Blank Customer Names Found : {blank_count}"
        )

        self.quality_results.append(
            [
                "Blank_Customer_Name",
                blank_count,
               
            ]
        )
        
    #9. Customer ID Format Validation
    def check_invalid_customer_ids(self, df):

        print("\n9. Checking Customer ID Format:-")

        invalid_count = (
            df.filter(
                ~col("customer_id").rlike("^CUST[0-9]+$")
            ).count()
        )

        print(
            f"Invalid Customer IDs Found : {invalid_count}"
        )

        self.logger.info(
            f"Invalid Customer IDs Found : {invalid_count}"
        )

        self.quality_results.append(
            [
                "Invalid_Customer_ID",
                invalid_count,
                
            ]
        )

    #10. Product ID Format Validation

    def check_invalid_product_ids(self, df):

        print("\n10. Checking Product ID Format:-")

        invalid_count = (
            df.filter(
                col("product_id").isNotNull()
            ).filter(
                ~col("product_id").rlike("^P[0-9]+$")
            ).count()
        )

        print(
            f"Invalid Product IDs Found : {invalid_count}"
        )

        self.logger.info(
            f"Invalid Product IDs Found : {invalid_count}"
        )

        self.quality_results.append(
            [
                "Invalid_Product_ID",
                invalid_count,
            ]
        )       


    #11. Schema Validation

    def validate_schema(self, df):

        print("\n11. Schema Validation:-")

        expected_columns = [
            "row_id",
            "order_id",
            "order_date",
            "ship_date",
            "ship_mode",
            "customer_id",
            "customer_name",
            "segment",
            "country",
            "city",
            "product_id",
            "product_name",
            "quantity",
            "sales_amount"
        ]

        missing_columns = [
            c for c in expected_columns
            if c not in df.columns
        ]

        print(
            f"Missing Columns : {missing_columns}"
        )

        self.logger.info(
            f"Missing Columns : {missing_columns}"
        )

        self.quality_results.append(
            [
                "Missing_Columns",
                len(missing_columns),
            ]
        )


    #12. Generate Data Quality Report

    def generate_quality_report(self):

        try:

            import os
            import pandas as pd

            os.makedirs(
                "data/reports",
                exist_ok=True
            )

            report_df = pd.DataFrame(
                self.quality_results,
                columns=[
                    "Rule_Name",
                    "Error_Count"
                ]
            )

            report_path = (
                "data/reports/data_quality_report.csv"
            )

            report_df.to_csv(
                report_path,
                index=False
            )

            self.logger.info(
                "Data Quality Report Generated Successfully"
            )

        except Exception as e:

            self._handle_exception(
                e,
                "Quality Report Layer"
            )
    # =====================================================
    #             SILVER LAYER CLEANING
    # =====================================================


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
    
    #rejected records
    def write_rejected_data(
        self,
        rejected_df,
        folder_name
    ):

        try:

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

        except Exception as e:

            self._handle_exception(
                e,
                "Rejected Records Layer"
            )
            
    #Export the cleaned DataFrame to Silver Layer
    def write_silver(self, df):

        try:

            (
                df.write
                .mode("overwrite")
                .partitionBy("order_year")
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

        except Exception as e:

            self._handle_exception(
                e,
                "Silver Write Layer"
            )



    
    # =====================================================
    #                     SALES GOLD
    # =====================================================
    
    def read_silver(self):
        """
        Read Silver layer data for Gold processing.
        """
        try:

                self.logger.info(
                    "Reading Silver Layer"
                )

                df = (
                    self.spark.read
                    .parquet(
                        self.config[
                            "silver_path"
                        ]
                    )
                )

                return df

        except Exception as e:

                self._handle_exception(
                    e,
                    "Gold Layer"
                )     

    def build_sales_gold(self, df):

        sales_gold_df = (
            df.select(
                "order_id",
                "customer_id",   
                "order_date",
                "ship_date",
                "ship_mode",
                "city",
                "order_year"
            )
        )

        return sales_gold_df

    def write_sales_gold(self, df):
        """
        Persist Sales Gold dataset.
        """
        try:

            (
                df.coalesce(1)
                .write
                .mode("overwrite")
                .partitionBy("order_year")
                .parquet(
                    "data/gold/sales"
                )
            )

            self.logger.info(
                "Sales Gold Dataset Written Successfully"
            )

        except Exception as e:

            self._handle_exception(
                e,
                "Sales Gold Write Layer"
            )

# =====================================================
#                     CUSTOMER GOLD
# =====================================================


    def build_customer_gold(self, df):

        df = df.withColumn(
            "order_date",
            to_date(
                col("order_date"),
                "dd-MM-yyyy"
            )
        )

        latest_order_date = (

            df.filter(
                col("order_date")
                <= current_date()
            )

            .selectExpr(
                "max(order_date) as max_date"
            )
            .collect()[0]["max_date"]
        )

        customer_gold_df = (

            df.withColumn(
                "first_name",
                split(
                    col("customer_name"),
                    " "
                ).getItem(0)
            )

            .withColumn(
                "last_name",
                split(
                    col("customer_name"),
                    " "
                ).getItem(1)
            )

            .groupBy(
                "customer_id",
                "first_name",
                "last_name",
                "segment",
                "country"
            )

            .agg(

                count(
                    when(
                        col("order_date")
                        >= date_sub(
                            lit(latest_order_date),
                            30
                        ),
                        True
                    )
                ).alias(
                    "orders_last_30_days"
                ),

                count(
                    when(
                        col("order_date")
                        >= date_sub(
                            lit(latest_order_date),
                            180
                        ),
                        True
                    )
                ).alias(
                    "orders_last_6_months"
                ),

                count(
                    when(
                        col("order_date")
                        >= date_sub(
                            lit(latest_order_date),
                            365
                        ),
                        True
                    )
                ).alias(
                    "orders_last_12_months"
                ),

                count("*").alias(
                    "orders_all_time"
                )
         )
        )

        return customer_gold_df

    def write_customer_gold(self, df):
        """
        Persist Customer Gold dataset.
        """
        try:

            (
                df.coalesce(1)
                .write
                .mode("overwrite")
                .parquet(
                    "data/gold/customer"
                )
            )

            self.logger.info(
                "Customer Gold Dataset Written Successfully"
            )

        except Exception as e:

            self._handle_exception(
                e,
                "Customer Gold Write Layer"
            )