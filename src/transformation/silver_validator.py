from pyspark.sql.functions import col, current_date, to_date, trim

from src.utils.spark_session import SparkSessionManager
from src.utils.config_reader import ConfigReader
from src.utils.logger import get_logger
from src.utils.exception import PipelineException


class SilverValidator:

    def __init__(self):

        self.spark = SparkSessionManager.get_spark_session()

        self.config = ConfigReader.load_config()

        self.logger = get_logger()

        self.quality_results = []


    def read_bronze(self):

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

            self.logger.info(
                f"Successfully loaded {df.count()} records from Bronze"
            )

            return df

        except Exception as e:

            self.logger.error(
                f"Error reading Bronze Layer: {str(e)}"
            )

            raise PipelineException(str(e))

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
                [f"Null_{column_name}", null_count]
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
            ["Duplicate_Row_ID", duplicate_count]
        )

    #3. Checking for Invalid Countries
    def check_distinct_countries(self, df):

        print("\nDistinct Countries:")

        df.select("country").distinct().show(100, False)

    def check_invalid_country(self, df):

        print("3. Checking Invalid Countries:-")

        valid_countries = [
            "India",
            "USA",
            "UK",
            "Canada",
            "Germany"
        ]

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
            ["Invalid_Country", invalid_count]
        )

    #4. Checking for Invalid Ship Modes
    def check_distinct_ship_modes(self, df):

        print("\nDistinct Ship Modes:")

        df.select("ship_mode").distinct().show(100, False)

    def check_invalid_ship_modes(self, df):

        print("4. Checking Invalid Ship Modes:-")

        valid_ship_modes = [
            "First Class",
            "Second Class",
            "Standard Class",
            "Same Day"
        ]

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
            ["Invalid_Ship_Mode", invalid_count]
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
            ["Future_Order_Date", future_count]
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
            ["Ship_Date_Before_Order_Date", invalid_count]
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
            ["Invalid_Order_Date_Format", invalid_Format_count]
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
            ["Invalid_Ship_Date_Format", invalid_count]
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
            ["Negative_Sales", negative_count]
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
            ["Invalid_Quantity", invalid_quantity_count]
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
            ["Blank_Customer_Name", blank_count]
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
            ["Invalid_Customer_ID", invalid_count]
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
            ["Invalid_Product_ID", invalid_count]
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
            ["Missing_Columns", len(missing_columns)]
        )


    #12. Generate Data Quality Report

    def generate_quality_report(self):

        import pandas as pd

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

        print(
            "\nData Quality Report Generated Successfully"
        )

        print(
            f"Report Location : {report_path}"
        )

        self.logger.info(
            "Data Quality Report Generated Successfully"
        )



if __name__ == "__main__":

    validator = SilverValidator()

    df = validator.read_bronze()

    print(
        f"Total Bronze Records : {df.count()}\n"
    )

    validator.check_nulls(df)

    validator.check_duplicate_row_ids(df)

    validator.check_distinct_countries(df)
    validator.check_invalid_country(df)

    validator.check_distinct_ship_modes(df)
    validator.check_invalid_ship_modes(df)

    validator.check_future_order_dates(df)
    validator.check_invalid_ship_dates(df)
    validator.check_invalid_order_date_format(df)
    validator.check_invalid_ship_date_format(df)

    validator.check_negative_sales(df)

    validator.check_invalid_quantity(df)

    validator.check_blank_customer_names(df)

    validator.check_invalid_customer_ids(df)

    validator.check_invalid_product_ids(df)

    validator.validate_schema(df)

    validator.generate_quality_report()