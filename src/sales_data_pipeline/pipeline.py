from src.ingestion.bronze_loader import BronzeLoader
from src.sales_data_pipeline.transformations import Transformations
from src.utils.logger import get_logger

class SalesPipeline:

    def __init__(self):

        self.logger = get_logger()

        self.transformer = Transformations()

    def extract(self):

        self.logger.info(
            "Starting Extraction Layer"
        )

        bronze = BronzeLoader()

        df = bronze.read_source()

        df = bronze.add_metadata(df)

        bronze.write_bronze(df)

        self.logger.info(
            f"Extraction Layer Completed Successfully - Records : {df.count()}"
        )

        return df

    def validate(self):

        self.logger.info(
            "Starting Validation Layer"
        )

        df = self.transformer.read_bronze()

        self.transformer.check_nulls(df)

        self.transformer.check_duplicate_row_ids(df)

        self.transformer.check_invalid_country(df)

        self.transformer.check_invalid_ship_modes(df)

        self.transformer.check_future_order_dates(df)

        self.transformer.check_invalid_ship_dates(df)

        self.transformer.check_invalid_order_date_format(df)

        self.transformer.check_invalid_ship_date_format(df)

        self.transformer.check_negative_sales(df)

        self.transformer.check_invalid_quantity(df)

        self.transformer.check_blank_customer_names(df)

        self.transformer.check_invalid_customer_ids(df)

        self.transformer.check_invalid_product_ids(df)

        self.transformer.validate_schema(df)

        self.transformer.generate_quality_report()

        self.logger.info(
            "Validation Layer Completed"
        )
        return df

    def clean(self):

        self.logger.info(
            "Starting Cleaning Layer"
        )

        df = self.transformer.read_bronze()

        df = self.transformer.reject_null_order_ids(df)

        df = self.transformer.handle_null_product_ids(df)

        df = self.transformer.handle_invalid_countries(df)

        df = self.transformer.handle_invalid_ship_modes(df)

        df = self.transformer.handle_negative_sales(df)

        df = self.transformer.deduplicate_row_ids(df)

        df = self.transformer.handle_null_customer_names(df)

        df = self.transformer.reject_future_order_dates(df)

        self.transformer.write_silver(df)

        self.logger.info(
            f"Cleaning Layer Completed Successfully - Records : {df.count()}"
        )

        return df
    
    def transform(self):

        self.logger.info(
            "Starting Gold Transformation Layer"
        )

        silver_df = (
            self.transformer.read_silver()
        )

        sales_df = (
            self.transformer.build_sales_gold(
                silver_df
            )
        )

        customer_df = (
            self.transformer.build_customer_gold(
                silver_df
            )
        )

        self.logger.info(
            "Gold Transformation Layer Completed"
        )

        return sales_df, customer_df

    def load(
        self,
        sales_df,
        customer_df
    ):

        self.logger.info(
            "Starting Load Layer"
        )

        self.transformer.write_sales_gold(
            sales_df
        )

        self.transformer.write_customer_gold(
            customer_df
        )

        self.logger.info(
            "Load Layer Completed"
        )
        
    def run(self):

        self.logger.info(
            "Starting Complete ETL Pipeline"
        )

        self.extract()

        self.validate()

        self.clean()

        sales_df, customer_df = self.transform()

        self.load(
            sales_df,
            customer_df
        )

        self.logger.info(
            "Complete ETL Pipeline Executed Successfully"
        )