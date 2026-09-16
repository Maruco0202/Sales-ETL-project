from src.utils.logger import get_logger

logger = get_logger()


def main():

    logger.info("Starting Full ETL Pipeline")

    print("=================================")
    print("Sales ETL Pipeline Started")
    print("=================================")

    print("\nStep 1 : Bronze Layer")
    from src.ingestion.bronze_loader import BronzeLoader

    bronze = BronzeLoader()

    bronze_df = bronze.read_source()

    bronze_df = bronze.add_metadata(
        bronze_df
    )

    bronze.write_bronze(
        bronze_df
    )

    print("Bronze Layer Completed")

    print("\nStep 2 : Silver Validation")

    from src.transformation.silver_validator import SilverValidator

    validator = SilverValidator()

    df = validator.read_bronze()

    validator.check_nulls(df)
    validator.check_duplicate_row_ids(df)
    validator.check_invalid_country(df)
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

    print("Silver Validation Completed")

    print("\nStep 3 : Silver Cleaning")

    from src.transformation.silver_cleaning import SilverCleaning

    silver = SilverCleaning()

    df = silver.read_bronze()

    df = silver.remove_null_order_ids(df)
    df = silver.remove_null_product_ids(df)
    df = silver.remove_invalid_countries(df)
    df = silver.remove_invalid_ship_modes(df)
    df = silver.remove_negative_sales(df)
    df = silver.remove_duplicate_row_ids(df)
    df = silver.remove_null_customer_names(df)
    df = silver.remove_future_order_dates(df)

    silver.write_silver(df)

    print("Silver Cleaning Completed")

    print("\nStep 4 : Sales Gold")

    from src.transformation.sales_gold import SalesGold

    sales_gold = SalesGold()

    silver_df = sales_gold.read_silver()

    sales_df = sales_gold.build_sales_gold(
        silver_df
    )

    sales_gold.write_gold(
        sales_df
    )

    print("Sales Gold Completed")

    print("\nStep 5 : Customer Gold")

    from src.transformation.customer_gold import CustomerGold

    customer_gold = CustomerGold()

    silver_df = customer_gold.read_silver()

    customer_df = (
        customer_gold.build_customer_gold(
            silver_df
        )
    )

    customer_gold.write_gold(
        customer_df
    )

    print("Customer Gold Completed")

    print("\n=================================")
    print("Pipeline Execution Completed")
    print("=================================")

    logger.info(
        "Full ETL Pipeline Completed"
    )


if __name__ == "__main__":

    main()