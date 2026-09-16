from src.transformation.silver_cleaning import SilverCleaning


class TestSilverCleaning:

    def test_read_bronze_success(self):

        silver = SilverCleaning()

        df = silver.read_bronze()

        assert df.count() > 0

    def test_dataframe_not_none(self):

        silver = SilverCleaning()

        df = silver.read_bronze()

        assert df is not None

    def test_remove_null_order_ids(self):

        silver = SilverCleaning()

        df = silver.read_bronze()

        cleaned_df = silver.remove_null_order_ids(df)

        assert cleaned_df.count() < df.count()

    def test_remove_null_product_ids(self):

        silver = SilverCleaning()

        df = silver.read_bronze()

        cleaned_df = silver.remove_null_product_ids(df)

        assert cleaned_df.count() < df.count()

    def test_remove_duplicate_row_ids(self):

        silver = SilverCleaning()

        df = silver.read_bronze()

        cleaned_df = silver.remove_duplicate_row_ids(df)

        assert cleaned_df.count() <= df.count()

    def test_remove_future_order_dates(self):

        silver = SilverCleaning()

        df = silver.read_bronze()

        cleaned_df = silver.remove_future_order_dates(df)

        assert cleaned_df.count() < df.count()

    def test_remove_invalid_countries(self):

        silver = SilverCleaning()

        df = silver.read_bronze()

        cleaned_df = (
            silver.remove_invalid_countries(df)
        )

        assert cleaned_df.count() < df.count()


    def test_remove_invalid_ship_modes(self):

        silver = SilverCleaning()

        df = silver.read_bronze()

        cleaned_df = (
            silver.remove_invalid_ship_modes(df)
        )

        assert cleaned_df.count() < df.count()

    def test_remove_negative_sales(self):

        silver = SilverCleaning()

        df = silver.read_bronze()

        cleaned_df = (
            silver.remove_negative_sales(df)
        )

        assert cleaned_df.count() < df.count()

    def test_remove_null_customer_names(self):

        silver = SilverCleaning()

        df = silver.read_bronze()

        cleaned_df = (
            silver.remove_null_customer_names(df)
        )

        assert cleaned_df.count() < df.count()        
