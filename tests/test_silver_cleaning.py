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
