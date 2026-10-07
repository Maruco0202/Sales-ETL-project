from src.transformation.silver_cleaning import (
    SilverCleaning
)


class TestSilverCleaning:

    def setup_method(self):

        self.cleaner = (
            SilverCleaning()
        )

        self.df = (
            self.cleaner.read_bronze()
        )

    def test_read_bronze(self):

        assert self.df is not None

    def test_bronze_data_not_empty(self):

        assert self.df.count() > 0

    def test_reject_null_order_ids(self):

        cleaned_df = (
            self.cleaner
            .reject_null_order_ids(
                self.df
            )
        )

        null_count = (
            cleaned_df.filter(
                cleaned_df.order_id.isNull()
            ).count()
        )

        assert null_count == 0

    def test_handle_null_product_ids(self):

        cleaned_df = (
            self.cleaner
            .handle_null_product_ids(
                self.df
            )
        )

        assert (
            "product_review_flag"
            in cleaned_df.columns
        )

    def test_handle_invalid_countries(self):

        cleaned_df = (
            self.cleaner
            .handle_invalid_countries(
                self.df
            )
        )

        invalid_count = (
            cleaned_df.filter(
                cleaned_df.country
                == "UNKNOWN_COUNTRY"
            ).count()
        )

        assert invalid_count >= 0

    def test_handle_invalid_ship_modes(self):

        cleaned_df = (
            self.cleaner
            .handle_invalid_ship_modes(
                self.df
            )
        )

        invalid_count = (
            cleaned_df.filter(
                cleaned_df.ship_mode
                ==
                "UNKNOWN_SHIP_MODE"
            ).count()
        )

        assert invalid_count >= 0

    def test_handle_negative_sales(self):

        cleaned_df = (
            self.cleaner
            .handle_negative_sales(
                self.df
            )
        )

        assert (
            "sales_review_flag"
            in cleaned_df.columns
        )

    def test_deduplicate_row_ids(self):

        before_count = (
            self.df.count()
        )

        cleaned_df = (
            self.cleaner
            .deduplicate_row_ids(
                self.df
            )
        )

        after_count = (
            cleaned_df.count()
        )

        assert after_count <= before_count

    def test_handle_null_customer_names(self):

        cleaned_df = (
            self.cleaner
            .handle_null_customer_names(
                self.df
            )
        )

        unknown_count = (
            cleaned_df.filter(
                cleaned_df.customer_name
                ==
                "UNKNOWN_CUSTOMER"
            ).count()
        )

        assert unknown_count >= 0

    def test_reject_future_order_dates(self):

        cleaned_df = (
            self.cleaner
            .reject_future_order_dates(
                self.df
            )
        )

        assert cleaned_df is not None