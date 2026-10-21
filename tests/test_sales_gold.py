from src.transformation.sales_gold import (
    SalesGold
)


class TestSalesGold:

    def setup_method(self):

        self.gold = (
            SalesGold()
        )

        self.df = (
            self.gold.read_silver()
        )

    def test_read_silver_success(self):

        assert self.df.count() > 0

    def test_sales_gold_not_none(self):

        sales_df = (
            self.gold.build_sales_gold(
                self.df
            )
        )

        assert sales_df is not None

    def test_sales_gold_columns(self):

        sales_df = (
            self.gold.build_sales_gold(
                self.df
            )
        )

        expected_columns = [

            "order_id",

            "customer_id",

            "order_date",

            "ship_date",

            "ship_mode",

            "city"
        ]

        for column_name in expected_columns:

            assert (
                column_name
                in sales_df.columns
            )

    def test_sales_gold_record_count(self):

        sales_df = (
            self.gold.build_sales_gold(
                self.df
            )
        )

        assert sales_df.count() > 0

    def test_customer_id_column_exists(self):

        sales_df = (
            self.gold.build_sales_gold(
                self.df
            )
        )

        assert (
            "customer_id"
            in sales_df.columns
        )

    def test_order_id_column_exists(self):

        sales_df = (
            self.gold.build_sales_gold(
                self.df
            )
        )

        assert (
            "order_id"
            in sales_df.columns
        )

    def test_common_key_exists(self):

        sales_df = (
            self.gold.build_sales_gold(
                self.df
            )
        )

        assert (
            "customer_id"
            in sales_df.columns
        )