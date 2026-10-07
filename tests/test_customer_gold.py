from src.transformation.customer_gold import (
    CustomerGold
)


class TestCustomerGold:

    def setup_method(self):

        self.gold = (
            CustomerGold()
        )

        self.df = (
            self.gold.read_silver()
        )

    def test_read_silver_success(self):

        assert self.df is not None

    def test_silver_dataframe_not_empty(self):

        assert self.df.count() > 0

    def test_customer_gold_not_none(self):

        customer_df = (
            self.gold.build_customer_gold(
                self.df
            )
        )

        assert customer_df is not None

    def test_customer_gold_record_count(self):

        customer_df = (
            self.gold.build_customer_gold(
                self.df
            )
        )

        assert customer_df.count() > 0

    def test_customer_gold_columns(self):

        customer_df = (
            self.gold.build_customer_gold(
                self.df
            )
        )

        expected_columns = [

            "customer_id",

            "first_name",

            "last_name",

            "segment",

            "country",

            "orders_last_30_days",

            "orders_last_6_months",

            "orders_last_12_months",

            "orders_all_time"
        ]

        for column_name in expected_columns:

            assert (
                column_name
                in customer_df.columns
            )

    def test_first_name_column_exists(self):

        customer_df = (
            self.gold.build_customer_gold(
                self.df
            )
        )

        assert (
            "first_name"
            in customer_df.columns
        )

    def test_last_name_column_exists(self):

        customer_df = (
            self.gold.build_customer_gold(
                self.df
            )
        )

        assert (
            "last_name"
            in customer_df.columns
        )

    def test_customer_id_column_exists(self):

        customer_df = (
            self.gold.build_customer_gold(
                self.df
            )
        )

        assert (
            "customer_id"
            in customer_df.columns
        )

    def test_customer_metrics_are_non_negative(self):

        customer_df = (
            self.gold.build_customer_gold(
                self.df
            )
        )

        row = customer_df.first()

        assert (
            row["orders_last_30_days"]
            >= 0
        )

        assert (
            row["orders_last_6_months"]
            >= 0
        )

        assert (
            row["orders_last_12_months"]
            >= 0
        )

        assert (
            row["orders_all_time"]
            >= 0
        )

    def test_orders_all_time_column_exists(self):

        customer_df = (
            self.gold.build_customer_gold(
                self.df
            )
        )

        assert (
            "orders_all_time"
            in customer_df.columns
        )

    def test_customer_metrics_columns_exist(self):

        customer_df = (
            self.gold.build_customer_gold(
                self.df
            )
        )

        expected_metrics = [

            "orders_last_30_days",

            "orders_last_6_months",

            "orders_last_12_months",

            "orders_all_time"
        ]

        for metric in expected_metrics:

            assert (
                metric
                in customer_df.columns
            )