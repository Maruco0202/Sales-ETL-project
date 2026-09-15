from src.transformation.sales_gold import SalesGold


class TestSalesGold:

    def test_read_silver_success(self):

        gold = SalesGold()

        df = gold.read_silver()

        assert df.count() > 0

    def test_sales_gold_not_none(self):

        gold = SalesGold()

        df = gold.read_silver()

        sales_df = gold.build_sales_gold(df)

        assert sales_df is not None

    def test_sales_gold_columns(self):

        gold = SalesGold()

        df = gold.read_silver()

        sales_df = gold.build_sales_gold(df)

        expected_columns = [
            "order_id",
            "order_date",
            "ship_date",
            "ship_mode",
            "city"
        ]

        for column_name in expected_columns:

            assert column_name in sales_df.columns

    def test_sales_gold_record_count(self):

        gold = SalesGold()

        df = gold.read_silver()

        sales_df = gold.build_sales_gold(df)

        assert sales_df.count() > 0