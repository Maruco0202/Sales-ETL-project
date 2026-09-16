from src.transformation.customer_gold import CustomerGold



class TestCustomerGold:

    def test_read_silver_success(self):

        gold = CustomerGold()

        df = gold.read_silver()

        assert df.count() > 0

    def test_customer_gold_not_none(self):

        gold = CustomerGold()

        df = gold.read_silver()

        customer_df = gold.build_customer_gold(df)

        assert customer_df is not None

    def test_customer_gold_columns(self):

        gold = CustomerGold()

        df = gold.read_silver()

        customer_df = gold.build_customer_gold(df)

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

            assert column_name in customer_df.columns

    def test_customer_gold_record_count(self):

        gold = CustomerGold()

        df = gold.read_silver()

        customer_df = gold.build_customer_gold(df)

        assert customer_df.count() > 0

    def test_first_name_column_exists(self):

        gold = CustomerGold()

        df = gold.read_silver()

        customer_df = gold.build_customer_gold(df)

        assert "first_name" in customer_df.columns

    def test_last_name_column_exists(self):

        gold = CustomerGold()

        df = gold.read_silver()

        customer_df = gold.build_customer_gold(df)

        assert "last_name" in customer_df.columns

    def test_orders_all_time_column_exists(self):

        gold = CustomerGold()

        df = gold.read_silver()

        customer_df = gold.build_customer_gold(df)

        assert "orders_all_time" in customer_df.columns


    def test_customer_metrics_are_non_negative(self):

        gold = CustomerGold()

        df = gold.read_silver()

        customer_df = gold.build_customer_gold(df)

        row = customer_df.first()

        assert row["orders_last_30_days"] >= 0
        assert row["orders_last_6_months"] >= 0
        assert row["orders_last_12_months"] >= 0
        assert row["orders_all_time"] >= 0 

    def test_customer_metrics_columns_exist(self):
    
            gold = CustomerGold()
    
            df = gold.read_silver()
    
            customer_df = gold.build_customer_gold(df)
    
            expected_metrics = [
                "orders_last_30_days",
                "orders_last_6_months",
                "orders_last_12_months",
                "orders_all_time"
            ]
    
            for metric in expected_metrics:
    
                assert metric in customer_df.columns        