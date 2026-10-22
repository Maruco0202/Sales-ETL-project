from src.sales_data_pipeline.transformations import Transformations


class TestTransformations:

    def setup_method(self):

        self.transformer = Transformations()

    # =====================================================
    # BRONZE TESTS
    # =====================================================

    def test_read_bronze(self):

        df = self.transformer.read_bronze()

        assert df is not None

        assert df.count() > 0

    # =====================================================
    # VALIDATION TESTS
    # =====================================================

    def test_check_nulls(self):

        df = self.transformer.read_bronze()

        self.transformer.check_nulls(df)

        assert len(
            self.transformer.quality_results
        ) > 0

    def test_check_duplicate_row_ids(self):

        df = self.transformer.read_bronze()

        self.transformer.check_duplicate_row_ids(df)

        assert len(
            self.transformer.quality_results
        ) > 0

    def test_check_invalid_country(self):

        df = self.transformer.read_bronze()

        self.transformer.check_invalid_country(df)

        assert len(
            self.transformer.quality_results
        ) > 0

    def test_check_invalid_ship_modes(self):

        df = self.transformer.read_bronze()

        self.transformer.check_invalid_ship_modes(df)

        assert len(
            self.transformer.quality_results
        ) > 0

    def test_future_order_dates(self):

        df = self.transformer.read_bronze()

        self.transformer.check_future_order_dates(df)

        assert len(
            self.transformer.quality_results
        ) > 0

    def test_validate_schema(self):

        df = self.transformer.read_bronze()

        self.transformer.validate_schema(df)

        assert len(
            self.transformer.quality_results
        ) > 0

    # =====================================================
    # CLEANING TESTS
    # =====================================================

    def test_reject_null_order_ids(self):

        df = self.transformer.read_bronze()

        cleaned_df = (
            self.transformer.reject_null_order_ids(df)
        )

        assert cleaned_df.count() <= df.count()

    def test_handle_null_product_ids(self):

        df = self.transformer.read_bronze()

        cleaned_df = (
            self.transformer.handle_null_product_ids(df)
        )

        assert (
            "product_review_flag"
            in cleaned_df.columns
        )

    def test_handle_invalid_countries(self):

        df = self.transformer.read_bronze()

        cleaned_df = (
            self.transformer.handle_invalid_countries(df)
        )

        assert cleaned_df.count() > 0

    def test_handle_invalid_ship_modes(self):

        df = self.transformer.read_bronze()

        cleaned_df = (
            self.transformer.handle_invalid_ship_modes(df)
        )

        assert cleaned_df.count() > 0

    def test_handle_negative_sales(self):

        df = self.transformer.read_bronze()

        cleaned_df = (
            self.transformer.handle_negative_sales(df)
        )

        assert (
            "sales_review_flag"
            in cleaned_df.columns
        )

    def test_deduplicate_row_ids(self):

        df = self.transformer.read_bronze()

        cleaned_df = (
            self.transformer.deduplicate_row_ids(df)
        )

        assert cleaned_df.count() <= df.count()

    def test_handle_null_customer_names(self):

        df = self.transformer.read_bronze()

        cleaned_df = (
            self.transformer.handle_null_customer_names(df)
        )

        assert cleaned_df.count() > 0

    def test_reject_future_order_dates(self):

        df = self.transformer.read_bronze()

        cleaned_df = (
            self.transformer.reject_future_order_dates(df)
        )

        assert cleaned_df.count() <= df.count()

    # =====================================================
    # SILVER TESTS
    # =====================================================

    def test_read_silver(self):

        df = self.transformer.read_silver()

        assert df is not None

    def test_write_silver(self):

        df = self.transformer.read_bronze()

        self.transformer.write_silver(df)

        silver_df = (
            self.transformer.spark.read
            .parquet(
                self.transformer.config[
                    "silver_path"
                ]
            )
        )

        assert silver_df.count() > 0

    # =====================================================
    # GOLD TESTS
    # =====================================================

    def test_build_sales_gold(self):

        df = self.transformer.read_silver()

        sales_df = (
            self.transformer.build_sales_gold(df)
        )

        assert sales_df.count() > 0

    def test_build_customer_gold(self):

        df = self.transformer.read_silver()

        customer_df = (
            self.transformer.build_customer_gold(df)
        )

        assert customer_df.count() > 0

    def test_write_sales_gold(self):

        df = self.transformer.read_silver()

        sales_df = (
            self.transformer.build_sales_gold(df)
        )

        self.transformer.write_sales_gold(
            sales_df
        )

        assert True

    def test_write_customer_gold(self):

        df = self.transformer.read_silver()

        customer_df = (
            self.transformer.build_customer_gold(df)
        )

        self.transformer.write_customer_gold(
            customer_df
        )

        assert True

    # =====================================================
    # REPORT TESTS
    # =====================================================

    def test_generate_quality_report(self):

        self.transformer.quality_results.append(
            ["Test", 1]
        )

        self.transformer.generate_quality_report()

        assert True