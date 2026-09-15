from src.transformation.silver_validator import SilverValidator


class TestSilverValidator:

    def test_read_bronze_success(self):

        validator = SilverValidator()

        df = validator.read_bronze()

        assert df.count() > 0

    def test_bronze_dataframe_not_none(self):

        validator = SilverValidator()

        df = validator.read_bronze()

        assert df is not None

    def test_expected_columns_exist(self):

        validator = SilverValidator()

        df = validator.read_bronze()

        expected_columns = [
            "row_id",
            "order_id",
            "customer_id",
            "customer_name",
            "country",
            "city",
            "product_id"
        ]

        for column_name in expected_columns:

            assert column_name in df.columns

    def test_quality_results_initialization(self):

        validator = SilverValidator()

        assert validator.quality_results == []

    def test_null_customer_name_records_exist(self):

        validator = SilverValidator()

        df = validator.read_bronze()

        null_count = (
            df.filter(
                df.customer_name.isNull()
            ).count()
        )

        assert null_count > 0

    def test_invalid_country_records_exist(self):

        validator = SilverValidator()

        df = validator.read_bronze()

        invalid_count = (
            df.filter(
                df.country == "UnknownCountry"
            ).count()
        )

        assert invalid_count > 0