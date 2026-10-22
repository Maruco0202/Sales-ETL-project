from src.sales_data_pipeline.pipeline import SalesPipeline


class TestPipeline:

    def setup_method(self):

        self.pipeline = SalesPipeline()

    def test_pipeline_object_creation(self):

        assert self.pipeline is not None

    def test_extract(self):

        df = self.pipeline.extract()

        assert df is not None

        assert df.count() > 0

    def test_validate(self):

        result = self.pipeline.validate()

        assert result is not None

    def test_clean(self):

        df = self.pipeline.clean()

        assert df.count() > 0

    def test_transform(self):

        sales_df, customer_df = (
            self.pipeline.transform()
        )

        assert sales_df.count() > 0

        assert customer_df.count() > 0

    def test_load(self):

        sales_df, customer_df = (
            self.pipeline.transform()
        )

        self.pipeline.load(
            sales_df,
            customer_df
        )

        assert True

    def test_run(self):

        self.pipeline.run()

        assert True