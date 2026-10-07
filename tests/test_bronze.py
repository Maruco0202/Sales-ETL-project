from src.ingestion.bronze_loader import BronzeLoader
from src.utils.exception import PipelineException


class TestBronzeLoader:

    def setup_method(self):

        self.bronze = BronzeLoader()

    def test_read_source_success(self):

        df = self.bronze.read_source()

        assert df is not None

    def test_source_dataframe_not_empty(self):

        df = self.bronze.read_source()

        assert df.count() > 0

    def test_add_metadata_columns(self):

        df = self.bronze.read_source()

        df = self.bronze.add_metadata(df)

        assert "ingestion_timestamp" in df.columns

        assert "load_date" in df.columns

        assert "source_file_name" in df.columns

    def test_metadata_values_created(self):

        df = self.bronze.read_source()

        df = self.bronze.add_metadata(df)

        first_row = df.first()

        assert (
            first_row["ingestion_timestamp"]
            is not None
        )

        assert (
            first_row["load_date"]
            is not None
        )

        assert (
            first_row["source_file_name"]
            ==
            self.bronze.config[
                "source_file_name"
            ]
        )

    def test_write_bronze_success(self):

        df = self.bronze.read_source()

        df = self.bronze.add_metadata(df)

        self.bronze.write_bronze(df)

        bronze_df = (
            self.bronze.spark.read
            .parquet(
                self.bronze.config[
                    "bronze_path"
                ]
            )
        )

        assert bronze_df.count() > 0

    def test_record_count_preserved(self):

        source_df = (
            self.bronze.read_source()
        )

        source_count = (
            source_df.count()
        )

        source_df = (
            self.bronze.add_metadata(
                source_df
            )
        )

        self.bronze.write_bronze(
            source_df
        )

        bronze_df = (
            self.bronze.spark.read
            .parquet(
                self.bronze.config[
                    "bronze_path"
                ]
            )
        )

        assert (
            bronze_df.count()
            ==
            source_count
        )

    def test_wrong_source_path_raises_exception(self):

        original_path = (
            self.bronze.config[
                "source_path"
            ]
        )

        self.bronze.config[
            "source_path"
        ] = "invalid/path.csv"

        try:

            self.bronze.read_source()

            assert False

        except PipelineException:

            assert True

        finally:

            self.bronze.config[
                "source_path"
            ] = original_path