import pytest

from src.ingestion.bronze_loader import BronzeLoader


class TestBronzeLoader:

    def test_read_source_success(self):

        bronze = BronzeLoader()

        df = bronze.read_source()

        assert df.count() > 0

    def test_bronze_dataframe_not_none(self):

        bronze = BronzeLoader()

        df = bronze.read_source()

        assert df is not None

    def test_wrong_file_path(self):

        bronze = BronzeLoader()

        bronze.config["source_path"] = "invalid/path.csv"

        with pytest.raises(Exception):

            bronze.read_source()

    def test_add_metadata(self):

        bronze = BronzeLoader()

        df = bronze.read_source()

        df = bronze.add_metadata(df)

        assert "ingestion_timestamp" in df.columns
        assert "load_date" in df.columns
        assert "source_file_name" in df.columns

    def test_write_bronze(self):

        bronze = BronzeLoader()

        df = bronze.read_source()

        df = bronze.add_metadata(df)

        bronze.write_bronze(df)

        assert True