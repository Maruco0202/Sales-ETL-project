from src.transformation.silver_validator import (
    SilverValidator
)

import os


class TestSilverValidator:

    def setup_method(self):

        self.validator = (
            SilverValidator()
        )

        self.df = (
            self.validator
            .read_bronze()
        )

    def test_read_bronze(self):

        assert self.df is not None

    def test_bronze_dataframe_not_empty(self):

        assert self.df.count() > 0

    def test_check_nulls(self):

        self.validator.check_nulls(
            self.df
        )

        assert (
            len(
                self.validator.quality_results
            ) > 0
        )

    def test_duplicate_row_validation(self):

        self.validator.check_duplicate_row_ids(
            self.df
        )

        assert (
            len(
                self.validator.quality_results
            ) > 0
        )

    def test_invalid_country_validation(self):

        self.validator.check_invalid_country(
            self.df
        )

        assert (
            len(
                self.validator.quality_results
            ) > 0
        )

    def test_invalid_ship_mode_validation(self):

        self.validator.check_invalid_ship_modes(
            self.df
        )

        assert (
            len(
                self.validator.quality_results
            ) > 0
        )


    def test_quality_results_initialized(self):

        assert (
            self.validator.quality_results
            == []
        )