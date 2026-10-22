import sys
import logging

from src.utils.config_reader import ConfigReader
from src.utils.exception import PipelineException
from src.utils.logger import get_logger
from src.utils.spark_session import SparkSessionManager


class TestUtils:

    # =====================================================
    # CONFIG READER TESTS
    # =====================================================

    def setup_method(self):

        self.config = (
            ConfigReader
            .load_config()
        )

    def test_config_loads(self):

        assert self.config is not None

    def test_source_path_exists(self):

        assert (
            "source_path"
            in self.config
        )

    def test_bronze_path_exists(self):

        assert (
            "bronze_path"
            in self.config
        )

    def test_silver_path_exists(self):

        assert (
            "silver_path"
            in self.config
        )

    def test_gold_path_exists(self):

        assert (
            "gold_path"
            in self.config
        )

    def test_rejected_path_exists(self):

        assert (
            "rejected_path"
            in self.config
        )

    def test_valid_countries_exist(self):

        assert (
            "valid_countries"
            in self.config
        )

        assert (
            len(
                self.config[
                    "valid_countries"
                ]
            )
            > 0
        )

    def test_valid_ship_modes_exist(self):

        assert (
            "valid_ship_modes"
            in self.config
        )

        assert (
            len(
                self.config[
                    "valid_ship_modes"
                ]
            )
            > 0
        )

    def test_logging_configuration_exists(self):

        assert (
            "logging"
            in self.config
        )

    def test_data_quality_thresholds_exist(self):

        assert (
            "quality_thresholds"
            in self.config
        )

    # =====================================================
    # LOGGER TESTS
    # =====================================================

    def test_logger_creation(self):

        logger = get_logger()

        assert logger is not None

    def test_logger_type(self):

        logger = get_logger()

        assert isinstance(
            logger,
            logging.Logger
        )

    def test_logger_name(self):

        logger = get_logger()

        assert (
            logger.name
            ==
            "sales_pipeline"
        )

    def test_logger_level(self):

        logger = get_logger()

        assert logger.level in [
            logging.INFO,
            logging.WARNING,
            logging.ERROR,
            logging.DEBUG
        ]

    def test_logger_handlers_exist(self):

        logger = get_logger()

        assert (
            len(
                logger.handlers
            )
            > 0
        )

    # =====================================================
    # SPARK SESSION TESTS
    # =====================================================

    def test_spark_session_creation(self):

        spark = (
            SparkSessionManager
            .get_spark_session()
        )

        assert spark is not None

    def test_spark_version(self):

        spark = (
            SparkSessionManager
            .get_spark_session()
        )

        assert (
            spark.version
            is not None
        )

    # =====================================================
    # EXCEPTION TESTS
    # =====================================================

    def test_pipeline_exception_creation(self):

        try:

            raise ValueError(
                "Test Error"
            )

        except Exception as e:

            exception = (
                PipelineException(
                    e,
                    sys,
                    "Test Layer"
                )
            )

            assert (
                "Test Error"
                in str(exception)
            )

    def test_layer_name_present(self):

        try:

            raise ValueError(
                "Layer Test"
            )

        except Exception as e:

            exception = (
                PipelineException(
                    e,
                    sys,
                    "Bronze Layer"
                )
            )

            assert (
                "Bronze Layer"
                in str(exception)
            )

    def test_error_type_present(self):

        try:

            raise ValueError(
                "Error Type Test"
            )

        except Exception as e:

            exception = (
                PipelineException(
                    e,
                    sys,
                    "Test Layer"
                )
            )

            assert (
                "ValueError"
                in str(exception)
            )

    def test_file_name_present(self):

        try:

            raise ValueError(
                "File Test"
            )

        except Exception as e:

            exception = (
                PipelineException(
                    e,
                    sys,
                    "Test Layer"
                )
            )

            assert (
                "File Name"
                in str(exception)
            )

    def test_line_number_present(self):

        try:

            raise ValueError(
                "Line Test"
            )

        except Exception as e:

            exception = (
                PipelineException(
                    e,
                    sys,
                    "Test Layer"
                )
            )

            assert (
                "Line Number"
                in str(exception)
            )