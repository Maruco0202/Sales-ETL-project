import logging

from src.utils.logger import get_logger


class TestLogger:

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
            logger.name ==
            "sales_pipeline"
        )

    def test_logger_level_from_config(self):

        logger = get_logger()

        assert (
            logger.level
            == logging.INFO
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
            len(logger.handlers)
            > 0
        )