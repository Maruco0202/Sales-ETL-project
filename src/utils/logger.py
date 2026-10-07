import logging
import os
from datetime import datetime

from src.utils.config_reader import ConfigReader


def get_logger(
    logger_name="sales_pipeline"
):
    """
    Creates and returns a centralized logger
    for the ETL pipeline.
    """

    config = (
        ConfigReader
        .load_config()
    )

    log_level = (
        config["logging"]
        .get(
            "level",
            "INFO"
        )
    )

    os.makedirs(
        "logs",
        exist_ok=True
    )

    log_file = os.path.join(
        "logs",
        f'pipeline_{datetime.now().strftime("%Y%m%d")}.log'
    )

    logger = logging.getLogger(
        logger_name
    )

    if not logger.handlers:

        logger.setLevel(
            getattr(
                logging,
                log_level.upper()
            )
        )

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        # File Handler

        file_handler = logging.FileHandler(
            log_file
        )

        file_handler.setFormatter(
            formatter
        )

        logger.addHandler(
            file_handler
        )

        # Console Handler

        console_handler = (
            logging.StreamHandler()
        )

        console_handler.setFormatter(
            formatter
        )

        logger.addHandler(
            console_handler
        )

    return logger