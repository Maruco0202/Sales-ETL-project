import logging
import os
from datetime import datetime


def get_logger(logger_name="sales_pipeline"):

    os.makedirs("logs", exist_ok=True)

    log_file = os.path.join(
        "logs",
        f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"
    )

    logger = logging.getLogger(logger_name)

    if not logger.handlers:

        logger.setLevel(logging.INFO)

        file_handler = logging.FileHandler(log_file)

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

    return logger
