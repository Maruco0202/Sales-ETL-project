import traceback
from datetime import datetime


class PipelineException(Exception):
    """
    Enterprise-grade custom exception for ETL failures.
    Captures debugging and monitoring information.
    """

    def __init__(
        self,
        error,
        error_detail,
        layer="Unknown Layer"
    ):

        _, _, exc_tb = error_detail.exc_info()

        self.timestamp = (
            datetime.now()
            .strftime("%Y-%m-%d %H:%M:%S")
        )

        self.layer = layer

        self.error_type = (
            type(error).__name__
        )

        self.file_name = (
            exc_tb.tb_frame.f_code.co_filename
        )

        self.function_name = (
            exc_tb.tb_frame.f_code.co_name
        )

        self.line_number = (
            exc_tb.tb_lineno
        )

        self.error_message = (
            str(error)
        )

        self.traceback_details = (
            traceback.format_exc()
        )

        super().__init__(
            self.__str__()
        )

    def __str__(self):

        return f"""
==================================================
PIPELINE EXCEPTION
==================================================

Timestamp     : {self.timestamp}

Layer         : {self.layer}

Error Type    : {self.error_type}

File Name     : {self.file_name}

Function      : {self.function_name}

Line Number   : {self.line_number}

Error Message : {self.error_message}

==================================================
"""