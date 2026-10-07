import sys
from datetime import datetime


class PipelineException(Exception):

    def __init__(
        self,
        error_message,
        error_detail,
        layer="Unknown Layer"
    ):

        _, _, exc_tb = error_detail.exc_info()

        self.file_name = (
            exc_tb.tb_frame.f_code.co_filename
        )

        self.function_name = (
            exc_tb.tb_frame.f_code.co_name
        )

        self.line_number = (
            exc_tb.tb_lineno
        )

        self.layer = layer

        self.timestamp = (
            datetime.now()
            .strftime("%Y-%m-%d %H:%M:%S")
        )

        self.error_message = f"""
==================================================
PIPELINE EXCEPTION
==================================================

Timestamp     : {self.timestamp}

Layer         : {self.layer}

File Name     : {self.file_name}

Function      : {self.function_name}

Line Number   : {self.line_number}

Error Message : {error_message}

==================================================
"""

        super().__init__(
            self.error_message
        )

    def __str__(self):

        return self.error_message