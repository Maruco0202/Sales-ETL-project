import sys

from src.utils.exception import PipelineException


try:

    raise ValueError(
        "Test Error"
    )

except Exception as e:

    raise PipelineException(
        e,
        sys,
        "Test Layer"
    )