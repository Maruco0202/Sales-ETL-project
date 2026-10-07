import sys
import pytest

from src.utils.exception import PipelineException


class TestPipelineException:

    def test_pipeline_exception_creation(self):

        try:

            raise ValueError(
                "Test Error"
            )

        except Exception as e:

            exception = (
                PipelineException(
                    str(e),
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
                "Layer Validation Test"
            )

        except Exception as e:

            exception = (
                PipelineException(
                    str(e),
                    sys,
                    "Bronze Layer"
                )
            )

            assert (
                "Bronze Layer"
                in str(exception)
            )

    def test_file_name_present(self):

        try:

            raise ValueError(
                "File Validation Test"
            )

        except Exception as e:

            exception = (
                PipelineException(
                    str(e),
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
                "Line Validation Test"
            )

        except Exception as e:

            exception = (
                PipelineException(
                    str(e),
                    sys,
                    "Test Layer"
                )
            )

            assert (
                "Line Number"
                in str(exception)
            )

    def test_timestamp_present(self):

        try:

            raise ValueError(
                "Timestamp Test"
            )

        except Exception as e:

            exception = (
                PipelineException(
                    str(e),
                    sys,
                    "Test Layer"
                )
            )

            assert (
                "Timestamp"
                in str(exception)
            )