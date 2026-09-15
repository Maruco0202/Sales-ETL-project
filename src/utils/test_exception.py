from exception import PipelineException

try:
    raise PipelineException(
        "Source file not found"
    )

except PipelineException as e:
    print(f"Pipeline Failed: {e}")
