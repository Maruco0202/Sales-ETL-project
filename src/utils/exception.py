class PipelineException(Exception):
    """
    Custom exception for pipeline failures.
    """

    def __init__(self, message):
        super().__init__(message)