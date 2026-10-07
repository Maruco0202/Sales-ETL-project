import sys
import yaml

from src.utils.exception import PipelineException


class ConfigReader:

    @staticmethod
    def load_config(
        config_path="src/config/config.yaml"
    ):

        try:

            with open(
                config_path,
                "r"
            ) as file:

                config = yaml.safe_load(
                    file
                )

            return config

        except Exception as e:

            raise PipelineException(
                e,
                sys,
                "Config Reader"
            )
