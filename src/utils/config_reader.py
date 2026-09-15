import yaml


class ConfigReader:

    @staticmethod
    def load_config():

        with open("src/config/config.yaml", "r") as file:
            config = yaml.safe_load(file)

        return config
    