from src.utils.config_reader import ConfigReader


class TestConfigReader:

    def test_config_loads(self):

        config = (
            ConfigReader
            .load_config()
        )

        assert config is not None

    def test_source_path_exists(self):

        config = (
            ConfigReader
            .load_config()
        )

        assert (
            "source_path"
            in config
        )

    def test_bronze_path_exists(self):

        config = (
            ConfigReader
            .load_config()
        )

        assert (
            "bronze_path"
            in config
        )
