from src.utils.config_reader import ConfigReader


class TestConfigReader:

    def setup_method(self):

        self.config = (
            ConfigReader
            .load_config()
        )

    def test_config_loads(self):

        assert self.config is not None

    def test_source_path_exists(self):

        assert (
            "source_path"
            in self.config
        )

    def test_bronze_path_exists(self):

        assert (
            "bronze_path"
            in self.config
        )

    def test_silver_path_exists(self):

        assert (
            "silver_path"
            in self.config
        )

    def test_gold_path_exists(self):

        assert (
            "gold_path"
            in self.config
        )

    def test_rejected_path_exists(self):

        assert (
            "rejected_path"
            in self.config
        )

    def test_valid_countries_exist(self):

        assert (
            "valid_countries"
            in self.config
        )

        assert (
            len(
                self.config[
                    "valid_countries"
                ]
            )
            > 0
        )

    def test_valid_ship_modes_exist(self):

        assert (
            "valid_ship_modes"
            in self.config
        )

        assert (
            len(
                self.config[
                    "valid_ship_modes"
                ]
            )
            > 0
        )

    def test_logging_configuration_exists(self):

        assert (
            "logging"
            in self.config
        )

    def test_data_quality_thresholds_exist(self):

        assert (
            "quality_thresholds"
            in self.config
        )