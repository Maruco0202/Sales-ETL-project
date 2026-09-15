from src.utils.spark_session import SparkSessionManager
from src.utils.config_reader import ConfigReader
from src.utils.logger import get_logger
from src.utils.exception import PipelineException


class SalesGold:

    def __init__(self):

        self.spark = SparkSessionManager.get_spark_session()

        self.config = ConfigReader.load_config()

        self.logger = get_logger()

    def read_silver(self):

        try:

            self.logger.info(
                "Reading Silver Layer"
            )

            df = (
                self.spark.read
                .parquet(
                    self.config["silver_path"]
                )
            )

            return df

        except Exception as e:

            self.logger.error(
                f"Error reading Silver Layer: {str(e)}"
            )

            raise PipelineException(str(e))

    def build_sales_gold(self, df):

        sales_gold_df = (
            df.select(
                "order_id",
                "order_date",
                "ship_date",
                "ship_mode",
                "city"
            )
        )

        return sales_gold_df

    def write_gold(self, df):

        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .parquet(
                "data/gold/sales_gold"
            )
        )

        print(
            "\nSales Gold Dataset Written Successfully"
        )

        self.logger.info(
            "Sales Gold Dataset Written Successfully"
        )


if __name__ == "__main__":

    gold = SalesGold()

    silver_df = gold.read_silver()

    print(
        f"Silver Records : {silver_df.count()}"
    )

    sales_gold_df = gold.build_sales_gold(
        silver_df
    )

    print(
        f"Sales Gold Records : {sales_gold_df.count()}"
    )

    sales_gold_df.show(5, False)

    gold.write_gold(
        sales_gold_df
    )