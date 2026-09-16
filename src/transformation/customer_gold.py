from pyspark.sql.functions import (
    col,
    split,
    max,
    date_sub,
    count,
    when,
    lit,
    to_date,
    current_date
)

from src.utils.spark_session import SparkSessionManager
from src.utils.config_reader import ConfigReader
from src.utils.logger import get_logger
from src.utils.exception import PipelineException


class CustomerGold:

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

            raise PipelineException(str(e))

    def build_customer_gold(self, df):

        # Convert order_date from string to date
        df = df.withColumn(
            "order_date",
            to_date(
                col("order_date"),
                "dd-MM-yyyy"
            )
        )

        # Get latest VALID business date
        latest_order_date = (
            df.filter(
                col("order_date") <= current_date()
            )
            .selectExpr(
                "max(order_date) as max_date"
            )
            .collect()[0]["max_date"]
        )

        customer_gold_df = (

            df.withColumn(
                "first_name",
                split(
                    col("customer_name"),
                    " "
                ).getItem(0)
            )

            .withColumn(
                "last_name",
                split(
                    col("customer_name"),
                    " "
                ).getItem(1)
            )

            .groupBy(
                "customer_id",
                "first_name",
                "last_name",
                "segment",
                "country"
            )

            .agg(

                count(
                    when(
                        col("order_date") >= date_sub(
                            lit(latest_order_date),
                            30
                        ),
                        True
                    )
                ).alias(
                    "orders_last_30_days"
                ),

                count(
                    when(
                        col("order_date") >= date_sub(
                            lit(latest_order_date),
                            180
                        ),
                        True
                    )
                ).alias(
                    "orders_last_6_months"
                ),

                count(
                    when(
                        col("order_date") >= date_sub(
                            lit(latest_order_date),
                            365
                        ),
                        True
                    )
                ).alias(
                    "orders_last_12_months"
                ),

                count("*").alias(
                    "orders_all_time"
                )
            )
        )

        return customer_gold_df


    def write_gold(self, df):

        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .parquet(
                "data/gold/customer_gold"
            )
        )

        print(
            "\nCustomer Gold Dataset Written Successfully"
        )


if __name__ == "__main__":

    gold = CustomerGold()

    silver_df = gold.read_silver()

    customer_gold_df = (
        gold.build_customer_gold(
            silver_df
        )
    )

    print(
        f"Customer Gold Records : {customer_gold_df.count()}"
    )

    customer_gold_df.show(
        50,
        False
    )

    gold.write_gold(
        customer_gold_df
    )

   