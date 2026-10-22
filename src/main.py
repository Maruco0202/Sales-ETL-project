from src.sales_data_pipeline.pipeline import SalesPipeline


def main():

    pipeline = SalesPipeline()

    pipeline.run()


if __name__ == "__main__":
    main()