import sys
from lib import DataManipulation, DataReader, Utils
from pyspark.sql.functions import *
from lib.logger import Log4j


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)

    job_run_env = sys.argv[1]

    print("Creating Spark Session")
    spark = Utils.get_spark_session(job_run_env)

    logger = Log4j(spark)

    logger.warn("Created Spark Session")

    orders_df = DataReader.read_orders(spark,job_run_env)
    orders_filtered = DataManipulation.filter_closed_orders(orders_df)
    customers_df = DataReader.read_customers(spark,job_run_env)
    joined_df = DataManipulation.join_orders_customers(orders_filtered,customers_df)
    aggregated_results = DataManipulation.count_orders_state(joined_df)
    counted_for_customer = DataManipulation.count_orders_state(customers_df)
    counted_for_customer.show()
    counted_for_customer.coalesce(1).write.format("csv").mode("overwrite").option("header", True).save("E:\demoproject\data\\test_results")

    logger.info("end of main")
    spark.stop()