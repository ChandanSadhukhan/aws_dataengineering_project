from pyspark.sql import *

from src.main.utility.logging_config import logger


def get_session():
    session = SparkSession.builder.master("local[*]") \
        .appName("Spark_Session") \
        .config("spark.driver.extraClassPath", "/Users/debajnidas/Documents/Spark/mysql-connector.deb") \
        .getOrCreate()

    if session.getActiveSession() is not None:
        logger.info(f"Spark Session is created:")
        return session
    else:
        return None


# test
# session = get_session()
