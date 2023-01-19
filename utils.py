
from pyspark.sql import SparkSession


def get_session_spark(appname):
    # Create and return a SparkSession
    spark = SparkSession.builder \
        .appName(appname) \
        .master("local") \
        .getOrCreate()

    return spark



