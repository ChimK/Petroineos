from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf


def transform(df):
    # Aggregate data
    agg_df = df.groupBy("Period").agg(_sum("Volume"))

    # Define UDF
    convert_time = udf(convert_period_to_hours, StringType())

    # Add Column with Hours
    return agg_df.withColumn("time", convert_time(df["Period"]))


def convert_period_to_hours(period):
    # Create a list of strings containing the hours and minutes in the desired format
    times = ["23:00", "00:00", "01:00", "02:00", "03:00", "04:00", "05:00", "06:00", "07:00",
             "08:00", "09:00", "10:00", "11:00", "12:00", "13:00", "14:00", "15:00", "16:00",
             "17:00", "18:00", "19:00", "20:00", "21:00", "22:00"]
    return times[period-1]
