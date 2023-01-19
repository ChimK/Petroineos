from datetime import datetime
from pyspark.sql.functions import col


def write_csv(spark, df, folder):
    # Select Columns for output and Write to csv
    df.select(col("time").alias("Local Time"),
              col("sum(Volume)").alias("Volume")).\
                coalesce(1).sort("Period").write.format("csv"). \
                options(header='true', delimiter=',').save(folder)

    rename_csvfile(spark, folder)


def rename_csvfile(spark, folder):
    # Generate new filename
    new_filename = folder + 'PowerPosition_' + datetime.now().strftime("%Y%m%d_%H%M") + '.csv'

    # Rename file
    hdp_path = spark._jvm.org.apache.hadoop.fs.Path(folder + '*')
    hdp_fs = hdp_path.getFileSystem(spark._jvm.org.apache.hadoop.conf.Configuration())

    statuses = hdp_fs.globStatus(hdp_path)
    filename = [file.getPath().getName() for file in statuses if file.getPath().getName().startswith('part-')][0]

    hdp_fs.rename(spark._jvm.org.apache.hadoop.fs.Path(f"{folder}{filename}"),
                  spark._jvm.org.apache.hadoop.fs.Path(new_filename))
