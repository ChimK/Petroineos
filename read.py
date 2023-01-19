

def load_csv(spark, csvfile):
    df = spark. \
        read. \
        format("csv").options(header='true', inferschema='true'). \
        load(csvfile)
    return df
