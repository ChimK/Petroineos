import sys

from utils import get_session_spark
from read import load_csv
from process import transform
from write import write_csv


def main(argv):
    try:
        # Get Spark Session
        spark = get_session_spark("Power Positions")
        # Import csv
        df = load_csv(spark, sys.argv[1])
        # Transform data
        df_transform = transform(df)
        # Persist file
        write_csv(spark, df_transform, sys.argv[2])
    except Exception as e:
        raise Exception("Error Processing file")


if __name__ == '__main__':
    main(sys.argv)
    # sys.argv[1] = path + csv filename to process
    # sys.argv[2] = /output/for/report/


