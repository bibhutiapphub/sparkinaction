from datetime import date

from pyspark.sql import *
from pyspark.sql.functions import to_date
from pyspark.sql.types import *


def get_converted_date_df(input_df, coln, format):
    transformed_df = input_df.withColumn(coln, to_date(coln, format))
    return transformed_df


if __name__ == "__main__":
    spark = (SparkSession.builder
             .master("local[2]")
             .appName("SparkTransformations")
             .enableHiveSupport()
             .getOrCreate())

    # Creating DataFrames From Sample Data
    event_data = [Row("123", "04/05/2020")
        , Row("456", "04/5/2020")
        , Row("789", "4/05/2020")
        , Row("345", "4/5/2020")]

    event_schema = StructType(
        [
            StructField("id", StringType())
            , StructField("event_date", StringType())
        ]
    )
    event_rdd = spark.sparkContext.parallelize(event_data)
    event_df = spark.createDataFrame(event_rdd, event_schema)
    converted_df = get_converted_date_df(event_df, "event_date", "M/d/y")

    # Check the rows
    df_rows = converted_df.collect()

    for row in df_rows:
        print(isinstance(row["event_date"],date)) # It will print True
