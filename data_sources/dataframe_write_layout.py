from pyspark.sql import *

if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .master("local[2]")
             .appName("SparkWriterDemo")
             .getOrCreate())

    flight_data_df = (spark.read
                      .format("parquet")
                      .load("../input_data/flight-time.parquet"))

    (flight_data_df.write
     .format("parquet")
     .mode("overwrite")
     .partitionBy("OP_CARRIER", "ORIGIN") # This benefits in partition elimination.
     .option("maxRecordsPerFile","10000")
     .option("path", "../data_sink/parquet/")
     .save())
