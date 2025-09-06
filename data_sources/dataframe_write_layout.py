from pyspark.sql import *

if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .master("local[2]")
             .appName("SparkWriterDemo")
             .enableHiveSupport()
             .getOrCreate())

    flight_data_df = (spark.read
                      .format("parquet")
                      .load("../input_data/flight-time.parquet"))

    # Write it as files
    # (flight_data_df.write
    #  .format("parquet")
    #  .mode("overwrite")
    #  .partitionBy("OP_CARRIER", "ORIGIN") # This benefits in partition elimination.
    #  .option("maxRecordsPerFile","10000")
    #  .option("path", "../data_sink/parquet/")
    #  .save())

    # Write it as table
    spark.sql("CREATE DATABASE IF NOT EXISTS airline_db")

    (flight_data_df.write
     .mode("overwrite")
     # .partitionBy("OP_CARRIER", "ORIGIN")  # This benefits in partition elimination. But results in large number of partitions
     # Especially with columns with unique values results in large number of partitions
     # Same can be achieved by bucketBy function.
     .bucketBy(5, "OP_CARRIER", "ORIGIN")
     .sortBy("OP_CARRIER", "ORIGIN")
     .saveAsTable("airline_db.flight_times"))

    is_table_exists = spark.catalog.tableExists("airline_db.flight_times")
    print(is_table_exists)
    print(spark.catalog.listTables("airline_db"))
    flight_time_df = spark.sql("SELECT * FROM airline_db.flight_times")
    flight_time_df.show()
