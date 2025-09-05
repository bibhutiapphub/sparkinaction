from pyspark.sql import *
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("SparkDataSources")
             .getOrCreate())

    # Applying schema into csv data.
    flight_data_schema = (StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ]))

    flight_data_schema_ddl = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, ORIGIN_CITY_NAME STRING
                                ,DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, WHEELS_ON INT
                                ,TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""
    csv_data = (spark.read
                .format("csv")
                .option("header", "true")
                .schema(flight_data_schema)
                .option("mode", "FAILFAST")
                .option("dateFormat", "M/d/y")
                .load("../input_data/flight-time.csv"))
    csv_data.printSchema()
    csv_data.show()
    json_data_df = (spark.read
                    .format("json")
                    .schema(flight_data_schema_ddl)
                    .option("dateFormat", "M/d/y")
                    .load("../input_data/flight-time.json"))
    json_data_df.show()

    parquet_data = (spark.read
                    .format("parquet")
                    .load("../input_data/flight-time.parquet"))
    print(parquet_data.schema.simpleString())
