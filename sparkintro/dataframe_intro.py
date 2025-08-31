from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrame Intro") \
    .getOrCreate()

my_df = spark.read \
    .format("csv") \
    .option("header","true") \
    .option("inferSchema","true") \
    .load("../input_data/fire_dept_calls.csv")

my_df.createGlobalTempView("fire_service_data")

from_view_df = spark.sql("select * from global_temp.fire_service_data")
from_view_df.show()
spark.stop()