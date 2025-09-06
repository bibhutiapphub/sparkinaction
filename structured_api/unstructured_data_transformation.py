from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# log_row = []
# log_row_schema = StructType([
#     StructField()
#     ,StructField()
#     ,StructField()
# ])
if __name__ == "__main__":
    spark = (SparkSession.builder
             .master("local[2]")
             .appName("SparkTransformations")
             .enableHiveSupport()
             .getOrCreate())

    data_df = spark.read.text("../input_data/apache_logs.txt")
    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'
    parsed_df = data_df.select(regexp_extract("value",log_reg,1).alias("ip")
                               ,regexp_extract("value",log_reg,4).alias("date")
                               ,regexp_extract("value",log_reg,6).alias("request")
                               ,regexp_extract("value",log_reg,10).alias("referrer"))

    result_df = (parsed_df
                 .withColumn("host",substring_index("referrer","/",3))
                 .groupBy("host")
                 .count())
    result_df.show(truncate=False)

    # data_rows = data_rdd.collect()
    #
    # for row in data_rows:
    #     print(row)
