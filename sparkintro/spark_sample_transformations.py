from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import to_date

parsed_col_dict = {}

spark = SparkSession.builder.appName("DataFrame Intro") \
    .getOrCreate()

fire_df = spark.read \
    .format("csv") \
    .option("header","true") \
    .option("inferSchema","true") \
    .load("../input_data/fire_dept_calls.csv")

# Standardise column names by removing spaces and replace with underscores.
for org_col in fire_df.columns:
    parsed_col = org_col.lower().replace(" ","_")
    parsed_col_dict.update({org_col:parsed_col})

# Date time parsed
renamed_col_df = fire_df.withColumnsRenamed(parsed_col_dict)
parsed_date_df = renamed_col_df.withColumn('call_date',to_date('call_date','MM/dd/yyyy')) \
                                .withColumn('watch_date',to_date('call_date','MM/dd/yyyy')) \
                                .withColumn('received_dttm',to_timestamp('received_dttm','yyyy MMM dd hh:mm:ss a')) \
                                .withColumn('entry_dttm',to_timestamp('entry_dttm','yyyy MMM dd hh:mm:ss a')) \
                                .withColumn('dispatch_dttm',to_timestamp('dispatch_dttm','yyyy MMM dd hh:mm:ss a')) \
                                .withColumn('response_dttm',to_timestamp('response_dttm','yyyy MMM dd hh:mm:ss a')) \
                                .withColumn('on_scene_dttm',to_timestamp('response_dttm','yyyy MMM dd hh:mm:ss a')) \
                                .withColumn('available_dttm',to_timestamp('available_dttm','yyyy MMM dd hh:mm:ss a')) \
                                .withColumn('data_as_of',to_timestamp('data_as_of','yyyy MMM dd hh:mm:ss a')) \
                                .withColumn('data_loaded_at',to_timestamp('data_loaded_at','yyyy MMM dd hh:mm:ss a')) \

parsed_date_df.printSchema()
