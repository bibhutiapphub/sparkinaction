from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, expr
from pyspark.sql.functions import to_date
from pyspark.sql.functions import count_distinct

parsed_col_dict = {}

spark = (SparkSession.builder
         .appName("DataFrame Intro")
         .getOrCreate())

fire_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("../input_data/fire_dept_calls.csv")

# Standardise column names by removing spaces and replace with underscores.
for org_col in fire_df.columns:
    parsed_col = org_col.lower().replace(" ", "_")
    parsed_col_dict.update({org_col: parsed_col})

# Date time parsed
renamed_col_df = fire_df.withColumnsRenamed(parsed_col_dict)

parsed_df = (renamed_col_df.withColumn('call_date', to_date('call_date', 'MM/dd/yyyy'))
             .withColumn('watch_date', to_date('call_date', 'MM/dd/yyyy'))
             .withColumn('received_dttm', to_timestamp('received_dttm', 'yyyy MMM dd hh:mm:ss a'))
             .withColumn('entry_dttm', to_timestamp('entry_dttm', 'yyyy MMM dd hh:mm:ss a'))
             .withColumn('dispatch_dttm', to_timestamp('dispatch_dttm', 'yyyy MMM dd hh:mm:ss a'))
             .withColumn('response_dttm', to_timestamp('response_dttm', 'yyyy MMM dd hh:mm:ss a'))
             .withColumn('on_scene_dttm', to_timestamp('response_dttm', 'yyyy MMM dd hh:mm:ss a'))
             .withColumn('available_dttm', to_timestamp('available_dttm', 'yyyy MMM dd hh:mm:ss a'))
             .withColumn('data_as_of', to_timestamp('data_as_of', 'yyyy MMM dd hh:mm:ss a'))
             .withColumn('data_loaded_at', to_timestamp('data_loaded_at', 'yyyy MMM dd hh:mm:ss a')))

parsed_df.cache()

# How many distinct types of calls were made to the Fire Department?
distinct_calls_count_df = (parsed_df.where("call_type is not null")
                           .select(count_distinct('call_type').alias("distinct_calls_count")))
distinct_calls_count_df.show(10, True)

# Another approach
count_of_distinct_calls_df = parsed_df.where("call_type is not null").select("call_type").distinct()
print(count_of_distinct_calls_df.count())

# List of distinct types of calls were made to the Fire Department?
distinct_calls_df = parsed_df.where("call_type is not null").select(
    col('call_type').alias("distinct_calls")).distinct()
distinct_calls_df.show(10, True)

# Querying DataFrames using Temporary Views
parsed_df.createOrReplaceTempView("fire_df_temp_view")
view_df = spark.sql("""
SELECT COUNT(DISTINCT call_type) AS distinct_calls FROM
fire_df_temp_view
WHERE call_type IS NOT NULL
""")
view_df.show()

# What are the most common call types
common_call_types_df = (parsed_df
                        .where("call_type IS NOT NULL")
                        .groupby("call_type")
                        .count()
                        .orderBy(col("count"), ascending=False)
                        .select(col("call_type")
                                , col("count").alias("distinct_calls_count")))

common_call_types_df.show()
