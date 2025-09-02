from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id
from utils.spark_utils import *

if __name__ == "__main__":
    spark = get_spark_session()
    file_path = "../input_data/sample.csv"
    survey_raw_df = load_survey_df(spark,file_path)
    survey_partitioned_df = survey_raw_df.repartition(2)
    count_country_df = get_count_by_country_df(survey_partitioned_df)
    count_country_df.collect()
    input("Please Enter to Abort")
